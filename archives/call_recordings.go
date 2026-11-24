package archives

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// MinCallRecordingsRetentionDays is the lowest retention period the step accepts; shorter
	// periods refuse startup so recordings can never be deleted earlier by configuration.
	MinCallRecordingsRetentionDays = 90

	// CallRecordingsRetentionTimeout bounds one run of the retention step.
	CallRecordingsRetentionTimeout = time.Hour

	callRecordingsListPageSize     = 1000
	callRecordingsDeleteBatchSize  = 1000 // DeleteObjects maximum
	callRecordingsS3MaxRetries     = 3
	callRecordingsDryRunSampleSize = 10
)

var (
	s3BucketNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	awsRegionPattern    = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]+$`)
)

// CallRecordingsSummary is the outcome of one run of the call recordings retention step.
type CallRecordingsSummary struct {
	Scanned      int64
	Eligible     int64
	Deleted      int64
	DeletedBytes int64 // in dry run, the bytes that would be deleted
	DeleteErrors int64
	Duration     time.Duration
	DryRun       bool
	Partial      bool // stopped by the deadline or a listing error
	OldestKeys   []SampledRecordingKey
}

// SampledRecordingKey is an eligible key kept for the dry-run summary.
type SampledRecordingKey struct {
	Key          string
	LastModified time.Time
}

type pendingRecording struct {
	key  string
	size int64
}

// ValidateCallRecordingsConfig checks the call recordings options. A period of 0 disables the step
// and ignores the other options.
func ValidateCallRecordingsConfig(config *Config) error {
	period := config.CallRecordingsRetentionPeriod
	if period == 0 {
		return nil
	}
	if period < MinCallRecordingsRetentionDays {
		return errors.Errorf("call recordings retention period must be 0 (disabled) or at least %d days, got %d", MinCallRecordingsRetentionDays, period)
	}
	if !s3BucketNamePattern.MatchString(config.CallRecordingsBucket) {
		return errors.Errorf("call recordings bucket %q is not a valid S3 bucket name", config.CallRecordingsBucket)
	}
	if !awsRegionPattern.MatchString(config.CallRecordingsRegion) {
		return errors.Errorf("call recordings region %q is not a valid AWS region", config.CallRecordingsRegion)
	}

	prefix := config.CallRecordingsPrefix
	if strings.TrimSpace(prefix) == "" {
		return errors.New("call recordings prefix must not be empty, an empty prefix would cover the whole bucket")
	}
	if !strings.HasSuffix(prefix, "/") || strings.HasPrefix(prefix, "/") {
		return errors.Errorf("call recordings prefix %q must end with / and must not start with /", prefix)
	}
	for _, segment := range strings.Split(prefix, "/") {
		if segment == ".." {
			return errors.Errorf("call recordings prefix %q must not contain a .. segment", prefix)
		}
	}
	return nil
}

func callRecordingsAWSConfig(config *Config) *aws.Config {
	return &aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecretAccessKey, ""),
		Endpoint:         aws.String(config.S3Endpoint),
		Region:           aws.String(config.CallRecordingsRegion),
		DisableSSL:       aws.Bool(config.S3DisableSSL),
		S3ForcePathStyle: aws.Bool(config.S3ForcePathStyle),
		MaxRetries:       aws.Int(callRecordingsS3MaxRetries),
	}
}

// NewCallRecordingsS3Client builds the S3 client for the call recordings bucket. It uses the archive
// client's credentials and endpoint with the recordings region, and logs no request headers.
func NewCallRecordingsS3Client(config *Config) (s3iface.S3API, error) {
	s3Session, err := session.NewSession(callRecordingsAWSConfig(config))
	if err != nil {
		return nil, errors.Wrap(err, "error creating call recordings S3 session")
	}
	return s3.New(s3Session), nil
}

// CheckCallRecordingsBucket lists one key under the recordings prefix, which needs the same
// permission as the retention step, and returns the error instead of exiting.
func CheckCallRecordingsBucket(s3Client s3iface.S3API, config *Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.CommonTimeout)*time.Minute)
	defer cancel()

	_, err := s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(config.CallRecordingsBucket),
		Prefix:  aws.String(config.CallRecordingsPrefix),
		MaxKeys: aws.Int64(1),
	})
	return errors.Wrap(err, "error listing call recordings bucket")
}

// DeleteExpiredCallRecordings deletes objects under the recordings prefix uploaded strictly before
// now minus the retention period. A listing error or the context deadline ends the run early with
// Partial set; the next cycle lists again.
func DeleteExpiredCallRecordings(ctx context.Context, now time.Time, config *Config, s3Client s3iface.S3API) (CallRecordingsSummary, error) {
	run := &callRecordingsRun{
		config: config,
		client: s3Client,
		prefix: config.CallRecordingsPrefix,
		cutoff: now.UTC().AddDate(0, 0, -config.CallRecordingsRetentionPeriod),
		log: logrus.WithFields(logrus.Fields{
			"component":   "call_recordings_retention",
			"cycle_start": now.UTC(),
			"bucket":      config.CallRecordingsBucket,
			"prefix":      config.CallRecordingsPrefix,
			"dry_run":     config.CallRecordingsDryRun,
		}),
		summary: CallRecordingsSummary{DryRun: config.CallRecordingsDryRun},
	}

	start := time.Now()
	err := run.execute(ctx)
	run.summary.Duration = time.Since(start)
	CallRecordingsLastRunTimestamp.Set(float64(time.Now().Unix()))
	run.logSummary()

	return run.summary, err
}

type callRecordingsRun struct {
	config  *Config
	client  s3iface.S3API
	prefix  string
	cutoff  time.Time
	log     *logrus.Entry
	summary CallRecordingsSummary
	pending []pendingRecording
}

func (r *callRecordingsRun) execute(ctx context.Context) error {
	var token *string
	for {
		if err := ctx.Err(); err != nil {
			r.summary.Partial = true
			return errors.Wrap(err, "call recordings retention stopped at its deadline")
		}

		page, err := r.listPage(ctx, token)
		if err != nil {
			r.summary.Partial = true
			r.log.WithError(err).WithField("operation", "ListObjectsV2").Error("error listing call recordings")
			r.flush(ctx)
			return errors.Wrap(err, "error listing call recordings")
		}

		for _, obj := range page.Contents {
			r.consider(ctx, obj)
		}

		if !aws.BoolValue(page.IsTruncated) || page.NextContinuationToken == nil {
			break
		}
		token = page.NextContinuationToken
	}

	r.flush(ctx)
	if err := ctx.Err(); err != nil {
		r.summary.Partial = true
		return errors.Wrap(err, "call recordings retention stopped at its deadline")
	}
	return nil
}

func (r *callRecordingsRun) listPage(ctx context.Context, token *string) (*s3.ListObjectsV2Output, error) {
	pageCtx, cancel := context.WithTimeout(ctx, time.Duration(r.config.CommonTimeout)*time.Minute)
	defer cancel()

	return r.client.ListObjectsV2WithContext(pageCtx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(r.config.CallRecordingsBucket),
		Prefix:            aws.String(r.prefix),
		MaxKeys:           aws.Int64(callRecordingsListPageSize),
		ContinuationToken: token,
	})
}

func (r *callRecordingsRun) consider(ctx context.Context, obj *s3.Object) {
	key := aws.StringValue(obj.Key)
	// guards against an S3-compatible endpoint returning keys outside the requested prefix
	if !strings.HasPrefix(key, r.prefix) {
		return
	}
	r.summary.Scanned++
	CallRecordingsScannedTotal.Inc()

	if obj.LastModified == nil || !obj.LastModified.Before(r.cutoff) {
		return
	}

	r.summary.Eligible++
	r.pending = append(r.pending, pendingRecording{key: key, size: aws.Int64Value(obj.Size)})
	if r.config.CallRecordingsDryRun {
		r.sample(key, *obj.LastModified)
	}
	if len(r.pending) >= callRecordingsDeleteBatchSize {
		r.flush(ctx)
	}
}

// sample keeps the oldest eligible keys, ordered from oldest to newest.
func (r *callRecordingsRun) sample(key string, lastModified time.Time) {
	keys := r.summary.OldestKeys
	if len(keys) == callRecordingsDryRunSampleSize && !lastModified.Before(keys[len(keys)-1].LastModified) {
		return
	}

	i := len(keys)
	for i > 0 && lastModified.Before(keys[i-1].LastModified) {
		i--
	}
	keys = append(keys, SampledRecordingKey{})
	copy(keys[i+1:], keys[i:])
	keys[i] = SampledRecordingKey{Key: key, LastModified: lastModified}
	if len(keys) > callRecordingsDryRunSampleSize {
		keys = keys[:callRecordingsDryRunSampleSize]
	}
	r.summary.OldestKeys = keys
}

func (r *callRecordingsRun) flush(ctx context.Context) {
	batch := r.pending
	r.pending = r.pending[:0]
	if len(batch) == 0 {
		return
	}

	if r.config.CallRecordingsDryRun {
		for _, rec := range batch {
			r.summary.DeletedBytes += rec.size
		}
		return
	}

	// a batch is never sent once the step deadline passed; the next cycle lists the keys again
	if ctx.Err() != nil {
		return
	}

	objects := make([]*s3.ObjectIdentifier, len(batch))
	for i, rec := range batch {
		objects[i] = &s3.ObjectIdentifier{Key: aws.String(rec.key)}
	}

	batchCtx, cancel := context.WithTimeout(ctx, time.Duration(r.config.CommonTimeout)*time.Minute)
	output, err := r.client.DeleteObjectsWithContext(batchCtx, &s3.DeleteObjectsInput{
		Bucket: aws.String(r.config.CallRecordingsBucket),
		Delete: &s3.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	cancel()

	if err != nil {
		r.summary.DeleteErrors += int64(len(batch))
		CallRecordingsDeleteErrorsTotal.Add(float64(len(batch)))
		r.log.WithError(err).WithField("operation", "DeleteObjects").WithField("batch_size", len(batch)).Error("error deleting call recordings batch")
		return
	}

	failed := make(map[string]bool, len(output.Errors))
	for _, keyErr := range output.Errors {
		key := aws.StringValue(keyErr.Key)
		failed[key] = true
		r.log.WithFields(logrus.Fields{
			"key":           key,
			"error_code":    aws.StringValue(keyErr.Code),
			"error_message": aws.StringValue(keyErr.Message),
		}).Warn("error deleting call recording")
	}

	var deleted, deletedBytes, errored int64
	for _, rec := range batch {
		if failed[rec.key] {
			errored++
			continue
		}
		deleted++
		deletedBytes += rec.size
	}
	r.summary.Deleted += deleted
	r.summary.DeletedBytes += deletedBytes
	r.summary.DeleteErrors += errored
	CallRecordingsDeletedTotal.Add(float64(deleted))
	CallRecordingsDeletedBytesTotal.Add(float64(deletedBytes))
	CallRecordingsDeleteErrorsTotal.Add(float64(errored))
}

func (r *callRecordingsRun) logSummary() {
	log := r.log.WithFields(logrus.Fields{
		"scanned":       r.summary.Scanned,
		"eligible":      r.summary.Eligible,
		"deleted":       r.summary.Deleted,
		"deleted_bytes": r.summary.DeletedBytes,
		"delete_errors": r.summary.DeleteErrors,
		"duration":      r.summary.Duration,
		"partial":       r.summary.Partial,
	})
	if r.summary.DryRun {
		log = log.WithField("oldest_keys", r.summary.OldestKeys)
	}
	log.Info("call recordings retention completed")
}
