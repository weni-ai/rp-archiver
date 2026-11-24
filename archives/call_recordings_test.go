package archives

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRecording struct {
	key          string
	lastModified time.Time
	size         int64
}

// fakeRecordingsS3 is an in-memory S3 for the call recordings step. Any method it does not
// implement panics through the nil embedded interface, so unexpected calls fail the test.
type fakeRecordingsS3 struct {
	s3iface.S3API

	t        *testing.T
	mu       sync.Mutex
	objects  map[string]fakeRecording
	stray    []fakeRecording
	pageSize int

	listCalls     []*s3.ListObjectsV2Input
	listDeadlines []bool
	listErrOnCall int
	listErr       error
	onList        func(call int)

	deleteBatches []int
	deleteKeyErrs map[string]string
	deleteReqErr  error
}

func newFakeRecordingsS3(t *testing.T, objects ...fakeRecording) *fakeRecordingsS3 {
	f := &fakeRecordingsS3{t: t, objects: make(map[string]fakeRecording), pageSize: 1000, deleteKeyErrs: make(map[string]string)}
	for _, o := range objects {
		f.objects[o.key] = o
	}
	return f
}

func (f *fakeRecordingsS3) ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listCalls = append(f.listCalls, input)
	_, hasDeadline := ctx.Deadline()
	f.listDeadlines = append(f.listDeadlines, hasDeadline)
	call := len(f.listCalls)
	if f.onList != nil {
		f.onList(call)
	}
	if f.listErrOnCall == call {
		return nil, f.listErr
	}

	prefix := aws.StringValue(input.Prefix)
	matching := make([]fakeRecording, 0, len(f.objects))
	for _, o := range f.objects {
		if strings.HasPrefix(o.key, prefix) {
			matching = append(matching, o)
		}
	}
	sort.Slice(matching, func(i, j int) bool { return matching[i].key < matching[j].key })

	// like S3, the continuation token resumes after the last returned key, so deleting while
	// listing does not skip objects
	start := 0
	if input.ContinuationToken != nil {
		start = sort.Search(len(matching), func(i int) bool { return matching[i].key > *input.ContinuationToken })
	}
	limit := f.pageSize
	if input.MaxKeys != nil && int(*input.MaxKeys) < limit {
		limit = int(*input.MaxKeys)
	}
	end := start + limit
	if end > len(matching) {
		end = len(matching)
	}

	out := &s3.ListObjectsV2Output{}
	if call == 1 {
		for _, o := range f.stray {
			out.Contents = append(out.Contents, toS3Object(o))
		}
	}
	for _, o := range matching[start:end] {
		out.Contents = append(out.Contents, toS3Object(o))
	}
	if end < len(matching) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(matching[end-1].key)
	} else {
		out.IsTruncated = aws.Bool(false)
	}
	return out, nil
}

func (f *fakeRecordingsS3) DeleteObjectsWithContext(ctx aws.Context, input *s3.DeleteObjectsInput, opts ...request.Option) (*s3.DeleteObjectsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n := len(input.Delete.Objects)
	if n > callRecordingsDeleteBatchSize {
		f.t.Fatalf("delete batch of %d keys exceeds %d", n, callRecordingsDeleteBatchSize)
	}
	assert.True(f.t, aws.BoolValue(input.Delete.Quiet), "delete must be quiet")
	f.deleteBatches = append(f.deleteBatches, n)
	if f.deleteReqErr != nil {
		return nil, f.deleteReqErr
	}

	out := &s3.DeleteObjectsOutput{}
	for _, id := range input.Delete.Objects {
		key := aws.StringValue(id.Key)
		if code, failed := f.deleteKeyErrs[key]; failed {
			out.Errors = append(out.Errors, &s3.Error{Key: aws.String(key), Code: aws.String(code), Message: aws.String("injected")})
			continue
		}
		delete(f.objects, key)
	}
	return out, nil
}

func (f *fakeRecordingsS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func toS3Object(o fakeRecording) *s3.Object {
	return &s3.Object{Key: aws.String(o.key), LastModified: aws.Time(o.lastModified), Size: aws.Int64(o.size)}
}

var testNow = time.Date(2026, 11, 18, 0, 1, 0, 0, time.UTC)

func recordingConfig() *Config {
	config := NewConfig()
	config.CallRecordingsRetentionPeriod = 90
	config.CallRecordingsBucket = "weni-call-recordings-staging"
	config.CallRecordingsRegion = "sa-east-1"
	return config
}

func recordingKey(i int) string {
	return fmt.Sprintf("recordings/v1/0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0/1a2b3c4d-5e6f-7081-92a3-b4c5d6e7f809/%08d.wav", i)
}

func recordingAt(i int, age time.Duration, size int64) fakeRecording {
	return fakeRecording{key: recordingKey(i), lastModified: testNow.Add(-age), size: size}
}

const day = 24 * time.Hour

func metricValue(c prometheus.Metric) float64 {
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		panic(err)
	}
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	return m.Gauge.GetValue()
}

type recordingMetrics struct {
	scanned, deleted, deletedBytes, deleteErrors float64
}

func readRecordingMetrics() recordingMetrics {
	return recordingMetrics{
		scanned:      metricValue(CallRecordingsScannedTotal),
		deleted:      metricValue(CallRecordingsDeletedTotal),
		deletedBytes: metricValue(CallRecordingsDeletedBytesTotal),
		deleteErrors: metricValue(CallRecordingsDeleteErrorsTotal),
	}
}

func (m recordingMetrics) since(before recordingMetrics) recordingMetrics {
	return recordingMetrics{
		scanned:      m.scanned - before.scanned,
		deleted:      m.deleted - before.deleted,
		deletedBytes: m.deletedBytes - before.deletedBytes,
		deleteErrors: m.deleteErrors - before.deleteErrors,
	}
}

func TestValidateCallRecordingsConfig(t *testing.T) {
	valid := func(mutate func(c *Config)) *Config {
		c := recordingConfig()
		mutate(c)
		return c
	}

	tcs := []struct {
		name   string
		config *Config
		errMsg string
	}{
		{"disabled with empty fields", valid(func(c *Config) {
			c.CallRecordingsRetentionPeriod = 0
			c.CallRecordingsBucket = ""
			c.CallRecordingsRegion = ""
			c.CallRecordingsPrefix = ""
		}), ""},
		{"negative period", valid(func(c *Config) { c.CallRecordingsRetentionPeriod = -1 }), "at least 90"},
		{"period 1", valid(func(c *Config) { c.CallRecordingsRetentionPeriod = 1 }), "at least 90"},
		{"period 89", valid(func(c *Config) { c.CallRecordingsRetentionPeriod = 89 }), "at least 90"},
		{"period 90", valid(func(c *Config) {}), ""},
		{"blank bucket", valid(func(c *Config) { c.CallRecordingsBucket = " " }), "bucket"},
		{"blank region", valid(func(c *Config) { c.CallRecordingsRegion = "" }), "region"},
		{"empty prefix", valid(func(c *Config) { c.CallRecordingsPrefix = "" }), "prefix"},
		{"prefix without trailing slash", valid(func(c *Config) { c.CallRecordingsPrefix = "recordings/v1" }), "prefix"},
		{"prefix with leading slash", valid(func(c *Config) { c.CallRecordingsPrefix = "/recordings/v1/" }), "prefix"},
		{"prefix with dot-dot segment", valid(func(c *Config) { c.CallRecordingsPrefix = "recordings/../" }), "prefix"},
		{"bucket with uppercase and underscore", valid(func(c *Config) { c.CallRecordingsBucket = "My_Bucket" }), "bucket"},
		{"bucket too short", valid(func(c *Config) { c.CallRecordingsBucket = "ab" }), "bucket"},
		{"bucket with leading dash", valid(func(c *Config) { c.CallRecordingsBucket = "-bucket" }), "bucket"},
		{"bucket with slash", valid(func(c *Config) { c.CallRecordingsBucket = "bucket/x" }), "bucket"},
		{"region with spaces", valid(func(c *Config) { c.CallRecordingsRegion = "sa east 1" }), "region"},
		{"region uppercase", valid(func(c *Config) { c.CallRecordingsRegion = "SA-EAST-1" }), "region"},
		{"region is an endpoint", valid(func(c *Config) { c.CallRecordingsRegion = "https://s3.amazonaws.com" }), "region"},
		{"gov region", valid(func(c *Config) { c.CallRecordingsRegion = "us-gov-west-1" }), ""},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCallRecordingsConfig(tc.config)
			if tc.errMsg == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestDeleteExpiredCallRecordings_StrictCutoff(t *testing.T) {
	fake := newFakeRecordingsS3(t,
		recordingAt(1, 89*day, 100),
		recordingAt(2, 90*day, 200),
		recordingAt(3, 91*day, 300),
	)
	before := readRecordingMetrics()

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, recordingConfig(), fake)
	require.NoError(t, err)

	assert.True(t, fake.has(recordingKey(1)))
	assert.True(t, fake.has(recordingKey(2)), "an object exactly at the cutoff is kept")
	assert.False(t, fake.has(recordingKey(3)))
	assert.Equal(t, int64(3), summary.Scanned)
	assert.Equal(t, int64(1), summary.Eligible)
	assert.Equal(t, int64(1), summary.Deleted)
	assert.Equal(t, int64(300), summary.DeletedBytes)
	assert.False(t, summary.Partial)
	assert.Equal(t, recordingMetrics{scanned: 3, deleted: 1, deletedBytes: 300}, readRecordingMetrics().since(before))
	assert.InDelta(t, float64(time.Now().Unix()), metricValue(CallRecordingsLastRunTimestamp), 5)
}

func TestDeleteExpiredCallRecordings_IgnoresOutsidePrefix(t *testing.T) {
	outside := fakeRecording{key: "archives/1/message_D20250101_abc.jsonl.gz", lastModified: testNow.Add(-400 * day), size: 50}
	stray := fakeRecording{key: "recordings/v0/old.wav", lastModified: testNow.Add(-400 * day), size: 60}
	fake := newFakeRecordingsS3(t, recordingAt(1, 100*day, 10), outside, stray)
	fake.stray = []fakeRecording{stray}
	before := readRecordingMetrics()

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, recordingConfig(), fake)
	require.NoError(t, err)

	assert.Equal(t, "recordings/v1/", aws.StringValue(fake.listCalls[0].Prefix))
	assert.True(t, fake.has(outside.key))
	assert.True(t, fake.has(stray.key), "a key returned outside the prefix is never deleted")
	assert.False(t, fake.has(recordingKey(1)))
	assert.Equal(t, int64(1), summary.Deleted)
	assert.Equal(t, recordingMetrics{scanned: 1, deleted: 1, deletedBytes: 10}, readRecordingMetrics().since(before))
}

func TestDeleteExpiredCallRecordings_BatchesOf1000(t *testing.T) {
	objects := make([]fakeRecording, 0, 2500)
	for i := 0; i < 2500; i++ {
		objects = append(objects, recordingAt(i, 120*day, 1))
	}
	fake := newFakeRecordingsS3(t, objects...)
	before := readRecordingMetrics()

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, recordingConfig(), fake)
	require.NoError(t, err)

	assert.Len(t, fake.listCalls, 3)
	for _, call := range fake.listCalls {
		assert.Equal(t, int64(callRecordingsListPageSize), aws.Int64Value(call.MaxKeys))
	}
	assert.True(t, fake.listDeadlines[0] && fake.listDeadlines[1] && fake.listDeadlines[2], "every page has a deadline")
	assert.Equal(t, []int{1000, 1000, 500}, fake.deleteBatches)
	assert.Equal(t, int64(2500), summary.Deleted)
	assert.Equal(t, recordingMetrics{scanned: 2500, deleted: 2500, deletedBytes: 2500}, readRecordingMetrics().since(before))
}

func TestDeleteExpiredCallRecordings_PerKeyErrors(t *testing.T) {
	objects := make([]fakeRecording, 0, 1500)
	for i := 0; i < 1500; i++ {
		objects = append(objects, recordingAt(i, 120*day, 2))
	}
	fake := newFakeRecordingsS3(t, objects...)
	fake.deleteKeyErrs[recordingKey(10)] = "AccessDenied"
	fake.deleteKeyErrs[recordingKey(20)] = "InternalError"
	before := readRecordingMetrics()

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, recordingConfig(), fake)
	require.NoError(t, err)

	assert.Equal(t, []int{1000, 500}, fake.deleteBatches, "the batch after the errors still runs")
	assert.True(t, fake.has(recordingKey(10)))
	assert.True(t, fake.has(recordingKey(20)))
	assert.Equal(t, int64(1498), summary.Deleted)
	assert.Equal(t, int64(2), summary.DeleteErrors)
	assert.Equal(t, recordingMetrics{scanned: 1500, deleted: 1498, deletedBytes: 2996, deleteErrors: 2}, readRecordingMetrics().since(before))
}

func TestDeleteExpiredCallRecordings_DryRun(t *testing.T) {
	objects := []fakeRecording{recordingAt(1, 10*day, 1)}
	for i := 2; i <= 15; i++ {
		objects = append(objects, recordingAt(i, time.Duration(90+i)*day, 100))
	}
	fake := newFakeRecordingsS3(t, objects...)
	config := recordingConfig()
	config.CallRecordingsDryRun = true
	before := readRecordingMetrics()

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, config, fake)
	require.NoError(t, err)

	assert.Empty(t, fake.deleteBatches)
	assert.True(t, summary.DryRun)
	assert.Equal(t, int64(14), summary.Eligible)
	assert.Equal(t, int64(0), summary.Deleted)
	assert.Equal(t, int64(1400), summary.DeletedBytes)
	require.Len(t, summary.OldestKeys, callRecordingsDryRunSampleSize)
	for i, sample := range summary.OldestKeys {
		assert.Equal(t, recordingKey(15-i), sample.Key)
	}
	assert.Equal(t, recordingMetrics{scanned: 15}, readRecordingMetrics().since(before))
}

func TestDeleteExpiredCallRecordings_ListError(t *testing.T) {
	fake := newFakeRecordingsS3(t, recordingAt(1, 100*day, 1), recordingAt(2, 100*day, 1), recordingAt(3, 100*day, 1))
	fake.pageSize = 2
	fake.listErrOnCall = 2
	fake.listErr = errors.New("connection reset")

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, recordingConfig(), fake)
	require.Error(t, err)

	assert.True(t, summary.Partial)
	assert.Len(t, fake.listCalls, 2, "no listing after the failed page")
	assert.Equal(t, []int{2}, fake.deleteBatches, "eligible keys from the first page are still deleted")
	assert.False(t, fake.has(recordingKey(1)))
	assert.False(t, fake.has(recordingKey(2)))
	assert.True(t, fake.has(recordingKey(3)))
}

func TestDeleteExpiredCallRecordings_BatchRequestError(t *testing.T) {
	objects := make([]fakeRecording, 0, 1200)
	for i := 0; i < 1200; i++ {
		objects = append(objects, recordingAt(i, 120*day, 1))
	}
	fake := newFakeRecordingsS3(t, objects...)
	fake.deleteReqErr = errors.New("AccessDenied")
	before := readRecordingMetrics()

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, recordingConfig(), fake)
	require.NoError(t, err)

	assert.Len(t, fake.listCalls, 2, "the next page is processed after a failed batch")
	assert.Equal(t, []int{1000, 200}, fake.deleteBatches)
	assert.Equal(t, int64(1200), summary.DeleteErrors)
	assert.Equal(t, int64(0), summary.Deleted)
	assert.False(t, summary.Partial)
	assert.Equal(t, recordingMetrics{scanned: 1200, deleteErrors: 1200}, readRecordingMetrics().since(before))
}

func TestDeleteExpiredCallRecordings_Deadline(t *testing.T) {
	fake := newFakeRecordingsS3(t, recordingAt(1, 100*day, 1), recordingAt(2, 100*day, 1), recordingAt(3, 100*day, 1))
	fake.pageSize = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake.onList = func(call int) {
		if call == 1 {
			cancel()
		}
	}
	CallRecordingsLastRunTimestamp.Set(0)

	summary, err := DeleteExpiredCallRecordings(ctx, testNow, recordingConfig(), fake)
	require.Error(t, err)

	assert.True(t, summary.Partial)
	assert.Len(t, fake.listCalls, 1)
	assert.Empty(t, fake.deleteBatches, "no request after the deadline")
	assert.NotZero(t, metricValue(CallRecordingsLastRunTimestamp))
}

func TestDeleteExpiredCallRecordings_BoundedMemory(t *testing.T) {
	objects := make([]fakeRecording, 0, 100000)
	for i := 0; i < 100000; i++ {
		objects = append(objects, recordingAt(i, time.Duration(91+i%500)*day, 1))
	}
	fake := newFakeRecordingsS3(t, objects...)

	summary, err := DeleteExpiredCallRecordings(context.Background(), testNow, recordingConfig(), fake)
	require.NoError(t, err)
	assert.Equal(t, int64(100000), summary.Deleted)
	assert.Len(t, fake.deleteBatches, 100)

	dryFake := newFakeRecordingsS3(t, objects...)
	config := recordingConfig()
	config.CallRecordingsDryRun = true
	summary, err = DeleteExpiredCallRecordings(context.Background(), testNow, config, dryFake)
	require.NoError(t, err)
	assert.Len(t, summary.OldestKeys, callRecordingsDryRunSampleSize)
}

func TestNewCallRecordingsS3Client_Config(t *testing.T) {
	config := recordingConfig()
	config.S3Endpoint = "https://s3.amazonaws.com"
	config.S3Region = "us-east-1"
	config.S3DisableSSL = true
	config.S3ForcePathStyle = true

	awsConfig := callRecordingsAWSConfig(config)

	assert.Equal(t, "sa-east-1", aws.StringValue(awsConfig.Region))
	assert.Equal(t, callRecordingsS3MaxRetries, aws.IntValue(awsConfig.MaxRetries))
	assert.Equal(t, 3, aws.IntValue(awsConfig.MaxRetries))
	assert.Equal(t, config.S3Endpoint, aws.StringValue(awsConfig.Endpoint))
	assert.True(t, aws.BoolValue(awsConfig.DisableSSL))
	assert.True(t, aws.BoolValue(awsConfig.S3ForcePathStyle))

	creds, err := awsConfig.Credentials.Get()
	require.NoError(t, err)
	assert.Equal(t, config.AWSAccessKeyID, creds.AccessKeyID)
}

func TestNewCallRecordingsS3Client_BucketUnreachable(t *testing.T) {
	fake := newFakeRecordingsS3(t)
	fake.listErrOnCall = 1
	fake.listErr = errors.New("AccessDenied")
	config := recordingConfig()

	err := CheckCallRecordingsBucket(fake, config)
	require.Error(t, err)

	require.Len(t, fake.listCalls, 1)
	assert.Equal(t, config.CallRecordingsBucket, aws.StringValue(fake.listCalls[0].Bucket))
	assert.Equal(t, config.CallRecordingsPrefix, aws.StringValue(fake.listCalls[0].Prefix))
	assert.Equal(t, int64(1), aws.Int64Value(fake.listCalls[0].MaxKeys))
	assert.True(t, fake.listDeadlines[0])

	fake.listErrOnCall = 0
	assert.NoError(t, CheckCallRecordingsBucket(fake, config))
}

func TestNewCallRecordingsS3Client_NoHeaderLogging(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>weni-call-recordings-staging</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	defer server.Close()

	config := recordingConfig()
	config.S3Endpoint = server.URL
	config.S3DisableSSL = true
	config.S3ForcePathStyle = true
	config.AWSAccessKeyID = "AKIDCALLRECORDINGSTEST"
	config.AWSSecretAccessKey = "call-recordings-test-secret"

	oldLevel, oldHooks := logrus.GetLevel(), logrus.StandardLogger().Hooks
	defer func() {
		logrus.SetLevel(oldLevel)
		logrus.StandardLogger().Hooks = oldHooks
	}()
	logrus.SetLevel(logrus.DebugLevel)
	logrus.StandardLogger().Hooks = make(logrus.LevelHooks)
	hook := test.NewGlobal()

	client, err := NewCallRecordingsS3Client(config)
	require.NoError(t, err)
	require.NoError(t, CheckCallRecordingsBucket(client, config))

	for _, entry := range hook.AllEntries() {
		line := entry.Message + fmt.Sprint(entry.Data)
		assert.NotContains(t, line, "Authorization")
		assert.NotContains(t, line, "X-Amz-")
		assert.NotContains(t, line, config.AWSSecretAccessKey)
	}
}
