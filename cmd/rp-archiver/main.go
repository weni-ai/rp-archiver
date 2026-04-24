package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/evalphobia/logrus_sentry"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/rp-archiver/archives"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func main() {
	config := archives.NewConfig()
	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", []string{"archiver.toml"})
	loader.MustLoad()

	if config.KeepFiles && !config.UploadToS3 {
		logrus.Fatal("cannot delete archives and also not upload to s3")
	}

	// configure our logger
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{})

	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		logrus.Fatalf("Invalid log level '%s'", level)
	}
	logrus.SetLevel(level)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(config.SentryDSN, []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		if err != nil {
			logrus.Fatalf("invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		logrus.StandardLogger().Hooks.Add(hook)
	}

	// our settings shouldn't contain a timezone, nothing will work right with this not being a constant UTC
	if strings.Contains(config.DB, "TimeZone") {
		logrus.WithField("db", config.DB).Fatalf("invalid db connection string, do not specify a timezone, archiver always uses UTC")
	}

	// force our DB connection to be in UTC
	if strings.Contains(config.DB, "?") {
		config.DB += "&TimeZone=UTC"
	} else {
		config.DB += "?TimeZone=UTC"
	}

	db, err := sqlx.Open("postgres", config.DB)
	if err != nil {
		logrus.Fatal(err)
	}
	db.SetMaxOpenConns(config.MaxOpenConnections)

	var s3Client s3iface.S3API
	if config.UploadToS3 {
		s3Client, err = archives.NewS3Client(config)
		if err != nil {
			logrus.WithError(err).Fatal("unable to initialize s3 client")
		}
	}

	// ensure that we can actually write to the temp directory
	err = archives.EnsureTempArchiveDirectory(config.TempDir)
	if err != nil {
		logrus.WithError(err).Fatal("cannot write to temp directory")
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := fmt.Sprintf(":%d", config.MetricsPort)
		if err := http.ListenAndServe(addr, nil); err != nil {
			logrus.WithError(err).Fatal("error starting metrics server")
		}
	}()

	semaphore := make(chan struct{}, config.MaxConcurrentArchivation)

	archiveTask := func(org archives.Org) {
		defer func() { <-semaphore }()
		// no single org should take more than 12 hours
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour*12)
		defer cancel()

		log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
		orgIDStr := strconv.Itoa(org.ID)

		// Mark org as in-progress: pending goes up now and back down when we finish.
		archives.CycleOrgsPending.Inc()
		archives.OrgPendingStatus.WithLabelValues(orgIDStr, org.Name).Set(1)
		defer func() {
			archives.CycleOrgsPending.Dec()
			archives.OrgPendingStatus.WithLabelValues(orgIDStr, org.Name).Set(0)
		}()

		hasError := false
		var msgCreated, runCreated []*archives.Archive

		if config.ArchiveMessages {
			msgCreated, _, err = archives.ArchiveOrg(ctx, time.Now(), config, db, s3Client, org, archives.MessageType)
			if err != nil {
				hasError = true
				log.WithError(err).WithField("archive_type", archives.MessageType).Error("error archiving org messages")
			}
		}
		if config.ArchiveRuns {
			runCreated, _, err = archives.ArchiveOrg(ctx, time.Now(), config, db, s3Client, org, archives.RunType)
			if err != nil {
				hasError = true
				log.WithError(err).WithField("archive_type", archives.RunType).Error("error archiving org runs")
			}
		}

		if len(msgCreated) == 0 && len(runCreated) == 0 {
			archives.CycleOrgsUpToDate.Inc()
		}

		if hasError {
			archives.CycleOrgsFailed.Inc()
			archives.OrgFailedStatus.WithLabelValues(orgIDStr, org.Name).Set(1)
		} else {
			archives.CycleOrgsCompleted.Inc()
		}
	}

	for {
		start := time.Now().In(time.UTC)

		archives.ResetCycleMetrics()

		// convert the starttime to time.Time
		layout := "15:04"
		hour, err := time.Parse(layout, config.StartTime)
		if err != nil {
			logrus.WithError(err).Fatal("invalid start time supplied, format: HH:mm")
		}

		var orgs []archives.Org
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*15)
		if config.ArchiveInactive {
			// get our inactive orgs
			orgs, err = archives.GetInactiveOrgs(ctx, db, config)
		} else {
			// get our active orgs
			orgs, err = archives.GetActiveOrgs(ctx, db, config)
		}
		cancel()
		if err != nil {
			logrus.WithError(err).Error("error getting orgs")
			time.Sleep(time.Minute * 5)

			// after this, reopen db connection to prevent using the same in case of connection problem that we have faced sometimes with broken pipe error
			db, err = sqlx.Open("postgres", config.DB)
			if err != nil {
				logrus.Fatal(err)
			}
			db.SetMaxOpenConns(2)

			continue
		}

		archives.CycleOrgsTotal.Set(float64(len(orgs)))

		// for each org, do our export
		for _, org := range orgs {
			semaphore <- struct{}{}
			go archiveTask(org)
		}

		// ok, we did all our work for our orgs, quit if so configured or sleep until the next day
		if config.ExitOnCompletion {
			break
		}

		// build up our next start
		now := time.Now().In(time.UTC)
		nextDay := time.Date(now.Year(), now.Month(), now.Day(), hour.Hour(), hour.Minute(), 0, 0, time.UTC)

		// if this time is before our actual start, add a day
		if nextDay.Before(start) {
			nextDay = nextDay.AddDate(0, 0, 1)
		}

		napTime := nextDay.Sub(time.Now().In(time.UTC))

		if napTime > time.Duration(0) {
			logrus.WithField("time", napTime).WithField("next_start", nextDay).Info("Sleeping until next UTC day")
			time.Sleep(napTime)
		} else {
			logrus.WithField("next_start", nextDay).Info("Rebuilding immediately without sleep")
		}
	}
}
