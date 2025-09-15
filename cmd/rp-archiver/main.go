package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/evalphobia/logrus_sentry"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/rp-archiver/archives"
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
	db.SetMaxOpenConns(2)

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

	semaphore := make(chan struct{}, config.MaxConcurrentArchivation)

	archiveTask := func(org archives.Org) {
		defer func() { <-semaphore }()
		// no single org should take more than 12 hours
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour*12)

		log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)

		if config.ArchiveMessages {
			_, _, err = archives.ArchiveOrg(ctx, time.Now(), config, db, s3Client, org, archives.MessageType)
			if err != nil {
				log.WithError(err).WithField("archive_type", archives.MessageType).Error("error archiving org messages")
			}
		}
		if config.ArchiveRuns {
			_, _, err = archives.ArchiveOrg(ctx, time.Now(), config, db, s3Client, org, archives.RunType)
			if err != nil {
				log.WithError(err).WithField("archive_type", archives.RunType).Error("error archiving org runs")
			}
		}
		cancel()
	}

	for {
		start := time.Now().In(time.UTC)

		// convert the starttime to time.Time
		layout := "15:04"
		hour, err := time.Parse(layout, config.StartTime)
		if err != nil {
			logrus.WithError(err).Fatal("invalid start time supplied, format: HH:mm")
		}

		var orgs []archives.Org
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

		if config.ArchiveOrgID > 0 {
			// archive specific org by ID
			org, err := archives.GetOrgByID(ctx, db, config, config.ArchiveOrgID)
			if err != nil {
				logrus.WithError(err).WithField("org_id", config.ArchiveOrgID).Error("error getting specific org")
			} else {
				orgs = []archives.Org{org}
				err = nil // reset error since we successfully got the org
			}
		} else if config.ArchiveInactive {
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

		// log what we're archiving
		if config.ArchiveOrgID > 0 {
			logrus.WithFields(logrus.Fields{
				"org_count": len(orgs),
				"org_id":    config.ArchiveOrgID,
			}).Info("starting archival for specific org")
		} else {
			logrus.WithField("org_count", len(orgs)).Info("starting archival for orgs")
		}

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
