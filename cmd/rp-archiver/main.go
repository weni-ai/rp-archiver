package main

import (
	"context"
	"flag"
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

	cmdDelete := flag.Bool("delete_archived", false, "cmd to only delete archived files needing to be deleted")
	cmdDeleteFromOrg := flag.Int("delete_from_org", 0, "cmd to only delete archived files needing to be deleted from specified org")
	flagArchiveType := flag.String("archive_type", "run", "archive type to be deleted (between run and msg) (default is run)")
	flag.Parse()

	archiveType := archives.RunType
	if *flagArchiveType == "message" {
		archiveType = archives.MessageType
	}

	if *cmdDelete {
		executeCmdDelete(db, s3Client, config, archiveType)
	}

	if *cmdDeleteFromOrg > 0 {
		executeCmdDeleteFromOrg(db, s3Client, config, archiveType, *cmdDeleteFromOrg)
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

		// get our active orgs
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		orgs, err := archives.GetActiveOrgs(ctx, db, config)
		cancel()

		if err != nil {
			logrus.WithError(err).Error("error getting active orgs")
			time.Sleep(time.Minute * 5)

			// after this, reopen db connection to prevent using the same in case of connection problem that we have faced sometimes with broken pipe error
			db, err = sqlx.Open("postgres", config.DB)
			if err != nil {
				logrus.Fatal(err)
			}
			db.SetMaxOpenConns(2)

			continue
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

func executeCmdDelete(db *sqlx.DB, s3Client s3iface.S3API, config *archives.Config, archiveType archives.ArchiveType) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	orgs, err := archives.GetActiveOrgs(ctx, db, config)
	cancel()
	if err != nil {
		logrus.WithError(err).Fatal("error fetching active orgs")
		os.Exit(1)
	}

	now := time.Now()

	for _, org := range orgs {
		log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour*12)
		deleted, err := archives.DeleteArchivedOrgRecords(ctx, now, config, db, s3Client, org, archiveType)
		cancel()
		if err != nil {
			log.WithError(err).WithField("archive_type", archiveType).Error("error archiving org runs")
			continue
		}
		log.WithError(err).WithField("archive_type", archiveType).Infof("archives deleted %d", len(deleted))
	}
	os.Exit(0)
}

func executeCmdDeleteFromOrg(db *sqlx.DB, s3Client s3iface.S3API, config *archives.Config, archiveType archives.ArchiveType, orgID int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	orgs, err := archives.GetActiveOrgs(ctx, db, config)
	cancel()
	if err != nil {
		logrus.WithError(err).Fatal("error fetching active orgs")
		os.Exit(1)
	}
	var org archives.Org
	for _, og := range orgs {
		if orgID == og.ID {
			org = og
		}
	}
	if err != nil {
		logrus.WithError(err).Fatalf("error fetching org by ID: %d", orgID)
		os.Exit(1)
	}

	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
	ctx, cancel = context.WithTimeout(context.Background(), time.Hour*12)
	deleted, err := archives.DeleteArchivedOrgRecords(ctx, time.Now(), config, db, s3Client, org, archiveType)
	cancel()
	if err != nil {
		log.WithError(err).WithField("archive_type", archiveType).Error("error archiving org runs")
		os.Exit(1)
	}
	log.Infof("archives deleted %d", len(deleted))
}
