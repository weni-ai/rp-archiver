package main

import (
	"context"
	"fmt"
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

	if config.ArchiveSingleMonth {
		executeCmdArchiveSingleMonth(db, s3Client, config)
	}

	if config.ArchiveRollupSingleMonth {
		executeCmdArchiveAndRollupSingleMonth(db, s3Client, config)
	}

	archiveType := archives.RunType
	if config.DeleteArchiveType == "message" {
		archiveType = archives.MessageType
	}

	if config.DeleteArchived {
		executeCmdDelete(db, s3Client, config, archiveType)
	}

	if config.DeleteFromOrg > 0 {
		executeCmdDeleteFromOrg(db, s3Client, config, archiveType, config.DeleteFromOrg)
	}

	if config.DeleteFromOrgSingleMonth {
		executeCmdDeleteFromOrgSingleMonth(db, s3Client, config, archiveType)
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
		log.WithField("archive_type", archiveType).Infof("archives deleted %d", len(deleted))
	}
	logrus.Info("Exiting...")
	time.Sleep(3 * time.Second)
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

	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
	ctx, cancel = context.WithTimeout(context.Background(), time.Hour*12)
	deleted, err := archives.DeleteArchivedOrgRecords(ctx, time.Now(), config, db, s3Client, org, archiveType)
	cancel()
	if err != nil {
		log.WithError(err).WithField("archive_type", archiveType).Error("error archiving org runs")
		os.Exit(1)
	}
	log.WithField("archive_type", archiveType).Infof("archives deleted %d", len(deleted))
	logrus.Info("Exiting...")
	time.Sleep(3 * time.Second)
	os.Exit(0)
}

func executeCmdDeleteFromOrgSingleMonth(db *sqlx.DB, s3Client s3iface.S3API, config *archives.Config, archiveType archives.ArchiveType) {
	validateOrgYearMonthConf(config)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	org, err := archives.GetOrg(ctx, db, config, config.OrgID)
	cancel()
	if err != nil {
		logrus.WithField("orgID", config.OrgID).Fatal("error on get org", err)
	}

	inputDate := fmt.Sprintf("%s-%s-01 00:00:00", config.Year, config.Month)
	startDate, err := time.Parse("2006-01-02 15:04:05", inputDate)
	if err != nil {
		logrus.Fatalf("error on parse date: %v", err)
	}
	endDate := startDate.AddDate(0, 1, 0).Add(time.Nanosecond * -1)

	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
	ctx, cancel = context.WithTimeout(context.Background(), time.Hour*12)
	err = archives.DeleteArchivedOrgRecordsForDate(ctx, time.Now(), config, db, s3Client, *org, archiveType, startDate, endDate)
	cancel()
	if err != nil {
		log.WithError(err).WithField("archive_type", archiveType).Error("error archiving org runs")
		os.Exit(1)
	}
	log.WithField("archive_type", archiveType).WithField("start_date", startDate).WithField("end_date", endDate).Infof("archives deleted")
	logrus.Info("Exiting...")
	time.Sleep(3 * time.Second)
	os.Exit(0)
}

func executeCmdArchiveSingleMonth(db *sqlx.DB, s3Client s3iface.S3API, config *archives.Config) {
	validateOrgYearMonthConf(config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	org, err := archives.GetOrg(ctx, db, config, config.OrgID)
	cancel()
	if err != nil {
		logrus.WithError(err).Fatal("error getting org for id ", config.OrgID)
	}
	if org == nil {
		logrus.Fatal("couldn't find org for id ", config.OrgID)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Hour*6)

	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)

	if config.ArchiveMessages {
		_, err = archives.ArchiveOrgSingleMonth(ctx, db, config, s3Client, *org, config.Year, config.Month, archives.MessageType)
		if err != nil {
			log.WithError(err).WithField("archive_type", archives.MessageType).Error("error archiving org messages")
		}
	}

	if config.ArchiveRuns {
		_, err = archives.ArchiveOrgSingleMonth(ctx, db, config, s3Client, *org, config.Year, config.Month, archives.RunType)
		if err != nil {
			log.WithError(err).WithField("archive_type", archives.RunType).Error("error archiving org runs")
		}
	}

	cancel()
	os.Exit(0)
}

func executeCmdArchiveAndRollupSingleMonth(db *sqlx.DB, s3Client s3iface.S3API, config *archives.Config) {
	validateOrgYearMonthConf(config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	org, err := archives.GetOrg(ctx, db, config, config.OrgID)
	cancel()
	if err != nil {
		logrus.WithError(err).Fatal("error getting org for id ", config.OrgID)
	}
	if org == nil {
		logrus.Fatal("couldn't find org for id ", config.OrgID)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Hour*12)
	defer cancel()

	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)

	if config.ArchiveMessages {
		_, err = archives.ArchiveRollupOrgSingleMonth(ctx, db, config, s3Client, *org, config.Year, config.Month, archives.MessageType)
		if err != nil {
			log.WithError(err).WithField("archive_type", archives.MessageType).Error("error archiving org messages")
		}
	}

	if config.ArchiveRuns {
		_, err = archives.ArchiveRollupOrgSingleMonth(ctx, db, config, s3Client, *org, config.Year, config.Month, archives.RunType)
		if err != nil {
			log.WithError(err).WithField("archive_type", archives.RunType).Error("error archiving org runs")
		}
	}

	os.Exit(0)
}

func validateOrgYearMonthConf(config *archives.Config) {
	if config.OrgID == "" {
		logrus.Fatal("on single month archive mode, argument OrgID should be provided")
	}
	if config.Year == "" {
		logrus.Fatal("on single month archive mode, argument Year should be provided ex: year=2022")
	}
	if config.Month == "" {
		logrus.Fatal("on single month archive mode, argument Month should be provided ex: month=01")
	}
}
