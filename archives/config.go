package archives

// Config is our top level configuration object
type Config struct {
	DB        string `help:"the connection string for our database"`
	LogLevel  string `help:"the log level, one of error, warn, info, debug"`
	SentryDSN string `help:"the sentry configuration to log errors to, if any"`

	S3Endpoint       string `help:"the S3 endpoint we will write archives to"`
	S3Region         string `help:"the S3 region we will write archives to"`
	S3Bucket         string `help:"the S3 bucket we will write archives to"`
	S3DisableSSL     bool   `help:"whether we disable SSL when accessing S3. Should always be set to False unless you're hosting an S3 compatible service within a secure internal network"`
	S3ForcePathStyle bool   `help:"whether we force S3 path style. Should generally need to default to False unless you're hosting an S3 compatible service"`

	AWSAccessKeyID     string `help:"the access key id to use when authenticating S3"`
	AWSSecretAccessKey string `help:"the secret access key id to use when authenticating S3"`

	TempDir    string `help:"directory where temporary archive files are written"`
	KeepFiles  bool   `help:"whether we should keep local archive files after upload (default false)"`
	UploadToS3 bool   `help:"whether we should upload archive to S3"`

	ArchiveMessages  bool   `help:"whether we should archive messages"`
	ArchiveRuns      bool   `help:"whether we should archive runs"`
	RetentionPeriod  int    `help:"the number of days to keep before archiving"`
	Delete           bool   `help:"whether to delete messages and runs from the db after archival (default false)"`
	ExitOnCompletion bool   `help:"whether archiver should exit after completing archiving job (default false)"`
	StartTime        string `help:"what time archive jobs should run in UTC HH:MM "`

	RollupOrgTimeout          int `help:"rollup timeout for all org archives, limit in hours (default 3)"`
	BuildRollupArchiveTimeout int `help:"rollup for single archive timeout, limit in hours (default 1)"`

	MaxConcurrentArchivation int `help:"max concurrent org archivation (default 2)"`

	DeleteArchived    bool   `help:"to only delete archived files needing to be deleted"`
	DeleteFromOrg     int    `help:"to only delete archived files needing to be deleted from specified org"`
	DeleteArchiveType string `help:"archive type to be deleted (between run and msg) (default is run)"`

	ArchiveSingleMonth       bool   `help:"wheter archiver should archive only a single month"`
	ArchiveRollupSingleMonth bool   `help:"wheter archiver should archive only a single month from files"`
	DeleteFromOrgSingleMonth bool   `help:"to only delete archived from selected date to be deleted from specified orgId "`
	OrgID                    string `help:"org id"`
	Year                     string `help:"year that archive should be created ex: 2022"`
	Month                    string `help:"month that archive should be created ex: 01"`
}

// NewConfig returns a new default configuration object
func NewConfig() *Config {
	config := Config{
		DB:       "postgres://localhost/archiver_test?sslmode=disable",
		LogLevel: "info",

		S3Endpoint:       "https://s3.amazonaws.com",
		S3Region:         "us-east-1",
		S3Bucket:         "dl-archiver-test",
		S3DisableSSL:     false,
		S3ForcePathStyle: false,

		AWSAccessKeyID:     "missing_aws_access_key_id",
		AWSSecretAccessKey: "missing_aws_secret_access_key",

		TempDir:    "/tmp",
		KeepFiles:  false,
		UploadToS3: true,

		ArchiveMessages:  true,
		ArchiveRuns:      true,
		RetentionPeriod:  90,
		Delete:           false,
		ExitOnCompletion: false,
		StartTime:        "00:01",

		RollupOrgTimeout:          3,
		BuildRollupArchiveTimeout: 1,

		MaxConcurrentArchivation: 2,

		DeleteArchived:    false,
		DeleteFromOrg:     0,
		DeleteArchiveType: "run",

		ArchiveSingleMonth:       false,
		ArchiveRollupSingleMonth: false,
	}

	return &config
}
