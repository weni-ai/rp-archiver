package archives

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// archiveDurationBuckets covers from 1s up to 4h, suitable for archive creation and deletion operations.
var archiveDurationBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 7200, 14400}

// uploadDurationBuckets covers from 1s up to 15m, suitable for S3 uploads.
var uploadDurationBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 900}

var (
	// --- Cycle metrics ----------------------------------------------------------

	// CycleOrgsTotal is the total number of orgs loaded at the start of the archiving cycle.
	CycleOrgsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "archiver_cycle_orgs_total",
		Help: "Total number of orgs loaded in the current archiving cycle",
	})

	// CycleOrgsPending is the number of orgs currently being processed (in-progress).
	// It increments when processing starts and decrements when it finishes (success or failure).
	CycleOrgsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "archiver_cycle_orgs_pending",
		Help: "Number of orgs currently being processed in the archiving cycle",
	})

	// CycleOrgsUpToDate is the number of orgs already fully archived (no missing archives).
	CycleOrgsUpToDate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "archiver_cycle_orgs_up_to_date",
		Help: "Number of orgs already fully archived in the current archiving cycle",
	})

	// CycleOrgsCompleted is the number of orgs that finished archiving successfully.
	CycleOrgsCompleted = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "archiver_cycle_orgs_completed",
		Help: "Number of orgs successfully archived in the current archiving cycle",
	})

	// CycleOrgsFailed is the number of orgs that encountered an error during archiving.
	CycleOrgsFailed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "archiver_cycle_orgs_failed",
		Help: "Number of orgs that failed during archiving in the current archiving cycle",
	})

	// OrgPendingStatus is set to 1 while the org is being processed, and back to 0 when it finishes.
	// Labels: org_id, org_name. Use `archiver_org_pending_status == 1` in Grafana to see in-progress orgs.
	OrgPendingStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "archiver_org_pending_status",
		Help: "Set to 1 while the org is being processed, 0 when finished (success or failure)",
	}, []string{"org_id", "org_name"})

	// OrgFailedStatus is set to 1 for each org that had an error during the current cycle.
	// Labels: org_id, org_name. Use `archiver_org_failed_status == 1` in Grafana to list them.
	OrgFailedStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "archiver_org_failed_status",
		Help: "Set to 1 if the org encountered an error during archiving in the current cycle",
	}, []string{"org_id", "org_name"})

	// --- Archive creation metrics -----------------------------------------------

	// ArchiveCreationDuration tracks how long each archive file takes to build (labels: archive_type, period).
	ArchiveCreationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "archiver_archive_creation_duration_seconds",
		Help:    "Duration in seconds to create an archive file",
		Buckets: archiveDurationBuckets,
	}, []string{"archive_type", "period"})

	// ArchiveRecordsTotal counts every record written across all archives (labels: archive_type, period).
	ArchiveRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "archiver_archive_records_total",
		Help: "Total number of records written to archive files",
	}, []string{"archive_type", "period"})

	// ArchiveFileSizeBytes tracks the compressed size of each archive uploaded (labels: archive_type, period).
	ArchiveFileSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "archiver_archive_file_size_bytes",
		Help:    "Compressed size in bytes of generated archive files",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 12), // 1 KB → ~4 GB
	}, []string{"archive_type", "period"})

	// --- S3 metrics -------------------------------------------------------------

	// S3UploadDuration tracks how long each S3 upload takes (labels: archive_type, period).
	S3UploadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "archiver_s3_upload_duration_seconds",
		Help:    "Duration in seconds of S3 upload operations",
		Buckets: uploadDurationBuckets,
	}, []string{"archive_type", "period"})

	// S3UploadErrorsTotal counts S3 upload failures.
	S3UploadErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "archiver_s3_upload_errors_total",
		Help: "Total number of S3 upload errors",
	})

	// S3UploadBytesTotal counts the total bytes successfully uploaded to S3.
	S3UploadBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "archiver_s3_upload_bytes_total",
		Help: "Total bytes successfully uploaded to S3",
	})

	// --- Deletion metrics -------------------------------------------------------

	// RecordsDeletedTotal counts records deleted from the database (labels: archive_type).
	RecordsDeletedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "archiver_records_deleted_total",
		Help: "Total number of records deleted from the database after archiving",
	}, []string{"archive_type"})

	// DeletionDuration tracks how long each delete operation takes (labels: archive_type).
	DeletionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "archiver_deletion_duration_seconds",
		Help:    "Duration in seconds of database deletion operations",
		Buckets: archiveDurationBuckets,
	}, []string{"archive_type"})

	// BroadcastsDeletedTotal counts the total number of broadcasts deleted.
	BroadcastsDeletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "archiver_broadcasts_deleted_total",
		Help: "Total number of broadcasts deleted from the database",
	})
)

// ResetCycleMetrics zeroes all per-cycle gauges at the start of a new archiving cycle.
func ResetCycleMetrics() {
	CycleOrgsTotal.Set(0)
	CycleOrgsPending.Set(0)
	CycleOrgsUpToDate.Set(0)
	CycleOrgsCompleted.Set(0)
	CycleOrgsFailed.Set(0)
	OrgPendingStatus.Reset()
	OrgFailedStatus.Reset()
}
