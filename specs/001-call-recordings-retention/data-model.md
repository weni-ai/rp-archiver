# Data Model: Call recordings retention step

No database entity. The step reads S3 listings and keeps only per-run, in-memory state.

## Configuration (`archives.Config` additions)

| Field | Env var | Default (`NewConfig()`) | Validation |
|---|---|---|---|
| `CallRecordingsRetentionPeriod int` | `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD` | `0` | `< 0` → error; `1..89` → error; `0` → disabled; `≥ 90` → enabled |
| `CallRecordingsBucket string` | `ARCHIVER_CALL_RECORDINGS_BUCKET` | `""` | non-blank when enabled |
| `CallRecordingsRegion string` | `ARCHIVER_CALL_RECORDINGS_REGION` | `""` | non-blank when enabled |
| `CallRecordingsPrefix string` | `ARCHIVER_CALL_RECORDINGS_PREFIX` | `recordings/v1/` | when enabled: non-blank, ends with `/`, no leading `/`, no `..` segment |
| `CallRecordingsDryRun bool` | `ARCHIVER_CALL_RECORDINGS_DRY_RUN` | `false` | — |

## Named constants (`archives/call_recordings.go`)

| Constant | Value | Purpose |
|---|---|---|
| `MinCallRecordingsRetentionDays` | 90 | floor enforced in code (BD-004, NFR-002) |
| `callRecordingsListPageSize` | 1000 | `MaxKeys` per `ListObjectsV2` |
| `callRecordingsDeleteBatchSize` | 1000 | `DeleteObjects` maximum |
| `callRecordingsRetentionTimeout` | 1 h | step deadline |
| `callRecordingsS3MaxRetries` | 3 | SDK `MaxRetries` on the recordings client |
| `callRecordingsDryRunSampleSize` | 10 | oldest keys logged in dry run |

Per-request deadlines reuse `Config.CommonTimeout` (minutes).

## CallRecordingsSummary (returned by `DeleteExpiredCallRecordings`, logged once)

| Field | Meaning |
|---|---|
| `Scanned int64` | objects listed under the prefix |
| `Eligible int64` | objects older than the cutoff |
| `Deleted int64` | keys deleted (0 in dry run) |
| `DeletedBytes int64` | bytes of deleted keys (in dry run: bytes that would be deleted) |
| `DeleteErrors int64` | per-key errors + keys of failed batches |
| `Duration time.Duration` | wall time of the run |
| `DryRun bool` | mode |
| `Partial bool` | stopped by deadline or listing error |
| `OldestKeys []sampledKey` | dry run only, ≤ 10, key + `LastModified` |

## Run state

```text
start ──list page──▶ filter ──batch full──▶ flush ──▶ list next page ... ──no more pages──▶ final flush ──▶ summary
   │                                          │
   │                                          └─request error: count batch as errors, continue
   ├─list error: Partial=true ──────────────────────────────────────────────▶ summary
   └─deadline: Partial=true ────────────────────────────────────────────────▶ summary
```

Eligibility per object: `strings.HasPrefix(key, prefix) && LastModified.Before(now.UTC().AddDate(0, 0, -period))`.

## Scheduler state (`cmd/rp-archiver/retention.go`)

| Field | Purpose |
|---|---|
| `startedThisCycle bool` | set by `StartForCycle`, cleared by `NextCycle` |
| `wg sync.WaitGroup` | awaited by `Wait()` before `ExitOnCompletion` break |

Accessed only by the main loop goroutine, except `wg`, which is synchronized by design.
