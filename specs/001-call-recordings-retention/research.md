# Research: Call recordings retention step

Resolved against `main` at `ed8b25e` and the `aws-sdk-go` v1.13.47 sources (the version pinned in `go.mod`). No `NEEDS CLARIFICATION` remains.

## R1. S3 API surface in aws-sdk-go v1.13.47

- **Decision**: use `ListObjectsV2WithContext` (manual paging with `ContinuationToken`), `DeleteObjectsWithContext` (`Quiet: true`), and, for the startup check, a one-key `ListObjectsV2WithContext` under the prefix with a `CommonTimeout` deadline. The existing `TestS3` is not reused because it calls `HeadBucket` without a context (known gap, Principle VIII). `HeadBucket` is not used at all because it is authorized by `s3:ListBucket` but carries no `prefix`, so the prefix-conditioned grant requested from Cloud denies it.
- **Rationale**: all three exist in v1.13.47 (`service/s3/api.go`). Manual paging (instead of `ListObjectsV2PagesWithContext`) gives each page its own `CommonTimeout` deadline, as Principle VIII requires, and lets the loop stop cleanly on the step deadline between pages.
- **Alternatives considered**: `ListObjectsV2PagesWithContext` takes a single context for all pages, so a per-page deadline is not possible (rejected). Upgrading the SDK is out of scope (Principle V).

## R2. Retry behaviour of the v1.13.47 default retryer

- **Decision**: set `aws.Config.MaxRetries = aws.Int(callRecordingsS3MaxRetries)` (3) on the recordings session; no custom retry loop.
- **Facts verified**: `client.DefaultRetryer.ShouldRetry` returns true for HTTP ≥ 500 except 501, for `RequestError` / `RequestTimeout` / `ResponseTimeout` codes, and for throttling (`shouldThrottle`: throttle codes and HTTP 429/502/503/504). It returns false for 403 `AccessDenied` and 404 `NoSuchBucket`. Backoff is exponential with jitter starting at 30 ms, capped near five minutes; with three retries the worst case is a few seconds.
- **Rationale**: matches Principle IX exactly (transient retried, defects not, bound explicit from a named constant). `DeleteObjects` and `ListObjectsV2` are idempotent.
- **Alternatives considered**: a custom retry loop would duplicate the SDK's classification (rejected).

## R3. Recordings client configuration

- **Decision**: `NewCallRecordingsS3Client(config)` takes the session settings of `NewS3Client` (static credentials from `AWSAccessKeyID`/`AWSSecretAccessKey`, `S3Endpoint`, `S3DisableSSL`, `S3ForcePathStyle`) with `Region: CallRecordingsRegion` and `MaxRetries`, built by the unexported helper `callRecordingsAWSConfig(config)` so tests can assert it. It does **not** copy the debug `Send` handler: that handler logs `r.HTTPRequest.Header` after signing, so the `Authorization` header reaches the logs, and production runs at debug level (Principle XII; recorded as a known gap in the constitution). If a debug line is wanted, it logs only `service` and `operation`. `CheckCallRecordingsBucket(client, config)` runs `ListObjectsV2WithContext` (`Prefix`, `MaxKeys: 1`) with a `CommonTimeout` deadline (R1) and returns the error instead of calling `Fatal`; `main.go` decides to `Fatal`.
- **Rationale**: 013 prescribes "a second S3 client for the recordings region with the same credentials it already uses". Production already signs regional buckets through the global endpoint (`https://s3.amazonaws.com` + `sa-east-1`). Returning the error makes the startup path testable with the fake. The existing `NewS3Client` keeps its own `Fatal` (unchanged, Principle V).
- **Alternatives considered**: (a) Use the AWS default chain (IRSA) for the new client. The deployments do set `serviceAccountName: flows`, but this would make the "archiver identity" differ between clients, contradicting 013's "same credentials", so it was rejected. (b) Refactor `NewS3Client` to take a region parameter. That changes the archive client for no functional need (Principle V), so it was rejected.

## R4. Identity that deletes (correction to 013 Assumptions)

- **Decision**: document in the spec, and in the Cloud request, that the deleting identity is the IAM user whose keys are in `archiver-secret`.
- **Facts**: `NewS3Client` always calls `credentials.NewStaticCredentials(...)`; `archiver-secret` is mounted via `envFrom` on `archiver`, `archiver-orgs-inactives`, and `archiver-proj-inactives`. An explicit static provider means the SDK never consults the IRSA web-identity provider, even though the pods run as `flows`.
- **Consequence**: the two inactive-org deployments hold the same credentials; they never delete recordings because their period stays `0` and they never build the client (FR-029 by configuration).

## R5. Age filter and clock

- **Decision**: `cutoff := now.UTC().AddDate(0, 0, -period)`; eligible iff `obj.LastModified.Before(cutoff)`. `now` is a parameter of `DeleteExpiredCallRecordings` (the cycle passes `time.Now()`), as `ArchiveOrg` already does.
- **Rationale**: FR-025 says "strictly older". Passing `now` makes the 89/90/91-day scenario deterministic. `AddDate` on UTC avoids DST effects.
- **Edge**: an object exactly 90 days old (`LastModified == cutoff`) is kept (scenario 3.1).

## R6. Batching across pages

- **Decision**: accumulate eligible keys from any number of pages into a pending batch; flush when it reaches `callRecordingsDeleteBatchSize` (1,000) and once at the end.
- **Rationale**: produces batches of exactly 1,000 regardless of how eligible objects are spread across pages; with the scenario 3.6 layout (pages of 1,000, 1,000, and 500 expired) the batches are 1,000, 1,000, and 500. Deleting while listing is safe for `ListObjectsV2` with continuation tokens, because S3 continues after the last returned key.
- **Alternatives considered**: deleting each page's eligible keys immediately would give smaller and more numerous batches when pages are partly eligible (rejected). Collecting everything first would make memory unbounded (rejected).

## R7. Scheduling inside the existing loop

- **Decision**: `retentionScheduler` in `cmd/rp-archiver/retention.go` with:
  - `newRetentionScheduler(step func(ctx context.Context, cycleStart time.Time))` and `StartForCycle(cycleStart time.Time)`: starts `step` in a goroutine with a `CallRecordingsRetentionTimeout` context, unless it already started for this cycle or a previous run is still in flight, and tracks it in a `sync.WaitGroup`. The step receives the cycle start, which becomes the `now` of `DeleteExpiredCallRecordings` and the `cycle_start` log field.
  - `NextCycle()`: clears the flag before the loop sleeps.
  - `Wait()`: used before `break` when `ExitOnCompletion` is set.

  `main.go` calls `StartForCycle` right after `ResetCycleMetrics()` and before fetching orgs.
- **Rationale**: an org-fetch failure `continue`s to the top of the loop every five minutes; the flag keeps the step at once per cycle (FR-028). Starting before the org fetch means a database outage does not block retention. Waiting before `break` makes the one-shot rollout check meaningful.
- **Alternatives considered**: (a) Run the step synchronously before orgs. That delays org archival by up to the step timeout, so it was rejected. (b) A separate ticker goroutine. Its timing would drift away from `StartTime` and it would need its own shutdown, so it was rejected.

## R8. Metrics

- **Decision**: five `promauto` metrics in `archives/metrics.go` with doc comments, no labels:

  | Metric | Type | Updated |
  |---|---|---|
  | `archiver_call_recordings_scanned_total` | counter | per listed object under the prefix |
  | `archiver_call_recordings_deleted_total` | counter | per key deleted (not in dry run) |
  | `archiver_call_recordings_deleted_bytes_total` | counter | sum of sizes of deleted keys |
  | `archiver_call_recordings_delete_errors_total` | counter | per key error, or per key of a failed batch |
  | `archiver_call_recordings_last_run_timestamp_seconds` | gauge | Unix seconds at step end (complete or partial) |

- **Rationale**: these are the names in 013. Counters (not per-cycle gauges) are not reset by `ResetCycleMetrics()`, so rates work across cycles.

## R9. Dry-run sample

- **Decision**: keep the `callRecordingsDryRunSampleSize` (10) oldest eligible keys, using a bounded slice that replaces the newest entry when an older one arrives, and log them as `oldest_keys` with their `LastModified` dates.
- **Rationale**: FR-027 ("the oldest eligible keys") in bounded memory.

## R10. Fake S3 for tests

- **Decision**: `fakeRecordingsS3` in `call_recordings_test.go` embeds `s3iface.S3API` (nil) and implements `ListObjectsV2WithContext` (paging over a sorted in-memory map, with a configurable page size and injectable errors) and `DeleteObjectsWithContext` (removes keys and returns injectable per-key errors or a request error). The startup check goes through the same `ListObjectsV2WithContext`. Calling any other method panics, so unexpected calls show up in tests.
- **Rationale**: Principle XVIII (hermetic, through `s3iface.S3API`); the age filter, prefix guard, and batching run unmocked.
