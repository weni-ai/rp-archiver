---

description: "Task list for the call recordings retention step"
---

# Tasks: Call recordings retention step

**Input**: Design documents from `specs/001-call-recordings-retention/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md)

**Tests**: Required. Principle XV requires flow tests with failure and refusal paths. Principle X's spirit applies to this new deletion path: the abort cases are tested. Principle XVIII requires a hermetic S3 fake. Tests come first and must fail.

**Organization**: one story, US3 (Journey 3 of spec 013, "Recordings are deleted after the retention period", P2). Journeys 1 and 2 belong to `weni-webchat-socket`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on an unfinished task)
- **[Story]**: US3
- File paths are exact, per `plan.md` Project Structure

---

## Phase 1: Setup (Shared Infrastructure)

- [ ] T001 Add the fields `CallRecordingsRetentionPeriod int`, `CallRecordingsBucket string`, `CallRecordingsRegion string`, `CallRecordingsPrefix string`, and `CallRecordingsDryRun bool` to `Config` in `archives/config.go`, each with a `help` tag. Set the defaults in `NewConfig()`: `0`, `""`, `""`, `"recordings/v1/"`, `false`. These are the only source of these defaults (Principle XIX).

**Checkpoint**: `go build ./...` passes; `ezconf` exposes `ARCHIVER_CALL_RECORDINGS_*`.

---

## Phase 2: Foundational (Blocking Prerequisites)

- [ ] T002 [P] Write `fakeRecordingsS3` in `archives/call_recordings_test.go` (research R10):
  - It embeds `s3iface.S3API` (nil).
  - It implements `ListObjectsV2WithContext`: sorted in-memory objects with key, `LastModified`, and size; configurable page size; `ContinuationToken` handling; an injectable error on the Nth call.
  - It implements `DeleteObjectsWithContext`: removes keys, records the batch sizes, supports injectable per-key errors and an injectable request error, and fails the test if a batch exceeds 1,000 keys.
  - It implements `HeadBucket` and `HeadBucketWithContext` with an injectable error.
  - Any other method panics.
- [ ] T003 [P] Write `TestValidateCallRecordingsConfig` in `archives/call_recordings_test.go` as a table test:
  - Period `0` with empty fields is valid.
  - Periods `-1`, `1`, and `89` are errors, and the message names the minimum 90.
  - Period `90` with a bucket, region, and the default prefix is valid.
  - With period `90`, each of these is an error: blank bucket, blank region, empty prefix, prefix without a trailing `/`, prefix with a leading `/`, prefix containing `..`.
- [ ] T004 Implement `archives/call_recordings.go`:
  - Constants: `MinCallRecordingsRetentionDays = 90`, `callRecordingsListPageSize = 1000`, `callRecordingsDeleteBatchSize = 1000`, `callRecordingsRetentionTimeout = time.Hour`, `callRecordingsS3MaxRetries = 3`, `callRecordingsDryRunSampleSize = 10`.
  - `ValidateCallRecordingsConfig(config *Config) error`, per data-model.md. A negative period, or 1..89, is an error. With a period ≥ 90 the bucket and region are non-blank, and the prefix ends with `/`, has no leading `/`, and has no `..` segment.
  - Depends on T001 and T003.
- [ ] T005 Add five `promauto` metrics with doc comments to `archives/metrics.go`, with no labels:
  - counters `archiver_call_recordings_scanned_total`, `archiver_call_recordings_deleted_total`, `archiver_call_recordings_deleted_bytes_total`, `archiver_call_recordings_delete_errors_total`;
  - gauge `archiver_call_recordings_last_run_timestamp_seconds`.
  - They are not reset by `ResetCycleMetrics()`.

**Checkpoint**: config validation green; fake and metrics compile.

---

## Phase 3: User Story 3 — Recordings deleted after the retention period (Journey 3, Priority: P2) 🎯 MVP

**Goal**: once per cycle, delete objects under the recordings prefix whose upload time is strictly older than the period. The step must never go below 90 days, never touch keys outside the prefix, and run independently of org archival. It supports dry run and reports metrics and one summary.

**Independent Test**: run `go test -p=1 ./archives/... ./cmd/rp-archiver/...` with the fake S3. All Journey 3 scenarios in `quickstart.md` §1 pass without AWS credentials.

### Tests for User Story 3 ⚠️ (write first, must fail)

- [ ] T006 [P] [US3] Write the retention flow tests in `archives/call_recordings_test.go` against `DeleteExpiredCallRecordings(ctx, now, config, fake)`:
  - `TestDeleteExpiredCallRecordings_StrictCutoff`: objects at 89, 90, and 91 days; only the 91-day one is deleted (3.1).
  - `TestDeleteExpiredCallRecordings_IgnoresOutsidePrefix`: an expired object outside the prefix stays; the fake returns a stray key under the listing and it is not deleted (3.2).
  - `TestDeleteExpiredCallRecordings_BatchesOf1000`: 2,500 expired objects over pages of 1,000, 1,000, and 500 give batches of 1,000, 1,000, and 500 (3.6).
  - `TestDeleteExpiredCallRecordings_PerKeyErrors`: with 2 per-key errors, the others are deleted, `delete_errors_total` goes up by 2, and the next batch runs (3.7).
  - `TestDeleteExpiredCallRecordings_DryRun`: zero `DeleteObjects` calls; the summary has `Eligible`, `DeletedBytes` (the would-be bytes), and the 10 oldest keys in order (3.8).
  - Assert the metric deltas with `prometheus/testutil` in each test.
- [ ] T007 [P] [US3] Write the failure-path tests in `archives/call_recordings_test.go`:
  - `TestDeleteExpiredCallRecordings_ListError`: a listing error on page 2 gives `Partial=true`, keeps page-1 deletions, and makes no further requests.
  - `TestDeleteExpiredCallRecordings_BatchRequestError`: the whole batch is counted in `delete_errors_total`, and the next page is processed.
  - `TestDeleteExpiredCallRecordings_Deadline`: a context expired between pages gives `Partial=true` with no request after the deadline, and `last_run_timestamp_seconds` is still set.
  - `TestDeleteExpiredCallRecordings_BoundedMemory`: 100,000 objects; the pending batch never exceeds 1,000 and the sample never exceeds 10, checked by fake-side assertions.
- [ ] T008 [P] [US3] Write `TestNewCallRecordingsS3Client_BucketUnreachable` and `TestNewCallRecordingsS3Client_Config` in `archives/call_recordings_test.go`:
  - A `HeadBucket` error is returned (not `Fatal`) through the exported check function `CheckCallRecordingsBucket(client, config)`.
  - The built `aws.Config` has `Region == CallRecordingsRegion`, `MaxRetries == 3`, and the same endpoint and SSL/path-style settings as the archive client. Expose an unexported `callRecordingsAWSConfig(config)` helper for this assertion.
- [ ] T009 [P] [US3] Write `cmd/rp-archiver/retention_test.go` with a fake step function:
  - `TestRetentionScheduler_OncePerCycle`: two `StartForCycle` calls in the same cycle (the org-fetch retry path) run the step once; after `NextCycle` it runs again.
  - `TestRetentionScheduler_IndependentOfOrgs`: a step that returns an error does not affect the caller's continuation; `Wait` returns after the step ends.
  - `TestRetentionScheduler_DisabledNeverRuns`: a nil step (period 0) never starts a goroutine (3.4).
  - `TestRetentionScheduler_Timeout`: the context passed to the step has a deadline of `callRecordingsRetentionTimeout`.

### Implementation for User Story 3

- [ ] T010 [US3] Implement `DeleteExpiredCallRecordings(ctx context.Context, now time.Time, config *Config, s3Client s3iface.S3API) (CallRecordingsSummary, error)` in `archives/call_recordings.go`:
  - `cutoff = now.UTC().AddDate(0, 0, -period)`; eligible iff `strings.HasPrefix(key, prefix) && LastModified.Before(cutoff)`.
  - Manual paging with `ListObjectsV2WithContext` and `MaxKeys = callRecordingsListPageSize`; each page gets its own `context.WithTimeout(ctx, CommonTimeout minutes)`.
  - The pending batch is flushed at `callRecordingsDeleteBatchSize` and at the end. Deletion uses `DeleteObjectsWithContext` with `Quiet: true` and a per-batch `CommonTimeout`.
  - Per-key errors are logged at warn level (`key`, `error_code`, `error_message`). A request error is logged at error level and counts the whole batch, and the step continues.
  - A listing error, or `ctx.Err()`, gives `Partial=true` and stops.
  - Dry run keeps a bounded oldest-10 sample (research R9) and issues no delete.
  - Metrics are updated as it goes, and `last_run_timestamp_seconds` is set at the end.
  - Log fields: `component=call_recordings_retention`, `cycle_start` (from `now`), `bucket`, `prefix`, `dry_run`.
  - One summary log, `call recordings retention completed`, with all fields from contracts.
  - Errors are wrapped with `errors.Wrap`.
  - Depends on T002, T004, T005, T006, and T007.
- [ ] T011 [US3] Implement `NewCallRecordingsS3Client(config *Config) (s3iface.S3API, error)` and `CheckCallRecordingsBucket(client s3iface.S3API, config *Config) error` (which reuses `TestS3`) in `archives/call_recordings.go`:
  - Static credentials from `AWSAccessKeyID`/`AWSSecretAccessKey`, plus `S3Endpoint`, `S3DisableSSL`, `S3ForcePathStyle`, `Region: CallRecordingsRegion`, and `MaxRetries: aws.Int(callRecordingsS3MaxRetries)` (research R3).
  - The same debug `Send` handler as `NewS3Client`.
  - No `Fatal` inside the function.
  - `NewS3Client` is unchanged.
  - Depends on T008.
- [ ] T012 [US3] Implement `retentionScheduler` in `cmd/rp-archiver/retention.go`:
  - `startedThisCycle bool` and `wg sync.WaitGroup`.
  - `StartForCycle(cycleStart time.Time, step func(ctx context.Context))` creates a `context.WithTimeout(context.Background(), archives.CallRecordingsRetentionTimeout)` and starts one goroutine. Export the constant, or pass it in, so the constant has a single source.
  - `NextCycle()` and `Wait()`.
  - It does nothing when the step is nil.
  - Depends on T009.
- [ ] T013 [US3] Wire `cmd/rp-archiver/main.go`:
  - After loading config, `ValidateCallRecordingsConfig`, with `logrus.Fatal` on error.
  - If the period is > 0: `NewCallRecordingsS3Client` + `CheckCallRecordingsBucket`, with `logrus.WithError(err).Fatal("call recordings bucket not reachable")` on failure, and the startup log `call recordings retention enabled` with `retention_days`, `bucket`, `prefix`, `dry_run`. This is independent of `UploadToS3`.
  - In the loop, right after `archives.ResetCycleMetrics()`, call `scheduler.StartForCycle(start, step)`, where `step` calls `DeleteExpiredCallRecordings(ctx, time.Now(), config, recordingsClient)` and logs a returned error at error level.
  - Call `scheduler.Wait()` before `break` when `ExitOnCompletion` is set, and `scheduler.NextCycle()` before the daily sleep.
  - Do not touch the org dispatch, org fetch, or the archive client (Principle V).
  - Depends on T010, T011, and T012.
- [ ] T014 [US3] Make T006–T009 pass and run `go test -p=1 ./...`.

**Checkpoint**: Journey 3 scenarios 3.1–3.9 pass hermetically. Period 0 behaves exactly as before.

---

## Phase 4: Polish & Cross-Cutting Concerns

- [ ] T015 [P] Document the five `ARCHIVER_CALL_RECORDINGS_*` options in `README.md`, using the semantics from `contracts/configuration-and-metrics.md`:
  - Set them only on the main deployment.
  - Run the first cycle in dry run.
  - Request the IAM permissions for the `archiver-secret` IAM user.
  - If the bucket is versioned, it needs a noncurrent-version expiration rule.
- [ ] T016 [P] Add the `1.7.0` entry to the top of `WENI-CHANGELOG.md` (Added: call recordings retention step, `ARCHIVER_CALL_RECORDINGS_*` config, `archiver_call_recordings_*` metrics). Leave `CHANGELOG.md` untouched (Principle XIV).
- [ ] T017 Run `gofmt -l .`, `go vet ./...`, and `go test -p=1 -cover ./archives/...`. `call_recordings.go` coverage must be ≥ 80%.
- [ ] T018 Release coordination (not code): open a separate PR bringing `c49e2e2` ("update context timeout for orgs fetch") to `main` before the `1.7.0` production tag (Principles III, V).
- [ ] T019 Infrastructure follow-up for Cloud (not code), written in the PR description:
  - On `weni-flows/archiver` only: `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD` (≥ 90; the production value is a product decision), `_BUCKET`, `_REGION`, and `_DRY_RUN=true` for the first cycle; `_PREFIX` only if non-default.
  - Nothing on `archiver-orgs-inactives` or `archiver-proj-inactives`.
  - IAM `s3:ListBucket` (prefix `recordings/`) and `s3:DeleteObject` (`recordings/*`) for the `archiver-secret` IAM user.
  - The bucket versioning policy.
  - Cross-reference CLOUDW-4787.
- [ ] T020 Open a PR to `main` with runtime impact (none with period 0), contract impact (additive), rollback (period 0), and manual coordination (T018, T019).
- [ ] T021 Staging validation per `quickstart.md` §2. The one-off pod sets `ARCHIVER_ARCHIVE_MESSAGES=false` and `ARCHIVER_ARCHIVE_RUNS=false`. This is a release gate.

---

## Dependencies & Execution Order

- **Setup (T001)** → **Foundational (T002–T005)** → **US3 tests (T006–T009)** → **US3 implementation (T010–T014)** → **Polish (T015–T021)**.
- T010 depends on T002, T004, T005, T006, and T007. T011 depends on T008. T012 depends on T009. T013 depends on T010, T011, and T012.
- T018 and T019 can be drafted at any time. T021 needs the Cloud bucket.

### Parallel Opportunities

- T002 and T003 run in parallel. T005 runs in parallel with T004.
- T006, T007, T008, and T009 run in parallel (T006, T007, and T008 share a file, so append in separate test functions and coordinate edits; T009 is in a separate file).
- T015 and T016 run in parallel.

---

## Parallel Example: User Story 3

```bash
Task: "T006 retention flow tests in archives/call_recordings_test.go"
Task: "T009 scheduler tests in cmd/rp-archiver/retention_test.go"
```

---

## Implementation Strategy

1. T001–T005: config, validation, fake, metrics.
2. US3 tests, then the step, client, scheduler, and wiring. Stop and validate with `quickstart.md` §1.
3. Polish, then the PR. `c49e2e2` goes to `main` separately. Then `1.7.0-staging`, staging dry run and the deletion proof, then the production tag, then production dry run.

---

## Notes

- Commits use Conventional Commits of at most 50 characters, for example `feat: add call recordings retention config` and `test: cover call recordings retention`.
- Never read object metadata; keys only, because they hold no phone numbers.
- Never log credentials. The debug `Send` handler already exists. Do not add header logging.
