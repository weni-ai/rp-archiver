# Engineering Specification: Call recordings retention step

**Repository**: rp-archiver — backend

**Feature Branch**: `001-call-recordings-retention`

**Created**: 2026-11-18

**Status**: Draft

**Cycle**: SDD Full (spec → plan → tasks → analyze → implement). Principle I recommends the full cycle for changes to a deletion path, a public contract, and cycle scheduling; this change touches all three.

## Inheritance from Product Spec

- Product Spec: Voice Mode Call Recording (Telephony) — https://github.com/weni-ai/vtex-cx-engine-specs/blob/40bc90263b2ae562db8dd3bc7d52a01514939967/specs/013-telephony-call-recording/spec.md
- Pinned version: `40bc90263b2ae562db8dd3bc7d52a01514939967`
- Architecture doc: none — the technical architecture lives in the optional "Technical Architecture & Technology Specifications" section of the pinned spec (same commit)
- Inherited binding decisions: BD-004 (at least 90 days; only `rp-archiver` deletes, only by age, period in `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD`, code refuses values below 90), BD-005 (contact deletion keeps recordings), BD-006 (Engine scope only), BD-010 (object key layout `recordings/v1/<project_uuid>/<channel_uuid>/<call_id>.wav`)
- Scope of this spec: deletion and cycle scheduling — a recordings retention step that runs once per archiver cycle, independently of message and run archival, and deletes objects under the recordings prefix whose upload time is older than the retention period
- Divergences: none

## Scope in This Repository

**This repository implements**:

- Five `ARCHIVER_CALL_RECORDINGS_*` configuration options with defaults in `archives.NewConfig()` and startup validation (FR-023, FR-030).
- A second S3 client for the recordings bucket region, with the same credentials as the archive client and an explicit SDK retry bound; `HeadBucket` on the recordings bucket at startup when the step is enabled (FR-030).
- The retention step: paginated listing under the prefix, strict age filter on `LastModified`, `DeleteObjects` in batches of at most 1,000 keys, per-key error counting, dry-run mode, step timeout (FR-022, FR-024 to FR-028).
- Scheduling inside the daily loop of `cmd/rp-archiver/main.go`: once per cycle, independent of org archival in both directions (FR-028).
- Metrics `archiver_call_recordings_scanned_total`, `archiver_call_recordings_deleted_total`, `archiver_call_recordings_deleted_bytes_total`, `archiver_call_recordings_delete_errors_total`, `archiver_call_recordings_last_run_timestamp_seconds`, and one summary log per cycle (FR-035).
- `README.md` configuration documentation and `WENI-CHANGELOG.md` entry.

**Out of scope here**:

- Recording, notice, and upload: `weni-webchat-socket` (spec `007-telephony-call-recording` in that repository).
- Bucket, IAM, encryption, versioning, lifecycle rule: Cloud team (CLOUDW-4787).
- Deployment configuration (`kubernetes-manifests-platform`, `weni-flows/archiver`): the `ARCHIVER_CALL_RECORDINGS_*` values on the main `archiver` deployment only. Handed to the Cloud team; listed in Rollout.
- Any change to message and run archival, rollup, record deletion, or org selection. The archive S3 client is not changed.
- Pre-existing behaviour of `ExitOnCompletion` for org workers (the loop exits after dispatching orgs without waiting for them). Not changed here (Principle V).

**Depends on**:

- Cloud (CLOUDW-4787): recordings bucket and region per environment; `s3:ListBucket` (prefix `recordings/`) and `s3:DeleteObject` (prefix `recordings/*`) granted to **the IAM user whose keys are in `archiver-secret`** (see Implementation Decisions); no other identity allowed to delete there. If the bucket is versioned, a noncurrent-version expiration rule is required, otherwise `DeleteObjects` only adds delete markers and the audio stays stored.
- Commit `c49e2e2` ("update context timeout for orgs fetch", today only on `update/1.6.0` and in production tag `1.6.0-b2`) reaching `main` before the production tag of this feature, so that the release does not regress production. It is a separate change, not part of this branch.
- Recordings exist only after the gateway side ships; this step is harmless before that (empty prefix).

## Current State

Surveyed on `main` at `ed8b25e`:

- `archives/config.go`: `Config` loaded by `ezconf` with prefix `archiver` (field `CallRecordingsRetentionPeriod` maps to `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD`). Defaults in `NewConfig()`. No recording options.
- `archives/s3.go` `NewS3Client`: one session with `credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecretAccessKey, "")`, `S3Endpoint`, `S3Region`, `S3DisableSSL`, `S3ForcePathStyle`; no explicit `MaxRetries`; `TestS3` runs `HeadBucket` and `logrus.Fatal` on failure. A debug `Send` handler logs operation and params. Only built when `UploadToS3` is true.
- `cmd/rp-archiver/main.go`: validation is inline (`KeepFiles && !UploadToS3`). The daily loop resets cycle metrics, fetches orgs with a one-minute context, and on error sleeps five minutes and `continue`s to the top of the loop. On success it dispatches each org to a goroutine bounded by `MaxConcurrentArchivation`, then `break`s if `ExitOnCompletion`, otherwise sleeps until `StartTime` the next UTC day. Org workers are not awaited before sleeping.
- `archives/metrics.go`: `promauto` metrics with the `archiver_` prefix and doc comments; `ResetCycleMetrics()` resets per-cycle gauges.
- Deployments (`kubernetes-manifests-platform/weni-flows/archiver`, `archiver-orgs-inactives`, `archiver-proj-inactives`): all three use `serviceAccountName: flows` and `envFrom` `archiver-secret`, which carries `ARCHIVER_AWS_ACCESS_KEY_ID` / `ARCHIVER_AWS_SECRET_ACCESS_KEY`. Production uses `ARCHIVER_S3_ENDPOINT=https://s3.amazonaws.com` with region `sa-east-1`; staging the same endpoint with `us-east-1`.
- No `specs/` directory before this one.

## Technical Approach

### Components Affected

| Component | Change | Why |
|-----------|--------|-----|
| `archives/config.go` | modified | Five `CallRecordings*` fields; defaults in `NewConfig()` (`0`, empty, empty, `recordings/v1/`, `false`) |
| `archives/call_recordings.go` | new | `ValidateCallRecordingsConfig`, `NewCallRecordingsS3Client`, `DeleteExpiredCallRecordings`, named constants |
| `archives/metrics.go` | modified | Five `archiver_call_recordings_*` metrics |
| `cmd/rp-archiver/main.go` | modified | Validation and client at startup; once-per-cycle scheduling of the step |
| `archives/call_recordings_test.go` | new | Flow and unit tests with a fake `s3iface.S3API` |
| `README.md` | modified | Configuration options |
| `WENI-CHANGELOG.md` | modified | `1.7.0` entry |

### Flow

#### Startup

1. `ValidateCallRecordingsConfig(config)` right after loading configuration:
   - `CallRecordingsRetentionPeriod < 0` → error.
   - `1 ≤ period < MinCallRecordingsRetentionDays` (90) → error naming the minimum (FR-023, NFR-002).
   - `period > 0`: `CallRecordingsBucket` and `CallRecordingsRegion` non-empty; `CallRecordingsPrefix` non-empty, ends with `/`, does not start with `/`, contains no `..` segment. An empty prefix is refused because it would turn the step into a bucket-wide deletion (FR-024).
   - `period == 0`: other fields are ignored.
   - Invalid → `logrus.Fatal`.
2. If `period > 0`: `NewCallRecordingsS3Client(config)` builds a session with the same static credentials, endpoint, SSL, and path-style settings as `NewS3Client`, region `CallRecordingsRegion`, `MaxRetries: callRecordingsS3MaxRetries`. `TestS3` (`HeadBucket`) on `CallRecordingsBucket`; failure → `logrus.Fatal` (FR-030). Built independently of `UploadToS3`.
3. If `period == 0`: no client, no request to the recordings bucket, ever (scenario 3.4).

#### Scheduling

- At the top of each daily cycle, before fetching orgs, the loop starts the step in its own goroutine with a context bounded by `callRecordingsRetentionTimeout` (FR-028). A failure or timeout of the step is logged and does not affect org archival. A failure to fetch orgs does not affect the step.
- The step runs **once per cycle**: a cycle-scoped flag set when the step starts prevents the five-minute org-fetch retry (`continue`) from starting it again; the flag resets when the loop moves to the next day.
- With `ExitOnCompletion`, the loop waits for the step (a `sync.WaitGroup` visible in `main.go`) before `break`, so a one-shot run (used in rollout to prove deletion) always completes the step.
- The step's own timeout (one hour) is far below the 24-hour cycle, so two runs never overlap.

#### Retention step: `DeleteExpiredCallRecordings(ctx, now, config, s3Client)`

1. `cutoff = now.UTC() − period days`.
2. List with `ListObjectsV2WithContext` (`Bucket`, `Prefix`, `MaxKeys: callRecordingsListPageSize` = 1,000), one page per call, each with a deadline of `CommonTimeout` minutes, following `NextContinuationToken`.
3. For each object: increment scanned. Skip if the key does not start with the prefix (defence against a misbehaving S3-compatible endpoint). Eligible only when `LastModified.Before(cutoff)` (strictly older, FR-025). Add eligible keys and sizes to the pending batch.
4. When the pending batch reaches `callRecordingsDeleteBatchSize` (1,000), and once more at the end, flush it:
   - Dry run: no request; add the batch to count and bytes, keep the `callRecordingsDryRunSampleSize` oldest keys for the summary (FR-027).
   - Otherwise: `DeleteObjectsWithContext` with `Quiet: true` and a `CommonTimeout` deadline. Keys in `Errors` are logged at warning level (key, code) and counted in `delete_errors_total`; all other keys in the batch count as deleted with their bytes. A request-level error (after the SDK retries) counts every key in the batch as an error, is logged at error level, and the step continues with the next page (FR-026).
5. On context deadline (step timeout) the step stops, logs a partial summary with `partial=true`, and returns. The next cycle re-lists what remains; deletion is idempotent.
6. At the end (complete or partial): set `last_run_timestamp_seconds` to the completion time and log one summary: `scanned`, `eligible`, `deleted`, `deleted_bytes`, `delete_errors`, `duration`, `dry_run`, `partial`, and in dry run the oldest sample keys (FR-035).

Memory is bounded: one listing page, one pending batch (≤ 1,000 keys), and the dry-run sample.

### Contracts

| Contract | Type | Change | SemVer impact |
|----------|------|--------|---------------|
| `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD` (default `0`; `1`–`89` refuse startup) | config | new | minor (additive, disabled by default) |
| `ARCHIVER_CALL_RECORDINGS_BUCKET` (required when period > 0) | config | new | minor |
| `ARCHIVER_CALL_RECORDINGS_REGION` (required when period > 0) | config | new | minor |
| `ARCHIVER_CALL_RECORDINGS_PREFIX` (default `recordings/v1/`) | config | new | minor |
| `ARCHIVER_CALL_RECORDINGS_DRY_RUN` (default `false`) | config | new | minor |
| `archiver_call_recordings_scanned_total`, `_deleted_total`, `_deleted_bytes_total`, `_delete_errors_total` (counters, no labels), `archiver_call_recordings_last_run_timestamp_seconds` (gauge, no labels) | Prometheus | new | minor |
| Recordings object layout (`recordings/v1/...`) | S3 layout owned by the gateway (BD-010) | consumed read-only by prefix and `LastModified`; no metadata read | none |

Archive file format, archive key layout, `archives_archive` rows, and existing metrics are unchanged.

## Data Model and Migrations

None. No database access; the step reads only S3 listings. No RapidPro schema dependency (Principle XXI not affected).

## Non-Functional Requirements

### Peak Load

- Message and run archival peaks (largest org records per day, archive backlog, largest archive, cycle duration) are unchanged by this spec.
- Recordings step, design peak:
  - Uploads: up to 50,000 recordings per day across the platform, so steady-state deletions are up to 50,000 objects per cycle (50 `DeleteObjects` calls).
  - Live objects under the prefix: up to 4.5 million (90 days × 50,000). The step lists the whole prefix every cycle because the key layout (`project/channel/call`) has no date segment to prune by, so that means 4,500 `ListObjectsV2` pages per cycle, about 15–25 minutes at 200–300 ms per page.
  - First production run after enabling (backlog): bounded by the same 4.5 million objects; deletions beyond what fits in the step timeout continue on the next cycle.
  - Step timeout `callRecordingsRetentionTimeout` = 1 hour; cycle window 24 hours.
- The step runs concurrently with org workers but shares no state with them: separate S3 client, no database connection, no use of `MaxConcurrentArchivation`.
- If `scanned_total` and the summary `duration` approach the timeout, the remedy is a dated key layout (`recordings/v2/`), which is a gateway contract change out of scope here.

### Observability

- Logs: structured `logrus` fields, `component=call_recordings_retention`. Per-key delete errors (warning): `key`, `error_code`. Request-level failures and startup failures (error, forwarded to Sentry by the existing hook): `bucket`, `prefix`, operation, wrapped error. Summary (info): see step 6.
- Keys may be logged: they contain only project, channel, and call UUIDs, never phone numbers (BD-010). No object metadata is read, so `did` / `caller-id` are never seen by the archiver.
- Identifiers (Principle XII): the step is not scoped to an org, so `org_id` does not exist for it and is not fabricated; the tenant identifier is the `project_uuid` segment of the key, and the correlation identifier is the object key plus the cycle start time (`cycle_start`).
- Metrics: the five Contracts metrics, no labels, so the `/metrics` exception of Principle XVII stays valid.

### Failure Handling

| Failure | Handling |
|---------|----------|
| Period 1–89, negative, or missing bucket/region/prefix with period > 0 | Refuse to start (`logrus.Fatal`) |
| Recordings bucket unreachable at startup (period > 0) | Refuse to start (FR-030) |
| Transient S3 error (connection, timeout, 5xx, throttling) | SDK retry up to `callRecordingsS3MaxRetries` with SDK exponential backoff |
| 403 / 404 / other request defect | Not retried by the SDK; logged at error level; listing failure ends the step for this cycle, delete failure counts the batch as errors and continues |
| Per-key errors in `DeleteObjects` | Counted and logged; other keys and batches continue |
| Step exceeds its timeout | Stops, partial summary, next cycle continues |
| Process restart mid-step | Next cycle re-lists; deletion idempotent |
| Org archival fails or org fetch retries | Step unaffected; runs once per cycle |

Every S3 call carries a context deadline (`CommonTimeout` per page or batch, inside the step deadline). No `panic` or `Fatal` inside the cycle.

### Security and Authorization

- No new exposed endpoint; `/metrics` gains label-free counters only.
- Credentials: the existing `ARCHIVER_AWS_*` static keys (no new secret). Least privilege for the recordings bucket, requested from Cloud: `s3:ListBucket` restricted to `recordings/` and `s3:DeleteObject` on `recordings/*`. No `GetObject` or `PutObject`.
- Only the main `archiver` deployment sets a period above 0 (FR-029). `archiver-orgs-inactives` and `archiver-proj-inactives` keep the default `0`, so they never build the client.
- Keys to delete come only from the S3 listing of the validated prefix, re-checked against the prefix before deletion; no external value is interpolated into a key.

## Test Strategy

| Area | Approach |
|------|----------|
| Flow coverage | `DeleteExpiredCallRecordings` against a fake `s3iface.S3API` holding an in-memory object set: objects at 89, 90, 91 days → only 91 deleted (scenario 3.1); expired object outside the prefix untouched (3.2); 2,500 expired across three pages → batches of 1,000, 1,000, 500 (3.6); `DeleteObjects` returns 2 key errors → others deleted, `delete_errors_total` += 2, next batch runs (3.7); dry run → no `DeleteObjects` call, summary count, bytes, oldest keys (3.8) |
| Failure paths | Listing error mid-way → step returns error, earlier deletions stand, metrics consistent; request-level `DeleteObjects` error → batch counted as errors, next page processed; context deadline → partial summary, no further requests |
| Config | Period 0 with empty fields → valid, no client built and no request to the fake (3.4); 1, 89 → error; 90 → valid (3.3); period > 0 without bucket, region, or with empty / leading-slash prefix → error (3.5); negative → error |
| Startup | `TestS3` on the recordings bucket failing → startup error path exercised through the function that builds and checks the client |
| Scheduling | The once-per-cycle guard and the `ExitOnCompletion` wait are extracted into a small function in `cmd/rp-archiver` and tested with a fake step: org-fetch retry does not run the step twice; step failure does not stop org dispatch (3.9) |
| Metrics | Counter and gauge deltas asserted with `testutil` |
| Stubbed at the boundary | S3 only, through the fake `s3iface.S3API` (Principle XVIII). No live S3, no AWS credentials. The age filter, prefix check, and batching are the subject and are never mocked |
| Integration (manual, staging) | One-shot run with `ARCHIVER_EXIT_ON_COMPLETION=true` against a test prefix containing objects older than 90 days, as the inherited rollout prescribes |

## Rollout and Rollback

- Release: `1.7.0` (minor: additive configuration and metrics; step disabled by default). Production tag on `main` after `c49e2e2` reaches `main`.
- Deployment changes, handed to Cloud (`kubernetes-manifests-platform`, `weni-flows/archiver` only):
  - `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD` (≥ 90; production value is a product decision), `ARCHIVER_CALL_RECORDINGS_BUCKET`, `ARCHIVER_CALL_RECORDINGS_REGION`, `ARCHIVER_CALL_RECORDINGS_DRY_RUN=true` for the first cycle; `ARCHIVER_CALL_RECORDINGS_PREFIX` only if different from `recordings/v1/`.
  - Nothing on `archiver-orgs-inactives` or `archiver-proj-inactives`.
- Order: ship with period `0` (no behaviour change) → staging, after the bucket exists: enable with dry run, check the summary log, then disable dry run; prove deletion with a one-shot run on a prefix holding disposable objects older than 90 days, with `ARCHIVER_ARCHIVE_MESSAGES=false` and `ARCHIVER_ARCHIVE_RUNS=false` in that pod so it never archives or deletes records alongside the main deployment (Principle XI) → production: enable with dry run until the first objects pass the retention period, then disable dry run.
- Rollback: `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD=0` stops deletion without a deploy. Raising the period keeps remaining objects longer; deleted objects do not come back. Previous image ignores the new variables.

## Constitution Alignment

| Principle | How this spec complies | Exception + justification |
|-----------|------------------------|---------------------------|
| I. Specification Traceability | Inherits 013 pinned by commit; scope names the slices (deletion, cycle scheduling); SDD Full | `vtex-cx-engine-specs` has no tags; pinned to the immutable merge commit of PR #15 on `main` |
| II. No Silent Divergence | No divergence. The identity correction below contradicts no requirement, scope item, or binding decision of 013 and is recorded as an implementation decision | none |
| III. Version Control and Review | PR to `main`; production tag on `main` | none |
| IV. Commit Messages | Conventional commits | none |
| V. Contained Changes | No change to archival, rollup, record deletion, `ExitOnCompletion` for orgs, or the archive client. `c49e2e2` goes to `main` in its own change | none |
| VI. Security and Secrets | No new secret; placeholders untouched; least-privilege IAM documented for Cloud | The existing static-key IAM user gains list/delete on the recordings prefix, which also covers the two inactive-org deployments sharing `archiver-secret`; contained by configuration (period 0 there). Isolating identities is Cloud's call (inherited Assumptions) |
| VII. Never Trust the Client | Config validated at startup with cross-field rules; prefix validated before the first request; listed keys re-checked against the prefix | none |
| VIII. Fail Gracefully and Predictably | Context deadlines on every S3 call; step failure contained; `Fatal` only at startup | none |
| IX. Bounded Retry Over REST | `MaxRetries` set explicitly on the new client from `callRecordingsS3MaxRetries`; no retry on 4xx; `DeleteObjects` is idempotent | Exhausted listing or delete failures are recovered by the next cycle re-listing, the equivalent of "period stays missing" |
| X. Verified Archive Before Deletion | Not applicable to RapidPro records (none touched). Same spirit for this new deletion path: floor of 90 days enforced in code, strict age comparison, prefix guard, dry run, abort-case tests | none |
| XI. Scalability and Peak Load | Peak declared for the step; single instance unchanged; no state outlives the step | none |
| XII. Observability and Diagnosable Errors | Structured logs, summary per cycle, error logs to Sentry with bucket/prefix/operation; no PII (keys hold UUIDs only) | `org_id` does not exist for this step; tenant identifier is the `project_uuid` key segment, correlation is key + `cycle_start` |
| XIII. Versioned Contracts | New config and metrics are additive, minor `1.7.0` | none |
| XIV. Changelog Maintenance | `WENI-CHANGELOG.md` `1.7.0` entry; `CHANGELOG.md` untouched | none |
| XV. Tests Exercise Flows | Flow tests through `DeleteExpiredCallRecordings`, success, failure, and refusal (outside prefix, below cutoff) paths | The step has no PostgreSQL persistence layer, so its flow tests run against the S3 fake only |
| XVI. Explicit Over Clever | Goroutine, `WaitGroup`, and once-per-cycle flag visible in `main.go`; all limits named constants | none |
| XVII. Explicit, Scoped Authorization | No endpoint added; `/metrics` exception holds (label-free counters, no personal data) | none |
| XVIII. Verification Integrity | Fake `s3iface.S3API`; age filter, prefix check, batching never mocked | none |
| XIX. Proportional Design | One new file in `archives`; defaults only in `NewConfig()`; no new abstraction beyond `s3iface.S3API` | none |
| XX. Code Clarity and Language | English | none |
| XXI. Backward-Compatible Data Evolution | No schema dependency | none |

## Implementation Decisions

- **Archiver identity for deletion (correction to the inherited Assumptions).** 013 says "Today the archiver uses the `flows` service account". The deployment does set `serviceAccountName: flows`, but `NewS3Client` always builds `credentials.NewStaticCredentials(AWSAccessKeyID, AWSSecretAccessKey, "")` from `archiver-secret`, so S3 requests are signed by that **IAM user**, not by the `flows` IRSA role. The recordings client reuses the same credentials (as 013 prescribes), so the "archiver identity" of NFR-004 is that IAM user, and Cloud must grant list and delete on `recordings/` to it. This contradicts no requirement or binding decision, so it is not a divergence (Principle II); the inherited text should be corrected in a later revision of 013.
- **Same endpoint settings as the archive client** (`S3Endpoint`, `S3DisableSSL`, `S3ForcePathStyle`), different region: 013 requires only a second region; production already uses the global endpoint with a regional signer.
- **Upload time from `LastModified`**, no metadata read (`HeadObject` per object would multiply requests by the object count and expose `did` / `caller-id` to the archiver).
- **Request-level delete failure counts the batch as errors and continues**, rather than ending the step: per-key semantics of FR-026 extended to the whole batch.
- **Listing failure ends the step for this cycle**: without a continuation token the listing cannot resume safely; the next cycle re-lists.
- **Initial constant values** (final values and rationale in the plan): `MinCallRecordingsRetentionDays` 90, `callRecordingsListPageSize` 1,000, `callRecordingsDeleteBatchSize` 1,000 (S3 maximum), `callRecordingsRetentionTimeout` 1 h, `callRecordingsS3MaxRetries` 3, `callRecordingsDryRunSampleSize` 10.

## Divergences

None.

## Open Questions

None. Decisions 2026-11-18: SDD Full; branch from `main`; deployment changes handed to Cloud with the variable list at the end of development; the identity correction is recorded only in this spec.
