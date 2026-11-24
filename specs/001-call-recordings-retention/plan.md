# Implementation Plan: Call recordings retention step

**Branch**: `001-call-recordings-retention` | **Date**: 2026-11-18 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-call-recordings-retention/spec.md`

## Summary

Add a recordings retention step to the archiver's daily cycle. When `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD` is at least 90, the archiver builds a second S3 client for the recordings bucket region (same static credentials as today, explicit retry bound), checks the bucket at startup, and once per cycle lists the recordings prefix, selects objects whose `LastModified` is strictly older than the period, and deletes them in batches of at most 1,000, or only logs them in dry run. The step runs in its own goroutine with its own timeout, independent of org archival, and reports five metrics and one summary log. Period `0` (the default) means no client, no request, and no change in behaviour.

## Technical Context

**Language/Version**: Go 1.23.0 (`go.mod`)

**Primary Dependencies**: `github.com/aws/aws-sdk-go` v1.13.47 (`ListObjectsV2WithContext`, `DeleteObjectsWithContext`, `HeadBucketWithContext`, `aws.Config.MaxRetries` all available), `github.com/nyaruka/ezconf`, `github.com/sirupsen/logrus`, `github.com/prometheus/client_golang` (`promauto`), `github.com/stretchr/testify`. No new module.

**Storage**: S3 recordings bucket (read listing, delete). No PostgreSQL access in this step.

**Testing**: `go test -p=1 ./...`; fake implementing `s3iface.S3API` (embedded interface, overriding the three methods used); `prometheus/testutil` for metric deltas

**Target Platform**: Linux container (`weni-flows/archiver`), single instance per RapidPro database

**Project Type**: scheduled batch service

**Performance Goals**: up to 4,500 listing pages and 50 delete batches per cycle within a 1-hour step timeout

**Constraints**: never delete below 90 days; never touch keys outside the prefix; never block or fail org archival; bounded memory (one page + one batch)

**Scale/Scope**: up to 4.5 million live objects under the prefix; up to 50,000 deletions per steady-state cycle

## Constitution Check

*GATE: evaluated before Phase 0 and re-checked after Phase 1. Constitution 2.0.0.*

| Principle | Pre-design | Post-design |
|-----------|------------|-------------|
| I. Specification Traceability | PASS with justified exception: pinned by commit, no tags in the product repository; slices named | PASS |
| II. No Silent Divergence | PASS: none; identity correction is an implementation decision | PASS |
| III. Version Control and Review | PASS | PASS |
| IV. Commit Messages | PASS | PASS |
| V. Contained Changes | PASS: archive client, org flow, `ExitOnCompletion` for orgs untouched | PASS: `main.go` diff limited to startup block, cycle-top trigger, and the exit wait |
| VI. Security and Secrets | PASS with justified exception: shared static-key IAM user | PASS: no new secret, defaults stay placeholders/empty |
| VII. Never Trust the Client | PASS: startup validation, prefix guard on listed keys | PASS: `ValidateCallRecordingsConfig` table-tested |
| VIII. Fail Gracefully and Predictably | PASS: deadlines on every S3 call; `Fatal` only at startup | PASS: `CommonTimeout` per page or batch inside the step deadline |
| IX. Bounded Retry Over REST | PASS: `MaxRetries` explicit; 4xx not retried by SDK v1.13.47 | PASS (research R2) |
| X. Verified Archive Before Deletion | N/A to RapidPro records; equivalent guards planned | PASS: floor, strict cutoff, prefix guard, dry run, abort tests |
| XI. Scalability and Peak Load | PASS: peak declared; no state outlives the step | PASS |
| XII. Observability and Diagnosable Errors | PASS with justified exception: no `org_id`; key + `cycle_start` | PASS |
| XIII. Versioned Contracts | PASS: additive config and metrics, `1.7.0` | PASS (`contracts/`) |
| XIV. Changelog Maintenance | PASS: `WENI-CHANGELOG.md` | PASS |
| XV. Tests Exercise Flows | PASS with justified exception: no PostgreSQL layer in this flow | PASS (`quickstart.md`) |
| XVI. Explicit Over Clever | PASS: goroutine, `WaitGroup`, cycle flag visible in `main.go`; named constants | PASS |
| XVII. Explicit, Scoped Authorization | PASS: no endpoint; label-free metrics | PASS |
| XVIII. Verification Integrity | PASS: fake `s3iface.S3API`; subject never mocked | PASS |
| XIX. Proportional Design | PASS: one new file; defaults only in `NewConfig()` | PASS: no new interface; scheduling helper is a plain function |
| XX. Code Clarity and Language | PASS | PASS |
| XXI. Backward-Compatible Data Evolution | PASS: no schema dependency | PASS |

No unjustified violation.

## Project Structure

### Documentation (this feature)

```text
specs/001-call-recordings-retention/
├── spec.md
├── plan.md              # this file
├── research.md          # Phase 0
├── data-model.md        # Phase 1
├── quickstart.md        # Phase 1
├── contracts/
│   └── configuration-and-metrics.md
├── checklists/requirements.md
└── tasks.md             # /speckit.tasks (not created here)
```

### Source Code (repository root)

```text
archives/
├── config.go                    # + five CallRecordings* fields and defaults
├── call_recordings.go           # new: constants, ValidateCallRecordingsConfig,
│                                #      NewCallRecordingsS3Client, DeleteExpiredCallRecordings,
│                                #      CallRecordingsSummary
├── call_recordings_test.go      # new: flow + unit tests, fake s3iface.S3API
└── metrics.go                   # + five archiver_call_recordings_* metrics

cmd/rp-archiver/
├── main.go                      # startup validation + client; cycle trigger; exit wait
├── retention.go                 # new: retentionScheduler (once-per-cycle flag, WaitGroup)
└── retention_test.go            # new: scheduling tests with a fake step

README.md                        # configuration options
WENI-CHANGELOG.md                # 1.7.0 entry
```

**Structure Decision**: domain logic in package `archives` (single new file), wiring in `cmd/rp-archiver`. The scheduling guard lives in a small file next to `main.go` so it can be tested without running the infinite loop; it is a struct with two methods, not an abstraction layer.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Product repository pinned by commit (I) | `vtex-cx-engine-specs` has no tags | Blocking on a tagging convention stalls both repositories |
| Shared static-key identity gains delete on recordings (VI) | 013 prescribes the same credentials; the archiver has no IRSA path | Adding IRSA to `NewS3Client` changes the archive client (Principle V) and needs a Cloud IAM change for archives too |
| No `org_id` in step logs (XII) | Step is not org-scoped | Fabricating an identifier is forbidden by XII |
| Flow tests without PostgreSQL (XV) | The step has no database layer | A database fixture would test nothing in this flow |

## Inherited Context

- Product Spec: Voice Mode Call Recording (Telephony) — https://github.com/weni-ai/vtex-cx-engine-specs/blob/40bc90263b2ae562db8dd3bc7d52a01514939967/specs/013-telephony-call-recording/spec.md
- Pinned version: `40bc90263b2ae562db8dd3bc7d52a01514939967`
- Architecture doc: none (the technical architecture lives in the optional section of the pinned spec, same commit)
- Engineering Spec: `specs/001-call-recordings-retention/spec.md`

## Delivery Sequence

1. **Config fields, defaults, validation** (`config.go`, `ValidateCallRecordingsConfig`): everything else reads them; scenarios 3.3–3.5 are pure validation.
2. **Retention step** (`DeleteExpiredCallRecordings`, constants, summary): the deletion logic and its abort tests (3.1, 3.2, 3.6–3.8) against the fake, before any wiring.
3. **Metrics** (`metrics.go`): incremented by step 2; asserted in its tests.
4. **Client and startup** (`NewCallRecordingsS3Client`, `TestS3` reuse): FR-030 path.
5. **Scheduling** (`cmd/rp-archiver/retention.go`, `main.go`): once per cycle, independent of orgs, wait on `ExitOnCompletion` (3.9).
6. **Docs and changelog** (`README.md`, `WENI-CHANGELOG.md` `1.7.0`).
7. **Release**: PR to `main`; `c49e2e2` lands on `main` in its own PR before the `1.7.0` production tag; `-staging` tag for staging validation.

**Cross-repository coordination**: none blocks the code. `weni-webchat-socket` `007-telephony-call-recording` produces the objects; until it ships the prefix is empty and the step is a no-op. Before enabling: Cloud bucket, list/delete IAM for the `archiver-secret` IAM user, versioning policy (CLOUDW-4787); deployment env vars on `weni-flows/archiver` only.

## Peak Load Plan

- Declared peak: up to 4.5 million live objects (4,500 listing pages) and 50,000 deletions (50 batches) per cycle; 1-hour step timeout; 24-hour window (spec Peak Load).
- How this plan sustains it:
  - Sequential paging with one in-flight request keeps S3 request rate at about 5 req/s, far below S3 prefix limits (3,500 writes and 5,500 reads per second).
  - Memory is bounded to one page (≤ 1,000 entries), one pending batch (≤ 1,000 keys), and a 10-key dry-run sample.
  - No shared state with org workers; no database connection.
- How it is verified:
  - Unit test with 2,500 objects across three pages (3.6) proves paging and batching.
  - Benchmark-style test with 100,000 fake objects asserts bounded allocations (no growth with object count beyond one page).
  - Staging and production dry runs report `scanned`, `eligible`, and `duration` in the summary log; `archiver_call_recordings_scanned_total` growth is watched against the timeout.

## Observability Plan

- Structured logs added (`component=call_recordings_retention`, `cycle_start`, `bucket`, `prefix`, `dry_run`):
  - Startup: `call recordings retention enabled` (info) with `retention_days`; `call recordings bucket not reachable` (fatal).
  - Per-key error: `call recording delete failed` (warn) with `key`, `error_code`, `error_message`.
  - Request failure: `call recordings list failed` / `call recordings batch delete failed` (error → Sentry) with `operation`, `batch_size`, wrapped error.
  - Summary: `call recordings retention completed` (info) with `scanned`, `eligible`, `deleted`, `deleted_bytes`, `delete_errors`, `duration`, `partial`, and in dry run `oldest_keys`.
- Correlation identifier: `cycle_start` (UTC timestamp of the cycle) on every line of a run; object `key` per object.
- Error reports: `bucket`, `prefix`, `operation`, `cycle_start`; `key` when per-object. No `org_id` (justified).
- Signals watched after rollout: `archiver_call_recordings_last_run_timestamp_seconds` advancing daily (alert if older than 26 h while enabled), `delete_errors_total` near 0, summary `partial=false`, `deleted_total` roughly tracking uploads from 90 days earlier.

## Rollout and Rollback Plan

- Rollout: tag `1.7.0-staging` → staging env vars with dry run → check summary → disable dry run → one-shot run with `ARCHIVER_EXIT_ON_COMPLETION=true`, `ARCHIVER_ARCHIVE_MESSAGES=false`, `ARCHIVER_ARCHIVE_RUNS=false` against a prefix holding disposable objects older than 90 days (see `quickstart.md`; org archival must stay off in the one-off pod, Principle XI) → production tag `1.7.0` on `main` (after `c49e2e2`) → production env vars with dry run until objects pass the period → disable dry run.
- Backward compatibility window: period `0` default keeps the previous behaviour exactly; the previous image ignores the new variables.
- Rollback: `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD=0` and rollout restart; or redeploy the previous tag. Safe for remaining objects; deleted objects are not recoverable (inherited).
- Migration reversibility: no schema.

## Divergence Gate

- Divergences found while planning: none.
- Tasks blocked by an open amendment: none. (Production enablement of recording in the gateway waits for the 004 amendment; this step is harmless before that.)
