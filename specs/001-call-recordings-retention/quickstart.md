# Quickstart: validating the call recordings retention step

## 1. Automated (per-change suite)

Prerequisites: the PostgreSQL service used by CI (`testdb.sql`). No AWS credentials; S3 is the fake `s3iface.S3API`.

```bash
go test -p=1 ./archives/... -run 'CallRecordings' -v -cover
go test -p=1 ./cmd/rp-archiver/... -run 'Retention' -v
go test -p=1 ./...
```

| Scenario (spec 013, Journey 3) | Test | Expected |
|---|---|---|
| 3.1 89/90/91 days | `TestDeleteExpiredCallRecordings_StrictCutoff` | only the 91-day object deleted |
| 3.2 outside prefix | `TestDeleteExpiredCallRecordings_IgnoresOutsidePrefix` | expired object outside prefix untouched |
| 3.3 period 1–89 / 90 | `TestValidateCallRecordingsConfig` | error for 1 and 89, ok for 90 |
| 3.4 period 0 | `TestValidateCallRecordingsConfig` + `TestRetentionScheduler_DisabledNeverRuns` | valid with empty fields; no client, no fake call |
| 3.5 missing bucket/region | `TestValidateCallRecordingsConfig` | error |
| 3.6 2,500 across 3 pages | `TestDeleteExpiredCallRecordings_BatchesOf1000` | `DeleteObjects` calls of 1,000, 1,000, 500 |
| 3.7 2 key errors | `TestDeleteExpiredCallRecordings_PerKeyErrors` | others deleted; `delete_errors_total` +2; next batch runs |
| 3.8 dry run | `TestDeleteExpiredCallRecordings_DryRun` | zero `DeleteObjects`; summary count, bytes, ≤ 10 oldest keys |
| 3.9 independence | `TestRetentionScheduler_OncePerCycle`, `TestRetentionScheduler_IndependentOfOrgs` | org-fetch retry does not rerun; step failure does not stop org dispatch; `Wait` blocks until done |
| Listing error | `TestDeleteExpiredCallRecordings_ListError` | `Partial=true`, earlier deletions stand |
| Batch request error | `TestDeleteExpiredCallRecordings_BatchRequestError` | batch counted as errors, next page processed |
| Deadline | `TestDeleteExpiredCallRecordings_Deadline` | `Partial=true`, no request after deadline |
| Startup bucket check | `TestNewCallRecordingsS3Client_BucketUnreachable` (via fake `HeadBucket`) | error returned |
| Bounded memory | `TestDeleteExpiredCallRecordings_BoundedMemory` | allocations do not grow with 100,000 objects beyond one page |

## 2. Staging (manual)

1. Deploy `1.7.0-staging` with period `0`: no `call recordings` log lines, no requests to the recordings bucket.
2. Set `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD=90`, bucket, region, `ARCHIVER_CALL_RECORDINGS_DRY_RUN=true` on `weni-flows/archiver` only; restart. Check `call recordings retention enabled` at startup.
3. Wait for the next cycle at `ARCHIVER_START_TIME` (or force one in a one-off pod with the settings from step 4). Check the summary: `dry_run=true`, `deleted=0`, `oldest_keys` present.
4. Prove deletion: `LastModified` cannot be backdated (a copy gets a new timestamp), and the code refuses periods below 90, so this needs a staging bucket and prefix that already hold disposable objects older than 90 days. Agree on the bucket/prefix with Cloud. Run once in a one-off pod with `ARCHIVER_CALL_RECORDINGS_BUCKET`/`_PREFIX` pointing there, `DRY_RUN=true` first, then `DRY_RUN=false`, `EXIT_ON_COMPLETION=true`. Expect objects older than the period gone, newer ones kept, `deleted_total` matching the dry-run count. **The one-off pod MUST set `ARCHIVER_ARCHIVE_MESSAGES=false` and `ARCHIVER_ARCHIVE_RUNS=false`**: the cycle still dispatches orgs, and a second process archiving or deleting records alongside the main deployment breaks the single-instance rule (Principle XI).
5. Negative checks: period `30` crashes at startup with a clear message; a wrong bucket crashes at startup.
6. Confirm `archiver-orgs-inactives` and `archiver-proj-inactives` logs never mention call recordings.
