# Contract: Configuration, metrics, and consumed storage layout

Public interfaces under Principle XIII. All additive; release `1.7.0`.

## Configuration (`ARCHIVER_*`, also `archiver.toml` and CLI flags through `ezconf`)

| Variable | Default | Behaviour |
|---|---|---|
| `ARCHIVER_CALL_RECORDINGS_RETENTION_PERIOD` | `0` | days; `0` disables the step (no client, no request); `1`–`89` and negatives refuse startup; `≥ 90` enables |
| `ARCHIVER_CALL_RECORDINGS_BUCKET` | — | required when enabled; valid S3 bucket name |
| `ARCHIVER_CALL_RECORDINGS_REGION` | — | required when enabled; AWS region format (for example `sa-east-1`); may differ from `ARCHIVER_S3_REGION` |
| `ARCHIVER_CALL_RECORDINGS_PREFIX` | `recordings/v1/` | only keys under it are considered; must end with `/` |
| `ARCHIVER_CALL_RECORDINGS_DRY_RUN` | `false` | list and log, delete nothing |

Reused, unchanged: `ARCHIVER_AWS_ACCESS_KEY_ID`, `ARCHIVER_AWS_SECRET_ACCESS_KEY`, `ARCHIVER_S3_ENDPOINT`, `ARCHIVER_S3_DISABLE_SSL`, `ARCHIVER_S3_FORCE_PATH_STYLE`, `ARCHIVER_COMMON_TIMEOUT`, `ARCHIVER_START_TIME`, `ARCHIVER_EXIT_ON_COMPLETION`.

Set only on the main `weni-flows/archiver` deployment.

IAM for the `archiver-secret` IAM user on the recordings bucket: `s3:ListBucket` (condition `s3:prefix` = `recordings/*`) and `s3:DeleteObject` on `<bucket>/recordings/*`. The startup check is a one-key `ListObjectsV2` under the prefix, authorized by the same prefix-conditioned `s3:ListBucket`; `HeadBucket` is not used because that condition denies it.

## Metrics (`GET /metrics` on `MetricsPort`)

| Name | Type | Labels |
|---|---|---|
| `archiver_call_recordings_scanned_total` | counter | none |
| `archiver_call_recordings_deleted_total` | counter | none |
| `archiver_call_recordings_deleted_bytes_total` | counter | none |
| `archiver_call_recordings_delete_errors_total` | counter | none |
| `archiver_call_recordings_last_run_timestamp_seconds` | gauge | none |

No label carries record content or personal data, so the `/metrics` exception of Principle XVII remains valid.

## Summary log (one per cycle)

Message `call recordings retention completed`, fields: `component=call_recordings_retention`, `cycle_start`, `bucket`, `prefix`, `dry_run`, `scanned`, `eligible`, `deleted`, `deleted_bytes`, `delete_errors`, `duration`, `partial`, and `oldest_keys` in dry run.

## Consumed storage layout (owned by `weni-webchat-socket`, BD-010)

- Keys `recordings/v1/<project_uuid>/<channel_uuid>/<call_id>.wav`; the archiver relies only on the prefix and on `LastModified` (upload time).
- Object metadata is never read.
