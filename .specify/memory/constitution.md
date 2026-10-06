<!--
Sync Impact Report
- Version change: unversioned template → 1.0.0 (first generation)
- Modified principles: none (all template placeholders replaced)
- Added principles:
  - I. Specification Traceability (root)
  - II. No Silent Divergence (root)
  - III. Version Control and Review (root)
  - IV. Commit Messages (root; justified exception for imported upstream commits)
  - V. Contained Changes (backend)
  - VI. Security and Secrets (root)
  - VII. Never Trust the Client (backend; justified exception for /metrics)
  - VIII. Fail Gracefully and Predictably (backend)
  - IX. Bounded Retry Over REST (backend)
  - X. Verified Archive Before Deletion (project-specific)
  - XI. Scalability and Peak Load (backend; justified single-instance exception)
  - XII. Observability and Diagnosable Errors (root Observability + backend Diagnosable Errors)
  - XIII. Versioned Contracts (root)
  - XIV. Changelog Maintenance (root; applied to a service, not a library)
  - XV. Tests Exercise Flows (backend)
  - XVI. Explicit Over Clever (backend)
- Added sections: Technology & Operational Constraints; Development Workflow & Quality Gates
- Removed sections: none
- Templates requiring updates (not modified by setup-engineering):
  - ✅ .specify/templates/plan-template.md: Constitution Check derives gates from this file
  - ⚠ .specify/templates/spec-template.md: lacks the "## Inheritance from Product Spec"
    opening section required by Principle I; add it manually to each spec until updated
  - ⚠ .specify/templates/tasks-template.md: marks tests as OPTIONAL; Principle XV prevails
    and requires flow-test tasks for every user story
- Follow-up TODOs:
  - TODO(PRODUCT_SPEC_REPO): identify the product spec repository and tagging convention
    referenced by Principles I and II
  - TODO(BRANCH_PROTECTION): confirm that main on weni-ai/rp-archiver blocks direct pushes
    and requires 1 approval + the CI "Test" check (could not be verified locally)
  - TODO(MERGE_STRATEGY): choose a merge strategy whose resulting commits on main comply
    with Principle IV (current history contains "Merge pull request #NN ..." commits)
  - TODO(VULN_SCAN): add a vulnerability check (e.g. govulncheck ./...) to CI; review
    end-of-support dependencies (aws-sdk-go v1, raven-go via logrus_sentry)
  - TODO(S3_RETRY_POLICY): set the S3 SDK retry bound explicitly (Principle IX)
  - TODO(S3_FLOW_TESTS): TestArchiveOrgMessages/TestArchiveOrgRuns pass silently without
    AWS credentials (Principle XV)
  - TODO(PEAK_LOAD): declare current peak figures (Principle XI)
  - TODO(CHANGELOG_FORMAT): decide whether WENI-CHANGELOG.md adopts Keep a Changelog
    categories (Principle XIV)
  - TODO(GO_TOOLCHAIN): CI uses golang:1.25 while go.mod and docker/Dockerfile use 1.23
- Known compliance gaps in current code (recorded, not changed):
  - cmd/rp-archiver/main.go logs the full DB URL (may include a password) when it contains
    TimeZone (VI, XII)
  - archives/s3.go debug handler logs signed AWS request headers (VI, XII)
  - archives/messages.go DeleteBroadcasts uses tx.Exec without a context; archives/s3.go
    TestS3 calls HeadBucket without a context (VIII)
  - inline meaningful literals (time.Hour*12, time.Hour*3, time.Minute*15, 5e9, 1e9,
    LIMIT 1000000) (XVI)

Provenance:
- Source: weni-ai/vtex-cx-engineering-constitutions (main)
- Bases: base-constitution.md, backend/base-constitution.md
- Domains: backend
- Project layer: weni-ai/rp-archiver (Go batch worker; fork of nyaruka/rp-archiver)
-->

# RP-Archiver Constitution

RP-Archiver is the Weni fork of the RapidPro component that archives old messages and
flow runs from the RapidPro PostgreSQL database into gzipped JSONL files on S3, and
optionally deletes the archived records. These principles govern every change to this
repository.

## Core Principles

### I. Specification Traceability

Every engineering spec under `specs/<NNN-feature>/spec.md` MUST derive from exactly one
approved product spec and MUST reference it by an immutable, pinned version (commit or
tag); a mutable URL or ID alone MUST NOT be used. The product spec MUST exist and be
tagged before its engineering spec is created. An engineering spec MUST NOT redefine the
"what" it inherits: problem, scope, success criteria, and binding decisions belong to the
product spec. A technical architecture document SHOULD be produced for non-trivial
features; when it exists it MUST be linked from the engineering spec, pinned by
commit/tag, but its absence MUST NOT block the engineering spec.

Every engineering spec MUST open with an inheritance section in exactly this format:

```
## Inheritance from Product Spec
- Product Spec: <title> — <URL>
- Pinned version: <commit/tag>
- Architecture doc: <none | URL + commit/tag>
- Inherited binding decisions: <short list>
- Scope of this spec: <slice implemented by this repo>
- Divergences: <none | link to amendment>
```

In this repository, `Scope of this spec` MUST name the archiver slice being changed
(archive building, rollup, upload, deletion, or cycle scheduling) and MUST NOT claim
behaviour owned by RapidPro or mailroom.

**Rationale:** Traceability from product intent to technical execution keeps decisions
auditable. Pinning the version guarantees every team implements the same revision of a
feature. A mandatory product spec prevents work without an agreed problem; an optional
architecture doc avoids blocking trivial designs; a single inheritance format keeps the
link uniform and machine-checkable.

### II. No Silent Divergence

When a technical need contradicts something inherited from the product spec (scope,
success criteria, or a binding decision), the divergence MUST NOT be implemented silently
in code. It MUST be raised as an amendment in the product repository and recorded in the
`Divergences` field of the inheritance section, linking to that amendment. Once the
amendment is approved and tagged, the spec's `Pinned version` MUST be updated to it. A
technical difference that contradicts nothing inherited is an implementation decision,
not a divergence, and MUST be recorded in the engineering spec.

**Rationale:** With the product spec as the single source of truth, a silent deviation in
code lets intent and implementation drift apart with no audit trail. Routing divergences
through amendments keeps every decision traceable to an agreed change.

### III. Version Control and Review

All code MUST enter `main` of `weni-ai/rp-archiver` through a pull request. A merge MUST
require at least one approved review and a green CI run of the `CI` workflow
(`.github/workflows/ci.yml`). Direct pushes to `main` MUST be blocked by GitHub branch
protection. Production release tags (`X.Y.Z`, no suffix), which deploy through
`.github/workflows/build-rp-archiver-push-tag-*.yaml`, MUST point to a commit reachable
from `main`; `-develop` and `-staging` tags MAY point to unmerged branches for pre-merge
validation.

**Rationale:** The policy is only real when the platform enforces it. Peer review and a
protected `main` keep history auditable; tying production tags to `main` keeps the
deployment path from bypassing review.

### IV. Commit Messages

Commits MUST follow Conventional Commits: `<type>: <description>`, with type one of
`feat`, `fix`, `docs`, `refactor`, `test`, `chore`. The description MUST be imperative,
specific, and no longer than 50 characters. Commits MUST be atomic: one logical change
per commit. Every commit that lands on `main`, including any commit produced by the
GitHub merge strategy, MUST comply.

Exception: commits imported verbatim when merging an upstream `nyaruka/rp-archiver` tag
are exempt, because rewriting them would break the shared history with upstream. The
merge commit that brings them in MUST comply (e.g. `chore: merge upstream v7.1.1`).

**Rationale:** Conventional commits enable automated changelogs and semantic versioning;
atomic commits make bisecting, reverting, and reviewing cheap.

### V. Contained Changes

A change MUST be limited to the context it was asked to address. Refactoring, renaming,
reformatting, or behaviour adjustments outside that context MUST NOT ride along; each
belongs to its own change. A change that stays within scope MAY span several commits,
each atomic per Principle IV. In this repository, merging an upstream
`nyaruka/rp-archiver` tag MUST be a change of its own and MUST NOT be combined with Weni
features or fixes. Pre-existing deviations from this constitution found outside the
change's scope MUST be fixed in a separate change.

**Rationale:** A change that reaches beyond its stated scope is a change nobody reviewed
on purpose. It hides the intended fix inside unrelated edits and turns a revert into a
choice between losing the fix and keeping a regression. Isolating upstream merges keeps
fork-specific behaviour reviewable and revertible.

### VI. Security and Secrets

Secrets MUST never be committed to the repository, including database URLs with
passwords, AWS access keys, the Sentry DSN, `archiver.toml` files with credentials, and
`.env` files. Secrets MUST be provided by an external secrets manager and injected at
runtime as `ARCHIVER_*` environment variables (or GitHub Actions secrets in CI). Defaults
in `archives.NewConfig()` MUST remain non-functional placeholders (e.g.
`missing_aws_access_key_id`). Access MUST follow least privilege: S3 credentials MUST be
scoped to the archive bucket and the operations the archiver performs, and the database
role MUST be limited to the RapidPro tables the archiver reads, writes, or deletes.
Dependencies MUST come only from trusted sources (Go module proxy and checksum database,
pinned in `go.sum`; official base images) and MUST be checked for known vulnerabilities.

**Rationale:** Leaked credentials and untrusted dependencies are among the most common
and most damaging breaches. This service holds write and delete access to production
message data, so prevention is far cheaper than remediation.

### VII. Never Trust the Client

Everything that reaches the archiver from a source it does not control MUST be treated
as potentially malicious, incomplete, or incorrect until validated. Here that means
configuration (file, environment, flags), rows written to the RapidPro database by other
services, and responses from S3.

- Configuration MUST be validated for type, format, range, and cross-field rules at
  startup, and the process MUST refuse to start on invalid configuration. A new
  configuration option MUST ship with its validation in the same change.
- SQL MUST use bind parameters (`$1`, sqlx named parameters); external values MUST NOT be
  interpolated into SQL text.
- Data read from the database MUST NOT be assumed well-formed. Unexpected values MUST
  fail the affected archive explicitly instead of producing a corrupt archive, and
  destructive actions MUST re-check counts first (Principle X).
- Authorization MUST be enforced server-side on every request to any HTTP endpoint the
  process exposes. Exception: `GET /metrics` is read-only and carries no archived record
  content. Its authorization boundary is the network, so it MUST be reachable only from
  the internal monitoring network, and exposing it publicly is a violation. Any other
  endpoint MUST implement server-side authorization.

**Rationale:** External sources run outside the archiver's control and can be wrong or
hostile. Validating at the boundary prevents injection, corrupt archives, and, because
this service deletes data, irreversible loss caused by inputs nobody checked.

### VIII. Fail Gracefully and Predictably

Every call to PostgreSQL or S3 MUST carry a `context.Context` with an explicit deadline,
using the context-aware API (`QueryxContext`, `ExecContext`, `BeginTxx(ctx, …)`,
`*WithContext`), and MUST NOT block indefinitely. Deadlines MUST come from `Config`
fields (`CommonTimeout`, `RollupOrgTimeout`, `BuildRollupArchiveTimeout`) or named
constants. Failures MUST be handled explicitly:

- A failure while archiving one org MUST be contained. It MUST be logged at error level
  with context (Principle XII) and reflected in `archiver_cycle_orgs_failed` and
  `archiver_org_failed_status`, and it MUST NOT stop the cycle for other orgs.
- `logrus.Fatal` and `panic` MUST be limited to startup validation and unrecoverable
  initialization; inside the archiving cycle, errors MUST be returned and handled.
- Errors MUST be wrapped with context (`errors.Wrap`) and MUST NOT leak internals such as
  connection strings or credentials.
- Partial work MUST NOT leak. Database writes for one archive MUST happen in a single
  transaction that is rolled back on error, and temporary files MUST be removed on
  failure unless `KeepFiles` is set.

**Rationale:** Failure is a certainty, not an edge case: database connections break and
S3 endpoints throttle. Explicit deadlines and per-org containment keep one bad org or one
slow dependency from stalling the whole daily cycle or crashing the process.

### IX. Bounded Retry Over REST

The archiver's REST dependency is the S3 API (AWS or an S3-compatible endpoint). A failed
S3 call MUST be retried rather than dropped when it could plausibly succeed on another
attempt: a connection error, a timeout, an HTTP 5xx, or an HTTP 429/throttling response.
It MUST NOT be retried on a 4xx that reflects a defect in the request itself (e.g. 403
AccessDenied, 404 NoSuchBucket). Retries MUST only apply to idempotent operations.
Archive object keys are deterministic
(`/<org_id>/<type>_<period><date>_<hash>.jsonl.gz`), so a repeated upload overwrites the
same object, and new S3 writes MUST preserve that property. Every retry policy MUST
define a maximum number of attempts and a backoff strategy, and the S3 SDK retry bound
MUST be set explicitly from a named constant or `Config` field rather than left to an
implicit default. Unbounded retry MUST NOT be used. When attempts are exhausted, the
failure MUST be logged at error level, counted (e.g. `archiver_s3_upload_errors_total`),
and MUST remain recoverable: the `archives_archive` row MUST NOT be written unless the
upload succeeded, so the period stays missing and is rebuilt in the next cycle.

**Rationale:** S3 fails for transient reasons far more often than permanent ones, so
bounded retry keeps archives converging. Retrying a request the server rejected on its
merits only adds load. Leaving the period "missing" on exhaustion is what makes the next
cycle a safe recovery path instead of a silent gap.

### X. Verified Archive Before Deletion

Deleting records from the RapidPro database is irreversible, so it MUST only happen
after the archive is proven intact:

- Messages and runs MUST be deleted only when `Delete` is enabled, and only for an archive
  with a committed `archives_archive` row marked `needs_deletion = TRUE`.
- Before deleting, the archiver MUST confirm the S3 object exists and that its ETag
  matches the archive's recorded hash. On absence or mismatch, deletion for that archive
  MUST abort.
- The archiver MUST confirm that the number of live records in the archive's date range
  does not exceed the archive's `record_count`; if it does, deletion MUST abort.
- Deletion MUST run in batches of `DeleteBatchSize`, each in its own transaction with a
  deadline. `needs_deletion` MUST be cleared and `deleted_on` set only after every batch
  has committed.
- Broadcasts MUST be deleted only when older than the org's retention period and no
  longer referenced by remaining messages.
- Any change to a deletion path MUST include flow tests for the abort cases (missing
  object, hash mismatch, count mismatch) as well as the success path.

**Rationale:** An archive that is missing, truncated, or out of date cannot be repaired
once its source rows are gone. Verifying the stored object and the record count right
before each delete is the only defence against permanent data loss caused by an upload,
rollup, or concurrency bug.

### XI. Scalability and Peak Load

State that outlives a unit of work (one archive: org × type × period) MUST NOT be kept in
process memory or on local disk; it MUST live in PostgreSQL (`archives_archive`) or S3.
Files under `TempDir` are scratch space for a single archive build and MUST be removed
when that build ends (`KeepFiles` is a debugging aid only). A restart MUST lose at most
in-flight work, and the next cycle MUST rebuild it from database state.

Exception to horizontal scaling: RP-Archiver MUST run as a single instance per RapidPro
database, as documented in `README.md`. Org selection has no cross-instance coordination,
so two instances would race to build and delete the same archives. Throughput scales
inside the process through `MaxConcurrentArchivation`, `ParallelArchiveFileCreation`,
`MaxConcurrentArchiveUploadDB`, and `MaxOpenConnections`. Running more than one instance
MUST NOT happen until a spec introduces cross-instance coordination and amends this
principle.

The peak load the archiver is expected to sustain MUST be declared in each engineering
spec as peak, not average. At minimum that means records per day for the largest org per
archive type, the total archive backlog per cycle, the largest expected archive size, and
the expected cycle duration against the 24-hour window between `StartTime` runs.

**Rationale:** Capacity is a design input, not something to discover during an incident.
Archiving load is dominated by the largest orgs and by backfills, so sizing for the
average guarantees an overrunning cycle. Keeping durable state external makes restarts
safe; declaring the single-instance limit makes the real scaling ceiling explicit and
reviewable.

### XII. Observability and Diagnosable Errors

Logs MUST be structured: emitted through `logrus` with context in fields (`WithField`,
`WithFields`, `WithError`) rather than interpolated into the message, in a
machine-parseable format. Logs and error reports MUST NEVER contain secrets (database
URLs, AWS keys, signed request headers, the Sentry DSN) or sensitive personal data. That
includes archived record content: message text, contact URNs (phone numbers, e-mail
addresses), contact names, and run values or results MUST NOT be logged or sent to
Sentry.

Every error-level log is forwarded to Sentry by the logrus hook (Error/Fatal/Panic), and
each one MUST carry enough context to be located and filtered without reproducing it.
The base identifiers map to this service as follows:

- Project and account identifier: `org_id`. The RapidPro org is the tenant boundary of
  every unit of work.
- Correlation identifier: the archive identity, made of `archive_type`, `period`, and
  `start_date` (plus the archive `id` once persisted). It ties logs, metrics, and retries
  across cycles to the same unit of work.
- User identifier: exception. Archiving is triggered by the schedule, not by a user, so
  no user identifier exists and one MUST NOT be fabricated. Any future path that acts on
  behalf of a user MUST attach that user's opaque identifier.

Identifiers MUST be opaque. `org.Name` is not an identifier: it MAY appear as a readable
label, but it MUST NOT replace `org_id`. New long-running or failure-prone operations
SHOULD expose duration and error metrics following the `archiver_*` conventions in
`archives/metrics.go`.

**Rationale:** Structured, privacy-safe telemetry makes incidents diagnosable without
creating new exposure. This service handles message content and contact URNs in bulk, so
one careless log line could leak personal data at scale. Opaque org and archive
identifiers give investigators exactly the filtering they need.

### XIII. Versioned Contracts

The public interfaces of RP-Archiver are:

- the archive file format: gzipped JSONL record shape for messages and runs
  (`archives/messages.go`, `archives/runs.go`, fixtures in `archives/testdata/`);
- the S3 object key layout and object metadata (content type, encoding, `md5chksum`);
- the `archives_archive` rows it writes (types, periods, rollup linkage), which RapidPro
  reads;
- the configuration surface: `ARCHIVER_*` variables, CLI flags, and their defaults;
- Prometheus metric names and label sets, which dashboards and alerts consume.

Any change to these interfaces MUST be versioned following SemVer through release tags.
Changes MUST be backward compatible or ship with an announced deprecation path recorded
in `WENI-CHANGELOG.md`. Archives already written MUST remain valid for their consumers.
Silent breaking changes MUST NOT be introduced.

**Rationale:** RapidPro, operators, and dashboards depend on these contracts, and
archives in S3 live for years. Explicit versioning and deprecation give consumers a
predictable path to adapt without outages or unreadable archives.

### XIV. Changelog Maintenance

The root rule requiring Keep a Changelog applies to public libraries. RP-Archiver is a
deployable service, so this project applies the rule as follows. Every user-facing
change (any contract in Principle XIII, plus behaviour changes in archiving, rollup, or
deletion) MUST be recorded in `WENI-CHANGELOG.md` under its release version before the
release tag is created. Version bumps MUST follow SemVer. `CHANGELOG.md` is the upstream
`nyaruka/rp-archiver` changelog and MUST NOT be edited for Weni changes. New entries
SHOULD be grouped by the Keep a Changelog categories (Added, Changed, Deprecated,
Removed, Fixed, Security).

**Rationale:** A maintained changelog tells operators what a deployment changes and
serves as release documentation. Keeping Weni entries separate from the upstream
changelog keeps upstream merges clean and shows what the fork adds.

### XV. Tests Exercise Flows

Every flow MUST have at least one test covering the complete use case, from input to
resulting effect. In this repository the flows include, at minimum:

- building missing monthly and daily archives for an org, for messages and runs;
- monthly rollups;
- S3 upload together with the `archives_archive` write;
- deletion of archived records;
- broadcast deletion;
- org selection and prioritization for a cycle.

Flow tests start from fixture database state (`testdb.sql`) and assert the resulting
archive rows, files or S3 objects, and deleted rows. Tests of a single function (e.g.
`GetMissingDailyArchives`, `chunkIDs`) SHOULD cover edge cases and input variations, but
they MUST NOT be the only coverage a flow has. Every flow MUST cover its success path and
its failure paths; an error path that no test exercises MUST NOT be considered covered.

Tests run against a real PostgreSQL with `go test -p=1 ./...`; packages share one
database and MUST NOT run in parallel. A flow test that silently passes when a
dependency is unavailable MUST NOT count as coverage. A test that needs S3 MUST either
run against a fake implementing `s3iface.S3API` or call `t.Skip` with a reason, and CI
MUST run it unskipped. Speckit `tasks.md` files MUST include flow-test tasks for every
user story.

**Rationale:** A suite of isolated function tests can be green while their composition is
broken. In the archiver, that means archives that never get uploaded or records deleted
without a valid archive. Failure paths are the least exercised in development and the
most expensive in production, especially when they guard deletion.

### XVI. Explicit Over Clever

What a piece of code does MUST be evident where it happens. Hidden side effects and
implicit control flow MUST NOT be introduced to save lines. Concurrency MUST be explicit:
goroutines, semaphores, and shared state MUST be visible at the point of use, and
variables written from goroutines MUST be goroutine-local or synchronized. Any literal
that carries meaning (a timeout, a size threshold such as the single-part upload limit,
a batch size, a query `LIMIT`, a step cap) MUST be a named constant or a `Config` field
rather than an inline value. Literals with no meaning beyond their own value, such as an
index of 0 or an increment of 1, are exempt. Added or modified code MUST comply;
pre-existing literals elsewhere are fixed in their own change (Principle V). Comments
MUST explain why: the constraint, the trade-off, or the non-obvious reason. A comment
that restates what the code says is a signal that the code SHOULD be rewritten to say it.

**Rationale:** Code is read far more often than it is written, usually by someone without
the original context. An unexplained timeout or size limit is a decision nobody can
review, and implicit sharing between concurrent org workers produces bugs that only
appear under production load.

## Technology & Operational Constraints

- **Language and layout**: Go, at the version declared in `go.mod` (currently `1.23.0`).
  A single binary is built from `cmd/rp-archiver`, with domain logic in package
  `archives`.
- **Database**: PostgreSQL with the RapidPro schema, accessed through `sqlx` and
  `lib/pq`. The schema is owned by RapidPro's migrations, and RP-Archiver MUST NOT create
  or alter it. Connections MUST run in UTC; database URLs MUST NOT specify `TimeZone`
  (enforced in `main.go`). The pool is bounded by `MaxOpenConnections`.
- **Object storage**: S3 or an S3-compatible endpoint through `aws-sdk-go`, behind
  `s3iface.S3API`. Objects MUST be written with a private ACL.
- **Configuration**: `ezconf` with three tiers, lowest precedence first: `archiver.toml`,
  then `ARCHIVER_*` environment variables, then CLI flags. Deployments configure through
  the environment.
- **Telemetry**: `logrus` with the Sentry hook (`logrus_sentry`) for Error, Fatal, and
  Panic, plus Prometheus metrics on `MetricsPort` at `/metrics`.
- **Packaging and deployment**: a multi-stage Alpine image (`docker/Dockerfile`).
  Tag-triggered GitHub Actions push to ECR and patch
  `weni-ai/kubernetes-manifests-platform` (`weni-flows/archiver`). Tags ending in
  `-develop` deploy to develop, `-staging` to staging, and unsuffixed tags to production.
- **Upstream**: this repository is a fork of `nyaruka/rp-archiver`. Upstream tags are
  merged deliberately and in isolation (Principles IV, V, XIV).

## Development Workflow & Quality Gates

Every pull request MUST pass these gates before merge:

1. Reviewed and approved by at least one person, with the CI `Test` job green
   (Principle III).
2. Commits follow Conventional Commits, apart from the upstream-import exception
   (Principle IV).
3. Scope limited to the stated change; upstream merges are isolated (Principle V).
4. Touched flows are covered end-to-end on success and failure paths (Principle XV), and
   deletion changes include the abort-case tests (Principle X).
5. Contract changes are versioned, backward compatible or deprecated, and recorded in
   `WENI-CHANGELOG.md` (Principles XIII, XIV).
6. No secrets or personal data appear in code, logs, or error reports (Principles VI,
   XII).
7. Dependency changes pass the vulnerability check (Principle VI).

For Speckit features, `spec.md` MUST open with the inheritance section (Principle I).
`plan.md` MUST evaluate every principle in its Constitution Check before Phase 0 and
again after Phase 1. Any accepted deviation MUST be justified in Complexity Tracking, and
any divergence from the product spec MUST go through an amendment (Principle II).

## Governance

This constitution supersedes all other development practices in this repository. On
conflict, precedence is: VTEX CX root engineering constitution, then the backend domain
constitution, then the project adaptations in this document. A project exception is
valid only when stated with its justification inside the affected principle, as done in
Principles IV, VII, XI, XII, and XIV.

Amendments MUST be made through a pull request that edits
`.specify/memory/constitution.md`, updates the Sync Impact Report, and follows Principle
III. Versioning follows SemVer. A MAJOR bump removes or redefines a principle or
narrows an exception's scope. A MINOR bump adds a principle or section or materially
expands guidance. A PATCH bump covers clarifications and wording. `Ratified` is
preserved; `Last Amended` is set to the amendment date. When the base constitutions in
`weni-ai/vtex-cx-engineering-constitutions` change, this document SHOULD be regenerated
with `setup-engineering` and versioned accordingly.

Compliance is verified at three points: the Constitution Check gate in every `plan.md`,
`/speckit.analyze`, which MUST treat any conflict with a MUST as CRITICAL, and pull
request review against the quality gates above.

**Version**: 1.0.0 | **Ratified**: 2026-10-06 | **Last Amended**: 2026-10-06
