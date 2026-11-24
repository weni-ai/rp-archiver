<!--
Sync Impact Report
- Version change: 1.0.0 → 2.0.0 (MAJOR: Principle I redefined to admit specs without a
  product spec and the SDD Lite cycle; Principles II, VII, VIII, XI, XII, XV, XVI
  materially rewritten; five principles added)
- Modified principles:
  - I. Specification Traceability: two origins (derived from a product spec, or born in
    engineering with "## Origin Without Product Spec"); full and lite cycles, chosen by
    complexity, not by origin
  - II. No Silent Divergence: scoped to specs derived from a product spec
  - VII. Never Trust the Client: no interpolation into queries, commands, templates, file
    paths, S3 keys, or outbound URLs; authenticity, expiry, and replay checks for any
    inbound callback or token; /metrics exception reinforced and moved to Principle XVII
  - VIII. Fail Gracefully and Predictably: atomic multi-step writes, no premature external
    side effects
  - XI. Scalability and Peak Load: bounded and indexed queries, no N+1 loops, no OS
    processes in HTTP handlers, scope-keyed caches with TTL
  - XII. Observability and Diagnosable Errors: errors MUST NOT be silently swallowed
  - XV. Tests Exercise Flows: real entry point and persistence; cross-org refusal paths;
    S3 handling moved to Principle XVIII
  - XVI. Explicit Over Clever: comment and language rules moved to Principle XX
- Added principles:
  - XVII. Explicit, Scoped Authorization (backend; justified exception for /metrics)
  - XVIII. Verification Integrity (root)
  - XIX. Proportional Design (root)
  - XX. Code Clarity and Language (root)
  - XXI. Backward-Compatible Data Evolution (backend; adapted, schema owned by RapidPro)
- Added sections: none (Development Workflow & Quality Gates gained lite-cycle and
  security gates)
- Removed sections: none
- Templates requiring updates (not modified by setup-engineering):
  - ✅ .specify/templates/plan-template.md: Constitution Check derives gates from this file
  - ⚠ .specify/templates/spec-template.md: still the stock Speckit template; it has neither
    "## Inheritance from Product Spec" nor "## Origin Without Product Spec" (Principle I).
    Install the vtex-cx-engineering preset, which replaces it
  - ⚠ .specify/templates/tasks-template.md: marks tests as OPTIONAL; Principle XV prevails
    and requires flow-test tasks for every user story in the full cycle
- Follow-up TODOs:
  - TODO(PRODUCT_SPEC_REPO): identify the product spec repository and tagging convention
    referenced by Principles I and II
  - TODO(SPECKIT_PRESET): install the vtex-cx-engineering preset (spec-template and
    speckit.specify with the origin gate)
  - TODO(BRANCH_PROTECTION): confirm that main on weni-ai/rp-archiver blocks direct pushes
    and requires 1 approval + the CI "Test" check (could not be verified locally)
  - TODO(MERGE_STRATEGY): choose a merge strategy whose resulting commits on main comply
    with Principle IV (current history contains "Merge pull request #NN ..." commits)
  - TODO(VULN_SCAN): add a vulnerability check (e.g. govulncheck ./...) to CI; review
    end-of-support dependencies (aws-sdk-go v1, raven-go via logrus_sentry)
  - TODO(S3_RETRY_POLICY): set the S3 SDK retry bound explicitly (Principle IX)
  - TODO(S3_HERMETIC_TESTS): TestArchiveOrgMessages/TestArchiveOrgRuns call live S3 in CI
    using repository secrets and pass silently without credentials; replace with a fake
    s3iface.S3API in the per-change suite and move live-S3 coverage to a labeled
    integration stage (Principles XV, XVIII)
  - TODO(METRICS_NETWORK): confirm that the metrics port is not exposed through any
    ingress or public load balancer in weni-ai/kubernetes-manifests-platform (Principle
    XVII exception)
  - TODO(PEAK_LOAD): declare current peak figures (Principle XI)
  - TODO(CHANGELOG_FORMAT): decide whether WENI-CHANGELOG.md adopts Keep a Changelog
    categories (Principle XIV)
  - TODO(GO_TOOLCHAIN): CI uses golang:1.25 while go.mod and docker/Dockerfile use 1.23
- Project adaptation to review: Principle I recommends the full cycle for deletion,
  contract, and cross-org concurrency changes. This SHOULD is a project choice, not a
  base rule
- Known compliance gaps in current code (recorded, not changed):
  - cmd/rp-archiver/main.go logs the full DB URL (may include a password) when it contains
    TimeZone, and logs the Sentry DSN when it is invalid (VI, XII)
  - archives/s3.go debug handler logs signed AWS request headers (VI, XII)
  - archives/messages.go DeleteBroadcasts uses db.Get and tx.Exec without a context;
    archives/s3.go TestS3 calls HeadBucket without a context (VIII)
  - archives/messages.go DeleteBroadcasts runs one count query per broadcast (N+1) (XI)
  - cmd/rp-archiver/main.go registers /metrics for every HTTP method instead of GET only
    (XVII)
  - cmd/rp-archiver/main.go reconnect path hard-codes SetMaxOpenConns(2) instead of
    config.MaxOpenConnections (XVI, XIX)
  - inline meaningful literals (time.Hour*12, time.Hour*3, time.Minute*15, 5e9, 1e9,
    LIMIT 1000000) (XVI)

Provenance:
- Source: weni-ai/vtex-cx-engineering-constitutions (main @ 6db621b)
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

An engineering spec under `specs/<NNN-feature>/spec.md` either derives from a product
spec or originates in engineering. Both are regular paths of the process, and neither
requires a justification.

When the change has a product spec, the engineering spec MUST derive from exactly one
approved product spec and MUST reference it by an immutable, pinned version (commit or
tag); a mutable URL or ID alone MUST NOT be used. The product spec MUST exist and be
tagged before its engineering spec is created. An engineering spec MUST NOT redefine the
"what" it inherits: problem, scope, success criteria, and binding decisions belong to the
product spec. A technical architecture document SHOULD be produced for non-trivial
features; when it exists it MUST be linked from the engineering spec, pinned by
commit/tag, but its absence MUST NOT block the engineering spec. Such a spec MUST open
with an inheritance section in exactly this format:

```
## Inheritance from Product Spec
- Product Spec: <title> — <URL>
- Pinned version: <commit/tag>
- Architecture doc: <none | URL + commit/tag>
- Inherited binding decisions: <short list>
- Scope of this spec: <slice implemented by this repo>
- Divergences: <none | link to amendment>
```

When the change is born in engineering (a hotfix, a fix, technical debt, an upstream
merge), the engineering spec states its own "what" and MUST open with an origin section
in exactly this format:

```
## Origin Without Product Spec
- Product Spec: none
- Problem: <what is wrong or missing, in one or two sentences>
- Acceptance criteria: <observable outcomes that prove the change is done>
```

A product spec that exists but cannot be pinned is a blocker, not an absence: the
origin section MUST NOT be used to bypass it. In this repository, `Scope of this spec`,
or the `Problem` of an engineering-born spec, MUST name the archiver slice being changed
(archive building, rollup, upload, deletion, or cycle scheduling) and MUST NOT claim
behaviour owned by RapidPro or mailroom.

Either origin MAY follow the full cycle (specify, plan, tasks, analyze, implement) or the
lite cycle (SDD Lite: specify, then implement directly). The cycle MUST be chosen by the
complexity of the change, not by its origin. A lite-cycle change is bound by every MUST
in this constitution exactly like a full-cycle change; it only skips the intermediate
artifacts. Changes to a deletion path (Principle X), to a public contract (Principle
XIII), or to how org workers share state (Principle XVI) SHOULD follow the full cycle.

**Rationale:** Traceability from intent to technical execution keeps decisions auditable
and lets any change be traced back to the need that justified it. When the change is
born in product, pinning the product spec guarantees every team implements the same
revision. When it is born in engineering, the spec's own problem and acceptance criteria
are what the change is verified against. Choosing the cycle by complexity keeps trivial
fixes cheap without letting risky changes, which in this service can delete production
data, skip planning.

### II. No Silent Divergence

This principle applies only to engineering specs derived from a product spec; a spec
without one inherits nothing it could diverge from. When a technical need contradicts
something inherited from the product spec (scope, success criteria, or a binding
decision), the divergence MUST NOT be implemented silently in code. It MUST be raised as
an amendment in the product repository and recorded in the `Divergences` field of the
inheritance section, linking to that amendment. Once the amendment is approved and
tagged, the spec's `Pinned version` MUST be updated to it. A technical difference that
contradicts nothing inherited is an implementation decision, not a divergence, and MUST
be recorded in the engineering spec.

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
services, responses from S3, and any HTTP request to a port the process exposes.

- Configuration MUST be validated for type, format, range, and cross-field rules at
  startup, and the process MUST refuse to start on invalid configuration. A new
  configuration option MUST ship with its validation in the same change. Values used to
  build outbound targets (`S3Endpoint`, `S3Region`, `S3Bucket`) MUST be validated before
  the first request is made.
- External values MUST NOT be interpolated into SQL text, shell commands, templates, file
  paths, S3 object keys, or outbound URLs. SQL MUST use bind parameters (`$1`, sqlx named
  parameters). File names under `TempDir` and S3 keys MUST be built only from validated
  typed fields (`org_id`, archive type, period, dates, hash), never from free-text values
  such as `org.Name`.
- Data read from the database MUST NOT be assumed well-formed. Unexpected values MUST
  fail the affected archive explicitly instead of producing a corrupt archive, and
  destructive actions MUST re-check counts first (Principle X).
- The archiver currently receives no webhooks, callbacks, queue messages, or tokens. Any
  that is introduced MUST be verified for authenticity, expiry, and replay before any side
  effect, using a server-fixed algorithm and constant-time secret comparison
  (`crypto/subtle`, `hmac.Equal`).
- Authorization for every HTTP endpoint the process exposes is governed by Principle
  XVII.

**Rationale:** External sources run outside the archiver's control and can be wrong,
replayed, or hostile. Validating at the boundary and keeping external values out of
queries, paths, and outbound targets prevents injection, path traversal, SSRF, and,
because this service deletes data, irreversible loss caused by inputs nobody checked.

### VIII. Fail Gracefully and Predictably

Every call to PostgreSQL or S3 MUST carry a `context.Context` with an explicit deadline,
using the context-aware API (`QueryxContext`, `ExecContext`, `GetContext`,
`BeginTxx(ctx, …)`, `*WithContext`), and MUST NOT block indefinitely. Deadlines MUST come
from `Config` fields (`CommonTimeout`, `RollupOrgTimeout`, `BuildRollupArchiveTimeout`)
or named constants. Failures MUST be handled explicitly:

- A failure while archiving one org MUST be contained. It MUST be logged at error level
  with context (Principle XII) and reflected in `archiver_cycle_orgs_failed` and
  `archiver_org_failed_status`, and it MUST NOT stop the cycle for other orgs.
- `logrus.Fatal` and `panic` MUST be limited to startup validation and unrecoverable
  initialization; inside the archiving cycle, errors MUST be returned and handled.
- Errors MUST be wrapped with context (`errors.Wrap`) and MUST NOT leak internals such as
  connection strings or credentials.
- Multi-step writes that must succeed or fail together MUST run atomically. Database
  writes for one archive MUST happen in a single transaction that is rolled back on
  error, and temporary files MUST be removed on failure unless `KeepFiles` is set.
- External side effects MUST NOT happen before the state that justifies them is durable:
  the `archives_archive` row is written only after the S3 upload succeeds, and records are
  deleted only after that row is committed (Principles IX, X).

**Rationale:** Failure is a certainty, not an edge case: database connections break and
S3 endpoints throttle. Explicit deadlines, atomic writes, and per-org containment keep
one bad org or one slow dependency from corrupting archive state, stalling the whole
daily cycle, or crashing the process.

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

Every collection query MUST be bounded by `org_id` and a date range, and by a named cap
or a streaming cursor when the range can be large. Queries MUST filter on indexed columns
of the RapidPro schema on the hot paths (`org_id`, `created_on`, `modified_on`). N+1
query loops MUST NOT be introduced: per-row lookups inside a loop over a result set MUST
be replaced by a set-based query or a batched one. OS processes MUST NOT be spawned from
HTTP handlers. The archiver keeps no shared cache today; a cache introduced for
org-scoped data MUST key on `org_id`, define an explicit TTL, and be invalidated or
versioned by every write path that mutates the underlying data.

The peak load the archiver is expected to sustain MUST be declared in each engineering
spec as peak, not average. At minimum that means records per day for the largest org per
archive type, the total archive backlog per cycle, the largest expected archive size, and
the expected cycle duration against the 24-hour window between `StartTime` runs.

**Rationale:** Capacity is a design input, not something to discover during an incident.
Archiving load is dominated by the largest orgs and by backfills, so sizing for the
average guarantees an overrunning cycle. Bounded, indexed, set-based queries keep the
largest org from exhausting connections or locking the shared RapidPro database. Keeping
durable state external makes restarts safe; declaring the single-instance limit makes the
real scaling ceiling explicit and reviewable.

### XII. Observability and Diagnosable Errors

Logs MUST be structured: emitted through `logrus` with context in fields (`WithField`,
`WithFields`, `WithError`) rather than interpolated into the message, in a
machine-parseable format. Logs and error reports MUST NEVER contain secrets (database
URLs, AWS keys, signed request headers, the Sentry DSN) or sensitive personal data. That
includes archived record content: message text, contact URNs (phone numbers, e-mail
addresses), contact names, and run values or results MUST NOT be logged or sent to
Sentry.

Errors MUST NOT be silently swallowed. A returned error MUST either be handled as part of
expected control flow (for example, an org skipped because it has nothing to archive) or
recorded with structured context; discarding it with `_` or an empty branch MUST NOT
happen without a comment stating why the failure is harmless.

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
one careless log line could leak personal data at scale. A swallowed error in a deletion
or upload path is a failure nobody can investigate. Opaque org and archive identifiers
give investigators exactly the filtering they need.

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

Every flow MUST have at least one test covering the complete use case through its real
entry point and persistence layer, from input to resulting effect. In this repository
the entry points are the exported cycle functions (`ArchiveOrg`, `GetActiveOrgs`,
`GetInactiveOrgs`, `DeleteBroadcasts`) running against a real PostgreSQL. The flows
include, at minimum:

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

Every flow scoped to an org MUST also cover its refusal paths: archiving or deleting for
one org MUST be shown to leave other orgs' records, archives, and broadcasts unchanged,
and every deletion abort (Principle X) MUST be shown to leave persisted state unchanged.
An HTTP endpoint that requires authorization MUST have tests proving that unauthenticated
and unauthorized requests are rejected.

Tests run with `go test -p=1 ./...`; packages share one database and MUST NOT run in
parallel. S3 handling in tests is governed by Principle XVIII. Speckit `tasks.md` files
MUST include flow-test tasks for every user story; a lite-cycle change MUST still ship
its flow tests in code.

**Rationale:** A suite of isolated function tests can be green while their composition is
broken. In the archiver, that means archives that never get uploaded, records deleted
without a valid archive, or one org's data leaking into another org's archive. Failure
and refusal paths are the least exercised in development and the most expensive in
production, especially when they guard deletion.

### XVI. Explicit Over Clever

What a piece of code does MUST be evident where it happens. Hidden side effects and
implicit control flow MUST NOT be introduced to save lines. Concurrency MUST be explicit:
goroutines, semaphores, and shared state MUST be visible at the point of use, and
variables written from goroutines MUST be goroutine-local or synchronized. Any literal
that carries meaning (a timeout, a size threshold such as the single-part upload limit,
a batch size, a query `LIMIT`, a step cap, a connection-pool size) MUST be a named
constant or a `Config` field rather than an inline value. Literals with no meaning beyond
their own value, such as an index of 0 or an increment of 1, are exempt. Added or
modified code MUST comply; pre-existing literals elsewhere are fixed in their own change
(Principle V).

**Rationale:** An unexplained timeout or size limit is a decision nobody can review, and
implicit sharing between concurrent org workers produces bugs that only appear under
production load.

### XVII. Explicit, Scoped Authorization

Every HTTP endpoint the process exposes MUST declare an explicit check that verifies the
caller may perform that action on that resource; identity alone MUST NOT be treated as
authorization. Access MUST be denied by default: a handler registered without such a
check MUST be rejected in review rather than left open. Endpoints MUST expose only the
HTTP methods their contract requires, registered with a method-qualified pattern (e.g.
`GET /metrics`); other verbs MUST be closed. The base rule's prohibition of Django REST
Framework's `IsAuthenticated` and `AllowAny` has no literal target in this Go service;
its intent applies: middleware that only checks for a valid credential, or that
unconditionally grants access, MUST NOT be used as an endpoint's authorization.

Exception: `GET /metrics` on `MetricsPort` is reachable without an authenticated caller
and without a dedicated in-process check. Its authorization boundary is the network: it
MUST be reachable only from the internal monitoring network that scrapes it (cluster
networking, never an ingress or public load balancer), and exposing it publicly is a
violation. This exception is justified because the endpoint is read-only, triggers no
side effect, carries no archived record content, contact data, or secrets (only counters,
gauges, `org_id`, and `org.Name` labels), and is consumed by the Prometheus scraper of
the platform cluster. A change that adds a label, a metric carrying record content or
personal data, a write action, or a new exposure path for this port voids the exception:
`/metrics` MUST then implement a dedicated check (such as a scrape bearer token compared
in constant time) and the engineering spec MUST record the justification. Any other
endpoint MUST implement its own dedicated check, named for the reason it exists.

The org is the scope boundary. Every query that reads, writes, or deletes RapidPro
records or `archives_archive` rows MUST bind `org_id` as a parameter, and a missing or
zero `org_id` MUST fail the operation rather than widen it across orgs. Related records
MUST be checked against the same org: a rollup MUST link only archives of its own org,
and broadcast deletion MUST select broadcasts by `org_id` before checking message
references. Org workers MUST re-read the archive and record state from the database
before each destructive step (Principle X) rather than trusting values computed earlier
in the cycle.

**Rationale:** The archiver holds delete access to every org's data, so an omitted org
predicate is a cross-tenant incident, not a bug. Denying by default and closing unused
verbs keep any future endpoint from opening silently. The /metrics exception is narrow
and conditional, and it is stated here so that any change crossing its limits is
visible in review.

### XVIII. Verification Integrity

Automated checks and tests MUST verify real behaviour and MUST fail when the behaviour,
contract, or guard they cover regresses. A test MUST NOT mock, stub, or bypass the
subject it claims to prove: the org filter, the deletion guards of Principle X, or the
SQL issued against PostgreSQL.

The per-change suite (the CI `Test` job, run on every push and pull request) MUST be
deterministic and hermetic. The PostgreSQL service container started by the job is part
of the test harness. S3 is outside it, so the per-change suite MUST exercise S3 through a
fake implementing `s3iface.S3API` and MUST NOT call a live S3 endpoint or depend on AWS
credentials. A test that silently passes when a dependency is unavailable MUST NOT count
as coverage. Tests that intentionally exercise live S3 MAY exist when labeled as
integration tests (e.g. an `integration` build tag), run in a dedicated CI stage separate
from the per-change suite, and target a non-production bucket. Failing checks, `go vet`,
linters, or coverage gates MUST NOT be silenced or skipped without an explicit
justification in the pull request.

**Rationale:** A green suite that skips S3 when credentials are absent, or mocks away the
guard it claims to test, creates false confidence exactly where deletion bugs hide.
Hermetic boundaries keep the per-change suite fast, reproducible, and free of shared
secrets, while a labeled integration stage still proves the real S3 wiring.

### XIX. Proportional Design

Every change MUST use the simplest design that satisfies the current specification within
the existing layout (`cmd/rp-archiver` for wiring, package `archives` for domain logic).
Abstractions, indirection layers, extension points, or configuration options MUST NOT be
introduced for hypothetical future needs when only one concrete use case exists; the
`s3iface.S3API` boundary is an existing architectural boundary, not a speculative one.
Every decision encoded in code or configuration MUST have a single source of truth: a
default belongs to `archives.NewConfig()` and MUST NOT be repeated as a literal elsewhere,
and a value configured in `Config` MUST NOT be overridden by a hard-coded one on another
code path. Incidental duplication MAY remain local when extracting it would create
premature coupling. Root causes MUST be fixed rather than masked with workarounds, and
redundant checks SHOULD NOT repeat invariants already enforced by types, the RapidPro
schema, or startup validation.

**Rationale:** Speculative abstractions and redundant defensive checks add cost without
solving a real problem. A setting with two sources of truth means the configured value is
not the running value, which is how a reconnect or retry path quietly behaves differently
from what operators deployed.

### XX. Code Clarity and Language

All code, identifiers, comments, commit messages, specs, and technical documentation
MUST be written in English; domain terms that only carry meaning in their original
language MAY remain untranslated. Code MUST prioritize clarity over brevity or
cleverness. Comments MUST explain why a non-obvious decision was made (the constraint,
the trade-off, or the external quirk such as an S3 or RapidPro schema behaviour) and MUST
NOT restate what the code already says; code that needs a comment to explain what it does
SHOULD be rewritten to say it directly. Dead branches and commented-out code MUST NOT be
committed. Code imported verbatim from upstream `nyaruka/rp-archiver` is held to this
principle only when Weni modifies it (Principle V).

**Rationale:** A shared language and self-explanatory code let any engineer read, review,
and maintain the fork without hidden context. Comments that capture reasoning preserve
constraints the code cannot express, while narrative comments and dead code drift out of
sync and mislead readers.

### XXI. Backward-Compatible Data Evolution

The base rule governs schema changes committed with the code that needs them. This
project owns no schema: the RapidPro schema belongs to RapidPro's migrations, and
RP-Archiver MUST NOT create or alter it (see Technology & Operational Constraints). The
rule therefore applies as follows. A schema change the archiver needs MUST be made in
RapidPro, and the archiver change that depends on it MUST NOT be released before that
migration is deployed. Archiver queries MUST remain compatible with both the currently
deployed RapidPro schema and the one being rolled out, so either side can roll back
without data loss. A destructive RapidPro change to a column the archiver reads or
deletes from (drop, rename, tightened type) MUST follow expand-and-contract, and the
archiver MUST stop depending on the old column before the contract step. The archive
file format evolves under Principle XIII.

**Rationale:** RapidPro and the archiver deploy independently and never switch
atomically. Keeping archiver queries valid across both schema versions keeps either
release revertible, and ordering the rollout prevents an archiver that queries a column
that does not exist yet, or deletes using one that has already changed meaning.

## Technology & Operational Constraints

- **Language and layout**: Go, at the version declared in `go.mod` (currently `1.23.0`).
  A single binary is built from `cmd/rp-archiver`, with domain logic in package
  `archives`.
- **Database**: PostgreSQL with the RapidPro schema, accessed through `sqlx` and
  `lib/pq`. The schema is owned by RapidPro's migrations, and RP-Archiver MUST NOT create
  or alter it (Principle XXI). Connections MUST run in UTC; database URLs MUST NOT specify
  `TimeZone` (enforced in `main.go`). The pool is bounded by `MaxOpenConnections`.
- **Object storage**: S3 or an S3-compatible endpoint through `aws-sdk-go`, behind
  `s3iface.S3API`. Objects MUST be written with a private ACL.
- **Configuration**: `ezconf` with three tiers, lowest precedence first: `archiver.toml`,
  then `ARCHIVER_*` environment variables, then CLI flags. Deployments configure through
  the environment.
- **Exposed surface**: the only listening port is `MetricsPort`, serving `GET /metrics`
  under the exception in Principle XVII.
- **Telemetry**: `logrus` with the Sentry hook (`logrus_sentry`) for Error, Fatal, and
  Panic, plus Prometheus metrics on `MetricsPort` at `/metrics`.
- **Packaging and deployment**: a multi-stage Alpine image (`docker/Dockerfile`).
  Tag-triggered GitHub Actions push to ECR and patch
  `weni-ai/kubernetes-manifests-platform` (`weni-flows/archiver`). Tags ending in
  `-develop` deploy to develop, `-staging` to staging, and unsuffixed tags to production.
- **Upstream**: this repository is a fork of `nyaruka/rp-archiver`. Upstream tags are
  merged deliberately and in isolation (Principles IV, V, XIV).

## Development Workflow & Quality Gates

Every pull request MUST pass these gates before merge, whichever cycle produced it:

1. Reviewed and approved by at least one person, with the CI `Test` job green
   (Principle III).
2. Commits follow Conventional Commits, apart from the upstream-import exception
   (Principle IV).
3. Scope limited to the stated change; upstream merges are isolated (Principle V).
4. Touched flows are covered end-to-end on success, failure, and cross-org refusal paths
   (Principle XV), and deletion changes include the abort-case tests (Principle X).
5. The per-change suite stays hermetic, and no check is silenced without justification
   (Principle XVIII).
6. External values stay out of SQL text, file paths, S3 keys, and outbound URLs; every
   query on org data binds `org_id`; no endpoint is added or widened without its check
   (Principles VII, XVII).
7. Contract changes are versioned, backward compatible or deprecated, and recorded in
   `WENI-CHANGELOG.md` (Principles XIII, XIV).
8. No secrets or personal data appear in code, logs, or error reports (Principles VI,
   XII).
9. Dependency changes pass the vulnerability check (Principle VI).

For Speckit features, `spec.md` MUST open with the inheritance section or the origin
section (Principle I). In the full cycle, `plan.md` MUST evaluate every principle in its
Constitution Check before Phase 0 and again after Phase 1. Any accepted deviation MUST be
justified in Complexity Tracking, and any divergence from the product spec MUST go
through an amendment (Principle II). In the lite cycle there is no `plan.md`: the pull
request MUST link the `spec.md`, and the reviewer verifies the gates above against it.

## Governance

This constitution supersedes all other development practices in this repository. On
conflict, precedence is: VTEX CX root engineering constitution, then the backend domain
constitution, then the project adaptations in this document. A project exception is
valid only when stated with its justification inside the affected principle, as done in
Principles IV, XI, XII, XIV, XVII, XX, and XXI.

Amendments MUST be made through a pull request that edits
`.specify/memory/constitution.md`, updates the Sync Impact Report, and follows Principle
III. Versioning follows SemVer. A MAJOR bump removes or redefines a principle or
narrows an exception's scope. A MINOR bump adds a principle or section or materially
expands guidance. A PATCH bump covers clarifications and wording. `Ratified` is
preserved; `Last Amended` is set to the amendment date. When the base constitutions in
`weni-ai/vtex-cx-engineering-constitutions` change, this document SHOULD be regenerated
with `setup-engineering` and versioned accordingly.

Compliance is verified at three points: the Constitution Check gate in every full-cycle
`plan.md`, `/speckit.analyze`, which MUST treat any conflict with a MUST as CRITICAL, and
pull request review against the quality gates above, which is the only gate for
lite-cycle changes.

**Version**: 2.0.0 | **Ratified**: 2026-10-06 | **Last Amended**: 2026-11-24
