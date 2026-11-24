# Engineering Spec Quality Checklist: Call recordings retention step

**Purpose**: Validate the engineering spec before planning (SDD Full)
**Created**: 2026-11-18
**Feature**: [spec.md](../spec.md)

## Inheritance

- [x] Exactly one Product Spec is referenced (013 — Voice Mode Call Recording)
- [x] Pinned version is an immutable commit or tag, not a bare URL or ID (`40bc90263b2ae562db8dd3bc7d52a01514939967`, merge commit of PR #15 on `main`; the repository has no tags — justified in Constitution Alignment I)
- [x] The inheritance section matches the format required by Principle I, field for field
- [x] Architecture doc is either `none` or pinned by commit/tag (`none`; the architecture lives in the pinned spec's technical section)
- [x] Inherited binding decisions are listed (BD-004, BD-005, BD-006, BD-010)
- [x] Scope of this spec names the archiver slices changed (deletion, cycle scheduling) and claims nothing owned by RapidPro or mailroom
- [x] The spec does not redefine problem, scope rationale, user journeys or success criteria
- [x] Divergences is `none` (the identity correction contradicts nothing inherited and is recorded as an implementation decision)

## Technical Completeness

- [x] Scope in this repository is bounded, and what other repos own is named (gateway, Cloud, deployment)
- [x] Current state references real paths in this codebase
- [x] Technical approach names the components affected and the flow between them
- [x] Every changed public interface appears in Contracts with its SemVer impact (five config options, five metrics)
- [x] Backward compatibility is stated (all additive, disabled by default)
- [x] Data model and migrations are covered (none; no database access)
- [x] Peak load is declared as a peak (deletions and live objects per cycle, listing pages, step timeout vs. 24 h window)
- [x] Observability states what is logged, with which context, and which correlation identifier (key + `cycle_start`; `org_id` exception justified)
- [x] Failure handling states timeouts, retriable failures, bounds and idempotency
- [x] Least-privilege IAM and the deletion identity are identified

## Verifiability

- [x] Every acceptance scenario of Journey 3 (3.1–3.9) maps to a test in this repository
- [x] Flow coverage goes through the step's entry point, not only helpers
- [x] Failure and refusal paths are covered (outside prefix, below cutoff, listing error, delete error, timeout)
- [x] Rollout and rollback are described, and rollback is safe

## Constitution

- [x] Every principle has a row in Constitution Alignment (I–XXI, constitution 2.0.0)
- [x] Every departure from a MUST is justified in the spec (untagged product repo; shared static-key identity; no `org_id`; flow tests without PostgreSQL)
- [x] No obligation of the constitution is left as an unfilled placeholder

## Notes

- SDD Full: next step is `/speckit.plan`. The repository has no vtex-cx-engineering preset installed (constitution TODO); the plan follows the constitution's Constitution Check requirement directly.
- Release coordination: `c49e2e2` must reach `main` before the production tag.
- Inherited text "the archiver uses the `flows` service account" should be corrected in a later 013 revision.
