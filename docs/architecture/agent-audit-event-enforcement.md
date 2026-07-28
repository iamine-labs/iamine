# AGENT-AUDIT-EVENT-ENFORCEMENT-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
ARCHITECTURE CHECKPOINT PASSED
FIELD QA PASSED
FINAL ARCHITECTURE REVIEW PASSED
MERGED
POST-MERGE VALIDATION PASSED
MERGED / VALIDATED / CLOSED
branch: feature/agent-audit-event-enforcement-001
base: c96014bee0927f57a72bdbbd52a9da1ef652766e
base tree: 01812f50a2f8c7c86db230accdf566d7c661d7e7
source commit: e2f867410f48990c09b9a3be90b9f8d409820ffd
source tree: c770f8a857a34f101ee42b5e14c119508ad5d194
feature tip: a81e874f4deec1ef8d222e26ed57554c8d12a9f3
merge commit: b9fe62d2ccf09cb8c24b51522d2eca226d41ea86
runtime behavior change: bounded in-memory audit enforcement evidence
execution availability change: none
```

## Objective

Connect the existing bounded Scope, Permission, and lifecycle audit
projections to an operator-local runtime authority. The authority issues
verifiable in-memory evidence while preserving the exact upstream event
vocabulary and fixed one-or-two-event bound.

The feature does not authorize execution, load a package, activate a runtime,
persist or transmit events, call a logger, prove an external side effect, or
remove a package-load blocker.

## Dependencies

```text
AGENT-AUDIT-EVENTS-001: CLOSED
AGENT-SCOPE-ENFORCEMENT-001: CLOSED
AGENT-PERMISSION-ENFORCEMENT-001: CLOSED
AGENT-EXECUTION-LIFECYCLE-ENGINE-001: CLOSED
```

Scope and Permission evaluations remain typed but are not authority-bound to
a package subject in their current APIs. Their audit evidence therefore
reports:

```text
upstream_authority_bound = false
```

This is safe only because audit evidence cannot authorize execution or prove
a side effect. Lifecycle evidence verifies the exact lifecycle authority,
execution identity, and current revision and reports:

```text
upstream_authority_bound = true
```

`AGENT-EXECUTION-AUTHORIZATION-001` must consume the independent owner
evidence directly. It must not substitute any audit event for Scope,
Permission, lifecycle, sandbox, routing, or other gate evidence.

## Ownership

| Area | Owner | This feature |
| --- | --- | --- |
| Scope decision | Scope enforcement | consumes typed projection |
| Permission decision | Permission enforcement | consumes typed projection |
| Lifecycle state and revision | lifecycle engine | verifies exact record |
| Event vocabulary and projection | `iamine-agents::audit_events` | reuses |
| Audit authority and evidence | audit event enforcement | owns |
| Persistence, retention, and access policy | future audit sink | forbidden |
| Execution authorization | later authorization owner | forbidden |
| Runtime, loader, scheduler, network, model, or MoE | existing owners | forbidden |

## Public API

```text
AuditEventEnforcementAuthority
AuditEventEnforcementEvidence
AuditEventEnforcementEvidenceStatus
AuditEventEnforcementBlockedAction
AuditEventEnforcementError
AuditEventEnforcementErrorCode
AuditEventEnforcementRequirement
AUDIT_EVENT_ENFORCEMENT_SCHEMA_VERSION
```

Authority operations:

```text
enforce_scope
enforce_permission
enforce_lifecycle
verifies
verifies_lifecycle
```

## Evidence Contract

Schema:

```text
iamine.agent.audit_event.enforced-0.1
```

Every evidence instance contains only:

```text
operator-local audit authority identity
existing bounded AuditEventSet
optional internal execution identity
optional lifecycle revision
upstream authority-binding classification
```

The existing event set remains bounded to one primary and at most one
secondary event. No event accepts free-form values, package IDs, task IDs,
prompts, output, paths, host data, credentials, timestamps, or caller-defined
correlation IDs.

Every evidence instance reports:

```text
event_recorded = true
blocked_action = treat_as_execution_authorization
execution_authorized = false
side_effect_verified = false
package_loaded = false
runtime_active = false
transport_started = false
persisted = false
external_event_emitted = false
```

## Lifecycle Binding

Lifecycle enforcement:

1. verifies that the supplied authority owns the record;
2. maps the exact runtime state to the established audit vocabulary;
3. binds the evidence to the record's internal execution identity;
4. records the current bounded revision;
5. rejects reuse after the record revision or execution changes.

A foreign lifecycle authority returns
`lifecycle_record_not_verified` without producing evidence.

## Non-Bypass Rules

- An audit authority cannot verify evidence issued by another audit authority.
- Typed Scope or Permission projection evidence cannot claim upstream identity
  binding.
- Lifecycle evidence cannot be replayed against another execution or revision.
- A handoff lifecycle projection remains one observation plus one handoff
  event; it does not dispatch work.
- Audit evidence cannot satisfy execution authorization.
- `RuntimeOwner::AuditEventEnforcement` remains `Unavailable`.
- `AuditEventEnforcementUnavailable` remains in the package-load blocker set.
- No filesystem, logger, process, socket, worker, package, model, scheduler,
  transport, sandbox activation, or inference action may start.

## Privacy And Security

- authority and execution identities are redacted in debug output;
- no package, task, user, host, peer, path, prompt, output, credential, wallet,
  model, or hardware identity is retained;
- no clock, randomness, serialization, persistence, or network API is used;
- only stable enum codes, counts, booleans, and a bounded lifecycle revision
  are exposed.

## Architecture Maintenance

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
Cargo changes: forbidden
iamine-agents audit projection changes: none
largest new production file: 202 lines
duplicated lifecycle transition logic: none
external audit sink: forbidden
distributed model MoE: forbidden
```

## Required Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test audit_event_enforcement
cargo test -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
cargo test -p iamine-agents
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Field QA must run the focused tests and runtime library tests on the exact
source commit across the Mac development machine, TS140, and four Proxmox
guest roles. It must not start a daemon, worker, package loader, logger,
network, model, sandbox, or inference process.

## Current Architecture Decision

```text
decision: MERGED / VALIDATED / CLOSED
focused validation: PASS, 10/10
runtime regression: PASS, 103/103
agents regression: PASS, 109/109
strict crate clippy: PASS
quality gate: PASS WITH WARNINGS, required_failures=0
optional tools skipped: cargo-audit, cargo-deny, gitleaks
field QA: PASS, 60/60 focused and 24/24 library across six platform roles
field QA identity: source e2f867410f48990c09b9a3be90b9f8d409820ffd
field QA tree: c770f8a857a34f101ee42b5e14c119508ad5d194
scope review: PASS
size review: PASS
known limitation: Scope/Permission upstream evidence is not identity-bound
execution impact: none
post-merge focused validation: PASS, 10/10
post-merge runtime regression: PASS, 103/103
post-merge agents regression: PASS, 109/109
post-merge strict crate clippy: PASS
post-merge quality gate: PASS WITH WARNINGS, required_failures=0
next feature: AGENT-EXECUTION-AUTHORIZATION-001
```
