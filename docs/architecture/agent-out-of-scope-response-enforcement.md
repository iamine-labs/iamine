# AGENT-OUT-OF-SCOPE-RESPONSE-ENFORCEMENT-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
FIELD QA AUTHORIZED
READY FOR MERGE REVIEW
APPROVED FOR MERGE
MERGED
POST-MERGE VALIDATION
MERGED / VALIDATED / CLOSED
branch: feature/agent-out-of-scope-response-enforcement-001
base: d246f68f4f419e3aa034c20f733304bd8057109b
base tree: 5bda26a4f07a0be8a1ff1761f9282fca68d2192b
runtime behavior change: bounded in-memory response evidence
execution availability change: none
field QA: PASS, 6/6 platform roles
merge commit: 0b9bdf0eb55d5a112001f31f039091ca1d13088b
post-merge validation: PASS WITH WARNINGS
```

## Objective

Implement the dedicated v0.11.2 owner for deterministic `refuse`, `clarify`,
`handoff`, and `blocked` response evidence. The owner consumes typed
Scope/Permission evaluations or recorded Handoff dispatch evidence and emits
only a fixed operator-visible classification.

It does not render arbitrary text, deliver a response, select or contact a
target, route work, grant permission, broaden scope, authorize execution,
persist data, emit audit events, or start runtime resources.

## Dependencies

```text
AGENT-OUT-OF-SCOPE-RESPONSE-001: CLOSED contract
AGENT-SCOPE-ENFORCEMENT-001: CLOSED
AGENT-PERMISSION-ENFORCEMENT-001: CLOSED
AGENT-HANDOFF-ENFORCEMENT-001: CLOSED
AGENT-INPUT-OUTPUT-ENFORCEMENT-001: CLOSED
```

Scope, Permission, and Handoff remain independent owners. This feature maps
their non-executable outcomes; it does not recalculate or override them.

## Ownership

| Area | Owner | This feature |
| --- | --- | --- |
| Scope evaluation | Scope enforcement | consumes |
| Permission evaluation | Permission enforcement | consumes |
| Handoff dispatch | Handoff enforcement | consumes |
| Response class/reason mapping | response enforcement | owns |
| Fixed operator summary | response enforcement | owns |
| Response delivery or UI | future integration | forbidden |
| Concrete target routing | later selector | forbidden |
| Audit projection | later audit enforcement | forbidden |
| Execution authorization | later authorization owner | forbidden |

## Canonical Taxonomy

Response classes:

```text
refuse
clarify
handoff
blocked
```

Response reasons:

```text
scope_mismatch
permission_missing
input_unsafe
input_ambiguous
risk_too_high
resource_unavailable
sandbox_unavailable
policy_conflict
```

Every reason maps to one fixed operator summary. The blocked action is always:

```text
continue_local_execution
```

## Deterministic Mapping

Scope:

| Scope decision | Scope reason | Response |
| --- | --- | --- |
| `allow` | `in_scope` | rejected; response not required |
| `clarify` | `ambiguous_task` | `clarify / input_ambiguous` |
| `refuse` | `dangerous_task` | `refuse / risk_too_high` |
| `refuse` | unsafe boundary reasons | `refuse / input_unsafe` |
| `handoff_to_orchestrator` | any valid reason | rejected until dispatch evidence exists |

Permission:

| Permission decision | Permission reason | Response |
| --- | --- | --- |
| `allow` | `permitted` | rejected; response not required |
| `require_confirmation` | `confirmation_required` | `blocked / permission_missing` |
| `refuse` | undeclared permission | `refuse / permission_missing` |
| `refuse` | invalid, forbidden, or blocked policy | `refuse / policy_conflict` |
| `handoff_to_orchestrator` | any valid reason | rejected until dispatch evidence exists |

Handoff:

- Only `HandoffDispatchEvidence` may produce class `handoff`.
- The typed target and reason are preserved.
- `timeout_or_cancelled` maps to `resource_unavailable`.
- `output_requires_review` maps to `policy_conflict`.
- The response records that dispatch and local cancellation occurred, but does
  not claim transport, target receipt, response delivery, or target execution.

## Evidence Contract

Schema:

```text
iamine.agent.out_of_scope_response.enforced-0.1
```

Every response records:

```text
source
source_reason
response_class
response_reason
operator_summary
handoff_target
blocked_action
```

Every response also reports:

```text
response_recorded = true
operator_visible = true
response_delivered = false
task_success = false
scope_expanded = false
permissions_expanded = false
execution_authorized = false
runtime_active = false
transport_performed = false
persisted = false
audit_emitted = false
```

`operator_input_required` is true only for `clarify` and `blocked`.
`handoff_dispatch_recorded` and `local_execution_cancelled` are true only when
copied from actual handoff dispatch evidence.

## Privacy And Compatibility

The historical draft shape listed `agent_id`, `task_type`, and `scope_id`.
Enforcement intentionally does not copy those values into response evidence.
Upstream authorities retain identity binding, while this owner exposes only
fixed enum labels and redacted authority identity.

No API accepts raw prompts, outputs, host data, paths, logs, credentials,
secrets, arbitrary summaries, or user-controlled response text.

## Non-Bypass Rules

- An allowed Scope or Permission decision cannot emit a failure response.
- A Scope/Permission handoff decision cannot claim handoff without dispatch.
- Clarification cannot grant permission or broaden scope.
- Refusal and blocked responses cannot be task success.
- Handoff cannot imply transport, target selection, receipt, or execution.
- Response recording cannot change lifecycle, package load, or runtime owner
  availability.
- No process, socket, worker, sandbox, model, package, filesystem, or network
  action may start.

## Architecture Maintenance

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
Cargo changes: forbidden
new production module limit: below 750 lines per file
free-form response generation: forbidden
duplicated Scope/Permission evaluation: forbidden
```

## Required Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
cargo test -p iamine-agents
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Field QA must run the focused response integration tests and runtime library
tests on the exact source commit across the Mac development machine, physical
Linux host, and four Linux VM roles. It must not start a daemon, worker,
socket, sandbox, model, package, transport, or inference process.

## Architecture Decision

```text
decision: MERGED / VALIDATED / CLOSED
implementation checkpoint: IMPLEMENTATION COMPLETE
focused validation: PASS
broad local validation: PASS WITH WARNINGS
Architecture checkpoint: PASS
field QA: PASS on source commit e357d15fdfe6459976d7b501263c4b5c72eac0f5
field QA matrix: 6/6 platform roles, 84/84 tests
scope review: PASS, no node, scheduler, transport, package-load, or Cargo changes
size review: PASS, largest production file 337 lines
final Architecture review: PASS
merge: PASS, 0b9bdf0eb55d5a112001f31f039091ca1d13088b
post-merge runtime tests: PASS, 83/83
post-merge strict crate clippy: PASS
post-merge quality gate: PASS WITH WARNINGS
```
