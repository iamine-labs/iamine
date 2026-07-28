# AGENT-HANDOFF-ENFORCEMENT-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
FIELD QA AUTHORIZED
FIELD QA PASSED
READY FOR MERGE REVIEW
APPROVED FOR MERGE
MERGED
POST-MERGE VALIDATION
MERGED / VALIDATED / CLOSED
branch: feature/agent-handoff-enforcement-001
closeout branch: feature/agent-handoff-enforcement-001-closeout-001
base: 12d34a8030de541bc9a9a0e882b079f41fa7f343
base tree: 4184677009c5c48fe16c4035f74fe62fec403cb4
source commit: 6246904245c3108e4478c17284959597d96f01c4
source tree: 1c35acfc300edbe7ffc6ec17c1091a69a1f99233
merge commit: 9e42136dedc9a90c13b2a353d6691607f156c38e
merge tree: 1803135d03df6015ce1e63094b43848962d75790
runtime behavior change: bounded in-memory handoff dispatch recording
execution availability change: none
field QA: PASS
```

## Objective

Implement the dedicated v0.11.2 owner for typed handoff targets, reasons, and
dispatcher evidence. The owner binds one handoff control to the exact
operator-local lifecycle execution and records the canonical transition from
`handoff_required` to `cancelled`.

The feature does not perform transport, select a concrete agent, start target
execution, complete human approval, persist records, emit audit events, grant
permissions, broaden scope, authorize execution, or change package-load
eligibility.

## Dependencies

```text
AGENT-HANDOFF-POLICY-001: CLOSED
AGENT-RUNTIME-CORE-001: CLOSED
AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001: CLOSED
AGENT-EXECUTION-LIFECYCLE-ENGINE-001: CLOSED
AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001: CLOSED
```

The handoff owner consumes only verified lifecycle identity and transition
evidence. Scope and Permission decisions remain independent gates and are not
reinterpreted as handoff authorization.

## Ownership

| Area | Owner | This feature |
| --- | --- | --- |
| Handoff target/reason taxonomy | handoff enforcement | owns |
| Handoff control identity | handoff enforcement | owns |
| Dispatch evidence | handoff enforcement | owns |
| Lifecycle state/revision | execution lifecycle | consumes |
| Handoff timeout | timeout/cancel | unchanged |
| Scope decision | Scope enforcement | unchanged |
| Permission decision | Permission enforcement | unchanged |
| Out-of-scope response text | later response enforcement | forbidden |
| Concrete target selection | later routing selector | forbidden |
| Audit projection | later audit enforcement | forbidden |
| Transport/orchestrator queue | future runtime integration | forbidden |

## Canonical Taxonomy

Targets:

```text
operator
orchestrator
specialized_agent
architecture_review
security_review
qa_review
blocked_state
```

Reasons:

```text
out_of_scope
permission_missing
risk_too_high
input_ambiguous
output_requires_review
sandbox_unavailable
timeout_or_cancelled
policy_conflict
```

`risk_too_high` requires an explicit operator, Architecture, Security, or
blocked-state target. `output_requires_review` requires an explicit operator
or review target. Other target/reason combinations remain typed requests, not
proof that a target exists or was selected.

## Control Contract

`HandoffEnforcementAuthority::prepare` requires:

1. the exact lifecycle authority and execution record;
2. current state `handoff_required`;
3. exact current transition evidence from `scope_check` to
   `handoff_required`;
4. a typed target and reason;
5. a target compatible with reasons that require explicit review.

It returns `HandoffControl`, which records:

```text
target
reason
operator_visible_summary
blocked_action = continue_local_execution
lifecycle_revision
```

The operator-visible summary is a fixed typed classification derived from the
reason. It never stores raw prompts, outputs, paths, host data, or arbitrary
text.

## Dispatch Contract

`HandoffEnforcementAuthority::dispatch`:

1. verifies authority, control, lifecycle authority, and execution identity;
2. rejects a stale lifecycle revision;
3. requires current state `handoff_required`;
4. asks the lifecycle owner to record `handoff_required -> cancelled`;
5. emits authority-bound `HandoffDispatchEvidence`.

The evidence schema is:

```text
iamine.agent.handoff.dispatch-0.1
```

Evidence means only that local handoff ownership was recorded and local
execution cannot continue. It explicitly reports:

```text
dispatch_recorded = true
local_execution_cancelled = true
transport_performed = false
concrete_target_selected = false
target_execution_started = false
human_approval_completed = false
scope_expanded = false
permissions_expanded = false
execution_authorized = false
runtime_active = false
persisted = false
audit_emitted = false
```

The `blocked_state` target still ends the local lifecycle as `cancelled`
because the canonical lifecycle permits only
`handoff_required -> cancelled`. It does not claim a remote blocked record.

## Replay And Race Safety

- Controls bind to the authority, lifecycle authority, execution identity, and
  current revision.
- A foreign authority cannot dispatch another authority's control.
- A transition from another execution cannot prepare a control.
- Timeout/cancel winning the lifecycle race advances the revision and makes a
  prepared handoff stale.
- A successful dispatch advances to terminal `cancelled`; replay cannot record
  a second dispatch.

## Privacy And Security

The module does not collect, retain, log, or expose:

- package, agent, task, or scope identifiers;
- usernames, hostnames, addresses, serials, machine identifiers, or paths;
- raw prompts, outputs, logs, process lists, credentials, or keys.

Debug and error output contains only fixed schema classes, enum labels, counts,
states, revisions, and redacted identities.

## Compatibility Note

The canonical handoff policy names the target `operator`. An older scope
metadata fixture contains the free-form value `human_operator`. This feature
does not consume or reinterpret that string, and does not change the Scope
metadata validator as a side effect. A future metadata-to-runtime integration
must explicitly normalize or reject the discrepancy before issuing a typed
handoff request.

## Non-Bypass Rules

- Handoff cannot transition to `running` or `completed`.
- A handoff request cannot grant Scope or Permission.
- A target class cannot select a concrete agent.
- A review target cannot imply completed approval.
- Recorded evidence cannot imply transport or target receipt.
- Handoff cannot change package-load blockers or runtime owner availability.
- No process, socket, worker, sandbox, model, package, filesystem, or network
  action may start.

## Architecture Maintenance

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
Cargo changes: forbidden
new production module limit: below 750 lines per file
duplicated routing logic: forbidden
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

Field QA must validate the exact source commit on the Mac development machine,
the physical Linux host, and the four Linux VM roles. QA runs only the focused
runtime test surface and must not start a daemon, worker, socket, sandbox,
model, package, transport, or inference process.

## Architecture Decision

```text
decision: MERGED / VALIDATED / CLOSED
implementation checkpoint: IMPLEMENTATION COMPLETE
local validation: PASS
Architecture checkpoint: PASS
field QA: PASS on six required platform roles
final Architecture review: PASS
controlled merge: PASS
post-merge runtime tests: PASS, 73/73
post-merge strict crate clippy: PASS
post-merge quality gate: PASS WITH WARNINGS
required failures: 0
optional tools skipped: cargo audit, cargo deny, gitleaks
next recommended feature: AGENT-OUT-OF-SCOPE-RESPONSE-ENFORCEMENT-001
```
