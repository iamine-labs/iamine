# AGENT-ROUTING-CANDIDATE-SELECTOR-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
FIELD QA AUTHORIZED
branch: feature/agent-routing-candidate-selector-001
base: a4fd73311009de46ebb434caf501888e3b2794d3
base tree: 7d3f51ab76705df62a204612d7d8a76bf44cdb3f
runtime behavior change: bounded in-memory candidate-selection evidence
execution availability change: none
field QA: pending exact source commit
```

## Objective

Implement the dedicated v0.11.2 owner for bounded, deterministic local
candidate selection. The owner consumes a typed task request and candidate
descriptors carrying Scope, Permission, runtime compatibility, resource,
risk, availability, and sandbox evidence.

It does not choose a distributed route, contact a peer, rank candidates,
mutate a scheduler, select a model, activate a sandbox, load a package,
authorize execution, persist state, emit audit events, or implement
distributed model MoE.

## Dependencies

```text
AGENT-ROUTING-CANDIDATE-SELECTION-001: CLOSED documentation contract
AGENT-SCOPE-ENFORCEMENT-001: CLOSED
AGENT-PERMISSION-ENFORCEMENT-001: CLOSED
AGENT-RUNTIME-COMPATIBILITY-GATE-001: CLOSED
AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001: CLOSED
```

Scope and Permission evaluations are typed but are not authority-bound to a
package subject in their current APIs. They are therefore advisory selection
inputs only. Runtime compatibility and sandbox evidence must verify against
their exact operator-local authorities and package subject.

This limitation is safe only because selection evidence explicitly cannot
authorize execution. `AGENT-EXECUTION-AUTHORIZATION-001` must consume stronger
identity-bound evidence before any runnable path exists.

## Ownership

| Area | Owner | This feature |
| --- | --- | --- |
| Scope decision | Scope enforcement | consumes typed result |
| Permission decision | Permission enforcement | consumes typed result |
| Runtime/resource compatibility | runtime compatibility | verifies evidence |
| Sandbox plan | sandbox enforcement | verifies evidence |
| Candidate bounds and exclusion | candidate selector | owns |
| Unique/multiple/no-candidate outcome | candidate selector | owns |
| Scoring or arbitrary tie breaking | future routing policy | forbidden |
| Scheduler or peer transport | existing runtime/network owners | forbidden |
| Model selection or distributed MoE | model/runtime owners | forbidden |
| Execution authorization | later authorization owner | forbidden |
| Audit event | later audit enforcement | forbidden |

## Input Contract

The request carries:

```text
task_type
operating_mode
minimum logical cores
minimum memory
minimum storage
minimum network availability
maximum risk
```

Each candidate carries:

```text
bounded opaque candidate_id
task_type
risk_class
availability
package review subject
Scope evaluation
Permission evaluation
runtime compatibility state/evidence
sandbox state/evidence
```

Limits:

```text
maximum candidates: 64
maximum candidate id: 128 bytes
maximum task type: 64 bytes
candidate ids: unique bounded ASCII identifiers
resource minimums: non-zero
```

Unknown metadata and foreign evidence fail closed.

## Outcomes And Exclusions

Outcomes:

```text
candidate_selected
multiple_candidates
no_candidate
handoff_required
blocked
```

Exclusion reasons:

```text
scope_mismatch
permission_mismatch
resource_mismatch
risk_too_high
node_incompatible
sandbox_unavailable
policy_conflict
metadata_unknown
```

Rules:

- one eligible candidate produces `candidate_selected`;
- more than one produces `multiple_candidates` with no selected id;
- no eligible candidate plus a safety/policy failure produces `blocked`;
- no eligible candidate plus clarification/confirmation produces
  `handoff_required`;
- ordinary unavailability or incompatibility produces `no_candidate`;
- prohibited risk always blocks, regardless of the request maximum;
- unknown or contradictory metadata never becomes eligibility.

Input order cannot resolve a tie. There is no score, weight, preference,
randomness, clock, host identity, or network state inside the selector.

## Evidence Contract

Schema:

```text
iamine.agent.routing_candidate_selection.enforced-0.1
```

Evidence records only bounded counts, a fixed outcome, fixed exclusion counts,
and the selected opaque candidate id when exactly one candidate is eligible.
Debug output redacts the candidate id, task type, package subject, authorities,
and upstream evidence.

Every evidence instance reports:

```text
selection_recorded = true
execution_authorized = false
concrete_route_created = false
scheduler_mutated = false
model_selected = false
distributed_moe_used = false
transport_started = false
persisted = false
audit_event_emitted = false
blocked_action = continue_local_execution
```

## Non-Bypass Rules

- Foreign compatibility or sandbox evidence is rejected as malformed input.
- Scope or Permission refusal cannot become an eligible candidate.
- Confirmation or clarification cannot become implicit authorization.
- Unknown candidate, compatibility, availability, or sandbox state fails
  closed.
- A selected candidate remains only a passive candidate.
- Multiple eligible candidates never receive an arbitrary winner.
- Selection cannot change runtime owner availability or package-load blockers.
- No process, socket, worker, model, package, sandbox, filesystem, scheduler,
  peer, or inference action may start.

## Architecture Maintenance

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
Cargo changes: forbidden
new production module limit: below 750 lines per file
largest new production file: 401 lines
selection scoring: forbidden
distributed model MoE: forbidden
duplicated compatibility or sandbox evaluation: forbidden
```

`RuntimeOwner::RoutingCandidateSelector` remains `Unavailable`. The owner
status describes executable runtime availability, not the presence of passive
selection evidence.

## Required Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test routing_candidate_selector
cargo test -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
cargo test -p iamine-agents
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Field QA must run the focused selector tests and runtime library tests on the
exact source commit across the Mac development machine, physical Linux host,
and four Linux VM roles. It must not start a daemon, worker, socket, active
sandbox, model, package, scheduler, transport, or inference process.

## Architecture Decision

```text
decision: FIELD QA AUTHORIZED
implementation checkpoint: IMPLEMENTATION COMPLETE
focused validation: PASS, 10/10
runtime regression: PASS, 93/93
strict crate clippy: PASS
scope review: PASS
size review: PASS
known limitation: Scope/Permission evaluations are typed but not authority-bound
execution impact: none
next gate: exact-source field QA
```
