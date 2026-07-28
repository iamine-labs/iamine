# AGENT-EXECUTION-AUTHORIZATION-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
ARCHITECTURE CHECKPOINT PASSED
FIELD QA AUTHORIZED
branch: feature/agent-execution-authorization-001
base: ff7ba1668ffdf61a71294ac3fa1921baf426ce43
base tree: f38b05e44c635989fa1594803eee8d97ea45ec5a
runtime behavior change: passive in-memory authorization decision
package load availability change: none
runtime execution availability change: none
```

## Objective

Implement the dedicated owner that emits the final typed execution
authorization decision after verifying every positive-path gate. The decision
is operator-local, authority-bound, package-bound, deterministic, in-memory,
and free of runtime side effects.

The feature does not load a package, activate a sandbox, transition lifecycle
to `running`, start a runtime, dispatch a task, mutate a scheduler, contact a
peer, select a model, persist state, emit an external event, or execute agent
code.

## Dependencies

```text
AGENT-PACKAGE-REVIEW-EVIDENCE-001: CLOSED
AGENT-RUNTIME-COMPATIBILITY-GATE-001: CLOSED
AGENT-INPUT-OUTPUT-ENFORCEMENT-001: CLOSED
AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001: CLOSED
AGENT-EXECUTION-LIFECYCLE-ENGINE-001: CLOSED
AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001: CLOSED
AGENT-ROUTING-CANDIDATE-SELECTOR-001: CLOSED
AGENT-AUDIT-EVENT-ENFORCEMENT-001: CLOSED
AGENT-SCOPE-ENFORCEMENT-001: CLOSED
AGENT-PERMISSION-ENFORCEMENT-001: CLOSED
```

Handoff and out-of-scope response enforcement own alternative non-executable
branches. An allowed authorization path must not require handoff or refusal
evidence; instead, any Scope or Permission result that needs clarification,
confirmation, refusal, or handoff is rejected before authorization.

## Authorization Boundary

Authorization requires:

1. exact package review authority, subject, and evidence;
2. exact runtime compatibility authority, subject, and evidence;
3. exact input/output enforcement authority, subject, and evidence;
4. exact sandbox authority, subject, and prepared evidence;
5. exact lifecycle authority and record at `scope_check`;
6. exact timeout/cancel control with no cancellation requested;
7. Scope and Permission requests targeting the reviewed package;
8. fresh recomputation of allowed Scope and Permission decisions;
9. exactly one routing candidate bound to the same sandbox evidence;
10. matching Scope, Permission, and lifecycle audit projections;
11. the current lifecycle execution identity and revision.

Missing, foreign, contradictory, stale, replayed, cancelled, ambiguous, or
non-allowed inputs return a typed error and produce no evidence.

## Identity Reinforcement

Scope and Permission evaluations are typed and not caller-constructible outside
`iamine-agents`, but they do not carry authority or subject identity. The
authorization owner therefore does not accept a caller-provided positive
evaluation as sufficient evidence. It:

- compares both request package IDs with the reviewed manifest;
- recomputes Scope from the supplied typed policy and request;
- recomputes Permission from that exact Scope result;
- verifies that audit projections match those recomputed results.

Routing evidence now retains only an internal `Arc` identity for the selected
sandbox evidence. The public candidate identifier remains bounded and its
debug representation remains redacted. Authorization rejects a route selected
for any other sandbox evidence, including another valid evidence instance for
the same package.

## Evidence Contract

Schema:

```text
iamine.agent.execution_authorization.decision-0.1
```

Authorized evidence records only:

```text
operator-local authorization authority identity
reviewed package subject identity
execution identity
sandbox evidence identity
bounded selected candidate id
lifecycle revision
```

It reports:

```text
authorization_recorded = true
execution_authorized = true
package_load_allowed = false
package_loaded = false
runtime_active = false
sandbox_active = false
scheduler_mutated = false
transport_started = false
persisted = false
external_event_emitted = false
```

`execution_authorized` is a passive decision for downstream owner
consumption. It is not proof that package bytes were loaded or code ran.

## Non-Bypass Rules

- Authorization evidence verifies only under its issuing authority.
- Evidence cannot be replayed after lifecycle revision or cancellation changes.
- A routing candidate must be unique and bound to the exact sandbox evidence.
- Audit events cannot replace their independent upstream evidence.
- Handoff, refusal, clarification, or pending confirmation cannot authorize.
- `RuntimeOwner::ExecutionAuthorization` remains `Unavailable`.
- `ExecutionAuthorizationUnavailable` remains a package-load blocker.
- Package-load evidence integration remains a separate future owner.
- No lifecycle transition, loader, executor, process, filesystem, socket,
  worker, model, scheduler, transport, persistence, or inference action starts.

## Privacy And Security

- package, authority, execution, sandbox, policy, and request identities are
  redacted in debug output;
- no user, host, peer, path, prompt, output, credential, wallet, model, or
  hardware identifier is retained;
- errors expose stable enum codes and fixed messages only;
- no serialization, clock, randomness, filesystem, process, logger, or network
  API is used by the owner.

## Architecture Maintenance

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
Cargo changes: forbidden
new production files: 6
largest new production file: 270 lines
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
package loader integration: forbidden
runtime executor integration: forbidden
```

## Required Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test execution_authorization
cargo test -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
cargo test -p iamine-agents
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Because this changes a runtime authorization contract, field QA must execute
the focused test and runtime library tests on the exact source commit across
Mac, TS140, and the four Proxmox guest roles. It must not start a daemon,
worker, package loader, active sandbox, network, model, or inference process.

## Architecture Checkpoint

```text
contract ownership: PASS
independent authority verification: PASS
package-bound Scope and Permission recomputation: PASS
exact sandbox-bound routing: PASS
exact lifecycle, timeout/cancel, and audit chain: PASS
passive evidence with all runtime side effects false: PASS
package-load and runtime owners remain unavailable: PASS
main.rs and cluster_registry.rs delta: 0
largest new production file: 270 lines
local validation: PASS
field QA: AUTHORIZED / NOT STARTED
```

Local evidence:

```text
execution authorization integration: 14/14 PASS
iamine-agent-runtime: 117/117 PASS
iamine-agents: 109/109 PASS
strict iamine-agent-runtime clippy: PASS
quality gate required failures: 0
quality gate result: PASS WITH WARNINGS
optional cargo audit: SKIPPED / unavailable
optional cargo deny: SKIPPED / unavailable
optional gitleaks: SKIPPED / unavailable
```

The quality-gate warnings are pre-existing `dead_code`, deprecation,
`too_many_arguments`, and `type_complexity` findings outside this feature
diff. The strict owner-crate Clippy gate passes with `-D warnings`.
