# AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
FIELD QA AUTHORIZED
FIELD QA PASSED
APPROVED FOR MERGE
MERGED
POST-MERGE VALIDATION
MERGED / VALIDATED / CLOSED
branch: feature/agent-timeout-cancel-enforcement-001
base: 740ef674213cc892e349169c75dbd8eeb2086b20
base tree: 92c993e549491a0c43d9705cf46a1eeb20c7489c
source commit: 98256990a3c50c8eb594b630263aefb71a1ddd0f
source tree: 8750700df1cf82dfb41ca2396f2ef9488060a902
merge commit: 2dbb7602a113bc37312cd4c2651a7081a3de6abe
merge tree: 7942138557858685c474b1ab2e00c2c2fb78af26
runtime behavior change: bounded in-memory timeout and cancellation enforcement
execution availability change: none
```

## Objective

Implement the dedicated v0.11.2 owner for bounded agent timeout and
cancellation enforcement. The owner binds one policy and one cancellation
signal to an exact lifecycle execution, observes monotonic deadlines, asks the
authoritative lifecycle owner to record a compatible terminal transition, and
emits typed cleanup-pending evidence.

This feature does not authorize or start agent execution, start a sandbox,
perform cleanup, delete files, terminate processes, persist evidence, emit
audit events, dispatch handoffs, load packages, start workers, open sockets, or
change node, scheduler, P2P, model, inference, reputation, or reward behavior.

## Dependencies

```text
AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001
AND AGENT-EXECUTION-LIFECYCLE-ENGINE-001
-> AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001
```

A control may be established only when the exact lifecycle authority verifies
the exact lifecycle record, prepared sandbox evidence, sandbox authority, and
package-review subject. Equivalent replacement evidence or a cloned package
resolution must fail closed.

## Timeout Policy

The implementation preserves all six canonical policy classes:

```text
permission_wait_timeout
scope_check_timeout
sandbox_start_timeout
execution_timeout
handoff_timeout
cleanup_timeout
```

Every class is required, non-zero, and bounded to at most 3,600,000
milliseconds. The execution timeout must also be less than or equal to the
prepared sandbox wall-time limit.

Timeout handles use `std::time::Instant`. They do not spawn threads or
background tasks. A handle captures one start instant, one checked deadline,
one timeout class, one lifecycle revision, and non-forgeable control/execution
identities.

## Lifecycle Mapping

Only the timeout class assigned to the current lifecycle phase may be armed:

| Lifecycle state | Timeout class |
| --- | --- |
| `queued` | `sandbox_start_timeout` |
| `permission_pending` | `permission_wait_timeout` |
| `scope_check` | `scope_check_timeout` |
| `handoff_required` | `handoff_timeout` |
| `running` | `execution_timeout` |
| terminal state with verified cleanup-pending evidence | `cleanup_timeout` |

Expired deadlines request only transitions already owned by the lifecycle
engine:

| Current state | Timeout terminal state |
| --- | --- |
| `queued` | `blocked` |
| `permission_pending` | `blocked` |
| `scope_check` | `blocked` |
| `handoff_required` | `cancelled` |
| `running` | `timeout` |

The typed timeout event remains in terminal evidence, so a
`permission_wait_timeout` that is fail-closed as `blocked` is not confused
with a policy denial or task failure. The public lifecycle API still blocks
entry to `running`; this feature adds no bypass. The `running -> timeout`
mapping is structurally covered for future execution authorization
integration.

## Cancellation Contract

Cancellation sources remain exactly:

```text
operator_cancelled
orchestrator_cancelled
permission_revoked
scope_violation_cancelled
sandbox_failure_cancelled
timeout_cancelled
shutdown_cancelled
```

`CancellationHandle` is cloneable so a future executor may observe the same
in-memory signal. Only the operator-local `TimeoutCancelAuthority` may set the
signal. The first request fixes the typed source; later requests cannot
overwrite it. A request is revision-bound and is not terminal evidence.

Cancellation enforcement asks the lifecycle authority for:

| Current state | Cancellation terminal state |
| --- | --- |
| `queued` | `blocked` |
| `permission_pending` | `blocked` |
| `scope_check` | `blocked` |
| `handoff_required` | `cancelled` |
| `running` | `cancelled` |

The event remains `Cancellation(source)` even when the canonical pre-running
terminal state is `blocked`. Cancellation is never reported as `completed`.

## Cleanup Boundary

The cleanup owner remains:

```text
RuntimeSandboxAdapter
```

Timeout/cancellation terminal evidence records:

```text
cleanup trigger: timeout or cancellation
cleanup result: pending
cleanup completed: false
```

The timeout/cancel authority cannot perform cleanup or claim completion.
`cleanup_timeout` may be armed only from verified terminal evidence emitted by
the same authority, control, execution, lifecycle authority, and lifecycle
record. Expiration records `TimedOut` without changing or reclassifying the
existing terminal state.

The future active sandbox adapter must own successful/failed cleanup evidence.
No current API can claim completed cleanup.

## Evidence Schemas

```text
iamine.agent.timeout_cancel.cancellation_request-0.1
iamine.agent.timeout_cancel.terminal-0.1
iamine.agent.timeout_cancel.cleanup_timeout-0.1
```

Evidence contains only typed event classes, lifecycle state/revision, cleanup
owner/trigger/result, and process-local non-forgeable identities. It contains
no username, hostname, address, identifier, path, prompt, output, process
list, credential, wallet, or persistent fingerprint. Debug output redacts
authority, control, execution, policy, clock, and signal internals.

## Ownership

Production behavior is split under:

```text
iamine-agent-runtime/src/timeout_cancel_enforcement/authority.rs
iamine-agent-runtime/src/timeout_cancel_enforcement/configuration.rs
iamine-agent-runtime/src/timeout_cancel_enforcement/control.rs
iamine-agent-runtime/src/timeout_cancel_enforcement/error.rs
iamine-agent-runtime/src/timeout_cancel_enforcement/evidence.rs
iamine-agent-runtime/src/timeout_cancel_enforcement/mod.rs
```

The lifecycle owner exposes only a crate-private identity verifier. Its public
transition matrix and authorization boundary do not change.

Forbidden production changes:

```text
iamine-node/src/main.rs
iamine-node/src/cluster_registry.rs
Cargo.toml
Cargo.lock
runtime owner availability
package-load blockers
active sandbox behavior
```

## Non-Bypass Rules

- A timeout policy is not execution authorization.
- A cancellation request is not a recorded terminal state.
- A terminal transition is not completed cleanup.
- A cleanup timeout cannot reclassify a terminal lifecycle state.
- Foreign authorities, controls, handles, evidence, and executions fail closed.
- Stale timers and cancellation requests fail without lifecycle mutation.
- Caller-selected monotonic instants are accepted only through the
  operator-local authority and are never persisted or emitted.
- No package-controlled value selects an authority or constructs evidence.
- Runtime owner availability and package-load blockers remain unchanged.

## Validation Matrix

Local validation covers:

- exact timeout and cancellation vocabularies;
- zero and excessive timeout rejection;
- execution timeout bounded by sandbox wall time;
- exact lifecycle/sandbox/subject evidence chain;
- phase-specific timeout arming;
- no mutation before a deadline;
- deterministic deadline expiration;
- stale and foreign handle rejection;
- cancellation one-shot behavior across cloned observers;
- cancellation request versus terminal evidence separation;
- canonical pre-running, handoff, and future-running terminal mappings;
- cleanup timeout requiring verified terminal evidence;
- terminal immutability after cleanup timeout;
- privacy-safe Debug and errors;
- unchanged execution availability and package-load blockers.

Field QA is required because monotonic clock and atomic cancellation behavior
are platform runtime primitives. The exact source tree must pass on macOS, one
physical Linux role, and four Linux VM roles before final Architecture review.

## Risks

- Treating a cancellation request as completed cancellation.
- Overwriting the first cancellation source.
- Replaying a stale timeout or request after lifecycle progression.
- Treating a pre-running timeout as execution timeout.
- Claiming sandbox cleanup without adapter evidence.
- Reclassifying a terminal lifecycle state after cleanup.
- Letting package-controlled code receive an operator authority.
- Adding active execution, process, or filesystem behavior prematurely.
- Growing lifecycle, sandbox, or node monoliths with foreign ownership.

## Architecture Decision

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
new Cargo dependency: forbidden
process, socket, worker, or sandbox startup: forbidden
filesystem mutation or persistence: forbidden
execution authorization: false
package-load blocker change: forbidden
field QA: required
decision: MERGED / VALIDATED / CLOSED
checkpoint: FIELD QA PASSED
post-merge quality gate: PASS WITH WARNINGS
required failures: 0
optional tools skipped: 3
recommendation: CLOSED
```
