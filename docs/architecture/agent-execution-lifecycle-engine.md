# AGENT-EXECUTION-LIFECYCLE-ENGINE-001

## State

```text
ARCHITECTURE REVIEW REQUIRED
branch: feature/agent-execution-lifecycle-engine-001
base: 0db1c0930c73655a75c2111599d32b24f08f58ef
base tree: 10f5845272a4f4558f3dfaccf0d7abbf03f21e4a
local validation: passed
runtime behavior change: authoritative in-memory lifecycle records
field QA: passed on macOS, physical Linux, and four Linux VM guests
```

## Objective

Implement the authoritative, typed transition owner for the canonical
v0.11.2 agent execution states. The engine creates one bounded in-memory
record for an exact reviewed package and prepared sandbox plan, validates
state progression, rejects stale or foreign authorities, and emits
non-forgeable transition evidence.

This feature does not execute agent code, start a sandbox, authorize entry to
`running`, start timers, deliver cancellation signals, dispatch handoffs,
persist records, emit audit events, load packages, start workers, or change
node, network, model, scheduler, inference, reputation, or reward behavior.

## Dependencies

```text
AGENT-RUNTIME-COMPATIBILITY-GATE-001
-> AGENT-INPUT-OUTPUT-ENFORCEMENT-001
-> AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001
-> AGENT-EXECUTION-LIFECYCLE-ENGINE-001
```

A lifecycle record may be queued only when an exact
`SandboxEnforcementAuthority` verifies prepared sandbox evidence for the exact
`PackageReviewSubject`. Prepared sandbox evidence does not imply that a
sandbox is active.

## Canonical State Vocabulary

The authoritative runtime vocabulary remains:

```text
queued
permission_pending
scope_check
handoff_required
running
completed
failed
cancelled
timeout
blocked
```

`iamine-agents::AuditLifecycleState` remains an observational projection. It
does not own runtime transitions. The new runtime state type must preserve the
same stable strings, and tests must compare every state to prevent vocabulary
drift until `AGENT-AUDIT-EVENT-ENFORCEMENT-001` integrates authoritative
lifecycle evidence.

## Transition Matrix

Structurally valid transitions:

| From | To |
| --- | --- |
| `queued` | `permission_pending`, `blocked` |
| `permission_pending` | `scope_check`, `blocked` |
| `scope_check` | `handoff_required`, `running`, `blocked` |
| `handoff_required` | `cancelled` |
| `running` | `completed`, `failed`, `cancelled`, `timeout` |

The engine may record every valid transition except entry to `running`.
`scope_check -> running` is recognized as part of the canonical shape but must
return `ExecutionAuthorizationRequired` without mutating the record.

The following states are terminal and have no outgoing transitions:

```text
completed
failed
cancelled
timeout
blocked
```

The historical documentation draft listed `timeout -> failed` and
`cancelled -> failed` while also classifying both source states as terminal.
This engine resolves that contradiction fail-closed: a terminal outcome is
immutable. Retry, cleanup outcome, or terminal reclassification requires an
explicit future contract and cannot be inferred here.

## Record Contract

`ExecutionLifecycleAuthority::queue` creates a non-cloneable record bound to:

- the lifecycle authority identity;
- one exact package-review subject;
- one exact sandbox authority identity;
- one exact sandbox evidence identity;
- one execution identity created by the authority;
- state `queued`;
- revision zero.

Only the same lifecycle authority may mutate the record. Each successful
transition increments a bounded revision exactly once. A rejected transition
must leave state and revision unchanged.

Records are process-local and in-memory. They contain no timestamp, hostname,
username, path, prompt, output, process identifier, network address, wallet,
credential, or persistent identifier.

## Running Boundary

The lifecycle engine owns transition validity, not execution authorization.

```text
canonical transition shape
AND future execution authorization evidence
AND active sandbox adapter evidence
AND future package-load evidence
-> running may be recorded by a later integration owner
```

For this feature:

```text
running reachable through public mutation API: false
execution authorized: false
runtime started: false
sandbox started: false
package loaded: false
```

No test-only or operator override may expose a public bypass into `running`.

## Architecture Checkpoint

Review found that the initial implementation retained the sandbox authority
and subject but did not retain the exact prepared evidence identity. That
would have allowed an equivalent-subject plan issued by the same authority to
be substituted during record verification.

The implementation now gives each prepared sandbox evidence object a private
identity and binds that identity into the lifecycle record. An adversarial
test creates replacement evidence from the same sandbox authority and subject
and confirms that verification fails closed. The identity is crate-private,
adds no public constructor, and changes no sandbox activation behavior.

## Evidence Contract

The transition evidence schema identifier is:

```text
iamine.agent.execution_lifecycle.transition-0.1
```

Evidence records only:

- source and target typed states;
- resulting bounded revision;
- status `Recorded`;
- non-forgeable lifecycle and execution identities.

Evidence reports false for execution authorization, runtime activity,
persistence, audit emission, cleanup completion, transport, and package load.
Debug output redacts authority, execution, subject, and sandbox identities.

## Ownership

Production behavior belongs to focused modules under `iamine-agent-runtime`:

```text
execution_lifecycle/authority.rs
execution_lifecycle/error.rs
execution_lifecycle/evidence.rs
execution_lifecycle/mod.rs
execution_lifecycle/record.rs
execution_lifecycle/state.rs
execution_lifecycle/transition.rs
```

`iamine-agent-runtime/src/lib.rs` may expose the typed public API. The sandbox
evidence owner may expose only crate-private identity access needed to bind the
exact plan.

Forbidden production changes:

```text
iamine-node/src/main.rs
iamine-node/src/cluster_registry.rs
iamine-agents audit behavior
Cargo.toml
Cargo.lock
package-load blockers
runtime owner availability
```

## Non-Bypass Rules

- A valid transition is not permission or execution authorization.
- A queued record is not package-load eligibility.
- Prepared sandbox evidence is not an active sandbox.
- `handoff_required` cannot continue to `running`.
- Terminal states cannot be reclassified.
- Rejected transitions cannot mutate revision or state.
- A foreign authority cannot mutate or verify another authority's record.
- A cloned package resolution cannot reuse lifecycle evidence.
- Audit observation cannot create or alter authoritative state.
- Timeout and cancellation signals remain owned by
  `AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001`.
- Handoff dispatch remains owned by `AGENT-HANDOFF-ENFORCEMENT-001`.
- No static package-load blocker is removed.

## Validation Matrix

Local validation:

- all ten state strings match the observational audit vocabulary;
- the transition matrix contains only the authorized edges;
- terminal states have no outgoing edges;
- exact sandbox authority, evidence, and subject are required to queue;
- foreign lifecycle authorities fail closed;
- valid non-running transitions advance state and revision once;
- invalid, stale, terminal, skipped, and self-transitions do not mutate;
- `scope_check -> running` fails with authorization required;
- handoff cannot bypass into `running`;
- evidence remains authority- and execution-bound;
- errors and Debug output remain privacy-safe;
- package-load blockers and foundation execution status remain unchanged.

Field QA:

- Mac validates the in-memory state engine on the macOS build;
- one physical Linux/x86_64 host validates the same contract;
- four Linux VM guests validate VM/cgroup environments;
- every host confirms no `iamine-node` process, socket, persistent file,
  package load, model load, worker, timer, or network runtime is created.

## Risks

- Treating structural validity as permission to execute.
- Exposing a public constructor for arbitrary records or evidence.
- Allowing record cloning and divergent transition branches.
- Reclassifying terminal outcomes after cleanup.
- Folding timeout, cancellation, handoff, or audit ownership into this engine.
- Persisting identifiers that create user or machine correlation.
- Growing one lifecycle module instead of preserving focused ownership.

## Architecture Decision

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
new Cargo dependency: forbidden
process, timer, socket, or sandbox startup: forbidden
package-load blocker change: forbidden
field QA: required
decision: DEVELOPMENT AUTHORIZED
checkpoint: FIELD QA PASSED
next state: ARCHITECTURE REVIEW REQUIRED
```
