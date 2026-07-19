# AGENT-SCOPE-ENFORCEMENT-001

## State

```text
LOCAL VALIDATION PASSED
branch: feature/agent-scope-enforcement-001
base: 435b391ccf9b3fd71c914426c09c4148f54252c7
field QA: pending exact implementation commit
```

## Objective

Implement a deterministic, fail-closed decision boundary for an already typed
and validated agent scope. The boundary allows only requests that match the
declared package, task type, task, operation, and input classes.

This feature implements a scope decision engine. It does not authorize agent
execution.

## Ownership

The engine belongs to `iamine-agents`:

```text
iamine-agents/src/scope_enforcement/
```

Shared identifier primitives remain crate-private in:

```text
iamine-agents/src/identifiers.rs
```

No logic is added to `iamine-node`, its `main.rs`, the scheduler, workers,
networking, model selection, inference, hardware profiling, or cluster state.

## Public API

```text
ScopePolicySpec
ScopePolicy
ScopePolicyError
ScopePolicyErrorCode
ScopeRequestClassification
ScopeRequestRef
ScopeDecision
ScopeReasonCode
ScopeEvaluation
evaluate_scope
```

`ScopePolicySpec` is untrusted declaration input. Only `ScopePolicy::try_from`
can produce the validated policy consumed by `evaluate_scope`.

## Decision Flow

```text
typed policy declaration
-> identifier, size, uniqueness, contradiction, privacy, and safety validation
-> validated ScopePolicy

trusted request classification
+ package/task/operation/input identifiers
-> safety-first classification decision
-> exact declared-boundary checks
-> typed allow, clarify, refuse, or orchestrator handoff
```

The precedence is deterministic:

1. ambiguous requests clarify;
2. dangerous, permission-escalation, prompt-injection, and role-confusion
   requests refuse;
3. cross-domain requests return to the orchestrator;
4. malformed requests, package mismatches, unsupported task types, unknown or
   out-of-scope tasks, unsupported operations, and unsupported inputs return to
   the orchestrator;
5. explicitly blocked actions and forbidden inputs refuse;
6. only an exact positive match returns `Allow`.

## Policy Guarantees

Policy construction rejects:

- non-IAMINE or malformed package identifiers;
- broad scope or task identifiers;
- empty, duplicate, or oversized boundary collections;
- overlap between allowed and blocked declarations;
- missing mandatory privacy deny entries;
- missing mandatory mutation, shell, network, model, VM, or publication denies;
- unsafe inputs or operations declared as allowed.

The validated policy has private fields and a redacted `Debug` implementation.
Evaluation reports contain only static typed decision and reason codes. They do
not retain package IDs, task IDs, inputs, prompts, outputs, host metadata, or
private values.

## Trust Boundary

`ScopeRequestClassification::InScopeCandidate` must be supplied by a trusted
orchestration or classification boundary. An agent package, prompt, model
output, or user confirmation cannot self-assert that classification.

This feature does not parse natural language and does not prove that a raw
prompt was classified correctly. Runtime integration must keep classification
evidence outside package control and must rerun scope evaluation for every
request before permission and execution gates.

## Non-Bypass Rules

- `Allow` means only that the scope gate passed.
- Scope cannot grant permissions or authorize execution.
- User confirmation cannot override a refusal or handoff.
- Scope cannot start a lifecycle, sandbox, worker, model, or inference request.
- Scope cannot emit or replace audit events.
- Scope cannot route a handoff; it only returns the typed decision.
- Scope cannot read a package, follow references, parse `agent-scope.toml`, or
  choose the unresolved child-manifest authoring format.

## Package Load Boundary

The package-load report keeps `ScopeManifestValidatorUnavailable` and
`ScopeEnforcementUnavailable`. This engine is not yet connected to a trusted
scope-manifest validator or package/runtime load path, so removing either
blocker would create forgeable eligibility evidence.

A later integration may remove the enforcement blocker only when the loader
consumes a validated policy and non-package-controlled classification evidence.

## Runtime Boundary

This feature performs no filesystem, process, network, persistence, clock,
randomness, model, inference, worker, scheduler, hardware, or CLI operation.
It changes only the executable in-memory decision surface of `iamine-agents`.

## Integration

This feature consumes the contracts from:

```text
AGENT-SCOPE-MANIFEST-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-INPUT-OUTPUT-CONTRACT-001
AGENT-HANDOFF-POLICY-001
AGENT-OUT-OF-SCOPE-RESPONSE-001
```

It feeds:

```text
AGENT-PERMISSION-ENFORCEMENT-001
AGENT-AUDIT-EVENTS-001
future trusted package/runtime integration
```

Permission enforcement remains an independent gate and is the next feature in
canonical roadmap order after this feature closes.
