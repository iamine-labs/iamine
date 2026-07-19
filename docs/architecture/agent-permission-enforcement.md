# AGENT-PERMISSION-ENFORCEMENT-001

## State

```text
LOCAL VALIDATION PASSED
branch: feature/agent-permission-enforcement-001
base: 5e61fedc21cc67ef209a770f767e89d7c56ad592
base tree: 4c11938e613784af86e795e65eceb43ede488cfe
field QA: pending exact implementation commit
```

## Objective

Implement a deterministic, deny-by-default permission gate for an already
scope-approved request and an already reviewed permission policy. The gate
allows only exact approved actions and permission categories.

A permission `Allow` decision means only that this gate passed. It does not
grant operating-system permissions or authorize package loading, runtime
execution, scheduling, worker startup, model use, or any side effect.

## Ownership

The gate belongs to `iamine-agents`:

```text
iamine-agents/src/permission_enforcement/
```

No logic is added to `iamine-node`, `main.rs`, the scheduler, workers,
networking, model selection, inference, hardware profiling, cluster state, or
service management.

## Public API

```text
PermissionDefaultPolicy
PermissionPolicySpec
PermissionPolicy
PermissionPolicyError
PermissionPolicyErrorCode
PermissionConfirmation
PermissionRequestRef
PermissionDecision
PermissionReasonCode
PermissionEvaluation
evaluate_permissions
```

`PermissionPolicySpec` represents trusted review output, not raw package
metadata. Only `PermissionPolicy::try_from` can create the validated policy
consumed by `evaluate_permissions`.

## Decision Flow

```text
trusted reviewed permission policy
-> identifier, default, size, uniqueness, contradiction, safety, and
   confirmation-boundary validation
-> validated PermissionPolicy

ScopeEvaluation with Allow
+ exact package, action, and required permission categories
+ trusted orchestrator confirmation evidence when required
-> typed allow, confirmation request, refusal, or orchestrator handoff
```

Evaluation precedence is deterministic:

1. a non-allowing Scope result returns to the orchestrator;
2. malformed, empty, duplicate, or oversized requests refuse;
3. a package mismatch refuses;
4. blocked actions refuse;
5. forbidden categories refuse;
6. undeclared actions refuse;
7. undeclared categories refuse;
8. an approved permission that requires confirmation requests confirmation;
9. only a complete exact match returns `Allow`.

## Policy Guarantees

Policy construction rejects:

- non-IAMINE package IDs and broad permission profile IDs;
- any default other than `Deny`;
- empty required collections, duplicates, and oversized collections;
- categories outside the supported release-phase allowlist;
- overlap between approved and forbidden categories;
- overlap between approved and blocked actions;
- omission of mandatory unsafe permission categories;
- omission of mandatory unsafe action classes;
- confirmation requirements for permissions that were not already approved.

The supported approved category set for this phase is intentionally bounded:

```text
local_readonly
user_provided_text
redacted_status_summary
package_relative_review_files
lan_readonly_metadata
```

Unknown categories are denied. The policy and request `Debug` implementations
emit counts and typed state only. Evaluations contain static decision and
reason codes, not package IDs, profile IDs, actions, categories, prompts,
outputs, paths, host data, or private values.

## Confirmation Boundary

Confirmation is evaluated only after action and category approval. It cannot:

- approve an undeclared action or category;
- override a forbidden category or blocked action;
- turn a failed Scope result into an allowed request;
- grant a permission not already present in trusted policy review output;
- authorize execution or create a runtime capability.

`PermissionConfirmation::TrustedOrchestratorConfirmed` must come from trusted
orchestrator evidence. Agent packages, prompts, model output, or a raw boolean
must not produce it during future integration.

## Non-Bypass Rules

- Permission enforcement runs after Scope and cannot expand Scope.
- Scope `Allow` does not imply permission `Allow`.
- Permission `Allow` does not imply execution authorization.
- Package metadata cannot self-approve requested categories or actions.
- User confirmation cannot override a refusal or handoff.
- The gate cannot read manifests, files, environment, process state, or network
  state.
- The gate cannot emit audit events or replace the future audit gate.
- The gate cannot start a lifecycle, sandbox, worker, model, or inference path.
- The gate cannot grant filesystem, process, network, wallet, credential,
  service, VM/container, model, marketplace, token, or mainnet access.

## Package Load Boundary

The package-load report intentionally keeps both:

```text
PermissionModelValidatorUnavailable
PermissionEnforcementUnavailable
```

This feature does not parse `metadata/agent-permissions.toml`, verify human
review evidence, or connect the gate to package/runtime loading. Removing those
blockers before trusted parser, review, and integration evidence exists would
create forgeable eligibility.

## Runtime Boundary

This feature performs no filesystem, process, network, persistence, clock,
randomness, model, inference, worker, scheduler, hardware, service, or CLI
operation. It changes only the pure in-memory decision surface of
`iamine-agents`.

## Integration

This feature consumes:

```text
AGENT-PERMISSION-MODEL-001
AGENT-SCOPE-ENFORCEMENT-001
AGENT-INPUT-OUTPUT-CONTRACT-001
AGENT-HANDOFF-POLICY-001
AGENT-OUT-OF-SCOPE-RESPONSE-001
```

It feeds:

```text
AGENT-AUDIT-EVENTS-001
future trusted package/runtime integration
```

Audit events remain an independent gate and are the next feature in canonical
roadmap order after this feature closes.

## Risks

- Treating raw requested metadata as approved policy would allow self-granting.
- Accepting confirmation before deny checks would create privilege escalation.
- Logging policy or request values could expose private metadata.
- Removing package-load blockers now would imply an integration that does not
  exist.
- Adding runtime or operating-system permission behavior would cross crate and
  feature ownership boundaries.
