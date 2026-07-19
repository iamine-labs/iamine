# AGENT-AUDIT-EVENTS-001

## State

```text
LOCAL VALIDATION PASSED
branch: feature/agent-audit-events-001
base: 247cbd08ea329c8f031ab9a898f1ca37f1468ad8
base tree: 8e48e8eab0c3daf73e41acfcc2506697b7ef97c8
field QA: pending exact implementation commit
```

## Objective

Implement bounded, redacted, deterministic in-memory evidence for canonical
lifecycle states and typed Scope or Permission evaluations. The projection may
describe a check, refusal, or handoff; it cannot authorize execution or prove
that a runtime side effect occurred.

## Ownership

The implementation belongs to:

```text
iamine-agents/src/audit_events/
```

It does not add behavior to `iamine-node`, workers, schedulers, P2P, PubSub,
models, inference, hardware profiling, services, installers, or package loading.

## Public API

```text
AUDIT_EVENT_SCHEMA_VERSION
MAX_AUDIT_EVENTS_PER_PROJECTION
AuditEventClass
AuditEventSource
AuditOutcome
AuditReasonCode
AuditLifecycleState
AgentAuditEvent
AuditEventSet
audit_lifecycle_state
audit_scope_evaluation
audit_permission_evaluation
```

## Event Boundary

Events contain only enums derived from canonical code paths. They do not accept
or retain package IDs, task IDs, user IDs, peer IDs, prompts, outputs, paths,
host identifiers, network addresses, credentials, timestamps, or arbitrary
strings.

Each projection returns one or two events:

```text
lifecycle state -> lifecycle_observed [+ handoff_required]
ScopeEvaluation -> scope_checked [+ refusal_recorded | handoff_required]
PermissionEvaluation -> permission_checked [+ refusal_recorded | handoff_required]
```

`AuditEventSet` always contains a primary event and at most one secondary event.
It cannot grow dynamically or retain an unbounded history.

## Lifecycle Boundary

The lifecycle projection observes the canonical v0.11.2 state vocabulary:

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

It does not validate transitions, store state, execute cleanup, schedule work,
start a sandbox, or claim that the observed state came from a trusted runtime.
Future runtime integration must supply authoritative state evidence.

## Scope And Permission Boundary

Scope and Permission events consume their existing evaluation types. Because
those evaluation fields and constructors are private to the crate, package
metadata cannot inject an arbitrary decision or reason into these projections.

An audit event is observational evidence only:

```text
Scope Allow != Permission Allow
Permission Allow != execution authorization
audit event != package-load eligibility
audit event != proof of a completed side effect
```

## Audit Policy Boundary

`AGENT-AUDIT-LOG-001` remains the declarative audit policy contract. This
feature does not parse `metadata/agent-audit.toml`, validate human review,
implement retention, enforce integrity, or decide access policy.

The package-load blockers remain explicit:

```text
AuditPolicyValidatorUnavailable
AuditEventEnforcementUnavailable
ExecutionAuthorizationUnavailable
```

Removing them requires trusted parser, review, runtime integration, and final
Architecture evidence outside this feature.

## Privacy And Security

- no raw prompts or outputs;
- no usernames, hostnames, addresses, MACs, serials, machine IDs, or paths;
- no wallet, key, credential, process-list, or model data;
- no timestamps, random IDs, or caller-controlled correlation values;
- no serialization, filesystem, process, network, environment, or logger API;
- debug output contains enum names and bounded reason codes only.

## Integration

This feature consumes:

```text
AGENT-AUDIT-LOG-001
AGENT-EXECUTION-LIFECYCLE-001
AGENT-SCOPE-ENFORCEMENT-001
AGENT-PERMISSION-ENFORCEMENT-001
AGENT-HANDOFF-POLICY-001
AGENT-OUT-OF-SCOPE-RESPONSE-001
```

It feeds future trusted package/runtime integration and the exhaustive
`V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001` gate.

## Risks

- treating events as authorization would bypass independent gates;
- accepting free-form evidence would permit privacy leakage;
- adding timestamps or identifiers would create correlation and fingerprinting;
- persisting events here would cross runtime and retention ownership;
- validating lifecycle transitions here would duplicate lifecycle ownership;
- removing package-load blockers would claim integration that does not exist.
