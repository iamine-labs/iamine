# DASHBOARD-LOCAL-AUTHORIZATION-001

## Status

```text
feature: DASHBOARD-LOCAL-AUTHORIZATION-001
state: READY FOR MERGE REVIEW
base: origin/develop at 0ecf6d16d6078923a07964d477692eae5e67b756
branch: feature/dashboard-local-authorization-001
owner: iamine-core
runtime behavior changed: no
server or port created: no
dashboard connectivity enabled: no
field QA: not required at this in-process contract boundary
```

## Purpose

Provide the local session and authorization gate required between validated
Local Control API ingress and future owner dispatch. The gate issues opaque
operator-local capabilities, makes explicit allow or deny decisions, records
bounded replay state, and emits privacy-safe audit handoffs.

This feature does not authenticate through HTTP, bind a listener, call an
owner, mutate runtime state, or execute an agent. Local authorization is one
required gate; it never replaces owner policy or agent-runtime authority.

## Ownership

The implementation lives in the modular
`iamine-core/src/dashboard_local_authorization/` owner:

| File | Responsibility |
| --- | --- |
| `authority.rs` | Session lifecycle, decision evaluation, replay state, and evidence consumption |
| `types.rs` | Policy, opaque capabilities, denial codes, evidence, consumption, and audit handoffs |
| `mod.rs` | Public owner exports only |

`iamine-node/src/main.rs`, `cluster_registry.rs`, dashboard TypeScript, P2P,
PubSub, scheduler, workers, models, inference, hardware execution, and agent
runtime remain unchanged.

## Authorization Flow

```text
trusted local lifecycle
-> LocalSessionIssuer capability
-> LocalSessionEvidence
-> validated Local Control API request
-> LocalAuthorizationAuthority::decide
-> approved decision plus audit OR typed denial plus audit
-> LocalAuthorizationAuthority::consume
-> LocalAuthorizationConsumption plus both audit handoffs
-> future owner policy gate
```

The session issuer, session evidence, authorization evidence, and consumption
are Rust capabilities with private identity fields. They are not serializable,
cloneable bearer tokens and are redacted in `Debug`. The future Local Control
API adapter must keep them in server-owned state and associate browser/native
session transport with that state without placing these capabilities in JSON.

## Policy And Time

`LocalAuthorizationPolicy` requires explicit values for:

- session lifetime in monotonic ticks;
- authorization-evidence lifetime in monotonic ticks;
- maximum retained sessions;
- maximum retained replay records.

Evidence lifetime cannot exceed session lifetime. Session and replay capacity
cannot exceed the compile-time limits of 32 sessions and 4,096 replay records.
Zero values, oversized capacities, arithmetic overflow, and a regressing clock
fail closed.

The caller owns conversion from its monotonic clock to ticks. Wall-clock time,
time zones, usernames, hostnames, addresses, or process identity are not part
of the contract.

## Session Boundary

`LocalAuthorizationAuthority::new_operator_local` creates an authority and a
separate `LocalSessionIssuer`. Only an issuer bound to that exact authority can
issue or revoke sessions. A session is bound to:

- its operator-local authority identity;
- an opaque session identity;
- a browser-dashboard or local-native classification;
- issue and expiry ticks.

Foreign, unknown, expired, and revoked sessions deny requests. A session alone
never authorizes an operation, is never persisted, and is not a wire token.

## Decision Rules

The authority recomputes the canonical
`LocalControlAuthorizationHandoff::for_operation` before every decision. A
contradictory or manually assembled handoff is denied before evidence exists.

| Operation class | Accepted intent | Replay behavior |
| --- | --- | --- |
| Read-only diagnostic | `proceed` or explicit `confirm` | Request correlation only |
| Read-only operational | `proceed` or explicit `confirm` | Request correlation only |
| Planned mutation | Explicit `confirm` | Request ID retained as single-use replay evidence |
| Runtime mutation | Explicit `confirm` | Request ID retained as single-use replay evidence |
| Agent operation | Explicit `confirm` | Request ID retained; agent runtime authority still required |

`deny` always produces a typed denial. A mutation attempted with `proceed`
produces `confirmation_required`. Both denied and approved mutating request IDs
are retained until their session expires; retrying the same request ID is
`replay_detected` and requires a new request.

Replay storage is memory-only and bounded. Capacity exhaustion is an explicit
unavailable outcome and never evicts a live security record to make a request
succeed.

## Evidence Consumption

An approved decision carries request-, operation-, session-, authority-, and
expiry-bound evidence. `consume` accepts the complete approved decision rather
than detached evidence, verifies the decision audit handoff, and consumes the
single-use identity where required.

The resulting `LocalAuthorizationConsumption` states only that the local gate
was satisfied. It deliberately returns:

```text
local_gate_satisfied: true
authorizes_owner_action: false
agent_runtime_authorization_required: true for agent operations
```

Future owner dispatch must consume this object by value and still run every
owner-specific policy gate. No method in this feature performs dispatch.

## Denial Semantics

Stable denial codes distinguish:

- session authority mismatch, unknown session, expiry, and revocation;
- contradictory request contract;
- required confirmation;
- explicit operator denial;
- replay detection;
- bounded replay-capacity exhaustion.

They map only to shared, redacted `InterfaceProblem` codes and operator actions.
They do not echo payloads, session identity, origin, socket metadata, paths,
credentials, prompts, or backend errors.

## Audit Boundary

Session issuance, session revocation, request approval, request denial, and
evidence consumption each create a typed `LocalAuthorizationAuditHandoff`.
Request handoffs contain only correlation ID, canonical operation, decision,
safe denial code, and monotonic tick.

An audit handoff explicitly reports:

```text
persisted: false
emitted: false
contains_payload: false
authorizes_action: false
```

The future API feature must deliver both decision and consumption handoffs to
the reviewed audit owner before dispatch. A handoff is not proof that audit
persistence already occurred.

## Agent Boundary

Local confirmation for `agent_permission`, `agent_execution`, or
`agent_cancellation` satisfies only the dashboard-local gate. It cannot grant
agent scope, permission, sandbox, lifecycle, routing, model, or execution
authority. `iamine-agents` and `iamine-agent-runtime` remain mandatory owners.

## Out Of Scope

This feature does not define or implement:

- HTTP server, port, cookie, header, CSRF mechanism, or browser storage;
- OS login, password, biometric, wallet, key, or remote authentication;
- dashboard API client or generated TypeScript types;
- operation-specific payloads;
- audit persistence or external event emission;
- owner dispatch, mutation, lifecycle, or agent execution;
- remote dashboard access.

## Acceptance Criteria

- sessions and issuers are authority-bound opaque capabilities;
- policy lifetimes and retained state are explicit and bounded;
- clock regression, overflow, expiry, revocation, and foreign evidence fail closed;
- mutating and agent requests require explicit confirmation;
- approved and denied mutating IDs cannot be replayed;
- evidence remains attached to decision audit through consumption;
- all audit surfaces are typed, redacted, and non-authorizing;
- agent local approval cannot replace agent runtime authority;
- no server, frontend, runtime, CLI, or owner behavior changes;
- production modules remain below the 750-line review threshold.

## Validation

Focused format, 73 `iamine-core` tests, and strict Clippy pass. The complete
quality gate passes with 1,168 workspace tests, zero required failures, zero
new warnings, and historical workspace Clippy warnings outside this feature.
`cargo audit`, `cargo deny`, and `gitleaks` are unavailable and reported as
skipped.

Fresh reconciliation confirms `origin/develop` remains at the exact feature
base and `origin/main` contains no commits missing from `develop`.

## Next Feature

`NODE-LOCAL-CONTROL-API-001` may implement the loopback server only after it
defines a server-owned browser/native session transport, delivers both audit
handoffs to the audit owner, consumes local authorization before dispatch, and
preserves every owner gate documented here.
