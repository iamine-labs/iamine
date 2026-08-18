# NODE-LOCAL-CONTROL-API-CONTRACT-001

## Status

```text
feature: NODE-LOCAL-CONTROL-API-CONTRACT-001
state: READY FOR MERGE REVIEW
base: origin/develop at 1170c4a67996d97f757fc18950bfebe4f2ea24e5
branch: feature/node-local-control-api-contract-001
runtime behavior change: none
server created or bound: no
dashboard connectivity enabled: no
field QA: not required at this contract-only boundary
```

## Purpose

Define the local transport boundary that will carry the shared GUI/CLI
contracts without implementing a server, a browser session, authorization,
audit persistence, owner dispatch, or dashboard connectivity.

The contract is an additive module in `iamine-core`. It validates transport
observations supplied by a future adapter and emits typed authorization and
audit requirements. A validated request is still only intent; it never grants
permission or invokes an owner.

## Transport Profile

The first Local Control API profile is JSON over loopback HTTP. It accepts only:

```text
transport: loopback HTTP
peer: IPv4 loopback or IPv6 loopback
method: POST
media type: application/json
operation endpoint: /api/v1/operations
browser origin: same origin
native origin: explicitly classified as non-browser
request limit: 64 KiB encoded body
response limit: 512 KiB encoded body
```

A future server must bind explicit loopback addresses. A wildcard bind,
non-loopback peer, cross-origin browser request, missing browser origin,
unreviewed method, route, or media type fails closed. Operation identity
comes from `InterfaceOperation`; a URL path, CLI string, or frontend label must
not become a second operation registry.

Ingress classifications are trusted adapter observations, not fields accepted
from the wire envelope. The future adapter owns conversion from socket and HTTP
metadata into these closed enums.

## Wire Envelopes

`LocalControlRequest<T>` carries only:

```text
schema_version
request_id
interface: InterfaceRequest<T>
```

`LocalControlResponse<T>` carries only:

```text
schema_version
request_id
interface: InterfaceResponse<T>
```

Both schemas are currently `1.0.0`, reject unknown fields, and reject an
unsupported version. Request IDs are non-null canonical lowercase hyphenated UUIDs used
for correlation and audit handoff; they are not credentials or replay proofs.
A response must preserve the request ID and operation. Generic payloads remain
owner-reviewed, redacted projections under the shared-contract rules.

The envelope intentionally has no token, hostname, peer address, filesystem
path, prompt, backend error string, or authorization result.

## Handoff Rules

Successful ingress validation produces `LocalControlValidatedRequest` and a
`LocalControlAuthorizationHandoff`. Both return `authorizes_action() == false`.

| Operation class | Authorization requirement | Replay requirement | Audit requirement |
| --- | --- | --- | --- |
| Read-only diagnostic | Verified local read-only session | None beyond request correlation | Request decision |
| Read-only operational | Verified local read-only session | None beyond request correlation | Request decision |
| Planned mutation | Explicit planned-mutation authorization | Single-use authorization evidence | Request decision and authorization |
| Runtime mutation | Explicit runtime-mutation authorization | Single-use authorization evidence | Request decision and authorization |
| Agent operation | Agent runtime authority | Single-use authorization evidence | Request decision and authorization |

This feature defines requirements but does not satisfy them. In particular:

- loopback reachability is not authentication;
- same-origin validation is not operator authorization;
- a request UUID is not a replay token;
- a permission-request event is not a grant;
- API validation is not owner policy validation;
- no mutation or agent operation can run from this contract alone.

`DASHBOARD-LOCAL-AUTHORIZATION-001` owns local session and authorization
evidence. Agent operations remain subject to `iamine-agents` and
`iamine-agent-runtime`; dashboard authorization cannot replace those owners.
The future API adapter must hand request decisions and authorization outcomes
to the reviewed audit owner instead of logging payloads itself.

## Error Contract

Transport failures map to stable shared problems without echoing transport or
payload details:

| Local failure | Shared problem |
| --- | --- |
| Unsupported API schema | `unsupported_schema` |
| Non-loopback transport/peer or rejected origin | `policy_blocked` |
| Invalid UUID, method, media type, request size, or operation mismatch | `invalid_request` |
| Oversized owner response | `internal_failure` |

The adapter may render a bounded generic summary from these codes. It must not
include IP addresses, origins, headers, credentials, raw payloads, prompts,
paths, or backend errors.

## Threat Model

| Threat | Required control |
| --- | --- |
| LAN or remote caller reaches the API | Explicit loopback bind and peer validation; no wildcard listener. |
| Hostile website sends a localhost request | Same-origin browser validation plus future local session authorization. |
| Local process reaches loopback | Treat locality as transport evidence only; require local authorization. |
| Request replay triggers a state change | Single-use authorization evidence for planned, runtime, and agent operations. |
| Oversized payload or response consumes memory | Enforce encoded byte limits before decode and before send. |
| Frontend becomes a policy engine | Use shared operation classes and owner decisions; no TypeScript authorization logic. |
| Adapter becomes a command bridge | Dispatch only reviewed typed operations; never shell out or accept arbitrary commands. |
| Diagnostics leak private data | Owner redaction, bounded typed problems, reviewed payload schemas, no raw logs. |
| Contract drift causes ambiguous behavior | Strict versions, unknown-field rejection, operation correlation, frozen JSON tests. |

## Integration Boundary

```text
socket and HTTP metadata
-> Local Control API adapter classification
-> local transport contract validation
-> local authorization handoff
-> owner policy and operation dispatch
-> redacted shared response
-> response-size validation and audit handoff
-> dashboard adapter
```

Only the first three contractual steps exist after this feature, and none bind
a port. `iamine-node/src/main.rs`, P2P, PubSub, scheduler, workers, models,
inference, hardware execution, agent execution, and dashboard code remain
unchanged.

## Out of Scope

This feature does not:

- choose or add an HTTP framework;
- bind a listener or publish a remote endpoint;
- define cookies, tokens, login UI, session storage, or CSRF implementation;
- generate TypeScript DTOs or connect the dashboard;
- implement authorization, replay storage, or audit persistence;
- invoke owners or change existing CLI output;
- authorize read, mutation, lifecycle, or agent execution.

## Acceptance Criteria

- local transport, peer, origin, method, media type, and byte limits are typed;
- request and response envelopes are strict, versioned, and correlated;
- unknown fields, incompatible versions, non-loopback access, and oversized
  bodies fail closed;
- every operation class reaches an explicit authorization/replay/audit handoff;
- validation and events cannot authorize an action;
- errors remain bounded and privacy-safe;
- no server, runtime, dashboard, CLI, or owner behavior changes;
- production and test modules remain below the 750-line review threshold.

## Validation Evidence

Focused validation passes with 43 existing `iamine-core` unit tests, 10 shared
interface-contract tests, and 9 new Local Control API contract tests. The new
tests freeze the wire envelope and endpoint, strict schema and UUID behavior,
IPv4/IPv6 loopback handling, browser/native origin rules, method/media/route
and byte limits, operation-class handoffs, non-authorizing behavior, error
redaction, and response correlation.

`./scripts/quality-gate.sh` passes with zero required failures and zero new
warnings. The workspace has 1,157 passing tests after adding the 9 contract
tests; workspace Clippy passes with historical warnings outside this feature.
`cargo audit`, `cargo deny`, and `gitleaks` are unavailable and reported as
skipped. The production module is 378 lines and its integration test module is
321 lines. `main.rs` and `cluster_registry.rs` have zero-line deltas.

No Mac runtime smoke, TS140, or Proxmox field QA is required because this
feature does not bind a listener, connect the dashboard, dispatch an owner, or
change runtime behavior. Field QA becomes mandatory when
`NODE-LOCAL-CONTROL-API-001` creates the real server and dispatch path.

## Next Feature

`DASHBOARD-LOCAL-AUTHORIZATION-001` will define and implement the local session,
authorization evidence, denial semantics, replay proof ownership, and audit
integration needed before the real Local Control API can dispatch operations.
