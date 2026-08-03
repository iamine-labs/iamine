# GUI-CLI-SHARED-CONTRACTS-001

## Status

```text
feature: GUI-CLI-SHARED-CONTRACTS-001
state: LOCAL VALIDATION PASSED
base: origin/develop at 1409b6fa9cb780d00fb840503c16f83bd35c0405
branch: feature/gui-cli-shared-contracts-001
runtime behavior change: none
dashboard application created: no
field QA: not required at this boundary
```

## Purpose

Establish the first shared, typed boundary that the CLI adapter, future Local
Control API, and dashboard adapter will consume. The contract lives in
`iamine-core`; it does not invoke owners, bind a server, authorize an action,
start a worker, or render a UI.

Rust types are the source of truth. A later API-contract feature will approve
transport schemas and a generated TypeScript representation. This feature does
not create a hand-maintained frontend DTO or a second policy engine.

## Contract Surface

`iamine-core/src/interface_contracts.rs` defines:

| Type | Responsibility |
| --- | --- |
| `InterfaceRequest<T>` | Versioned operation intent plus an owner-defined typed payload. |
| `InterfaceResponse<T>` | Versioned operation result with no implicit success fallback. |
| `InterfaceOperation` | Closed operation ID and its validated operation class. |
| `InterfaceOutcome<T>` | `success`, `attention`, `blocked`, `unavailable`, `stale`, or `unknown`. |
| `InterfaceProblem` | Stable problem code and bounded operator action; no backend text. |
| `InterfaceWarnings` | Bounded typed warnings that cannot change the outcome variant. |
| `InterfaceProvenance` | Owner/mock source, evidence scope, redaction declaration, and authority. |
| `InterfaceEvent` | Versioned stream identity, ordering sequence, and typed lifecycle payload. |

The initial operation IDs map to the architecture classes as follows:

```text
read_only_diagnostic -> node evidence, hardware, node config, node identity
read_only_operational -> cluster status, task stats, task trace, model catalog
planned_mutation -> support bundle, config, identity, and hardware plans
runtime_mutation -> worker lifecycle
agent_operation -> permission, execution, and cancellation intents
```

The operation class is derived from the operation ID when constructed. A
deserialized payload with a contradictory class is rejected rather than
silently reclassified.

## Outcome and Privacy Rules

- `Success`, `Attention`, and `Stale` carry owner data.
- `Blocked`, `Unavailable`, and `Unknown` carry a typed problem and no data.
- A warning is a bounded enum pair, not arbitrary backend text.
- The maximum warning count is eight per outcome.
- Problems contain no prompt, path, hostname, peer address, credential, or raw
  backend message.
- `OwnerModule` provenance is authoritative only when it is declared as such;
  `MockFixture` provenance is always non-authoritative.
- Unknown or incompatible schema versions fail deserialization and must be
  mapped by a future adapter to an explicit unavailable/error state.

The generic payload `T` remains the responsibility of the owner module. It
must be a reviewed redacted projection; this contract does not make an unsafe
payload safe by wrapping it.

## Event Rules

Events are identified by `(stream, sequence)` and are safe to drop or replay.
The initial payloads cover snapshot reconciliation, operation lifecycle,
rejection, and permission-request signals. `InterfaceEvent::authorizes_action`
is permanently false: a permission-request event is not a grant, and a
completion event is not authorization for a later action.

The dashboard will consume events only through the future Local Control API.
It must not tail NDJSON files or subscribe directly to P2P/PubSub topics.

## Integration Boundary

```text
owner module
-> iamine-core shared contract
-> future reviewed Local Control API schema
-> generated TypeScript transport types
-> dashboard adapter and view models
```

This feature intentionally leaves the CLI, Local Control API, dashboard
application, service lifecycle, local authorization, and audit integration
unchanged. Those concerns remain in their roadmap features and cannot bypass
the shared contract boundary.

## Validation Evidence

Focused tests cover schema version rejection, operation/class consistency,
warning bounds, blocked-without-data semantics, mock provenance authority, and
non-authorizing ordered events. `cargo test -p iamine-core`, owner clippy,
workspace tests, the node build, and the repository guards pass.

The quality-gate summary recorded one transient isolated `iamine-node` failure
from the concurrent Node Doctor feature (`RuntimeRejected`); the exact test
rerun passed (`1 passed, 495 filtered out`) and the subsequent full workspace
run passed all `496` node tests. The unrelated baseline `iamine-network`
reload observation also passed in the serial gate run. No Proxmox or TS140
field QA is required because this change does not execute or alter node runtime
behavior.

## Next Feature

`NODE-LOCAL-CONTROL-API-CONTRACT-001` will define the localhost transport,
request validation, threat model, and audit handoff over these types. The
dashboard remains mock-only until that contract and local authorization close.
