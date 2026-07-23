# AGENT-RUNTIME-CORE-001

## State

```text
READY FOR MERGE REVIEW
branch: feature/agent-runtime-core-001
base: bcec6f5c806fae11cc40a9b3f049f3e029a512ec
base tree: a8d1650d41c75536f8720f12814408a77d2915c7
runtime behavior change: none
field QA: not required; passive in-memory foundation only
quality gate: PASS WITH WARNINGS
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

## Objective

Introduce the dedicated `iamine-agent-runtime` crate and stable ownership
boundaries required by the canonical v0.11.2 executable sequence. This feature
creates only a passive, fail-closed foundation. It does not read packages,
start processes, execute agents, or authorize later runtime features.

## Ownership

```text
iamine-agents
  owns package declarations, parsers, validators, and pure policy contracts

iamine-agent-runtime
  depends one way on iamine-agents
  owns future side-effect-capable agent runtime behavior

iamine-node
  unchanged; no runtime wiring is authorized
```

The crate is split into:

- `contract`: a passive typed reference to a declared package;
- `owner`: the explicit future runtime owner registry;
- `foundation`: a static blocked report while every owner remains unavailable.

No owner module may be implemented as a side effect of this feature.

## Public Contract

`DeclaredAgentPackage` borrows an `AgentPackageManifest`. It does not retain
values beyond that borrow, expose manifest fields, or represent review,
compatibility, authorization, loading, or execution evidence. Its Debug output
is redacted.

`inspect_runtime_foundation` accepts that passive reference and returns a
`RuntimeFoundationReport` with:

```text
status: Blocked
package access available: false
execution available: false
future runtime owners: 15, all Unavailable
```

The owner list follows the remaining canonical implementation order:

```text
package reference resolver
package review evidence
runtime compatibility
input/output enforcement
sandbox enforcement
execution lifecycle
timeout/cancel enforcement
handoff enforcement
out-of-scope response enforcement
routing candidate selection
audit event enforcement
execution authorization
package-load evidence integration
package loader
runtime executor
```

The report is structural visibility only. It is not package-load evidence and
must not be consumed as authorization.

## Security And Privacy

- No filesystem, network, process, environment, hardware, model, or secret
  access is permitted.
- No package field may appear in Debug output.
- No caller boolean or free-form string may become trusted evidence.
- Unknown or unavailable owners remain blocked.
- The existing `iamine-agents` package-load report remains unchanged and
  always blocked.

## Explicitly Out Of Scope

- package path resolution, containment, symlink policy, or file reads;
- local registry, language, dependency, or human-review evidence;
- runtime or resource compatibility;
- input/output enforcement or persistence;
- sandboxing, process spawning, environment construction, or cleanup;
- lifecycle transitions, timers, cancellation, or handoff dispatch;
- candidate routing or scheduler integration;
- audit enforcement, final authorization, package loading, or execution;
- `iamine-node`, worker, controller, P2P, PubSub, model, inference, installer,
  service, rewards, reputation, wallet, marketplace, or public beta behavior.

## Integration Sequence

```text
AGENT-BOUNDARY-EVAL-VALIDATOR-001
-> AGENT-RUNTIME-CORE-001
-> AGENT-PACKAGE-REFERENCE-RESOLVER-001
-> AGENT-PACKAGE-REVIEW-EVIDENCE-001
-> AGENT-RUNTIME-COMPATIBILITY-GATE-001
```

Later features may add owner modules to this crate one at a time. They must not
remove package-load blockers or imply execution until their independent
evidence integration features are authorized.

## Risks

- Adding process or package I/O here would bypass the resolver and sandbox
  owners.
- Treating a typed manifest as trusted evidence would create a fail-open path.
- Defining lifecycle transitions now would preempt their authoritative owner.
- Wiring the crate into `iamine-node` would create untested runtime behavior.
- Combining the future owners in one file would create a new monolith.

## Success Criteria

- The new crate is a workspace member with a one-way dependency on
  `iamine-agents`.
- Public types are explicit and the report has no positive state.
- All 15 future owners are unique, ordered, and unavailable.
- Debug output does not reveal package declarations.
- Existing package-load behavior remains blocked.
- No runtime side effect or node integration exists.
- Focused tests, workspace formatting, Clippy, and architecture guards pass.
