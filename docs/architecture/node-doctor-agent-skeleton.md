# NODE-DOCTOR-AGENT-001-SKELETON

## Objective

Define the first official P0 agent skeleton as a narrow, documentation-only
Node Doctor contract. It reserves the local-readonly readiness-report boundary
without creating an executable package, runtime integration, agent code, or
new IAMINE command.

## Scope

This feature adds:

```text
docs/agents/node-doctor-agent-skeleton.md
docs/architecture/node-doctor-agent-skeleton.md
docs/qa/node-doctor-agent-skeleton.md
```

It originally added the v0.12.0 state table. The roadmap now records
`NODE-DOCTOR-AGENT-001-SKELETON` as `CLOSED`, which closes only the planning
contract and does not authorize execution or user availability.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, Cargo
manifests, generated manifests, runtime startup, agent execution, sandboxing,
workers, schedulers, queues, P2P, PubSub, model gates, model loading, model
downloads, inference, hardware profiling, persistence, networking, CLI
behavior, `iamine-node lan doctor`, registry storage, marketplace behavior,
installer, updater, rewards, wallet, settlement, mainnet, or distributed model
MoE behavior.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| P0 Node Doctor skeleton boundary | `NODE-DOCTOR-AGENT-001-SKELETON` | yes |
| Canonical package file placement | `AGENT-SKELETON-STANDARD-001` | no |
| Package manifest schema | `AGENT-PACKAGE-MANIFEST-001` | no |
| Scope, permissions, resources, audit, and eval schemas | v0.11.1 contracts | no |
| Runtime, sandbox, lifecycle, and handoff enforcement | v0.11.2 contracts | no |
| Existing LAN diagnostic command | `LAN-NODE-DOCTOR-001` | no |
| Functional P0 Node Doctor implementation | `NODE-DOCTOR-AGENT-001` | no |

## Integration Contract

The skeleton consumes the closed package, scope, capability, expertise,
resource, permission, audit, boundary-eval, input/output, timeout, handoff,
out-of-scope, sandbox, and runtime-baseline contracts. It also consumes the
v0.11.0 beta-pack selection for persona and local-readonly product alignment.

It provides a bounded product-specific contract for a later functional Node
Doctor implementation. That later feature must own package artifacts, concrete
input adaptation, redaction, permission enforcement, audit emission, sandbox
integration, lifecycle wiring, and any CLI or runtime surface it requires.

The functional feature remains blocked until the manifest parser, package load
gate, executable runtime lifecycle, scope enforcement, permission enforcement,
audit events, sandbox, handoff, out-of-scope response, and dedicated Node
Doctor evidence provider all have implementation and validation evidence.

The skeleton must not treat the existing `iamine-node lan doctor` CLI as an
agent runtime adapter. Any future use of its data requires a dedicated,
privacy-safe adapter and does not authorize invoking the command, reading its
raw output, or expanding its behavior.

## Invariants

- The only planned mode is `local_readonly`.
- No runtime execution, permission, audit, or sandbox claim is implied.
- No shell, arbitrary filesystem, network, process, service, VM, container,
  router, model, or hardware-probe action is in scope.
- Node Doctor may explain approved redacted readiness evidence but may not
  create, fetch, mutate, or retain that evidence.
- Unknown, missing, contradictory, broad, unsafe, stale, unverifiable, or
  privacy-invasive metadata blocks progression by default.
- User confirmation cannot elevate a blocked action.
- Out-of-scope, ambiguous, dangerous, cross-domain, private-data, and
  prompt-injected requests must refuse, clarify, or hand off.
- The skeleton creates no code or executable package under `src/`.

## Compatibility

This contract is additive. It preserves CPU-only nodes, GPU nodes, macOS,
Linux, VMs, containers, mock workers, cgroups, constrained hosts, existing
Node Doctor CLI behavior, and all model eligibility gates.

It does not claim hardware compatibility, backend availability, scheduler
priority, node trust, reputation, result validity, agent quality, rewards, or
distribution eligibility.

## Failure Policy

Future validation must block a Node Doctor package when any required contract
is absent or contradictory, its scope exceeds readiness reporting, its
permissions exceed local read-only review, its inputs include private data, its
outputs claim a mutation, its boundary tests are incomplete, or its evidence is
unredacted.

## Required Future Validation

Before implementation, the functional feature must add positive capability,
negative capability, scope-boundary, permission-boundary, handoff, unsafe-
action, prompt-injection, role-confusion, privacy-redaction, and local-only
evals. It must separately demonstrate that no worker, P2P, model load,
download, inference, dynamic hardware probe, network listener, or persistent
state is started as a side effect.

## Recommendation

Keep this feature documentation-only. The next v0.12.0 skeleton feature is
`REPORTER-AGENT-001-SKELETON`; it should remain separately owned and must not
expand this Node Doctor contract.
