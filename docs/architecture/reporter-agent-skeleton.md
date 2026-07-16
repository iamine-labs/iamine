# REPORTER-AGENT-001-SKELETON

## Objective

Define the second official P0 agent skeleton as a narrow, documentation-only
Privacy-Safe Support Reporter contract. It reserves a local-readonly support
report boundary without creating an executable package, report renderer,
evidence collector, export, runtime integration, agent code, or new IAMINE
command.

## Scope

This feature adds:

```text
docs/agents/reporter-agent-skeleton.md
docs/architecture/reporter-agent-skeleton.md
docs/qa/reporter-agent-skeleton.md
```

It marks `REPORTER-AGENT-001-SKELETON` as `ACTIVE` in the v0.12.0 roadmap
state table.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, Cargo
manifests, generated manifests, runtime startup, agent execution, report
rendering, evidence collection, redaction implementation, persistence, export,
network transfer, sandboxing, workers, schedulers, queues, P2P, PubSub, model
gates, model loading, model downloads, inference, hardware profiling, CLI
behavior, `iamine-node support bundle`, registry storage, marketplace behavior,
installer, updater, rewards, wallet, settlement, mainnet, or distributed model
MoE behavior.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| P0 support-reporter skeleton boundary | `REPORTER-AGENT-001-SKELETON` | yes |
| Canonical package file placement | `AGENT-SKELETON-STANDARD-001` | no |
| Reporter template boundary | `AGENT-TEMPLATE-REPORTER-001` | no |
| Package manifest schema | `AGENT-PACKAGE-MANIFEST-001` | no |
| Scope, permissions, resources, audit, and eval schemas | v0.11.1 contracts | no |
| Runtime, sandbox, lifecycle, and handoff enforcement | v0.11.2 contracts | no |
| Existing support-bundle diagnostics | `USER-DIAGNOSTICS-SUPPORT-001` | no |
| Functional P0 Reporter implementation | `REPORTER-AGENT-001` | no |

## Integration Contract

The skeleton consumes the closed reporter template, package, scope, capability,
expertise, resource, permission, audit, boundary-eval, input/output, timeout,
handoff, out-of-scope, sandbox, and runtime-baseline contracts. It also
consumes v0.11.0 beta-pack selection for the support-reporter persona and
local-readonly product alignment.

It provides a bounded product-specific contract for a later functional Reporter
implementation. That later feature must own concrete evidence intake, source
provenance, redaction enforcement, report rendering, export policy, permission
enforcement, audit emission, sandbox integration, lifecycle wiring, and any CLI
or runtime surface it requires.

The skeleton must not treat `iamine-node support bundle` as an agent runtime
adapter. Any future use of its data requires a dedicated privacy-safe adapter
and does not authorize invoking the command, reading its raw output, exporting
its artifacts, or expanding its behavior.

## Invariants

- The only planned mode is `local_readonly`.
- No runtime execution, evidence collection, permission, audit, redaction, or
  sandbox claim is implied.
- The agent may format approved redacted evidence but may not fetch, verify,
  mutate, retain, export, publish, or transmit evidence.
- Reports must distinguish supplied evidence, missing evidence, and unsupported
  claims; unverified assertions cannot become diagnoses.
- No shell, arbitrary filesystem, network, process, service, VM, container,
  router, model, hardware-probe, export, or third-party contact action is in
  scope.
- Unknown, missing, contradictory, broad, unsafe, stale, unverifiable, or
  privacy-invasive metadata blocks progression by default.
- User confirmation cannot elevate a blocked action.
- Out-of-scope, ambiguous, dangerous, cross-domain, private-data,
  prompt-injected, role-confusion, and unsupported-claim requests must refuse,
  clarify, or hand off.
- The skeleton creates no code or executable package under `src/`.

## Compatibility

This contract is additive. It preserves CPU-only nodes, GPU nodes, macOS,
Linux, VMs, containers, mock workers, cgroups, constrained hosts, existing
support-bundle behavior, and all model eligibility gates.

It does not claim report delivery, hardware compatibility, backend availability,
scheduler priority, node trust, reputation, result validity, agent quality,
rewards, or distribution eligibility.

## Failure Policy

Future validation must block a Reporter package when any required contract is
absent or contradictory, an input is not operator-approved and redacted, its
scope exceeds local report formatting, its permissions exceed local read-only
review, it collects or exports evidence, it presents unsupported claims as
facts, its boundary tests are incomplete, or its evidence is unredacted.

## Required Future Validation

Before implementation, the functional feature must add positive capability,
negative capability, scope-boundary, permission-boundary, handoff, unsafe-
action, prompt-injection, role-confusion, privacy-redaction, unsupported-claim,
and local-only evals. It must separately demonstrate that no worker, P2P,
model load, download, inference, dynamic hardware probe, network listener,
evidence collection, report export, or persistent state is started as a side
effect.

## Recommendation

Keep this feature documentation-only. The next v0.12.0 skeleton feature is
`LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON`; it must remain separately owned
and must not expand this Reporter contract.
