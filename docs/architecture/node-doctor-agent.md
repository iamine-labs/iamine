# Node Doctor Agent Architecture

Feature:

```text
NODE-DOCTOR-AGENT-001
```

Current state:

```text
APPROVED FOR MERGE
PUSH AUTHORIZATION REQUIRED
```

## Purpose

Implement the first functional P0 official agent as a complete, bounded
vertical through the existing agent runtime. Node Doctor explains IAMINE node
readiness from the typed, redacted, read-only evidence provider and recommends
only non-destructive operator actions.

Package identity:

```text
package_id: iamine.beta.node-doctor
task_type: diagnostic_report
scope_id: node_readiness_diagnostic_report
mode: local_readonly
```

## Package And Runtime Integration

The reviewed package lives under:

```text
agents/official/node-doctor/
```

Its YAML manifest and seven referenced metadata documents are parsed by their
owner validators. Runtime loading requires typed equality for the manifest and
compares every policy-bearing reference byte-for-byte with the package snapshot
compiled into `iamine-node`. A caller-selected directory therefore cannot
change executable policy, scope, or review inputs after the executable was
built. Package README and review prose are not runtime authority.

The package declaration keeps `execution_authorized: false`. Package metadata
cannot self-authorize execution. The operator-local runtime independently
establishes package review, compatibility, input/output enforcement, sandbox,
lifecycle, timeout, scope, permission, routing, audit, execution authorization,
load evidence, package loading, official Rust program registration, execution,
and result verification.

## CLI Boundary

```bash
iamine-node agents node-doctor --package-root PATH [--json]
```

The command is dispatched before node identity, P2P, model, scheduler, worker,
or inference startup. The package path is mandatory while distribution remains
`local_dev` plus `manual_review`; installer and registry discovery belong to
later productization features.

The command consumes `collect_node_doctor_evidence()` directly. It never
invokes or parses `iamine-node lan doctor`.

## Output Contract

Schema:

```text
iamine.agent.node_doctor.output-0.1
```

The output contains only static category names, typed readiness states, static
reason codes, and one bounded next-step code. Complete evidence yields
`diagnostic_report`. Missing required evidence yields
`blocked_action_report` and requests operator review. Owner messages and detail
maps are never copied.

Categories:

```text
node_status
hardware_profile
configuration_status
model_readiness
peer_network_status
remote_inference_readiness
```

## Security And Privacy

The official program rejects any request that enables network access, shell
execution, child processes, or persistence. Policies deny by default and block
filesystem mutation, service mutation, model loading or download, network
discovery or mutation, VM/container changes, credentials, keys, host
identifiers, and unrestricted files.

The runtime reports sandbox-adapter activity explicitly and does not claim OS
isolation. That distinction must remain visible until a later platform sandbox
provides process-level isolation.

## Ownership And Compatibility

Implementation is split under:

```text
iamine-node/src/node_doctor_agent/
```

`main.rs` contains module wiring only. `cluster_registry.rs` is unchanged.
The additive `blocked_action_report` output class belongs to
`iamine-agent-runtime`; safe package identity and exact-reference comparison
are exposed by `PackageReviewSubject` without exposing reference content.

This feature does not change scheduler policy, P2P, PubSub, workers, models,
inference, hardware profiling, rewards, reputation, registry publication,
marketplace behavior, installer behavior, or public beta availability.

## Validation Gate

Required before merge review:

```text
package and metadata validation
nine boundary-eval classes
positive runtime execution
modified-package fail-closed behavior
missing-evidence blocked output and privacy redaction
complete iamine-node, runtime, and agents regressions
Mac, TS140, and four Proxmox/R5500 role smokes
zero process, HOME, network, scheduler, transport, and persistence side effects
quality gate and size guards
```

Feature-local QA may recommend `READY FOR ARCHITECTURE MERGE REVIEW`. It cannot
close v0.12.0; the milestone remains open until all functional P0 agents and
`V0.12.0-P0-OFFICIAL-AGENTS-MILESTONE-QA-001` are closed.

## Final Architecture Review

Exact runtime checkpoint:

```text
commit: 2349499c94209f2b82665289cc08abce84625ea5
tree: 2656459419d0a2bb68c07395998cd06dc0da1327
base: 3374e27f7b6b132b39c3e979af7a1a03cd5daf9b
field roles: 6 of 6 PASS
```

Architecture confirms that execution traverses the existing owner chain; the
package cannot self-authorize, altered policy-bearing files fail closed, and
the CLI returns before node-network startup. The feature introduces no
scheduler, transport, model, worker, persistence, or public-distribution side
effects. `main.rs` decreases by three lines and `cluster_registry.rs` is
unchanged.

Accepted non-blocking limits are explicit: distribution remains manual
`local_dev`, the caller supplies the package root, peer/network evidence is
passive, and the runtime reports a bounded sandbox adapter without claiming OS
isolation. Later installer, registry, discovery, or platform-sandbox features
must not be inferred from this checkpoint.

Recommendation:

```text
APPROVED FOR MERGE
```

Push and controlled merge still require Merge Owner authority.
