# LAN Inference Beta Contract

Feature:

```text
LAN-INFERENCE-BETA-CONTRACT-001
```

## Purpose

The LAN inference beta is the first installable IAMINE product line. It proves
that an operator can run a bounded local network of IAMINE nodes, select an
approved model, execute real inference on a LAN worker, and recover from common
local failures with actionable diagnostics.

This contract defines the supported topology, user flows, failure semantics,
release boundaries, and QA evidence required by later Milestone 1 features. It
does not implement new runtime behavior.

## Supported Topology

The beta topology is deliberately small:

- one operator-controlled LAN;
- one controller or client node;
- one to five worker nodes;
- optional relay behavior only when it is already supported by the existing
  node mode;
- macOS or Linux controller;
- Linux workers, including TS140 and Proxmox/R5500 guests;
- CPU-only, GPU, VM, container, and constrained hosts when their gates report a
  compatible and available backend.

Unsupported in this beta:

- WAN discovery;
- NAT traversal;
- public peer admission;
- multi-operator identity trust;
- payments, rewards, staking, or slashing;
- automatic model marketplace discovery;
- silent model downloads or hidden worker startup.

## User Flows

### 1. Diagnose The Local Node

The operator must be able to inspect local readiness before starting inference.
Later `LAN-NODE-DOCTOR-001` work owns the final diagnostic CLI, but it must
reuse existing gates rather than invent parallel policy:

- hardware profile visibility;
- approved model catalog and compatibility;
- registry integrity;
- license policy and local license acceptance;
- network policy;
- backend availability;
- worker startup policy;
- metrics availability;
- LAN peer and PubSub readiness when network checks are requested.

Diagnostics must not start workers, P2P, PubSub, model downloads, model loads,
or inference unless the user explicitly invokes a runtime command.

### 2. Select An Approved Model

The supported model path is:

```text
models catalog
-> models select
-> models license accept <model_id> --yes, when required
-> models download <model_id>, when gates permit
```

Selection must remain explainable. A requested model must not be silently
replaced by a fallback. New download attempts must stop before network access
when any admission gate blocks.

### 3. Start Workers Explicitly

Workers must start only through explicit operator action. Startup must report:

- worker port;
- backend mode;
- skip-model-load state;
- legacy CPU daemon mode;
- real inference availability;
- advertised task and model capability state;
- metrics status or fallback.

Mock workers may advertise simple tasks, but must not claim real LLM execution.
Workers with unavailable real backends may continue degraded only when policy
permits and must expose that degraded state.

### 4. Run LAN Inference

The beta inference flow must keep the existing assignment and result
acceptance rules:

```text
controller request
-> compatible worker selection
-> assigned worker executes
-> result accepted only from assigned worker
-> final outcome emitted
```

Real inference is permitted only when the model is registered, trusted,
licensed or accepted, hardware-compatible, backend-available, network-allowed,
locally available, and executable by the worker.

### 5. Observe And Recover

The operator must receive enough evidence to distinguish:

- no worker discovered;
- worker discovered but not ready;
- model not installed;
- model blocked by policy;
- license acceptance required;
- hardware incompatible;
- backend unavailable;
- metrics unavailable but non-blocking;
- timeout before first progress;
- stalled progress;
- worker disconnect;
- duplicate, late, or wrong-worker result;
- successful retry or fallback;
- final failure after bounded recovery.

## Failure Semantics

Failures must be explicit and bounded:

| Failure | Required behavior |
| --- | --- |
| No LAN peer | Report no compatible worker; do not fake readiness. |
| No PubSub subscriber | Wait only within configured readiness bounds. |
| Model policy blocked | Stop before download, load, or execution. |
| License acceptance required | Print the acceptance next step. |
| Model absent locally | Report missing local model for execution; do not auto-download unless the command explicitly owns download. |
| Backend unavailable | Advertise degraded state and reject real inference. |
| Mock backend | Preserve simple task support but reject real LLM execution. |
| Metrics bind unavailable | Continue only with explicit metrics fallback evidence. |
| Worker disconnect | Retry or fail within bounded policy; keep trace evidence. |
| Late result | Record late result without converting a completed failure into success unless the active attempt policy permits it. |
| Wrong worker result | Reject the result and preserve the assigned worker rule. |
| Duplicate result | Reject after the first accepted result. |

## Privacy And Security

The LAN beta must not collect, persist, log, or commit:

- usernames;
- home directories;
- full hostnames;
- MAC addresses;
- IP addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- user process lists;
- personal paths;
- permanent hardware fingerprints;
- wallet keys, private keys, tokens, secrets, or credentials.

SSH aliases may appear in QA instructions. Resolved addresses and credentials
must stay in local SSH configuration and out of repository files.

## Release Boundaries

The LAN beta is v0.8. It is not IAMINE v1 and it is not a private or public
testnet.

The release gate for `LAN-INFERENCE-BETA-001` must prove:

- clean install or upgrade path;
- one to five workers;
- real inference over LAN;
- diagnostics before runtime;
- reboot recovery;
- worker stop, restart, and readiness recovery;
- bounded failure recovery;
- rollback path;
- privacy-safe support evidence.

The beta may use existing local trust assumptions. Public identity,
authenticated WAN transport, node admission, and economic policy belong to
later milestones.

## Ownership Rules

Later LAN beta features must keep ownership narrow:

- diagnostics in an `iamine-node` diagnostics module;
- worker lifecycle in worker runtime owner modules;
- configuration schema and migration in configuration owner modules;
- metrics allocation in metrics owner modules;
- inference CLI wiring in CLI and dispatch modules only;
- resilience behavior in existing runtime, scheduler, network, and model owner
  modules.

Do not add new domain behavior to `iamine-node/src/main.rs` except wiring.
Do not grow `iamine-node/src/cluster_registry.rs` without an extraction plan.

## QA Matrix

This contract is documentation-only, so field QA is not required for this
feature.

Later Milestone 1 runtime or worker changes require:

- Mac local validation;
- TS140 validation;
- Proxmox/R5500 validation when worker, runtime, scheduler, broadcast,
  inference execution, capabilities, cluster status, or hardware profiling
  behavior changes.

Minimum validation before release:

```text
models catalog/select/download preflight
hardware inspect/show/refresh where relevant
cluster status human and JSON
worker startup ready/degraded evidence
LAN inference success
worker restart recovery
bounded failure and retry evidence
quality gate
```

## Success Criteria

`LAN-INFERENCE-BETA-CONTRACT-001` is satisfied when this contract is merged to
`develop`, the roadmap marks Milestone 0 closed, and the next Milestone 1
feature can use this document as its Architecture input.
