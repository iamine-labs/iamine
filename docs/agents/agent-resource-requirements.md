# IAMINE Agent Resource Requirements

Feature:

```text
AGENT-RESOURCE-REQUIREMENTS-001
```

## Purpose

Define how an IAMINE agent package declares reviewable resource requirements
without authorizing execution, scheduler placement, worker startup, hardware
profiling, model loading, backend selection, reputation, reward, marketplace
behavior, or distributed model MoE.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, permission enforcement, sandboxing, registry
publication, marketplace publication, third-party agents, or public beta
launch.

## Metadata Contract

Resource requirements answer a narrow review question:

```text
What bounded resources should reviewers expect this agent package to need?
```

They do not answer:

- whether the task is in scope;
- whether a permission is allowed;
- whether the runtime can execute the agent;
- whether a node is compatible;
- whether the scheduler should pick the agent;
- whether a worker should start;
- whether a model backend is available;
- whether a model should be loaded or downloaded;
- whether the agent is trusted or reputable.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.resources.draft-0.1
```

The default skeleton path is:

```text
metadata/agent-resources.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Resource requirements schema identifier. |
| `package_id` | yes | Package that owns the resource metadata. |
| `resource_profile_id` | yes | Stable resource profile identifier. |
| `resource_profile_version` | yes | Version for review and upgrades. |
| `operating_modes` | yes | Bounded mode-specific resource declarations. |
| `cpu` | yes | CPU expectations by mode. |
| `memory` | yes | Memory expectations by mode. |
| `storage` | yes | Storage expectations by mode. |
| `network` | yes | Network expectations by mode. |
| `model_dependencies` | yes | Declarative model dependency expectations. |
| `accelerators` | yes | Optional accelerator expectations. |
| `constraints` | yes | Explicit resource and runtime constraints. |
| `degradation` | yes | Reduced-capability behavior. |
| `privacy` | yes | Privacy limits for resource metadata. |
| `review` | yes | Human review requirements. |

## Example Shape

```toml
schema = "iamine.agent.resources.draft-0.1"
package_id = "iamine.beta.node-doctor"
resource_profile_id = "node_doctor_local_readonly_resources"
resource_profile_version = "0.1.0"

operating_modes = ["local_readonly"]

[cpu.local_readonly]
min_logical_cores = 1
recommended_logical_cores = 2
max_background_threads = 1

[memory.local_readonly]
min_ram_mb = 256
recommended_ram_mb = 512
max_working_set_mb = 512

[storage.local_readonly]
package_size_mb = 20
temp_workspace_mb = 64
cache_budget_mb = 0

[network.local_readonly]
mode = "none"
opens_ports = false
downloads_artifacts = false

[model_dependencies]
requires_model_download = false
requires_model_load = false
backend_class = "none"

[accelerators]
required = "none"
optional = []

[constraints]
runs_dynamic_hardware_probe = false
allows_unrestricted_filesystem = false
starts_worker = false
overrides_scheduler = false

[degradation]
on_insufficient_resources = "require_human_review"
```

This example is not executable. It only shows the intended metadata shape.

## Required Alignment

Resource requirements must align with:

- `agent.yaml`;
- `agent-scope.toml`;
- `metadata/agent-capabilities.toml`;
- `metadata/agent-expertise.toml`;
- `metadata/agent-permissions.toml` once defined;
- `evals/agent-boundary-tests.toml` once defined;
- `review/human-review.md`.

If resource requirements contradict package identity, scope, capabilities,
expertise, permissions, audit, eval requirements, or human review requirements,
install, registry admission, and execution must remain blocked.

## Blocked Resource Claims

Resource requirements must not claim:

- scheduler priority;
- node compatibility;
- backend availability;
- model admission;
- model download authorization;
- model load authorization;
- worker startup authorization;
- hardware profile mutation;
- dynamic hardware probing;
- permanent hardware fingerprinting;
- GPU reward eligibility;
- trust, reputation, certification, or reward eligibility;
- public marketplace publication;
- third-party publication;
- distributed model MoE.

## Privacy Rules

Resource requirements must not include:

- credentials;
- private keys;
- wallet keys;
- usernames;
- full hostnames;
- IP addresses;
- MAC addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- private paths;
- raw hardware inventories;
- permanent hardware fingerprints;
- unredacted logs.

## Review Requirements

Human review must confirm:

- requirements are bounded and unit-explicit;
- operating modes align with package, scope, capability, and expertise
  metadata;
- network requirements do not enable remote execution;
- model dependency declarations do not bypass model gates;
- accelerator expectations do not imply compatibility or rewards;
- degradation behavior blocks or requires review;
- metadata does not expand scope, permissions, scheduling, compatibility,
  trust, reputation, rewards, or execution;
- next required contracts remain pending where applicable.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-PERMISSION-MODEL-001
```
