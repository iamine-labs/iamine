# AGENT-RESOURCE-REQUIREMENTS-001

## Objective

Define the first IAMINE agent resource requirements contract before runtime
placement, scheduling, worker execution, model loading, or hardware eligibility
decisions exist for agents.

## Scope

This feature adds:

```text
docs/agents/agent-resource-requirements.md
docs/architecture/agent-resource-requirements.md
docs/qa/agent-resource-requirements.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-RESOURCE-REQUIREMENTS-001` and aligns package manifest resource
references with the existing skeleton path `metadata/agent-resources.toml`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- agent skeleton generator;
- package manifest parser;
- scope manifest parser;
- capability metadata parser;
- expertise metadata parser;
- resource requirement parser;
- permission enforcement;
- sandboxing;
- audit log implementation;
- agent registry;
- router or scheduler behavior;
- worker startup or worker capability advertisement;
- model routing, model loading, backend selection, inference execution, or
  distributed inference behavior;
- hardware profiler behavior or hardware compatibility gates;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, installer, updater, rollback, reputation, reward, wallet,
  settlement, token, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts,
  dependencies, package generation, or schema generation.

## Resource Requirements Role

Resource requirements describe what an agent package declares it may need under
reviewable, bounded operating modes.

They answer:

```text
What minimum and recommended resources should reviewers expect this agent to need?
```

They must not decide:

- whether an agent is executable;
- whether a task is in scope;
- whether a permission is granted;
- whether a node is compatible;
- whether a scheduler should choose the agent;
- whether a worker should start;
- whether a backend is available;
- whether a model should be loaded or downloaded;
- whether an agent is trusted, reputable, certified, or rewarded;
- whether a distributed model, MoE shard, or expert router exists.

Those decisions remain owned by later roadmap features.

## Draft Schema

The initial documentation schema is:

```text
iamine.agent.resources.draft-0.1
```

Default file name inside the skeleton:

```text
metadata/agent-resources.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Top-Level Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Resource requirements schema identifier. |
| `package_id` | yes | Package that owns the resource declaration. |
| `resource_profile_id` | yes | Stable resource profile identifier. |
| `resource_profile_version` | yes | Reviewable resource contract version. |
| `operating_modes` | yes | Bounded mode-specific resource declarations. |
| `cpu` | yes | CPU expectations by mode. |
| `memory` | yes | Memory expectations by mode. |
| `storage` | yes | Storage expectations by mode. |
| `network` | yes | Network expectations by mode. |
| `model_dependencies` | yes | Declarative model dependency expectations. |
| `accelerators` | yes | Optional accelerator expectations. |
| `constraints` | yes | Explicit resource and runtime constraints. |
| `degradation` | yes | Allowed reduced-capability behavior. |
| `privacy` | yes | Privacy limits for resource metadata. |
| `review` | yes | Human review requirements and evidence links. |

## Field Rules

### `schema`

Allowed value for this feature:

```text
iamine.agent.resources.draft-0.1
```

Unknown schema versions must block install, registry admission, and execution.

### `package_id`

The package ID must match the package manifest. It must not contain local host
identity, credentials, secrets, or private paths.

### `resource_profile_id`

The resource profile ID must be stable, lowercase, and narrow.

Allowed example:

```text
node_doctor_local_readonly_resources
```

Blocked examples:

```text
any_hardware
all_nodes
gpu_required_for_everything
best_performance
mainnet_worker
```

### `operating_modes`

Operating modes must align with the package manifest, scope manifest, capability
metadata, and expertise metadata.

Allowed draft modes:

```text
local_readonly
local_planning
lan_readonly
```

Blocked modes for this phase:

```text
remote_execution
public_marketplace
third_party_publication
mainnet
wallet
reward
settlement
arbitrary_shell
unrestricted_filesystem
unrestricted_network
```

### `cpu`

CPU fields are declarative expectations only. They cannot start worker threads,
change runtime thread pools, or override scheduler decisions.

Allowed fields:

```text
min_logical_cores
recommended_logical_cores
max_background_threads
```

### `memory`

Memory fields must be bounded and unit-explicit. They cannot reserve memory or
alter runtime allocation behavior in this feature.

Allowed fields:

```text
min_ram_mb
recommended_ram_mb
max_working_set_mb
```

### `storage`

Storage fields must describe package and temporary workspace needs. They must
not request model downloads, unrestricted filesystem access, or private path
inspection.

Allowed fields:

```text
package_size_mb
temp_workspace_mb
cache_budget_mb
```

### `network`

Network fields must remain declarative. They cannot open ports, discover peers,
download artifacts, or enable remote execution.

Allowed draft values:

```text
none
local_only
lan_readonly
```

Blocked values:

```text
internet_required
public_ingress
wallet_network
mainnet
arbitrary_endpoint
```

### `model_dependencies`

Model dependency fields must describe expected local model or backend classes
without downloading, loading, selecting, or admitting models.

Rules:

- missing model dependency metadata blocks runtime eligibility later;
- model dependency declarations do not bypass model registration, license,
  checksum, backend, or hardware compatibility gates;
- declarations must not include local absolute paths, private model locations,
  credentials, or tokens.

### `accelerators`

Accelerator fields are optional expectations only. They must not decide GPU
compatibility, hardware class, reputation, rewards, or scheduler placement.

Allowed values:

```text
none
optional_gpu
optional_neural_engine
```

Blocked values:

```text
required_private_gpu
hardware_fingerprint_required
reward_boost_gpu
mainnet_validator_gpu
```

### `constraints`

Constraints must explicitly block unsafe or unsupported resource behavior:

- no dynamic hardware probes;
- no unrestricted filesystem scanning;
- no background downloads;
- no model loading;
- no service restart;
- no VM or container mutation;
- no scheduler override;
- no runtime priority claim.

### `degradation`

Degradation metadata describes allowed reduced-capability behavior when
resources are unavailable. It cannot start fallback execution or select another
node in this feature.

Allowed values:

```text
block_runtime_eligibility
require_human_review
offer_planning_only
```

### `privacy`

Resource metadata must not include raw hardware inventories, permanent hardware
fingerprints, private paths, hostnames, IP addresses, serial numbers, disk UUIDs,
machine IDs, usernames, credentials, or unredacted logs.

### `review`

Review metadata must require human review before registry or runtime
eligibility. Self-attestation alone is insufficient.

## Non-Bypass Rules

- Resource requirements cannot expand scope.
- Resource requirements cannot create capabilities.
- Resource requirements cannot grant permissions.
- Resource requirements cannot authorize execution.
- Resource requirements cannot select a runtime language.
- Resource requirements cannot start workers.
- Resource requirements cannot run hardware probes.
- Resource requirements cannot load or download models.
- Resource requirements cannot imply scheduler priority.
- Resource requirements cannot imply node compatibility.
- Resource requirements cannot imply trust, reputation, reward, or certification.
- Resource requirements cannot replace capability metadata.
- Resource requirements cannot replace expertise metadata.
- Resource requirements cannot replace permission review.
- Resource requirements cannot replace boundary evals.
- Resource requirements cannot claim distributed model MoE.

## Dependency Boundary

This documentation feature does not add dependencies. Future source-of-truth
schema work may reuse the same dependency policy already listed for agent
metadata implementation:

```text
serde
serde_json
serde_yaml
schemars
jsonschema
thiserror
```

Dependency changes must wait for the later implementation feature that creates
source-of-truth types, generated schemas, and validators.

## Integration

This feature consumes:

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-SKELETON-STANDARD-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
```

It feeds:

```text
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-ROUTING-CANDIDATE-SELECTION-001
```

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, privacy-invasive, or
unbounded resource metadata must block install, registry admission, and
execution by default.

## Risks

- Treating resource requirements as scheduler priority would move placement
  policy into metadata.
- Treating resource requirements as node compatibility would bypass hardware
  and backend gates.
- Treating resource requirements as permission grants would bypass permission
  review.
- Allowing dynamic probes would turn static review metadata into runtime
  hardware profiling.
- Adding parser behavior here would jump ahead of schema source-of-truth work.

## Recommendation

If QA confirms this feature remains documentation-only, roadmap-aligned, and
non-executable, proceed to:

```text
AGENT-PERMISSION-MODEL-001
```
