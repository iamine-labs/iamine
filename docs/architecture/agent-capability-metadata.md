# AGENT-CAPABILITY-METADATA-001

## Objective

Define the first IAMINE agent capability metadata contract without enabling
agent execution, routing, scheduler priority, reputation, or permission grants.

## Scope

This feature adds:

```text
docs/agents/agent-capability-metadata.md
docs/architecture/agent-capability-metadata.md
docs/qa/agent-capability-metadata.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-CAPABILITY-METADATA-001`.

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
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, worker, model, inference, installer, updater, rollback,
  reputation, reward, wallet, settlement, token, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts,
  dependencies, or package generation.

## Capability Metadata Role

Capability metadata describes what an agent package claims it can handle in a
reviewable, bounded form.

It must not decide:

- whether an agent is executable;
- whether a task is in scope;
- whether a permission is granted;
- whether a node has enough resources;
- whether an agent is trusted;
- whether a scheduler should prioritize it;
- whether a result is valid;
- whether a user should pay, reward, stake, or settle anything.

Each of those remains owned by later roadmap features.

## Draft Schema

The initial documentation schema is:

```text
iamine.agent.capabilities.draft-0.1
```

Default file name inside the skeleton:

```text
metadata/agent-capabilities.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Top-Level Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Capability metadata schema identifier. |
| `package_id` | yes | Package that owns the capability declaration. |
| `capability_id` | yes | Stable capability identifier. |
| `capability_version` | yes | Reviewable capability contract version. |
| `declared_task_types` | yes | Bounded task classes the agent claims to support. |
| `operations` | yes | Non-executable operation labels for review. |
| `input_classes` | yes | Allowed input classes by name only. |
| `output_classes` | yes | Expected output classes by name only. |
| `execution_modes` | yes | Planned local/LAN/remote mode labels. |
| `limitations` | yes | Explicit non-capabilities. |
| `risk_profile` | yes | Review risk labels, not permission grants. |
| `review` | yes | Human review requirements and evidence links. |

## Field Rules

### `schema`

Allowed value for this feature:

```text
iamine.agent.capabilities.draft-0.1
```

Unknown schema versions must block install, registry admission, and execution.

### `package_id`

The package ID must match the package manifest. It must not contain local host
identity, credentials, secrets, or private paths.

### `capability_id`

The capability ID must be stable, lowercase, and narrow.

Allowed example:

```text
node_readiness_diagnostic_summary
```

Blocked examples:

```text
general_assistant
do_anything
system_admin
all_files
all_networks
unrestricted_automation
```

### `declared_task_types`

Task types must be explicit and bounded. They must align with the scope
manifest and cannot add new scope.

Allowed examples:

```text
diagnostic_report
readiness_explanation
non_destructive_recommendation
```

Blocked examples:

```text
general_help
automation
admin
repair
publish
settlement
mainnet
```

### `operations`

Operations are descriptive labels only. They do not authorize execution.

Allowed examples:

```text
read_declared_summary
classify_status
draft_explanation
suggest_non_destructive_next_steps
```

Blocked examples:

```text
run_shell
write_files
delete_files
restart_services
scan_network
mutate_vm_or_container
download_models
publish_agent
transfer_funds
```

### `input_classes`

Input classes must be privacy-safe labels. They must not request raw local
files, host identity, credentials, private logs, secrets, or unrestricted
filesystem access.

### `output_classes`

Output classes must be bounded and non-promissory. They must not claim repair,
execution, publication, payment, reward, settlement, or mainnet effects.

### `execution_modes`

Execution modes are planned labels only. They do not authorize runtime.

Allowed draft labels:

```text
local_readonly
local_planning
lan_readonly
```

Blocked labels for this phase:

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

## Non-Bypass Rules

- Capability metadata cannot expand scope.
- Capability metadata cannot grant permissions.
- Capability metadata cannot authorize execution.
- Capability metadata cannot select a runtime language.
- Capability metadata cannot imply scheduler priority.
- Capability metadata cannot imply trust, reputation, reward, or certification.
- Capability metadata cannot replace expertise metadata.
- Capability metadata cannot replace resource requirements.
- Capability metadata cannot replace boundary evals.
- Capability metadata cannot claim distributed model MoE.

## Dependency Boundary

The roadmap lists a future minimal Rust dependency set for implementation:

```text
serde
serde_json
serde_yaml
schemars
jsonschema
thiserror
```

This documentation feature does not add those dependencies. Dependency changes
must wait for the later implementation feature that creates source-of-truth
types, generated schemas, and validators.

## Integration

This feature consumes:

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-SKELETON-STANDARD-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
```

It feeds:

```text
AGENT-EXPERTISE-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-ROUTING-CANDIDATE-SELECTION-001
```

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, or privacy-invasive capability
metadata must block install, registry admission, and execution by default.

## Risks

- Treating capabilities as permissions would bypass permission review.
- Treating capabilities as scope would weaken the scope-bound agent rule.
- Treating capabilities as scheduler priority would move policy into metadata.
- Treating capabilities as reputation would bypass trust and evidence gates.
- Adding parser behavior here would jump ahead of schema source-of-truth work.

## Recommendation

If QA confirms this feature remains documentation-only, roadmap-aligned, and
non-executable, proceed to:

```text
AGENT-EXPERTISE-METADATA-001
```
