# AGENT-EXPERTISE-METADATA-001

## Objective

Define the first IAMINE agent expertise metadata contract without enabling
agent execution, routing, scheduler priority, reputation, reward,
certification, or distributed model MoE claims.

## Scope

This feature adds:

```text
docs/agents/agent-expertise-metadata.md
docs/architecture/agent-expertise-metadata.md
docs/qa/agent-expertise-metadata.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-EXPERTISE-METADATA-001` and aligns the package manifest reference list
with the existing skeleton path `metadata/agent-expertise.toml`.

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
- model routing, backend selection, inference execution, or distributed
  inference behavior;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, worker, model, installer, updater, rollback, reputation, reward,
  wallet, settlement, token, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts,
  dependencies, package generation, or schema generation.

## Expertise Metadata Role

Expertise metadata describes reviewable evidence that an agent package was
designed for a narrow domain and task family.

It answers:

```text
What narrow domain knowledge and review evidence supports this agent package?
```

It must not decide:

- whether an agent is executable;
- whether a task is in scope;
- whether a permission is granted;
- whether a node has enough resources;
- whether the scheduler should choose the agent;
- whether the agent is trusted, reputable, certified, or rewarded;
- whether a model backend is available;
- whether an inference result is correct;
- whether a distributed model, MoE shard, or expert router exists;
- whether a user should pay, reward, stake, or settle anything.

Those decisions remain owned by later roadmap features.

## Draft Schema

The initial documentation schema is:

```text
iamine.agent.expertise.draft-0.1
```

Default file name inside the skeleton:

```text
metadata/agent-expertise.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Top-Level Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Expertise metadata schema identifier. |
| `package_id` | yes | Package that owns the expertise declaration. |
| `expertise_id` | yes | Stable expertise identifier. |
| `expertise_version` | yes | Reviewable expertise contract version. |
| `domain` | yes | Narrow domain label. |
| `task_families` | yes | Task families aligned with scope and capabilities. |
| `supported_capabilities` | yes | Capability IDs this expertise supports. |
| `expertise_claims` | yes | Non-promissory knowledge claims. |
| `evidence` | yes | Reviewable evidence references. |
| `evaluation_requirements` | yes | Required future eval coverage. |
| `limitations` | yes | Explicit non-expertise. |
| `freshness` | yes | Review cadence and stale metadata behavior. |
| `review` | yes | Human review requirements and evidence links. |

## Field Rules

### `schema`

Allowed value for this feature:

```text
iamine.agent.expertise.draft-0.1
```

Unknown schema versions must block install, registry admission, and execution.

### `package_id`

The package ID must match the package manifest. It must not contain local host
identity, credentials, secrets, or private paths.

### `expertise_id`

The expertise ID must be stable, lowercase, and narrow.

Allowed example:

```text
node_readiness_diagnostics
```

Blocked examples:

```text
general_ai_expert
all_domains
system_admin
best_agent
certified_operator
distributed_model_expert
```

### `domain`

The domain must be a narrow product or operational domain, not a broad
assistant persona.

Allowed examples:

```text
iamine_node_readiness
lan_cluster_preflight
local_install_diagnostics
```

Blocked examples:

```text
general_computing
software_engineering
system_administration
financial_advice
medical_advice
legal_advice
```

### `task_families`

Task families must align with the scope manifest and capability metadata. They
cannot add new scope.

Allowed examples:

```text
readiness_explanation
diagnostic_summary
non_destructive_next_steps
```

Blocked examples:

```text
repair
remote_execution
wallet_operation
reward_optimization
mainnet_operation
```

### `supported_capabilities`

Supported capabilities are references to capability IDs already declared by
`metadata/agent-capabilities.toml`.

Rules:

- expertise metadata cannot create new capabilities;
- missing referenced capabilities block install, registry admission, and
  execution;
- references must be package-local identifiers, not URLs or filesystem paths;
- broad wildcard references are blocked.

### `expertise_claims`

Expertise claims are descriptive, non-promissory, and reviewable.

Allowed examples:

```text
can_explain_readiness_status
can_map_known_preflight_findings_to_next_steps
can_identify_when_operator_handoff_is_required
```

Blocked examples:

```text
guarantees_correct_answer
certified_expert
best_available_agent
always_safe
trustworthy
earns_rewards
routes_distributed_moe
```

### `evidence`

Evidence must be package-relative and reviewable. It must not point to a user
home directory, private machine path, raw private log, credential, host
identifier, or secret-bearing artifact.

Allowed evidence types:

```text
design_note
human_review
eval_plan
bounded_fixture
redacted_example
```

Blocked evidence types:

```text
private_log
credential_dump
host_inventory
wallet_record
unredacted_user_data
runtime_reputation_score
payment_history
```

### `evaluation_requirements`

Evaluation requirements name future checks that must exist before runtime
eligibility. They do not report pass/fail results in this phase.

Required coverage:

- in-domain tasks;
- adjacent-domain refusals;
- dangerous task refusals;
- ambiguous task handoff;
- capability mismatch;
- stale expertise metadata;
- prompt injection or role confusion attempts;
- privacy-sensitive input rejection.

### `limitations`

Limitations must explicitly describe what the agent is not expert in. Missing
limitations block install, registry admission, and execution.

### `freshness`

Freshness records how review should treat stale expertise metadata. It cannot
trigger network refresh, self-update, package download, or runtime behavior.

Allowed stale behavior:

```text
block_runtime_eligibility
require_human_review
```

### `review`

Review metadata must require human review before registry or runtime
eligibility. Self-attestation alone is insufficient.

## Privacy Rules

Expertise metadata must not include:

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
- unredacted logs;
- permanent hardware fingerprints;
- runtime reputation scores tied to a private operator.

## Non-Bypass Rules

- Expertise metadata cannot expand scope.
- Expertise metadata cannot create capabilities.
- Expertise metadata cannot grant permissions.
- Expertise metadata cannot authorize execution.
- Expertise metadata cannot select a runtime language.
- Expertise metadata cannot imply scheduler priority.
- Expertise metadata cannot imply trust, reputation, reward, or certification.
- Expertise metadata cannot replace capability metadata.
- Expertise metadata cannot replace resource requirements.
- Expertise metadata cannot replace boundary evals.
- Expertise metadata cannot claim distributed model MoE.
- Expertise metadata cannot select model backends or inference placement.

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
```

It feeds:

```text
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-ROUTING-CANDIDATE-SELECTION-001
AGENT-EXPERTISE-TEMPLATE-001
```

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, or privacy-invasive
expertise metadata must block install, registry admission, and execution by
default.

## Risks

- Treating expertise as scheduler priority would move routing policy into
  metadata.
- Treating expertise as trust would bypass reputation and review gates.
- Treating expertise as certification would create unsupported product claims.
- Treating expertise as distributed model MoE would conflict with the roadmap
  boundary.
- Adding parser behavior here would jump ahead of schema source-of-truth work.

## Recommendation

If QA confirms this feature remains documentation-only, roadmap-aligned, and
non-executable, proceed to:

```text
AGENT-RESOURCE-REQUIREMENTS-001
```
