# IAMINE Agent Expertise Metadata

Feature:

```text
AGENT-EXPERTISE-METADATA-001
```

## Purpose

Define how an IAMINE agent package declares reviewable expertise without
authorizing execution, permissions, scheduler priority, routing, reputation,
reward, certification, marketplace behavior, or distributed model MoE.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, permission enforcement, sandboxing, registry
publication, marketplace publication, third-party agents, or public beta
launch.

## Metadata Contract

Expertise metadata answers a narrow review question:

```text
What reviewable domain evidence supports this agent package?
```

It does not answer:

- whether the task is in scope;
- whether a permission is allowed;
- whether the runtime can execute the agent;
- whether a node has enough resources;
- whether the agent capability exists;
- whether the scheduler should pick the agent;
- whether the agent is trusted or reputable;
- whether the output is valid;
- whether a distributed model expert router exists.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.expertise.draft-0.1
```

The default skeleton path is:

```text
metadata/agent-expertise.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Expertise metadata schema identifier. |
| `package_id` | yes | Package that owns the expertise metadata. |
| `expertise_id` | yes | Stable expertise identifier. |
| `expertise_version` | yes | Version for review and upgrades. |
| `domain` | yes | Narrow domain label. |
| `task_families` | yes | Task families aligned with scope and capabilities. |
| `supported_capabilities` | yes | Capability IDs this expertise supports. |
| `expertise_claims` | yes | Non-promissory knowledge claims. |
| `evidence` | yes | Reviewable evidence references. |
| `evaluation_requirements` | yes | Required future eval coverage. |
| `limitations` | yes | Explicit non-expertise. |
| `freshness` | yes | Stale metadata behavior. |
| `review` | yes | Human review requirements. |

## Example Shape

```toml
schema = "iamine.agent.expertise.draft-0.1"
package_id = "iamine.beta.node-doctor"
expertise_id = "node_readiness_diagnostics"
expertise_version = "0.1.0"
domain = "iamine_node_readiness"

task_families = [
  "readiness_explanation",
  "diagnostic_summary",
  "non_destructive_next_steps",
]

supported_capabilities = [
  "node_readiness_diagnostic_summary",
]

expertise_claims = [
  "can_explain_readiness_status",
  "can_map_known_preflight_findings_to_next_steps",
  "can_identify_when_operator_handoff_is_required",
]

limitations = [
  "not_a_system_repair_agent",
  "not_a_remote_execution_agent",
  "not_a_scheduler_policy",
  "not_a_reputation_signal",
  "not_a_distributed_model_expert_router",
]

[freshness]
review_interval_days = 90
stale_behavior = "require_human_review"

[[evidence]]
type = "design_note"
path = "review/expertise-review.md"

[[evidence]]
type = "eval_plan"
path = "evals/agent-boundary-tests.toml"

[[evaluation_requirements]]
class = "in_domain_task"
required = true

[[evaluation_requirements]]
class = "dangerous_task_refusal"
required = true
```

This example is not executable. It only shows the intended metadata shape.

## Required Alignment

Expertise metadata must align with:

- `agent.yaml`;
- `agent-scope.toml`;
- `metadata/agent-capabilities.toml`;
- `metadata/agent-permissions.toml` once defined;
- `metadata/agent-resources.toml` once defined;
- `evals/agent-boundary-tests.toml` once defined;
- `review/human-review.md`.

If expertise metadata contradicts package identity, scope, capabilities,
permissions, resources, audit, eval requirements, or human review requirements,
install, registry admission, and execution must remain blocked.

## Blocked Expertise Claims

Expertise metadata must not claim:

- broad general assistant expertise;
- arbitrary system administration;
- medical, legal, or financial advice;
- destructive repair;
- service restart;
- VM, container, router, wallet, reward, settlement, token, or mainnet
  operation;
- public marketplace publication;
- third-party publication;
- scheduler priority;
- trust, reputation, certification, or reward eligibility;
- model backend availability;
- distributed model MoE.

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
- private operator reputation scores.

## Review Requirements

Human review must confirm:

- domain is narrow;
- task families align with scope and capabilities;
- supported capabilities already exist;
- claims are non-promissory;
- evidence paths are package-relative and privacy-safe;
- limitations are explicit;
- stale behavior blocks or requires review;
- metadata does not expand scope, permissions, routing, trust, reputation,
  rewards, or execution;
- next required contracts remain pending where applicable.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-RESOURCE-REQUIREMENTS-001
```
