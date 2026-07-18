# IAMINE Agent Capability Metadata

Feature:

```text
AGENT-CAPABILITY-METADATA-001
```

## Purpose

Define how an IAMINE agent package declares reviewable capabilities without
authorizing execution, permissions, scheduler priority, reputation, routing, or
marketplace behavior.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, permission enforcement, sandboxing, registry
publication, marketplace publication, third-party agents, or public beta
launch.

## Metadata Contract

Capability metadata answers a narrow review question:

```text
What bounded task classes does this agent claim it can support?
```

It does not answer:

- whether the task is in scope;
- whether a permission is allowed;
- whether the runtime can execute the agent;
- whether a node has enough resources;
- whether the agent is expert in the domain;
- whether the scheduler should pick the agent;
- whether the agent is trusted or reputable;
- whether the output is valid.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.capabilities.draft-0.1
```

The default skeleton path is:

```text
metadata/agent-capabilities.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Capability metadata schema identifier. |
| `package_id` | yes | Package that owns the capability metadata. |
| `capability_id` | yes | Stable capability identifier. |
| `capability_version` | yes | Version for review and upgrades. |
| `declared_task_types` | yes | Bounded task classes. |
| `operations` | yes | Descriptive non-executable operations. |
| `input_classes` | yes | Privacy-safe input class labels. |
| `output_classes` | yes | Expected output class labels. |
| `execution_modes` | yes | Planned mode labels, not runtime grants. |
| `limitations` | yes | Explicit non-capabilities. |
| `risk_profile` | yes | Review labels, not permissions. |
| `review` | yes | Human review requirements. |

## Example Shape

```toml
schema = "iamine.agent.capabilities.draft-0.1"
package_id = "iamine.beta.node-doctor"
capability_id = "node_readiness_diagnostic_summary"
capability_version = "0.1.0"

declared_task_types = [
  "diagnostic_report",
  "readiness_explanation",
  "non_destructive_recommendation",
]

operations = [
  "read_declared_summary",
  "classify_status",
  "draft_explanation",
  "suggest_non_destructive_next_steps",
]

input_classes = [
  "iamine_node_status_summary",
  "iamine_readiness_checklist",
  "user_provided_error_text",
]

output_classes = [
  "human_readable_summary",
  "non_destructive_next_steps",
]

execution_modes = ["local_readonly"]
limitations = ["no_repairs", "no_shell", "no_service_restart"]
```

This example is not executable. It only shows the intended metadata shape.

## Required Alignment

Capability metadata must align with:

- `agent.yaml`;
- `agent-scope.toml`;
- `metadata/agent-permissions.toml` once defined;
- `metadata/agent-resources.toml` once defined;
- `evals/agent-boundary-tests.toml` once defined.

If capability metadata contradicts package identity, scope, permissions,
resources, audit, or eval requirements, install, registry admission, and
execution must remain blocked.

## Blocked Capability Claims

Capability metadata must not claim:

- broad general assistant behavior;
- arbitrary shell;
- unrestricted filesystem;
- unrestricted network;
- destructive repair;
- service restart;
- VM, container, router, wallet, reward, settlement, token, or mainnet
  mutation;
- public marketplace publication;
- third-party publication;
- scheduler priority;
- trust, reputation, certification, or reward eligibility;
- distributed model MoE.

## Privacy Rules

Capability metadata must not include:

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
- permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- task types are narrow;
- operations are non-executable labels;
- input classes are privacy-safe;
- output classes do not promise mutation or repair;
- limitations are explicit;
- execution modes are only planned labels;
- metadata does not expand scope or permissions;
- next required contracts remain pending where applicable.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-EXPERTISE-METADATA-001
```
