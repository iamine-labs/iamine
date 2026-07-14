# IAMINE Agent Permission Model

Feature:

```text
AGENT-PERMISSION-MODEL-001
```

## Purpose

Define how an IAMINE agent package declares explicit permission categories,
forbidden categories, blocked actions, and denial-by-default behavior without
authorizing execution, runtime enforcement, sandboxing, scheduler placement,
worker startup, model loading, registry admission, marketplace behavior, or
distributed model MoE.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, permission enforcement, sandboxing, registry
publication, marketplace publication, third-party agents, or public beta
launch.

## Metadata Contract

Permission metadata answers a narrow review question:

```text
What permissions does this agent request, and what remains forbidden?
```

It does not answer:

- whether the task is in scope;
- whether the runtime can execute the agent;
- whether enforcement exists;
- whether a node is compatible;
- whether the scheduler should pick the agent;
- whether a worker should start;
- whether a model backend is available;
- whether the agent is trusted or reputable.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.permissions.draft-0.1
```

The default skeleton path is:

```text
metadata/agent-permissions.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Permission model schema identifier. |
| `package_id` | yes | Package that owns the permission metadata. |
| `permission_profile_id` | yes | Stable permission profile identifier. |
| `permission_profile_version` | yes | Version for review and upgrades. |
| `default_policy` | yes | Required denial-by-default policy. |
| `requested_categories` | yes | Explicit requested permission categories. |
| `forbidden_categories` | yes | Explicitly forbidden permission categories. |
| `blocked_actions` | yes | Actions blocked even with review. |
| `confirmation_requirements` | yes | Human confirmation requirements. |
| `data_access` | yes | Declared data access classes. |
| `network_access` | yes | Declared network access classes. |
| `filesystem_access` | yes | Declared filesystem access classes. |
| `process_access` | yes | Declared process or service access classes. |
| `escalation` | yes | Escalation and handoff behavior. |
| `review` | yes | Human review requirements. |

## Example Shape

```toml
schema = "iamine.agent.permissions.draft-0.1"
package_id = "iamine.beta.node-doctor"
permission_profile_id = "node_doctor_local_readonly_permissions"
permission_profile_version = "0.1.0"
default_policy = "deny"

requested_categories = [
  "local_readonly",
  "user_provided_text",
  "redacted_status_summary",
]

forbidden_categories = [
  "arbitrary_shell",
  "unrestricted_filesystem",
  "credential_access",
  "private_key_access",
  "wallet_access",
  "destructive_write",
  "service_mutation",
  "network_mutation",
  "model_download",
  "model_load",
  "marketplace_publish",
  "mainnet_operation",
]

blocked_actions = [
  "run_shell",
  "read_private_files",
  "write_files",
  "restart_services",
  "download_models",
  "load_models",
  "publish_agent",
]

[data_access]
allowed = ["user_provided_error_text", "redacted_status_summary"]
forbidden = ["credentials", "private_keys", "wallet_keys", "raw_private_logs"]

[network_access]
mode = "none"

[filesystem_access]
mode = "package_relative_review_only"

[process_access]
mode = "none"

[escalation]
on_forbidden_request = "return_to_orchestrator"
```

This example is not executable. It only shows the intended metadata shape.

## Required Alignment

Permission metadata must align with:

- `iamine-agent-package.toml`;
- `agent-scope.toml`;
- `metadata/agent-capabilities.toml`;
- `metadata/agent-expertise.toml`;
- `metadata/agent-resources.toml`;
- `evals/agent-boundary-tests.toml` once defined;
- `review/human-review.md`.

If permission metadata contradicts package identity, scope, capabilities,
expertise, resources, audit, eval requirements, or human review requirements,
install, registry admission, and execution must remain blocked.

## Blocked Permission Claims

Permission metadata must not claim:

- runtime enforcement;
- execution authorization;
- sandbox availability;
- scheduler priority;
- node compatibility;
- backend availability;
- model admission;
- model download authorization;
- model load authorization;
- worker startup authorization;
- hardware profile mutation;
- public marketplace publication;
- third-party publication;
- trust, reputation, certification, or reward eligibility;
- distributed model MoE.

## Privacy Rules

Permission metadata must not include:

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
- raw process lists;
- unredacted logs;
- permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- default policy is deny;
- requested categories are narrow;
- forbidden categories cover unsafe actions;
- blocked actions remain blocked even with confirmation;
- filesystem access is package-relative or absent;
- network access is absent or explicitly bounded;
- process access is absent by default;
- escalation returns to orchestrator or human review;
- metadata does not expand scope, capabilities, resources, scheduling,
  compatibility, trust, reputation, rewards, or execution;
- next required contracts remain pending where applicable.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-AUDIT-LOG-001
```
