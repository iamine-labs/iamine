# IAMINE Agent Audit Log

Feature:

```text
AGENT-AUDIT-LOG-001
```

## Purpose

Define how an IAMINE agent package declares privacy-safe audit evidence
requirements without authorizing runtime logging, execution, permission
enforcement, sandboxing, registry admission, marketplace behavior, reputation,
reward, or distributed model MoE.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, permission enforcement, sandboxing, registry
publication, marketplace publication, third-party agents, or public beta
launch.

## Metadata Contract

Audit metadata answers a narrow review question:

```text
What redacted evidence must exist for this agent package to be reviewable?
```

It does not answer:

- whether the task is in scope;
- whether permissions are enforced;
- whether the runtime can execute the agent;
- whether audit logging exists;
- whether a node is compatible;
- whether the scheduler should pick the agent;
- whether a worker should start;
- whether the agent is trusted or reputable.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.audit.draft-0.1
```

The default skeleton path is:

```text
metadata/agent-audit.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Audit policy schema identifier. |
| `package_id` | yes | Package that owns the audit policy. |
| `audit_profile_id` | yes | Stable audit profile identifier. |
| `audit_profile_version` | yes | Version for review and upgrades. |
| `event_classes` | yes | Allowed future event classes. |
| `required_evidence` | yes | Evidence required before review. |
| `redaction_policy` | yes | Redaction and blocked fields. |
| `retention_policy` | yes | Retention expectations. |
| `integrity_policy` | yes | Tamper-evidence expectations. |
| `access_policy` | yes | Visibility and sharing limits. |
| `failure_policy` | yes | Missing or unsafe evidence behavior. |
| `review` | yes | Human review requirements. |

## Example Shape

```toml
schema = "iamine.agent.audit.draft-0.1"
package_id = "iamine.beta.node-doctor"
audit_profile_id = "node_doctor_privacy_safe_audit"
audit_profile_version = "0.1.0"

event_classes = [
  "review_started",
  "scope_checked",
  "permission_checked",
  "redaction_checked",
  "handoff_required",
  "refusal_recorded",
]

required_evidence = [
  "review/human-review.md",
  "review/qa-evidence.md",
  "evals/agent-boundary-tests.toml",
]

[redaction_policy]
default = "redact"
blocks_raw_prompts = true
blocks_raw_outputs = true
blocks_private_paths = true
blocks_host_identifiers = true
blocks_credentials = true

[retention_policy]
mode = "review_only"
operator_local_only = true

[integrity_policy]
future_tamper_evidence_required = true
publishes_artifacts = false

[access_policy]
visibility = "operator_local"
third_party_sharing = false
marketplace_publication = false

[failure_policy]
missing_audit_policy = "block"
unredacted_evidence = "block"
unsafe_event_class = "block"
```

This example is not executable. It only shows the intended metadata shape.

## Required Alignment

Audit metadata must align with:

- `iamine-agent-package.toml`;
- `agent-scope.toml`;
- `metadata/agent-capabilities.toml`;
- `metadata/agent-expertise.toml`;
- `metadata/agent-resources.toml`;
- `metadata/agent-permissions.toml`;
- `evals/agent-boundary-tests.toml` once defined;
- `review/human-review.md`;
- `review/qa-evidence.md`.

If audit metadata contradicts package identity, scope, capabilities, expertise,
resources, permissions, eval requirements, or human review requirements,
install, registry admission, and execution must remain blocked.

## Blocked Audit Claims

Audit metadata must not claim:

- runtime audit logging;
- execution authorization;
- permission enforcement;
- sandbox availability;
- registry admission;
- scheduler priority;
- node compatibility;
- backend availability;
- worker startup authorization;
- public marketplace publication;
- third-party publication;
- trust, reputation, certification, or reward eligibility;
- distributed model MoE.

## Privacy Rules

Audit metadata and future audit evidence must not include:

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
- raw prompts;
- raw outputs;
- raw process lists;
- unredacted logs;
- permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- event classes are bounded;
- raw prompts and raw outputs are blocked;
- redaction is default;
- required evidence is package-relative;
- retention is operator-local and review-only;
- access policy blocks third-party and marketplace sharing;
- failure policy blocks missing or unsafe evidence;
- metadata does not expand scope, permissions, resources, scheduling,
  compatibility, trust, reputation, rewards, or execution;
- next required contracts remain pending where applicable.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-REGISTRY-LOCAL-001
```
