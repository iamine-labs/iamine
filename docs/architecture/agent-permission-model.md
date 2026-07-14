# AGENT-PERMISSION-MODEL-001

## Objective

Define the first IAMINE agent permission model contract with explicit
permission categories, denial-by-default behavior, and review requirements
without enabling runtime permission enforcement or agent execution.

## Scope

This feature adds:

```text
docs/agents/agent-permission-model.md
docs/architecture/agent-permission-model.md
docs/qa/agent-permission-model.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-PERMISSION-MODEL-001` and aligns package manifest permission references
with the existing skeleton path `metadata/agent-permissions.toml`.

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
- permission parser or runtime permission enforcement;
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

## Permission Model Role

The permission model describes what an agent package may request for future
review. It does not grant permissions.

It answers:

```text
What explicit permission categories does this agent package request or forbid?
```

It must not decide:

- whether an agent is executable;
- whether runtime enforcement exists;
- whether a task is in scope;
- whether a capability exists;
- whether resources are sufficient;
- whether a scheduler should choose the agent;
- whether a worker should start;
- whether a model should be loaded or downloaded;
- whether an agent is trusted, reputable, certified, or rewarded;
- whether a distributed model, MoE shard, or expert router exists.

Those decisions remain owned by later roadmap features.

## Draft Schema

The initial documentation schema is:

```text
iamine.agent.permissions.draft-0.1
```

Default file name inside the skeleton:

```text
metadata/agent-permissions.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Top-Level Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Permission model schema identifier. |
| `package_id` | yes | Package that owns the permission declaration. |
| `permission_profile_id` | yes | Stable permission profile identifier. |
| `permission_profile_version` | yes | Reviewable permission contract version. |
| `default_policy` | yes | Required denial-by-default policy. |
| `requested_categories` | yes | Explicit requested permission categories. |
| `forbidden_categories` | yes | Explicitly forbidden permission categories. |
| `blocked_actions` | yes | Actions that remain blocked even with review. |
| `confirmation_requirements` | yes | Human confirmation requirements. |
| `data_access` | yes | Declared data access classes. |
| `network_access` | yes | Declared network access classes. |
| `filesystem_access` | yes | Declared filesystem access classes. |
| `process_access` | yes | Declared process or service access classes. |
| `escalation` | yes | Escalation and handoff behavior. |
| `review` | yes | Human review requirements and evidence links. |

## Field Rules

### `schema`

Allowed value for this feature:

```text
iamine.agent.permissions.draft-0.1
```

Unknown schema versions must block install, registry admission, and execution.

### `package_id`

The package ID must match the package manifest. It must not contain local host
identity, credentials, secrets, or private paths.

### `permission_profile_id`

The permission profile ID must be stable, lowercase, and narrow.

Allowed example:

```text
node_doctor_local_readonly_permissions
```

Blocked examples:

```text
all_permissions
admin
root
system_control
wallet_access
mainnet_operator
```

### `default_policy`

The default policy must be:

```text
deny
```

Any missing or permissive default policy must block install, registry admission,
and execution.

### `requested_categories`

Requested categories must be explicit and narrow. They are review inputs only.

Allowed draft categories:

```text
local_readonly
user_provided_text
redacted_status_summary
package_relative_review_files
lan_readonly_metadata
```

Blocked categories for this phase:

```text
arbitrary_shell
unrestricted_filesystem
credential_access
private_key_access
wallet_access
destructive_write
service_mutation
network_mutation
model_download
model_load
vm_or_container_mutation
marketplace_publish
mainnet_operation
```

### `forbidden_categories`

Forbidden categories must include known unsafe categories for the agent phase.
Missing forbidden categories block install, registry admission, and execution.

### `blocked_actions`

Blocked actions remain blocked even if a user asks for them. User confirmation
cannot turn blocked actions into allowed actions.

Required blocked action classes:

- shell execution;
- unrestricted filesystem reads or writes;
- credential collection;
- private key or wallet access;
- destructive writes;
- service restart or mutation;
- network mutation;
- model download or model loading;
- VM or container mutation;
- public marketplace publication;
- mainnet, reward, settlement, or token operations.

### `confirmation_requirements`

Confirmation requirements describe when future runtime must ask the user or
handoff. This feature does not implement confirmation prompts.

### `data_access`

Data access classes must be privacy-safe labels. They must not request raw
private logs, home directories, credentials, secrets, host identifiers, wallet
data, or private machine paths.

### `network_access`

Network access must be explicit and disabled by default.

Allowed draft values:

```text
none
local_only
lan_readonly_metadata
```

### `filesystem_access`

Filesystem access must be package-relative or explicitly absent. It cannot
reference user home directories, absolute private paths, model stores, wallets,
SSH directories, or system configuration paths.

### `process_access`

Process access must be absent by default. Process lists, service control, and
worker mutation remain blocked for this phase.

### `escalation`

Escalation behavior must hand off to the orchestrator or human review. It cannot
grant extra permissions, run a command, mutate files, or continue execution.

### `review`

Review metadata must require human review before registry or runtime
eligibility. Self-attestation alone is insufficient.

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

## Non-Bypass Rules

- Permission metadata cannot expand scope.
- Permission metadata cannot create capabilities.
- Permission metadata cannot authorize execution.
- Permission metadata cannot implement runtime enforcement.
- Permission metadata cannot start workers.
- Permission metadata cannot run shell commands.
- Permission metadata cannot read private files.
- Permission metadata cannot load or download models.
- Permission metadata cannot imply scheduler priority.
- Permission metadata cannot imply node compatibility.
- Permission metadata cannot imply trust, reputation, reward, or certification.
- Permission metadata cannot replace capability metadata.
- Permission metadata cannot replace expertise metadata.
- Permission metadata cannot replace resource requirements.
- Permission metadata cannot replace boundary evals.
- Permission metadata cannot claim distributed model MoE.

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
source-of-truth types, generated schemas, validators, and enforcement gates.

## Integration

This feature consumes:

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-SKELETON-STANDARD-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
```

It feeds:

```text
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-ROUTING-CANDIDATE-SELECTION-001
```

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, permissive, or privacy-invasive
permission metadata must block install, registry admission, and execution by
default.

## Risks

- Treating permission metadata as runtime enforcement would create a false
  security boundary.
- Treating confirmation as permission would bypass denial-by-default behavior.
- Allowing broad permission categories would weaken scope-bound agents.
- Adding parser or enforcement behavior here would jump ahead of schema
  source-of-truth and runtime gate work.

## Recommendation

If QA confirms this feature remains documentation-only, roadmap-aligned, and
non-executable, proceed to:

```text
AGENT-AUDIT-LOG-001
```
