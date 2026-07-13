# IAMINE Agent Package Manifest

Feature:

```text
AGENT-PACKAGE-MANIFEST-001
```

## Purpose

Define the documentation contract for an IAMINE agent package manifest.

This document is an architecture artifact. It does not authorize executable
agents, agent runtime, permission enforcement, sandboxing, audit logs, registry
publication, marketplace publication, third-party agents, or public beta launch.

## Manifest Role

The package manifest identifies an agent package and links it to the later
contracts that make execution reviewable:

- scope manifest;
- capability metadata;
- resource requirements;
- permission model;
- audit policy;
- boundary tests.

The package manifest must not replace any of those contracts. Missing,
contradictory, or unknown referenced metadata blocks installation and execution
by default.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.package.draft-0.1
```

The default manifest file name is:

```text
iamine-agent-package.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Top-Level Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Manifest schema identifier. |
| `package_id` | yes | Stable package identifier. |
| `package_version` | yes | Package version for review and upgrades. |
| `display_name` | yes | Human-readable agent name. |
| `summary` | yes | Short, non-promissory description. |
| `official_pack` | yes | Pack membership, if any. |
| `status` | yes | Planning, review, beta, blocked, or deprecated state. |
| `execution_authorized` | yes | Must be `false` until runtime gates authorize execution. |
| `agent` | yes | Agent-family metadata. |
| `references` | yes | Required follow-on contract files. |
| `distribution` | yes | Allowed distribution channels and install modes. |
| `security` | yes | Privacy and safety declarations. |
| `review` | yes | Human review and evidence requirements. |

## Field Rules

### `schema`

Allowed value for this feature:

```text
iamine.agent.package.draft-0.1
```

Unknown schema versions must block install and execution.

### `package_id`

The package ID must be stable, lowercase, and product-scoped. It must not
contain:

- usernames;
- hostnames;
- IP addresses;
- MAC addresses;
- serial numbers;
- machine IDs;
- private paths;
- secrets.

Recommended pattern:

```text
iamine.beta.<agent-name>
```

### `package_version`

The version must be explicit. A missing version blocks install and execution.

### `display_name`

The display name is user-facing. It must not imply execution capability before
runtime gates exist.

### `summary`

The summary must describe a narrow task. It must not claim broad automation,
unrestricted diagnosis, repair, publication, marketplace availability, payment,
reward, settlement, or mainnet behavior.

### `official_pack`

For the first selected beta pack:

```text
iamine-local-readiness-beta-pack
```

This field records product grouping only. It does not grant registry placement
or runtime access.

### `status`

Allowed draft states:

```text
planning
review
beta_candidate
blocked
deprecated
```

The first package manifests must remain `planning` until later features define
scope, permissions, audit logs, and boundary evals.

### `execution_authorized`

This field must be:

```text
false
```

For this phase, any package manifest that sets `execution_authorized = true`
must be treated as invalid.

## Agent Section

The `agent` section declares product identity only:

```toml
[agent]
family = "node_doctor"
personas = ["home_troubleshooter"]
earliest_mode = "local_readonly"
task_class = "diagnostic_report"
```

Rules:

- `family` must be one narrow family, not a generic assistant category;
- `personas` must map to documented personas;
- `earliest_mode` must be one of the bounded modes below;
- `task_class` must be narrow enough for future scope tests.

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
```

## References Section

The `references` section names required follow-on contracts:

```toml
[references]
scope_manifest = "agent-scope.toml"
capability_metadata = "agent-capabilities.toml"
resource_requirements = "agent-resources.toml"
permission_model = "agent-permissions.toml"
audit_policy = "agent-audit.toml"
boundary_tests = "agent-boundary-tests.toml"
```

Rules:

- referenced files are required before an agent can become executable;
- missing references block install and execution;
- references must be package-relative, not absolute local paths;
- references must not point to user home directories or private machine paths;
- the package manifest must not inline broad scope, permissions, or tests.

## Distribution Section

The `distribution` section declares where a package may be reviewed:

```toml
[distribution]
allowed_channels = ["local_dev"]
allowed_install_modes = ["manual_review"]
public_beta = false
marketplace = false
third_party_publication = false
```

For this phase, public beta, marketplace publication, and third-party
publication must remain `false`.

## Security Section

The `security` section declares privacy and safety constraints:

```toml
[security]
collects_credentials = false
collects_host_identifiers = false
requires_network = false
allows_destructive_actions = false
allows_arbitrary_shell = false
allows_unrestricted_filesystem = false
```

Rules:

- any `true` value in these fields blocks the first beta pack unless a later
  architecture feature explicitly creates a narrower safe exception;
- absent security fields block install and execution;
- contradictory claims block install and execution.

## Review Section

The `review` section records evidence requirements:

```toml
[review]
requires_human_review = true
requires_scope_manifest = true
requires_permission_review = true
requires_audit_policy = true
requires_boundary_tests = true
```

The package manifest cannot self-approve these gates.

## Minimal Draft Example

```toml
schema = "iamine.agent.package.draft-0.1"
package_id = "iamine.beta.node-doctor"
package_version = "0.1.0"
display_name = "Node Doctor"
summary = "Explain IAMINE node readiness and safe next steps."
official_pack = "iamine-local-readiness-beta-pack"
status = "planning"
execution_authorized = false

[agent]
family = "node_doctor"
personas = ["home_troubleshooter", "non_technical_caretaker"]
earliest_mode = "local_readonly"
task_class = "diagnostic_report"

[references]
scope_manifest = "agent-scope.toml"
capability_metadata = "agent-capabilities.toml"
resource_requirements = "agent-resources.toml"
permission_model = "agent-permissions.toml"
audit_policy = "agent-audit.toml"
boundary_tests = "agent-boundary-tests.toml"

[distribution]
allowed_channels = ["local_dev"]
allowed_install_modes = ["manual_review"]
public_beta = false
marketplace = false
third_party_publication = false

[security]
collects_credentials = false
collects_host_identifiers = false
requires_network = false
allows_destructive_actions = false
allows_arbitrary_shell = false
allows_unrestricted_filesystem = false

[review]
requires_human_review = true
requires_scope_manifest = true
requires_permission_review = true
requires_audit_policy = true
requires_boundary_tests = true
```

## Official Beta Pack Draft IDs

The selected beta pack may use these draft package IDs later:

| Selected agent | Draft package ID | Earliest mode |
| --- | --- | --- |
| Node Doctor | `iamine.beta.node-doctor` | `local_readonly` |
| Privacy-Safe Support Reporter | `iamine.beta.privacy-safe-support-reporter` | `local_readonly` |
| LAN Readiness Reporter | `iamine.beta.lan-readiness-reporter` | `lan_readonly` |
| Agent Manifest Wizard | `iamine.beta.agent-manifest-wizard` | `local_planning` |

These IDs are not package artifacts and are not registry entries in this
feature.

## Invalid Manifest Examples

The following conditions must block install and execution:

- `execution_authorized = true`;
- missing `scope_manifest`;
- missing `permission_model`;
- missing `boundary_tests`;
- `earliest_mode = "remote_execution"`;
- `allowed_channels = ["public_marketplace"]`;
- `allows_arbitrary_shell = true`;
- `allows_unrestricted_filesystem = true`;
- `collects_credentials = true`;
- package IDs containing local host identity or private paths;
- summaries that claim automatic repair, broad device control, rewards,
  settlement, or mainnet behavior.

## Recommendation

Proceed to:

```text
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
```

Do not implement executable manifests or runtime loading before scope,
permissions, audit logs, and boundary evals are defined.
