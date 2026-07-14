# IAMINE Agent Local Registry

Feature:

```text
AGENT-REGISTRY-LOCAL-001
```

## Purpose

Define how IAMINE records local, operator-controlled agent registry review
state before any public registry, marketplace, runtime execution, sandboxing,
permission enforcement, scheduler placement, worker startup, model loading,
reputation, reward, or distributed model MoE behavior exists.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, package installation, sandboxing, registry
publication, marketplace publication, third-party agents, public beta launch,
or public agent discovery.

## Registry Contract

The local registry answers one narrow review question:

```text
Is this agent package recorded as a local review candidate for this phase?
```

It does not answer:

- whether the package can execute;
- whether a runtime exists;
- whether permissions are enforced;
- whether sandboxing exists;
- whether boundary evals have passed;
- whether a scheduler should route work to the agent;
- whether a worker should start;
- whether a model backend is available;
- whether the agent is trusted, reputable, certified, or rewarded;
- whether the package may be published publicly.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.registry.local.draft-0.1
```

The local registry is operator-local control-plane state. It is not a required
file inside the agent package skeleton and is not a public registry artifact in
this phase.

This feature does not implement TOML parsing, registry storage, installation,
network synchronization, publication, or runtime loading. The schema is a
planning contract for later implementation.

## Required Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Local registry schema identifier. |
| `registry_id` | yes | Stable local registry identifier. |
| `registry_version` | yes | Version for registry review rules. |
| `package_id` | yes | Package being reviewed. |
| `package_version` | yes | Package version being reviewed. |
| `source_channel` | yes | Source channel allowed for this phase. |
| `intended_phase` | yes | Roadmap phase that owns the review. |
| `review_state` | yes | Local review state. |
| `required_contracts` | yes | Contracts required before review can advance. |
| `contract_results` | yes | Recorded result for each required contract. |
| `distribution_policy` | yes | Local-only distribution constraints. |
| `privacy_policy` | yes | Privacy limits for registry metadata. |
| `failure_policy` | yes | Behavior when metadata is unsafe or incomplete. |
| `review` | yes | Human review requirements and evidence links. |

## Example Shape

```toml
schema = "iamine.agent.registry.local.draft-0.1"
registry_id = "operator_local_agent_registry"
registry_version = "0.1.0"
package_id = "iamine.beta.node-doctor"
package_version = "0.1.0"
source_channel = "local_dev"
intended_phase = "v0.11_agent_architecture_foundation"
review_state = "under_review"

[required_contracts]
package_manifest = "required"
skeleton_layout = "required"
scope_manifest = "required"
capability_metadata = "required"
expertise_metadata = "required"
resource_requirements = "required"
permission_model = "required"
audit_policy = "required"
boundary_evals = "required_before_registry_review_ready"
human_review = "required"
qa_evidence = "required"

[contract_results]
package_manifest = "present"
skeleton_layout = "present"
scope_manifest = "present"
capability_metadata = "present"
expertise_metadata = "present"
resource_requirements = "present"
permission_model = "present"
audit_policy = "present"
boundary_evals = "pending"
human_review = "pending"
qa_evidence = "pending"

[distribution_policy]
allowed_channels = ["local_dev"]
allowed_install_modes = ["manual_review"]
public_beta = false
marketplace = false
third_party_publication = false
network_publication = false

[privacy_policy]
operator_local_only = true
stores_host_identity = false
stores_private_paths = false
stores_raw_prompts = false
stores_raw_outputs = false
stores_credentials = false

[failure_policy]
unknown_schema = "block"
missing_contract = "block"
contradictory_metadata = "block"
unsafe_distribution = "block"
privacy_violation = "block"
public_publication_request = "block"
```

This example is not executable. It only shows the intended metadata shape.

## Review States

Allowed local registry review states are:

```text
candidate
under_review
blocked
registry_review_ready
deprecated
```

`registry_review_ready` must not be used until all required contracts exist and
pass, including boundary evals and human review. During this feature, boundary
evals are still owned by the next roadmap feature, so local registry records
cannot become executable or public.

## Required Alignment

Local registry metadata must align with:

- `iamine-agent-package.toml`;
- `agent-scope.toml`;
- `metadata/agent-capabilities.toml`;
- `metadata/agent-expertise.toml`;
- `metadata/agent-resources.toml`;
- `metadata/agent-permissions.toml`;
- `metadata/agent-audit.toml`;
- `evals/agent-boundary-tests.toml` once defined;
- `review/human-review.md`;
- `review/qa-evidence.md`.

If local registry metadata contradicts package identity, scope, capabilities,
expertise, resources, permissions, audit, eval requirements, or human review
requirements, install, registry review advancement, and execution must remain
blocked.

## Blocked Registry Claims

Local registry metadata must not claim:

- runtime execution authorization;
- package installation authorization;
- sandbox availability;
- permission enforcement;
- boundary eval pass status before boundary evals exist;
- scheduler priority;
- node compatibility;
- backend availability;
- model admission;
- worker startup authorization;
- public registry availability;
- public marketplace publication;
- third-party publication;
- public beta launch;
- trust, reputation, certification, or reward eligibility;
- distributed model MoE.

## Privacy Rules

Local registry metadata must not include:

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

- source channel is `local_dev`;
- distribution remains manual and local;
- public beta, marketplace, third-party publication, and network publication
  remain blocked;
- all required contracts are listed;
- missing contracts block advancement;
- boundary evals are required before `registry_review_ready`;
- registry metadata does not expand scope, permissions, capabilities,
  resources, scheduling, compatibility, trust, reputation, rewards, or
  execution;
- privacy-sensitive identifiers and secrets are absent;
- next required contracts remain pending where applicable.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-SCOPE-BOUNDARY-EVALS-001
```
