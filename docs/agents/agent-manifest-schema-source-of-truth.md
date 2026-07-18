# IAMINE Agent Manifest Schema Source Of Truth

Feature:

```text
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
```

## Purpose

Define the source of truth for IAMINE agent manifest schemas and generated
validation artifacts before schema generation, validator implementation,
runtime execution, package installation, sandboxing, registry readiness,
marketplace publication, worker startup, model loading, reputation, reward, or
distributed model MoE behavior exists.

This document is an architecture artifact. It does not authorize executable
agents, schema generation, validator execution, runtime execution, dependency
installation, package manager execution, sandboxing, registry publication,
marketplace publication, third-party agents, public beta launch, or public
agent discovery.

## Source Of Truth Contract

The source-of-truth policy answers one narrow review question:

```text
Where do agent manifest schema definitions originate?
```

It does not answer:

- whether validators exist;
- whether schemas are generated;
- whether packages may be installed;
- whether agents can execute;
- whether a runtime exists;
- whether dependencies may be installed;
- whether sandboxing exists;
- whether a package is safe;
- whether an agent is trusted, reputable, certified, or rewarded;
- whether a package may be published publicly.

## Draft Schema

The first draft policy identifier is:

```text
iamine.agent.schema_source.draft-0.1
```

This is a repository-level policy contract. It is not a required file inside
the agent package skeleton in this phase.

## Format Policy

The canonical manifest format policy is:

```text
Authoring: YAML
Internal representation: Rust structs
Validation: generated JSON Schema
Runtime/API payloads: JSON
Source of truth: Rust types
```

Rust types are the future source of truth. YAML authoring files, generated JSON
Schema, JSON payloads, and docs are derived or review surfaces; they must not
silently diverge.

## Implementation Status

`AGENT-MANIFEST-PARSER-VALIDATOR-001` implements this derivation for the root
`package_manifest` family only:

```text
Rust source: iamine-agents
YAML root: agent.yaml
generated JSON Schema: available through manifest_json_schema
bounded parser: available through parse_and_validate_yaml
semantic validator: available through validate_manifest
package loading: unavailable
runtime execution: unavailable
```

Other schema families remain contract-only until their owner features add
canonical types and validators. The root parser treats their references as
opaque package-relative paths and does not open or validate them.

## Schema Families

Future schema source-of-truth work must cover:

```text
package_manifest
scope_manifest
capability_metadata
expertise_metadata
resource_requirements
permission_model
audit_policy
boundary_evals
local_registry
language_policy
dependency_policy
runtime_language_matrix
```

## Required Review Fields

Future schema source metadata must make these fields explicit:

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Source policy schema identifier. |
| `schema_family` | yes | Manifest or metadata family. |
| `source_owner` | yes | Owning crate or module for future Rust type. |
| `authoring_format` | yes | YAML for human-authored manifests. |
| `internal_format` | yes | Rust structs for source of truth. |
| `validation_artifact` | yes | Generated JSON Schema. |
| `runtime_payload_format` | yes | JSON for runtime/API payloads. |
| `generation_available` | yes | Must be false until generator implementation exists. |
| `validator_available` | yes | Must be false until validator implementation exists. |
| `failure_policy` | yes | Behavior when schemas are missing or divergent. |
| `review` | yes | Human review requirements and evidence links. |

## Blocked Claims

Schema source metadata must not claim:

- schema generation availability;
- validator execution availability;
- package installation authorization;
- runtime execution authorization;
- dependency installation authorization;
- package manager execution authorization;
- sandbox availability;
- permission enforcement;
- scheduler priority;
- node compatibility;
- backend availability;
- model admission;
- worker startup authorization;
- local registry readiness;
- public registry availability;
- public marketplace publication;
- third-party publication;
- public beta launch;
- trust, reputation, certification, or reward eligibility;
- wallet, settlement, token, or mainnet behavior;
- distributed model MoE.

## Privacy Rules

Schema source metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- Rust types are the future source of truth;
- YAML is the authoring format, not the source of truth;
- generated JSON Schema is a validation artifact, not manually maintained
  truth;
- runtime/API payloads use JSON only after runtime gates authorize payload use;
- generated artifacts cannot diverge from Rust types;
- missing or divergent schemas block local registry review advancement,
  install, and execution;
- this feature does not add dependencies or generator code.

## Next Roadmap Step

The next package lifecycle feature after the root parser is:

```text
AGENT-PACKAGE-LOAD-GATE-001
```
