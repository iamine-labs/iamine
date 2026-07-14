# IAMINE Agent Skeleton Generator

Feature:

```text
AGENT-SKELETON-GENERATOR-001
```

## Purpose

Define the future IAMINE agent skeleton generator contract without
implementing a generator command, file writer, package installer, runtime,
template engine, registry publication, marketplace publication, or agent
execution behavior.

This document does not authorize executable agents, shell execution,
filesystem mutation, unrestricted network access, dependency installation,
public third-party publishing, wallet, reward, settlement, mainnet behavior, or
distributed model MoE.

## Generator Question

Skeleton-generator policy answers:

```text
What files and metadata must a future generated agent skeleton contain?
```

It does not answer whether file generation, runtime execution, packaging,
validation, publication, audit logging, or template rendering exists.

## Draft Schema

```text
iamine.agent.skeleton_generator.draft-0.1
```

This feature does not implement parsers, generators, file writes, CLI commands,
template rendering, package managers, dependency resolution, sandbox startup,
or runtime enforcement.

## Required Skeleton Shape

Future generated skeletons must be bounded to one package root:

```text
agent/
agent/manifest.iamine.json
agent/README.md
agent/src/
agent/tests/
agent/qa/
```

The generated skeleton must not write outside its package root.

## Required Manifest Fields

Future generated manifests must include:

```text
schema_version
agent_id
display_name
task_types
declared_scope
permissions
runtime_language
input_contract
output_contract
timeout_policy
handoff_policy
out_of_scope_policy
boundary_tests
```

Unknown, missing, contradictory, broad, unsafe, stale, or unverifiable fields
must block validation by default.

## Blocked Defaults

Generated skeletons must default to:

```text
no_shell
no_unrestricted_filesystem
no_unrestricted_network
no_secret_access
no_wallet_access
no_auto_publication
no_runtime_execution
manual_validation_required
```

## Privacy Rules

Skeleton metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- A skeleton cannot be treated as an approved agent.
- A skeleton cannot grant permissions.
- A skeleton cannot imply runtime availability.
- A skeleton cannot publish to a registry or marketplace.
- A skeleton cannot bypass template validation, scope review, permission
  review, boundary tests, or manual validation.

## Next Roadmap Step

```text
AGENT-TEMPLATE-VALIDATION-001
```
