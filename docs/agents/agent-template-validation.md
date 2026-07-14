# IAMINE Agent Template Validation

Feature:

```text
AGENT-TEMPLATE-VALIDATION-001
```

## Purpose

Define validation rules for future IAMINE agent skeletons and templates without
implementing validators, CLIs, file writes, template rendering, package
installation, registry publication, marketplace publication, runtime startup,
or agent execution.

This document does not authorize executable agents, filesystem mutation, shell
execution, unrestricted network access, dependency installation, auto
publication, wallet, reward, settlement, mainnet behavior, or distributed model
MoE.

## Validation Question

Template validation answers:

```text
What must be true before a future agent template can be accepted?
```

It does not answer whether validators, CLI commands, audit logs, generation,
packaging, publication, or runtime enforcement exists.

## Draft Schema

```text
iamine.agent.template_validation.draft-0.1
```

## Required Validation Gates

Future template validation must check:

```text
manifest_schema_valid
agent_id_valid
declared_scope_bounded
permissions_bounded
runtime_language_allowed
input_contract_present
output_contract_present
timeout_policy_present
handoff_policy_present
out_of_scope_policy_present
boundary_tests_present
no_forbidden_defaults
```

Unknown, missing, contradictory, broad, unsafe, stale, or unverifiable template
metadata must block validation by default.

## Forbidden Defaults

Templates must fail validation when they request:

```text
generic_do_anything_scope
arbitrary_shell
unrestricted_filesystem
unrestricted_network
secret_access
wallet_access
auto_publication
runtime_execution_by_default
```

## Validation Outcomes

Future validation must return one outcome:

```text
valid
invalid
blocked
manual_review_required
```

Validation success cannot imply runtime approval, trust, reputation,
marketplace publication, or production readiness.

## Privacy Rules

Template validation metadata must not include credentials, private keys, wallet
keys, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Validation policy cannot authorize runtime execution.
- Validation policy cannot grant permissions.
- Validation policy cannot publish templates.
- Validation policy cannot install dependencies.
- Validation policy cannot bypass scope review, permission review, boundary
  tests, manual validation, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-FRAMEWORK-BASELINE-001
```
