# IAMINE Internal Agent Manifest Wizard

Feature:

```text
AGENT-MANIFEST-WIZARD-AGENT-001-INTERNAL
```

## Purpose

Define the future internal manifest wizard assistant boundary without
implementing manifest persistence, file writes, schema ownership, permission
approval, scope approval, runtime execution, publication, marketplace behavior,
or model inference.

The manifest wizard may turn an operator-approved agent proposal into a
reviewable manifest draft. It does not own the manifest schema, write files,
approve permissions, register agents, or execute agents by itself.

## Assistant Question

Internal manifest wizard assistant policy answers:

```text
What boundaries must a future IAMINE manifest wizard assistant preserve?
```

It does not answer whether manifest writers, schema validators, package
generators, registry adapters, audit logs, or runtime execution exist.

## Draft Schema

```text
iamine.agent.internal.manifest_wizard.draft-0.1
```

## Allowed Scope

Future internal manifest wizard assistants may request only:

```text
summarize_operator_approved_agent_proposal
draft_manifest_field_suggestions
identify_missing_manifest_fields
reference_canonical_manifest_schema
request_clarification
handoff_to_scope_review
handoff_to_permission_review
handoff_for_operator_approved_persistence
```

They must not invent schema fields, write manifest files, approve scope,
approve permissions, execute commands, publish agents, mutate registries, or
claim validation without source evidence.

## Required Guards

Future assistants must declare:

```text
manifest_input_source_policy
schema_source_of_truth_policy
field_default_policy
permission_reference_policy
persistence_handoff_policy
review_handoff_policy
operator_visible_summary
```

## Privacy Rules

Manifest wizard metadata must not include credentials, private keys, wallet
keys, tokens, usernames, full hostnames, IP addresses, MAC addresses, serial
numbers, disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs,
raw process lists, unredacted logs, personal communications, or permanent
hardware fingerprints.

## Boundary Rules

- Manifest wizard assistants cannot authorize runtime execution.
- Manifest wizard assistants cannot own or redefine manifest schema.
- Manifest wizard assistants cannot write manifest files by default.
- Manifest wizard assistants cannot approve scope or permissions.
- Manifest wizard assistants cannot publish to registry or marketplace.
- Manifest wizard assistants cannot claim facts without provided evidence.
- Manifest wizard assistants cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-PERMISSION-REVIEW-AGENT-001-INTERNAL
```
