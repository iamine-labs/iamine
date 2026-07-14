# IAMINE Internal Agent Permission Review Assistant

Feature:

```text
AGENT-PERMISSION-REVIEW-AGENT-001-INTERNAL
```

## Purpose

Define the future internal permission review assistant boundary without
implementing permission grants, manifest mutation, runtime authorization,
policy engine changes, publication, marketplace behavior, package installation,
or model inference.

The permission review assistant may analyze requested permissions and produce
operator-visible review findings. It does not grant permissions, approve
execution, modify manifests, or publish agents by itself.

## Assistant Question

Internal permission review assistant policy answers:

```text
What boundaries must a future IAMINE permission review assistant preserve?
```

It does not answer whether policy engines, permission stores, approval UI,
runtime enforcement, audit logs, or registry adapters exist.

## Draft Schema

```text
iamine.agent.internal.permission_review.draft-0.1
```

## Allowed Scope

Future internal permission review assistants may request only:

```text
summarize_requested_permissions
map_permissions_to_declared_scope
identify_broad_or_destructive_permissions
identify_missing_permission_justification
request_clarification
recommend_manual_review_questions
handoff_to_scope_review
handoff_for_operator_approval
```

They must not grant permissions, approve permissions, edit manifests, execute
commands, install dependencies, publish agents, mutate registries, or claim
validation without source evidence.

## Required Guards

Future assistants must declare:

```text
permission_source_policy
least_privilege_policy
destructive_permission_policy
filesystem_permission_policy
network_permission_policy
approval_handoff_policy
operator_visible_summary
```

## Privacy Rules

Permission review metadata must not include credentials, private keys, wallet
keys, tokens, usernames, full hostnames, IP addresses, MAC addresses, serial
numbers, disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs,
raw process lists, unredacted logs, personal communications, or permanent
hardware fingerprints.

## Boundary Rules

- Permission review assistants cannot authorize runtime execution.
- Permission review assistants cannot grant or approve permissions.
- Permission review assistants cannot mutate manifests or policy stores.
- Permission review assistants cannot approve destructive permissions by
  default.
- Permission review assistants cannot publish to registry or marketplace.
- Permission review assistants cannot claim facts without provided evidence.
- Permission review assistants cannot bypass validation, scope review,
  boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-SCOPE-REVIEW-AGENT-001-INTERNAL
```
