# IAMINE Internal Agent Scope Review Assistant

Feature:

```text
AGENT-SCOPE-REVIEW-AGENT-001-INTERNAL
```

## Purpose

Define the future internal scope review assistant boundary without implementing
scope approval, manifest mutation, permission grants, runtime authorization,
policy engine changes, publication, marketplace behavior, or model inference.

The scope review assistant may analyze declared scope and produce
operator-visible review findings. It does not approve scope, expand scope,
grant permissions, modify manifests, or publish agents by itself.

## Assistant Question

Internal scope review assistant policy answers:

```text
What boundaries must a future IAMINE scope review assistant preserve?
```

It does not answer whether scope engines, approval UI, manifest writers,
runtime enforcement, audit logs, or registry adapters exist.

## Draft Schema

```text
iamine.agent.internal.scope_review.draft-0.1
```

## Allowed Scope

Future internal scope review assistants may request only:

```text
summarize_declared_agent_scope
map_scope_to_declared_goals
identify_broad_or_ambiguous_scope
identify_out_of_scope_actions
request_clarification
recommend_manual_review_questions
handoff_to_permission_review
handoff_to_boundary_test_generator
```

They must not approve scope, expand scope, edit manifests, grant permissions,
execute commands, publish agents, mutate registries, or claim validation
without source evidence.

## Required Guards

Future assistants must declare:

```text
scope_source_policy
goal_alignment_policy
out_of_scope_policy
broad_scope_policy
permission_alignment_policy
boundary_test_handoff_policy
operator_visible_summary
```

## Privacy Rules

Scope review metadata must not include credentials, private keys, wallet keys,
tokens, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, personal communications, or permanent hardware
fingerprints.

## Boundary Rules

- Scope review assistants cannot authorize runtime execution.
- Scope review assistants cannot approve or expand scope.
- Scope review assistants cannot mutate manifests or policy stores.
- Scope review assistants cannot grant or approve permissions.
- Scope review assistants cannot accept generic do_anything scope.
- Scope review assistants cannot claim facts without provided evidence.
- Scope review assistants cannot bypass validation, permission review,
  boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-BOUNDARY-TEST-GENERATOR-AGENT-001-INTERNAL
```
