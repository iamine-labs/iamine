# IAMINE Internal Agent Builder Assistant

Feature:

```text
AGENT-BUILDER-ASSISTANT-AGENT-001-INTERNAL
```

## Purpose

Define the future internal agent builder assistant boundary without
implementing file generation, manifest persistence, permission approval, scope
approval, runtime execution, package installation, publication, marketplace
behavior, or model inference.

The agent builder assistant may transform operator-provided requirements into a
reviewable agent proposal. It does not create files, approve the proposal,
grant permissions, publish agents, or execute the generated agent by itself.

## Assistant Question

Internal agent builder assistant policy answers:

```text
What boundaries must a future IAMINE agent builder assistant preserve?
```

It does not answer whether builders, manifest writers, package generators,
permission prompts, registry adapters, audit logs, or runtime execution exist.

## Draft Schema

```text
iamine.agent.internal.builder_assistant.draft-0.1
```

## Allowed Scope

Future internal agent builder assistants may request only:

```text
summarize_operator_requested_agent_goal
draft_agent_scope_proposal
draft_permission_request_proposal
identify_missing_requirements
request_clarification
handoff_to_manifest_wizard
handoff_to_scope_review
handoff_to_permission_review
```

They must not write files, generate packages, approve scope, approve
permissions, execute commands, install dependencies, publish agents, mutate
registries, or claim validation without source evidence.

## Required Guards

Future assistants must declare:

```text
requirements_source_policy
scope_proposal_policy
permission_proposal_policy
file_generation_policy
publication_policy
review_handoff_policy
operator_visible_summary
```

## Privacy Rules

Agent builder metadata must not include credentials, private keys, wallet keys,
tokens, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, personal communications, or permanent hardware
fingerprints.

## Boundary Rules

- Agent builder assistants cannot authorize runtime execution.
- Agent builder assistants cannot generate files or packages by default.
- Agent builder assistants cannot approve scope or permissions.
- Agent builder assistants cannot publish to registry or marketplace.
- Agent builder assistants cannot execute commands or install dependencies.
- Agent builder assistants cannot claim facts without provided evidence.
- Agent builder assistants cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-MANIFEST-WIZARD-AGENT-001-INTERNAL
```
