# IAMINE Agent Text Assistant Template

Feature:

```text
AGENT-TEMPLATE-TEXT-ASSISTANT-001
```

## Purpose

Define the future text assistant agent template boundary without implementing
chat runtime, prompt routing, tool execution, file reads, network access,
persistence, publication, package installation, marketplace behavior, or model
inference.

Text assistant templates transform operator-provided text. They do not collect
new evidence by themselves, execute actions, mutate state, impersonate users, or
claim validation without supplied source material.

## Template Question

Text assistant template policy answers:

```text
What boundaries must a future text-assistance agent preserve?
```

It does not answer whether chat UI, memory, model routing, tool adapters,
runtime execution, audit logs, or agent marketplace publication exist.

## Draft Schema

```text
iamine.agent.template.text_assistant.draft-0.1
```

## Allowed Scope

Future text assistant templates may request only:

```text
rewrite_operator_provided_text
summarize_operator_provided_text
draft_operator_visible_response
identify_missing_context
request_clarification
handoff_for_evidence_collection_or_action
```

They must not read arbitrary files, browse networks, execute commands, infer
hidden facts, disclose private data, fabricate evidence, approve scope, approve
permissions, or claim validation without source evidence.

## Required Guards

Future templates must declare:

```text
context_source_policy
prompt_data_policy
unsupported_claim_policy
action_boundary_policy
redaction_policy
handoff_policy
operator_visible_summary
```

## Privacy Rules

Text assistant metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, personal communications, or permanent hardware
fingerprints.

## Boundary Rules

- Text assistant templates cannot authorize runtime execution.
- Text assistant templates cannot collect evidence by themselves.
- Text assistant templates cannot read arbitrary files or probe networks.
- Text assistant templates cannot execute commands or mutate state.
- Text assistant templates cannot impersonate users or services.
- Text assistant templates cannot claim facts without provided evidence.
- Text assistant templates cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-TEMPLATE-OS-DIAGNOSTIC-001
```
