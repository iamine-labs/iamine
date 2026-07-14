# IAMINE Agent Reporter Template

Feature:

```text
AGENT-TEMPLATE-REPORTER-001
```

## Purpose

Define the future reporter agent template boundary without implementing report
generation, file reads, network probes, runtime execution, persistence,
publication, package installation, marketplace behavior, or model inference.

Reporter templates format operator-approved evidence. They do not collect new
evidence by themselves, execute actions, mutate state, or publish reports.

## Template Question

Reporter template policy answers:

```text
What boundaries must a future report-producing agent preserve?
```

It does not answer whether report rendering, file export, transport,
approval UI, audit logs, or runtime adapters exist.

## Draft Schema

```text
iamine.agent.template.reporter.draft-0.1
```

## Allowed Scope

Future reporter templates may request only:

```text
summarize_provided_evidence
format_operator_visible_report
highlight_missing_evidence
request_clarification
handoff_for_collection_or_action
```

They must not collect files, probe networks, infer hidden facts, disclose
private data, fabricate evidence, or claim validation without source evidence.

## Required Guards

Future templates must declare:

```text
evidence_source_policy
redaction_policy
unsupported_claim_policy
operator_visible_summary
export_policy
handoff_policy
```

## Privacy Rules

Reporter metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Reporter templates cannot authorize runtime execution.
- Reporter templates cannot collect evidence by themselves.
- Reporter templates cannot read arbitrary files or probe networks.
- Reporter templates cannot export or publish reports by default.
- Reporter templates cannot claim facts without provided evidence.
- Reporter templates cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-TEMPLATE-TEXT-ASSISTANT-001
```
