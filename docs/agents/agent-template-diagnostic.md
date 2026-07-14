# IAMINE Agent Diagnostic Template

Feature:

```text
AGENT-TEMPLATE-DIAGNOSTIC-001
```

## Purpose

Define the future diagnostic agent template boundary without implementing an
agent, runtime, command execution, file reads, network probes, persistence,
package installation, registry publication, marketplace publication, or model
inference.

Diagnostic templates are for structured status reports only. This document does
not authorize shell execution, destructive actions, unrestricted filesystem or
network access, secret access, wallet access, rewards, settlement, mainnet
behavior, or distributed model MoE.

## Template Question

Diagnostic template policy answers:

```text
What may a diagnostic template claim and report before implementation?
```

It does not answer whether diagnostic commands, probes, runtime adapters,
approval UI, audit logs, or transports exist.

## Draft Schema

```text
iamine.agent.template.diagnostic.draft-0.1
```

## Allowed Diagnostic Scope

Future diagnostic templates may request only bounded diagnostic intent:

```text
read_status
summarize_health
explain_findings
request_clarification
handoff_for_action
```

They must not mutate state, install packages, restart services, open network
listeners, change configuration, delete files, or collect secrets.

## Required Output Classes

Diagnostic outputs must be operator-safe:

```text
diagnostic_summary
finding_list
blocked_action_report
clarification_request
handoff_request
```

## Privacy Rules

Diagnostic template metadata must not include credentials, private keys, wallet
keys, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Diagnostic templates cannot authorize runtime execution.
- Diagnostic templates cannot execute shell commands.
- Diagnostic templates cannot read arbitrary files.
- Diagnostic templates cannot perform network scans.
- Diagnostic templates cannot fix, restart, install, delete, or mutate state.
- Diagnostic templates cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-TEMPLATE-FILE-READONLY-001
```
