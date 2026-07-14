# IAMINE Agent OS Diagnostic Template

Feature:

```text
AGENT-TEMPLATE-OS-DIAGNOSTIC-001
```

## Purpose

Define the future OS diagnostic agent template boundary without implementing
system probes, command execution, file reads, process inspection, network
access, persistence, publication, package installation, marketplace behavior, or
model inference.

OS diagnostic templates reason over operator-provided operating-system facts.
They do not inspect the machine by themselves, fingerprint hosts, mutate state,
or claim validation without supplied source material.

## Template Question

OS diagnostic template policy answers:

```text
What boundaries must a future operating-system diagnostic agent preserve?
```

It does not answer whether probes, shell adapters, privilege escalation,
telemetry, audit logs, runtime execution, or agent marketplace publication
exist.

## Draft Schema

```text
iamine.agent.template.os_diagnostic.draft-0.1
```

## Allowed Scope

Future OS diagnostic templates may request only:

```text
summarize_operator_provided_os_facts
classify_missing_os_context
identify_privacy_sensitive_fields
request_clarification
handoff_for_operator_approved_probe
handoff_for_platform_specific_diagnostic
```

They must not execute commands, read arbitrary files, inspect processes, probe
networks, infer hidden machine identity, disclose private data, approve scope,
approve permissions, or claim validation without source evidence.

## Required Guards

Future templates must declare:

```text
os_metadata_source_policy
platform_scope_policy
unsupported_probe_policy
identity_redaction_policy
process_data_policy
network_metadata_policy
handoff_policy
operator_visible_summary
```

## Privacy Rules

OS diagnostic metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, personal communications, or permanent hardware
fingerprints.

## Boundary Rules

- OS diagnostic templates cannot authorize runtime execution.
- OS diagnostic templates cannot execute shell commands.
- OS diagnostic templates cannot read arbitrary files or inspect processes.
- OS diagnostic templates cannot probe networks or collect host identity.
- OS diagnostic templates cannot mutate OS configuration or install packages.
- OS diagnostic templates cannot claim facts without provided evidence.
- OS diagnostic templates cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
IAMINE-DEV-SETUP-AGENT-001-INTERNAL
```
