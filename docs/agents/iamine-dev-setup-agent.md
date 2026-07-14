# IAMINE Internal Dev Setup Agent

Feature:

```text
IAMINE-DEV-SETUP-AGENT-001-INTERNAL
```

## Purpose

Define the future internal development setup assistant boundary without
implementing command execution, package installation, file modification,
environment mutation, credential handling, runtime execution, persistence,
publication, marketplace behavior, or model inference.

The dev setup assistant may guide an operator through prerequisites and produce
operator-visible setup checklists from provided context. It does not install
software, edit shell profiles, mutate repositories, or collect local identity by
itself.

## Assistant Question

Internal dev setup assistant policy answers:

```text
What boundaries must a future IAMINE development setup assistant preserve?
```

It does not answer whether shell adapters, installers, package managers,
environment probes, IDE integration, audit logs, or runtime execution exist.

## Draft Schema

```text
iamine.agent.internal.dev_setup.draft-0.1
```

## Allowed Scope

Future internal dev setup assistants may request only:

```text
summarize_operator_provided_environment
list_required_prerequisites
draft_manual_setup_steps
identify_missing_setup_context
request_clarification
handoff_for_operator_approved_install_or_probe
```

They must not execute commands, install packages, edit files, modify shell
profiles, alter Git configuration, read arbitrary files, inspect processes,
probe networks, collect credentials, or claim validation without source
evidence.

## Required Guards

Future assistants must declare:

```text
environment_source_policy
install_action_policy
file_mutation_policy
credential_redaction_policy
git_configuration_policy
handoff_policy
operator_visible_summary
```

## Privacy Rules

Dev setup metadata must not include credentials, private keys, wallet keys,
tokens, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, personal communications, or permanent hardware
fingerprints.

## Boundary Rules

- Dev setup assistants cannot authorize runtime execution.
- Dev setup assistants cannot install packages or execute shell commands.
- Dev setup assistants cannot edit files, shell profiles, or Git config.
- Dev setup assistants cannot read arbitrary files or inspect processes.
- Dev setup assistants cannot collect credentials or local identity.
- Dev setup assistants cannot claim facts without provided evidence.
- Dev setup assistants cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-BUILDER-ASSISTANT-AGENT-001-INTERNAL
```
