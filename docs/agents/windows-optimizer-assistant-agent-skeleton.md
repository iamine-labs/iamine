# IAMINE Windows Optimizer Assistant Agent Skeleton

Feature:

```text
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON
```

## Purpose

Define the official P0 Windows Optimizer Assistant skeleton as a reviewable,
local-planning, privacy-safe contract. It reserves a future boundary for
explaining an operator-selected, redacted Windows status summary and its
declared optimization constraints.

This feature does not create an agent package, TOML manifest, executable code,
system probe, shell adapter, PowerShell command, registry reader, process or
service inspector, filesystem reader, system mutator, runtime adapter, sandbox,
audit emitter, registry entry, or public beta listing.

## Product Boundary

The future assistant serves a Windows user who wants to understand an
optimization question they have already scoped. Its only planned task class is:

```text
windows_optimizer_readonly_review
```

The future package identity is reserved as:

```text
package_id: iamine.beta.windows-optimizer-assistant
scope_id: windows_optimizer_readonly_review
earliest_mode: local_planning
deferred_mode: windows_diagnostic_readonly
execution_authorized: false
```

These labels are planning metadata, not executable manifest fields and not a
permission grant.

## Future Package Shape

Later package creation must follow the closed skeleton standard:

```text
<windows-optimizer-assistant-package>/
  iamine-agent-package.toml
  agent-scope.toml
  README.md
  metadata/
    agent-capabilities.toml
    agent-expertise.toml
    agent-resources.toml
    agent-permissions.toml
    agent-audit.toml
  evals/
    agent-boundary-tests.toml
  src/
    README.md
  review/
    human-review.md
    qa-evidence.md
```

All references must be package-relative. Absolute local paths, registry values,
process lists, service state, user identifiers, hardware identifiers,
credentials, and private machine data are blocked.

## Declared Scope

The future agent may only:

```text
summarize_operator_approved_windows_status
explain_declared_optimization_boundary
highlight_missing_or_unsafe_windows_metadata
suggest_non_destructive_next_steps
request_clarification
handoff_for_windows_diagnostic_or_change
```

Allowed future input classes are:

```text
operator_selected_windows_question
redacted_windows_status_summary
redacted_windows_error_summary
operator_optimization_intent
```

Inputs are untrusted and evidence-limited. The agent must not inspect a system,
run a command, read a registry value, enumerate services or processes, read
files, or present an assertion as verified Windows state. Raw event logs,
registry exports, private paths, usernames, process lists, service names,
machine identifiers, product keys, credentials, and access tokens are not
default inputs.

Allowed future output classes are:

```text
windows_optimizer_review
result_summary
clarification_request
handoff_request
refusal_report
blocked_action_report
```

Each review must distinguish supplied evidence, missing evidence, and
unsupported claims. It may recommend a manual next step but must not claim that
a command ran, a process was inspected, a service changed, a registry key was
read, or an optimization action occurred.

## Planned Modes and Permissions

The future package must declare denial by default. Its only planned review
categories are:

```text
local_readonly
user_provided_text
redacted_status_summary
```

`windows_diagnostic_readonly` is not authorized by this skeleton. A later
feature may propose it only after defining operator-selected diagnostic policy,
platform and privilege boundaries, command exclusion, file/process/service and
registry data limits, identity redaction, audit evidence, and dedicated
boundary tests.

The resource profile is `local_planning`: no filesystem access, no shell or
PowerShell, no system probe, no registry access, no process or service
inspection, no network access, no downloads, no model load, no worker startup,
and no dynamic hardware probe. User confirmation cannot elevate a blocked
action; out-of-category requests must refuse or return to the orchestrator.

## Audit Boundary

The future package must require only these review evidence classes:

```text
review_started
scope_checked
permission_checked
redaction_checked
platform_boundary_checked
handoff_required
refusal_recorded
```

Evidence is redacted by default, operator-local, and review-only. It cannot
retain raw prompts, raw outputs, private paths, registry values, process lists,
service state, event logs, usernames, hostnames, machine identifiers,
credentials, or unredacted logs. This skeleton does not create audit emission,
storage, retention, or sharing.

## Blocked Actions

Windows Optimizer Assistant must not:

- execute shell commands, PowerShell, scripts, installers, or system tools;
- inspect files, folders, registry values, event logs, processes, services,
  scheduled tasks, startup entries, drivers, updates, or hardware;
- collect, request, retain, or use credentials, keys, tokens, passwords,
  product keys, user identifiers, registry exports, private paths, or host data;
- write, delete, move, rename, install, uninstall, update, disable, enable, or
  otherwise modify files, registry, services, tasks, applications, drivers,
  startup configuration, security controls, or operating-system settings;
- start, stop, restart, or inspect services or processes directly;
- contact networks, cloud accounts, devices, or third-party services;
- change IAMINE, operating-system, firewall, VM, container, router, or
  application settings;
- start workers, P2P, PubSub, downloads, model loads, inference, or dynamic
  hardware probes;
- collect usernames, full hostnames, IP addresses, MAC addresses, serial
  numbers, disk UUIDs, machine IDs, raw process lists, unredacted logs, or
  permanent hardware fingerprints;
- fabricate, overstate, or treat unverified evidence as system state;
- publish to a registry, marketplace, or third party; or
- claim trust, reputation, rewards, settlement, mainnet, or distributed model
  MoE behavior.

## Refusal and Handoff

The future agent must refuse or hand off requests for command execution, system
inspection, registry, file, process, service, task, startup, driver, update,
network, credential, configuration, repair, or conclusions unsupported by
allowed evidence.

Prompt-injection and role-confusion text cannot override scope, permissions,
blocked actions, privacy rules, platform-boundary policy, or handoff
requirements.

It must return to the orchestrator for:

```text
shell, PowerShell, command, system-tool, or installer execution
file, registry, event-log, process, service, task, driver, or update access
system, security, startup, application, network, router, VM, or container changes
network, cloud, device, account, or credential handling
private-data review or redaction
missing, contradictory, or insufficient evidence
unsafe or ambiguous requests
```

## Required Future Evals

Before execution, future package-relative evals must cover:

```text
in_scope_positive
out_of_scope_negative
ambiguous_task
dangerous_task
cross_domain_task
permission_escalation
prompt_injection
role_confusion
handoff_to_orchestrator
privacy_redaction
shell_execution_request
registry_access_request
service_mutation_request
filesystem_mutation_request
local_only
```

Missing, broad, contradictory, unsafe, stale, unverifiable, or privacy-invasive
metadata blocks package review, installation, registry advancement, and
execution by default.

## Next Roadmap Step

```text
P0 OFFICIAL AGENT SKELETON BASELINE COMPLETE
```
