# IAMINE Node Doctor Agent Skeleton

Feature:

```text
NODE-DOCTOR-AGENT-001-SKELETON
```

## Purpose

Define the official P0 Node Doctor agent skeleton as a reviewable, local-only,
read-only planning contract. It describes the narrow future package boundary
for explaining IAMINE node readiness from already approved, redacted IAMINE
evidence.

This feature does not create an agent package, TOML manifest, executable code,
CLI command, agent runtime, model dependency, sandbox, audit emitter, registry
entry, or public beta listing.

## Status

```text
skeleton
non_executable
not_user_available
execution_authorized: false
```

The skeleton feature is closed as a planning contract. Functional
`NODE-DOCTOR-AGENT-001` development remains blocked until every executable
runtime and enforcement prerequisite has implementation and validation
evidence.

## Product Boundary

Node Doctor serves the following documented personas:

```text
Home Troubleshooter
Non-Technical Caretaker
Multi-PC Household Operator for single-node readiness questions
```

Its only planned task class is:

```text
diagnostic_report
```

The future package identity is reserved as:

```text
package_id: iamine.beta.node-doctor
scope_id: node_readiness_diagnostic_report
earliest_mode: local_readonly
execution_authorized: false
```

These labels are planning metadata, not executable manifest fields and not a
permission grant.

## Future Package Shape

When a later feature creates the package instance, it must follow the closed
agent skeleton standard and remain inside one package root:

```text
<node-doctor-package>/
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
    README.md
  src/
    README.md
  review/
    human-review.md
    qa-evidence.md
```

All future references must be package-relative. Absolute local paths, host
identifiers, credentials, and private machine data are blocked.

## Declared Scope

The future agent may only produce a review response from bounded inputs:

```text
explain_iamine_node_readiness
summarize_allowed_status_evidence
identify_known_readiness_gaps
suggest_non_destructive_next_steps
request_clarification
handoff_to_orchestrator
```

Allowed future input classes are limited to:

```text
iamine_node_status_summary
iamine_readiness_checklist
redacted_hardware_profile_summary
user_provided_error_text
```

The skeleton must treat all inputs as untrusted. Raw logs, private files, raw
process lists, raw hardware inventories, and machine-specific identifiers are
not default inputs.

Allowed future output classes are:

```text
diagnostic_report
result_summary
clarification_request
handoff_request
refusal_report
blocked_action_report
```

Outputs may explain a finding and recommend a manual next step, but they must
not claim that a repair, command, probe, service action, or runtime execution
occurred.

## Permissions and Resources

The future Node Doctor package must declare denial by default. Its only planned
review categories are:

```text
local_readonly
user_provided_text
redacted_status_summary
```

The initial resource profile is bounded to `local_readonly`: no network access,
no open ports, no artifact downloads, no model download, no model load, no
accelerator requirement, no worker startup, and no dynamic hardware probe.

User confirmation cannot make a blocked action allowed. Any requested
permission outside the declared categories must refuse or return control to the
orchestrator.

## Audit Boundary

The future package must reference the closed audit-policy contract and require
only the following review evidence classes:

```text
review_started
scope_checked
permission_checked
redaction_checked
handoff_required
refusal_recorded
```

Future evidence must be redacted by default, operator-local, and review-only.
It must not retain raw prompts, raw outputs, private paths, host identifiers,
credentials, or unredacted logs. This skeleton does not create an audit
emitter, storage record, retention mechanism, or sharing channel.

## Blocked Actions

Node Doctor must not:

- execute shell commands or scripts;
- read arbitrary files or directories;
- write, delete, rename, or upload files;
- change IAMINE, operating-system, router, firewall, VM, or container settings;
- start, stop, restart, or inspect services or processes directly;
- start workers, P2P, PubSub, downloads, model loads, inference, or dynamic
  hardware probes;
- scan the LAN, contact a remote node, open a network listener, or transmit
  evidence;
- request or retain credentials, private keys, wallet material, or tokens;
- collect usernames, full hostnames, IP addresses, MAC addresses, serial
  numbers, disk UUIDs, machine IDs, private paths, raw prompts, raw outputs,
  raw process lists, unredacted logs, or permanent hardware fingerprints;
- publish to a registry, marketplace, or third party; or
- claim trust, reputation, rewards, settlement, mainnet, or distributed model
  MoE behavior.

## Refusal and Handoff

The future agent must refuse or hand off when a request is outside the declared
readiness-report scope, needs a broader diagnostic source, requests private
data, asks for a mutation, asks for remote or LAN execution, or attempts to
override these limits.

Prompt-injection and role-confusion text cannot override the scope, permissions,
blocked actions, privacy rules, or handoff requirement.

It must return to the orchestrator for:

```text
system repair
service administration
network or router changes
VM or container changes
file recovery or modification
credential handling
remote execution
missing or contradictory evidence
unsafe or ambiguous requests
```

## Required Future Evals

Before a later Node Doctor package can execute, its package-relative boundary
evals must cover:

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
local_only
```

Missing, broad, contradictory, unsafe, stale, unverifiable, or privacy-invasive
metadata must block package review, installation, registry advancement, and
execution by default.

## Relationship to LAN Node Doctor

`iamine-node lan doctor` remains an existing node CLI diagnostic surface. This
skeleton neither invokes nor wraps that command, and it does not change the
command's checks, output contract, or runtime boundary. A later implementation
may consume only an approved, redacted summary through its dedicated contracts.

That dedicated source is `NODE-DOCTOR-EVIDENCE-PROVIDER-001`. It is a
separate, non-agent feature and must expose only typed, redacted, read-only
evidence. It cannot authorize command execution, raw log access, node mutation,
or direct user-facing agent behavior.

## Next Roadmap Step

```text
AGENT-MANIFEST-PARSER-VALIDATOR-001
```

The parser/validator feature may create canonical types, schemas, fixtures,
validators, and tests. It must not load or execute agent packages. The
functional Node Doctor feature remains blocked until the complete prerequisite
chain in `docs/roadmap/iamine-agent-network-roadmap.md` is satisfied.
