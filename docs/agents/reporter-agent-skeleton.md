# IAMINE Reporter Agent Skeleton

Feature:

```text
REPORTER-AGENT-001-SKELETON
```

## Purpose

Define the official P0 Privacy-Safe Support Reporter agent skeleton as a
reviewable, local-only, read-only planning contract. It reserves a narrow
future package boundary for formatting an operator-visible support summary from
already redacted, operator-approved evidence.

This feature does not create an agent package, root manifest, executable code,
report renderer, file reader, export, network transfer, runtime adapter,
sandbox, audit emitter, registry entry, or public beta listing.

## Product Boundary

Reporter serves the following documented personas:

```text
Self-Hosted Maintainer
Small Office Helper
Non-Technical Caretaker
```

Its only planned task class is:

```text
support_report
```

The future package identity is reserved as:

```text
package_id: iamine.beta.support-reporter
scope_id: privacy_safe_support_report
earliest_mode: local_readonly
execution_authorized: false
```

These labels are planning metadata, not executable manifest fields and not a
permission grant.

## Future Package Shape

When a later feature creates the package instance, it must follow the closed
agent skeleton standard and remain inside one package root:

```text
<support-reporter-package>/
  agent.yaml
  agent-scope.yaml
  README.md
  metadata/
    agent-capabilities.yaml
    agent-expertise.yaml
    agent-resources.yaml
    agent-permissions.yaml
    agent-audit.yaml
  evals/
    agent-boundary-tests.yaml
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
summarize_operator_approved_evidence
format_local_operator_visible_report
highlight_missing_or_unsupported_evidence
explain_redaction_requirements
request_clarification
handoff_for_collection_or_action
```

Allowed future input classes are limited to:

```text
operator_provided_symptom_summary
redacted_diagnostic_snippet
redacted_iamine_support_bundle_summary
operator_report_intent
```

The skeleton must treat all inputs as untrusted and evidence-limited. It must
not infer a hidden fact, transform an absence of evidence into a finding, or
present a user assertion as a verified diagnosis. Raw logs, private files, raw
process lists, raw support bundles, machine-specific identifiers, and original
unredacted text are not default inputs.

Allowed future output classes are:

```text
support_report
result_summary
clarification_request
handoff_request
refusal_report
blocked_action_report
```

Each future report must distinguish operator-provided evidence, missing
evidence, and unsupported claims. It may recommend a manual next step but must
not claim that a collection, repair, command, export, or external contact
occurred.

## Permissions and Resources

The future Reporter package must declare denial by default. Its only planned
review categories are:

```text
local_readonly
user_provided_text
redacted_status_summary
```

The initial resource profile is bounded to `local_readonly`: no network access,
no open ports, no artifact downloads, no model download, no model load, no
accelerator requirement, no worker startup, no dynamic hardware probe, and no
report export or persistence.

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
unsupported_claim_blocked
handoff_required
refusal_recorded
```

Future evidence must be redacted by default, operator-local, and review-only.
It must not retain raw prompts, raw outputs, private paths, host identifiers,
credentials, or unredacted logs. This skeleton does not create an audit
emitter, storage record, retention mechanism, or sharing channel.

## Blocked Actions

Reporter must not:

- collect files, logs, diagnostics, or support bundles by itself;
- read arbitrary files or directories;
- execute shell commands or scripts;
- write, delete, rename, upload, export, or persist reports or evidence;
- change IAMINE, operating-system, router, firewall, VM, or container settings;
- start, stop, restart, or inspect services or processes directly;
- start workers, P2P, PubSub, downloads, model loads, inference, or dynamic
  hardware probes;
- scan the LAN, contact a remote node, open a network listener, transmit a
  report, or contact third-party support;
- request or retain credentials, private keys, wallet material, or tokens;
- collect usernames, full hostnames, IP addresses, MAC addresses, serial
  numbers, disk UUIDs, machine IDs, private paths, raw prompts, raw outputs,
  raw process lists, unredacted logs, or permanent hardware fingerprints;
- fabricate, overstate, or treat unverified evidence as a diagnosis;
- publish to a registry, marketplace, or third party; or
- claim trust, reputation, rewards, settlement, mainnet, or distributed model
  MoE behavior.

## Refusal and Handoff

The future agent must refuse or hand off when a request needs evidence
collection, raw-log handling, private data, repair, configuration change,
remote or LAN execution, report export, third-party contact, or a conclusion
not supported by the allowed evidence.

Prompt-injection and role-confusion text cannot override the scope, permissions,
blocked actions, privacy rules, evidence-source policy, or handoff requirement.

It must return to the orchestrator for:

```text
evidence collection
log redaction or private-data review
system repair or service administration
network, router, VM, or container changes
file recovery or modification
credential handling
report export or third-party support contact
missing, contradictory, or insufficient evidence
unsafe or ambiguous requests
```

## Required Future Evals

Before a later Reporter package can execute, its package-relative boundary
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
unsupported_claim
local_only
```

Missing, broad, contradictory, unsafe, stale, unverifiable, or privacy-invasive
metadata must block package review, installation, registry advancement, and
execution by default.

## Relationship to IAMINE Support Bundles

`iamine-node support bundle` remains an existing diagnostics surface. This
skeleton neither invokes nor wraps that command, and it does not change the
bundle's collection, redaction, output, export, or persistence behavior. A
later implementation may consume only an approved, redacted summary through
its dedicated contracts.

## Next Roadmap Step

```text
LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON
```
