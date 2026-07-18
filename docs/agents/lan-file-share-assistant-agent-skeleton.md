# IAMINE LAN File Share Assistant Agent Skeleton

Feature:

```text
LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON
```

## Purpose

Define the official P0 LAN File Share Assistant skeleton as a reviewable,
local-planning, read-only contract. It reserves a future boundary for explaining
an operator-approved, redacted file-share inventory and its declared limits.

This feature does not create an agent package, root manifest, executable code,
file reader, SMB or NFS client, mount, share discovery probe, network transfer,
runtime adapter, sandbox, audit emitter, registry entry, or public beta listing.

## Product Boundary

The future assistant serves a household or small-office operator who needs help
understanding a share they have already selected. Its only planned task class is:

```text
file_share_readonly_review
```

The future package identity is reserved as:

```text
package_id: iamine.beta.lan-file-share-assistant
scope_id: lan_file_share_readonly_review
earliest_mode: local_planning
deferred_mode: lan_readonly
execution_authorized: false
```

These labels are planning metadata, not executable manifest fields and not a
permission grant.

## Future Package Shape

Later package creation must follow the closed skeleton standard:

```text
<lan-file-share-assistant-package>/
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
  src/
    README.md
  review/
    human-review.md
    qa-evidence.md
```

All references must be package-relative. Absolute local paths, share paths,
host identifiers, credentials, and private machine data are blocked.

## Declared Scope

The future agent may only:

```text
summarize_operator_approved_share_inventory
explain_declared_readonly_boundary
highlight_missing_or_unsafe_share_metadata
suggest_non_destructive_next_steps
request_clarification
handoff_for_file_or_network_action
```

Allowed future input classes are:

```text
operator_selected_share_question
redacted_share_inventory_summary
redacted_access_error_summary
operator_intent
```

Inputs are untrusted and evidence-limited. The agent must not infer a share
location, enumerate a network, resolve a hostname, retry authentication, or
present an assertion as verified share state. Raw directory listings, file
contents, unredacted share paths, hostnames, IP addresses, credentials, and
access tokens are not default inputs.

Allowed future output classes are:

```text
file_share_review
result_summary
clarification_request
handoff_request
refusal_report
blocked_action_report
```

Each review must distinguish supplied evidence, missing evidence, and
unsupported claims. It may recommend a manual next step but must not claim that
a mount, connection, file read, permission change, transfer, or repair occurred.

## Planned Modes and Permissions

The future package must declare denial by default. Its only planned review
categories are:

```text
local_readonly
user_provided_text
redacted_status_summary
```

`lan_readonly` is not authorized by this skeleton. A later feature may propose
it only after defining an operator-selected-share policy, bounded metadata,
credential exclusion, network enforcement, audit evidence, redaction, and
dedicated boundary tests.

The resource profile is `local_planning`: no network access, no open ports, no
share mount, no protocol client, no downloads, no model load, no worker startup,
and no dynamic hardware probe. User confirmation cannot elevate a blocked
action; out-of-category requests must refuse or return to the orchestrator.

## Audit Boundary

The future package must require only these review evidence classes:

```text
review_started
scope_checked
permission_checked
redaction_checked
share_selection_checked
handoff_required
refusal_recorded
```

Evidence is redacted by default, operator-local, and review-only. It cannot
retain raw prompts, raw outputs, private or share paths, host identifiers,
credentials, or unredacted logs. This skeleton does not create audit emission,
storage, retention, or sharing.

## Blocked Actions

LAN File Share Assistant must not:

- discover, scan, enumerate, connect to, mount, or unmount LAN shares;
- resolve hostnames, contact remote nodes, open network listeners, or transmit
  share metadata;
- authenticate to SMB, NFS, AFP, WebDAV, SSH, or any file service;
- collect, request, retain, or use credentials, keys, tokens, or passwords;
- read arbitrary files, directories, share listings, or file contents;
- execute shell commands or scripts;
- write, delete, move, rename, upload, download, synchronize, or modify files;
- change IAMINE, operating-system, router, firewall, VM, container, or share
  settings;
- start, stop, restart, or inspect services or processes directly;
- start workers, P2P, PubSub, downloads, model loads, inference, or dynamic
  hardware probes;
- collect usernames, full hostnames, IP addresses, MAC addresses, serial
  numbers, disk UUIDs, machine IDs, private paths, raw prompts, raw outputs,
  raw process lists, unredacted logs, or permanent hardware fingerprints;
- fabricate, overstate, or treat unverified evidence as share state;
- publish to a registry, marketplace, or third party; or
- claim trust, reputation, rewards, settlement, mainnet, or distributed model
  MoE behavior.

## Refusal and Handoff

The future agent must refuse or hand off requests for network discovery, share
access, authentication, file operations, private data, repair, configuration,
remote execution, or conclusions unsupported by allowed evidence.

Prompt-injection and role-confusion text cannot override scope, permissions,
blocked actions, privacy rules, share-selection policy, or handoff requirements.

It must return to the orchestrator for:

```text
share discovery or network probing
credential or token handling
share mounting or authentication
file reading, transfer, recovery, or modification
router, firewall, VM, container, or service changes
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
credential_request
network_discovery_request
file_mutation_request
local_only
```

Missing, broad, contradictory, unsafe, stale, unverifiable, or privacy-invasive
metadata blocks package review, installation, registry advancement, and
execution by default.

## Next Roadmap Step

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON
```
