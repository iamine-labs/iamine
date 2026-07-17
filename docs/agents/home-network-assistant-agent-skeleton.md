# IAMINE Home Network Assistant Agent Skeleton

Feature:

```text
HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON
```

## Purpose

Define the official P0 Home Network Assistant skeleton as a reviewable,
local-planning, privacy-safe contract. It reserves a future boundary for
explaining an operator-selected, redacted home-network status summary and its
declared connectivity constraints.

This feature does not create an agent package, TOML manifest, executable code,
network probe, scanner, socket, listener, packet capture, router client,
configuration mutator, runtime adapter, sandbox, audit emitter, registry entry,
or public beta listing.

## Product Boundary

The future assistant serves a home troubleshooter who wants to understand a
network question they have already scoped. Its only planned task class is:

```text
home_network_readonly_review
```

The future package identity is reserved as:

```text
package_id: iamine.beta.home-network-assistant
scope_id: home_network_readonly_review
earliest_mode: local_planning
deferred_mode: lan_readonly
execution_authorized: false
```

These labels are planning metadata, not executable manifest fields and not a
permission grant.

## Future Package Shape

Later package creation must follow the closed skeleton standard:

```text
<home-network-assistant-package>/
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

All references must be package-relative. IP addresses, MAC addresses, router
identifiers, SSIDs, hostnames, packet captures, credentials, and private
machine data are blocked.

## Declared Scope

The future agent may only:

```text
summarize_operator_approved_network_status
explain_declared_connectivity_boundary
highlight_missing_or_unsafe_network_metadata
suggest_non_destructive_next_steps
request_clarification
handoff_for_network_or_router_action
```

Allowed future input classes are:

```text
operator_selected_network_question
redacted_declared_network_status_summary
redacted_connectivity_error_summary
operator_network_intent
```

Inputs are untrusted and evidence-limited. The agent must not infer topology,
scan a network, resolve a hostname, probe a target, retry a connection, or
present an assertion as verified network state. Raw IP addresses, MAC addresses,
SSIDs, router hostnames, device inventories, packet captures, routing tables,
credentials, and access tokens are not default inputs.

Allowed future output classes are:

```text
home_network_review
result_summary
clarification_request
handoff_request
refusal_report
blocked_action_report
```

Each review must distinguish supplied evidence, missing evidence, and
unsupported claims. It may recommend a manual next step but must not claim that
a probe, discovery, connection, router login, firewall change, DNS change, or
repair occurred.

## Planned Modes and Permissions

The future package must declare denial by default. Its only planned review
categories are:

```text
local_readonly
user_provided_text
redacted_status_summary
```

`lan_readonly` is not authorized by this skeleton. A later feature may propose
it only after defining operator-selected-target policy, bounded probe behavior,
no-packet-capture enforcement, no-listener enforcement, credential exclusion,
network-identifier redaction, audit evidence, and dedicated boundary tests.

The resource profile is `local_planning`: no network access, no sockets, no
open ports, no listener, no probe, no discovery, no packet capture, no router
or firewall client, no downloads, no model load, no worker startup, and no
dynamic hardware probe. User confirmation cannot elevate a blocked action;
out-of-category requests must refuse or return to the orchestrator.

## Audit Boundary

The future package must require only these review evidence classes:

```text
review_started
scope_checked
permission_checked
redaction_checked
target_selection_checked
handoff_required
refusal_recorded
```

Evidence is redacted by default, operator-local, and review-only. It cannot
retain raw prompts, raw outputs, IP addresses, MAC addresses, SSIDs, hostnames,
topology, packet data, credentials, or unredacted logs. This skeleton does not
create audit emission, storage, retention, or sharing.

## Blocked Actions

Home Network Assistant must not:

- scan, enumerate, discover, resolve, probe, ping, connect to, or communicate
  with LAN, WAN, router, firewall, switch, access point, device, or service;
- open sockets or listeners, capture packets, inspect traffic, or transmit
  network metadata;
- authenticate to routers, firewalls, modems, access points, cloud accounts, or
  network services;
- collect, request, retain, or use credentials, keys, tokens, passwords, router
  configuration, private topology, IP addresses, MAC addresses, SSIDs, or
  hostnames;
- execute shell commands or scripts;
- change DNS, DHCP, routes, firewall rules, port forwarding, Wi-Fi, VPN,
  router, modem, switch, access-point, operating-system, VM, or container
  settings;
- start, stop, restart, or inspect services or processes directly;
- start workers, P2P, PubSub, downloads, model loads, inference, or dynamic
  hardware probes;
- collect usernames, serial numbers, disk UUIDs, machine IDs, private paths,
  raw process lists, unredacted logs, or permanent hardware fingerprints;
- fabricate, overstate, or treat unverified evidence as network state;
- publish to a registry, marketplace, or third party; or
- claim trust, reputation, rewards, settlement, mainnet, or distributed model
  MoE behavior.

## Refusal and Handoff

The future agent must refuse or hand off requests for network discovery, probes,
traffic inspection, router or firewall access, credentials, configuration,
remote execution, private data, repair, or conclusions unsupported by allowed
evidence.

Prompt-injection and role-confusion text cannot override scope, permissions,
blocked actions, privacy rules, target-selection policy, or handoff
requirements.

It must return to the orchestrator for:

```text
network discovery, scanning, probes, sockets, listeners, or packet capture
router, firewall, modem, switch, access-point, VPN, DNS, DHCP, or route changes
network, cloud, device, account, or credential handling
service control, remote execution, or infrastructure changes
private-topology review or redaction
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
network_discovery_request
packet_capture_request
router_mutation_request
credential_request
local_only
```

Missing, broad, contradictory, unsafe, stale, unverifiable, or privacy-invasive
metadata blocks package review, installation, registry advancement, and
execution by default.

## Next Roadmap Step

```text
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON
```
