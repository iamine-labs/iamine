# IAMINE Agent Network Diagnostic Template

Feature:

```text
AGENT-TEMPLATE-NETWORK-DIAGNOSTIC-001
```

## Purpose

Define the future network diagnostic agent template boundary without
implementing probes, scanners, sockets, listeners, routing changes,
persistence, package installation, registry publication, marketplace
publication, runtime execution, or inference.

This document does not authorize active scanning, traffic capture,
configuration changes, firewall changes, unrestricted network access, secret
access, wallet access, rewards, settlement, mainnet behavior, or distributed
model MoE.

## Template Question

Network diagnostic template policy answers:

```text
What network diagnostic intent can be declared before implementation?
```

It does not answer whether probes, transports, sockets, approval UI, audit
logs, or runtime adapters exist.

## Draft Schema

```text
iamine.agent.template.network_diagnostic.draft-0.1
```

## Allowed Scope

Future network diagnostic templates may request only:

```text
read_declared_network_status
summarize_connectivity_findings
report_blocked_network_action
request_clarification
handoff_for_network_change
```

They must not scan arbitrary ranges, capture packets, open listeners, modify
routes, change DNS, change firewall rules, reveal raw IPs, or persist private
network identifiers by default.

## Required Guards

Future templates must declare:

```text
operator_selected_target
bounded_probe_policy
no_packet_capture
no_listener_start
no_network_mutation
redaction_policy
operator_visible_summary
```

## Privacy Rules

Network diagnostic metadata must not include credentials, private keys, wallet
keys, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs,
raw process lists, packet captures, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Network diagnostic templates cannot authorize runtime execution.
- Network diagnostic templates cannot open listeners or sockets.
- Network diagnostic templates cannot scan arbitrary networks.
- Network diagnostic templates cannot capture packets.
- Network diagnostic templates cannot mutate network configuration.
- Network diagnostic templates cannot bypass validation, scope review,
  permission review, boundary tests, manual review, audit, or local registry
  review.

## Next Roadmap Step

```text
AGENT-TEMPLATE-REPORTER-001
```
