# P2P Protocol Versioning

Feature:

```text
P2P-PROTOCOL-VERSIONING-001
```

## Purpose

Milestone 2 needs explicit protocol compatibility before private testnet nodes
cross LAN and operator boundaries. Versioning must reject unsupported peers
without changing scheduler, inference, model policy, identity, admission, or
transport security responsibilities.

## Contract

The current IAMINE P2P wire contract is:

```text
identify protocol: /iamine/1.0
task stream:       /iamine/task/1.0
result stream:     /iamine/result/1.0
```

A peer is protocol-compatible only when:

- its identify protocol is exactly `/iamine/1.0`;
- it advertises `/iamine/task/1.0`;
- it advertises `/iamine/result/1.0`.

Missing or unsupported protocol metadata is rejected explicitly. This feature
does not introduce multi-version fallback. Future versions must add an explicit
compatibility matrix before accepting more than one wire contract.

## Runtime Behavior

On `identify` receive, the node evaluates the remote identify protocol and
required request-response stream protocols. Compatible peers remain connected.
Incompatible peers are logged with a reason code and disconnected.

Reason codes:

```text
missing_identify_protocol
unsupported_identify_protocol
missing_required_stream_protocol
```

## Boundaries

This feature must not:

- change task format, result format, scheduler selection, or inference;
- infer identity, admission, reputation, or trust;
- enable WAN discovery, bootnodes, NAT traversal, or secure transport policy;
- log hostnames, IP addresses, MAC addresses, serial numbers, usernames, paths,
  keys, tokens, or other secrets.

## Validation

Required validation:

- unit tests for current version constants;
- unit tests for compatible and incompatible peer decisions;
- `network_config` uses shared constants instead of duplicated protocol strings;
- runtime identify handler rejects incompatible peers without touching
  scheduler or inference paths;
- local quality gate;
- field QA on Mac, TS140, and Proxmox/R5500 before merge review.
