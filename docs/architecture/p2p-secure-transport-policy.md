# P2P Secure Transport Policy

Feature:

```text
P2P-SECURE-TRANSPORT-POLICY-001
```

## Purpose

Milestone 2 requires private testnet peers to use an explicit authenticated
transport before remote inference APIs and multi-operator operations are added.
The transport policy must reject downgrade paths without taking ownership of
identity registration, node admission, scheduler policy, task format, inference,
or model eligibility.

## Contract

The current IAMINE P2P transport profile is:

```text
base transport: TCP
security:       Noise authenticated transport
multiplexer:    Yamux
upgrade:        libp2p V1
policy:         tcp-noise-yamux-v1
```

Only this profile is allowed. Plaintext, unauthenticated, unsupported security
protocols, unsupported multiplexers, unsupported base transports, and unsupported
upgrade versions are explicit policy rejections.

## Runtime Behavior

IAMINE builds its libp2p swarm transport through one owner helper. The helper
checks the current secure transport profile before constructing the TCP + Noise
+ Yamux transport. If the profile is changed later to an unsupported or
unauthenticated variant, startup fails before the swarm is created.

Reason codes:

```text
unsupported_base_transport
plaintext_transport_rejected
unauthenticated_transport_rejected
unsupported_security_protocol
unsupported_multiplexer
unsupported_upgrade_version
```

## Boundaries

This feature must not:

- create remote inference authentication or API authorization;
- decide which node identities are admitted to a testnet;
- change bootnode, WAN peer, NAT relay, scheduler, PubSub, task, result, model,
  reward, reputation, or inference behavior;
- log hostnames, IP addresses, MAC addresses, serial numbers, usernames, paths,
  keys, tokens, or other secrets.

## Validation

Required validation:

- unit tests for the current secure transport profile;
- unit tests for plaintext and unauthenticated downgrade rejection;
- node runtime construction uses the shared secure transport helper;
- `main.rs` contains wiring only for transport construction;
- local quality gate;
- field QA on Mac, TS140, and Proxmox/R5500 before merge review.
