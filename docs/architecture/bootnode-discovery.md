# Bootnode Discovery

Feature:

```text
BOOTNODE-DISCOVERY-001
```

## Purpose

Private testnet nodes need an explicit bootstrap path before WAN discovery,
NAT traversal, admission, secure transport policy, and remote APIs can be
validated. Bootnode discovery lets an operator provide a bounded, replaceable
set of libp2p multiaddrs at startup.

## Contract

Runtime modes that start networking may accept repeated bootnode flags:

```text
--bootnode ADDR
--bootnode=ADDR
```

Each value must be a valid libp2p multiaddr. Invalid values fail startup
explicitly instead of being ignored. Duplicate addresses are ignored after the
first occurrence. The bootnode set is bounded by:

```text
32 addresses
```

When a bootnode address ends in `/p2p/<peer_id>`, IAMINE registers that peer in
Kademlia and as an explicit Gossipsub peer before dialing the address. When the
address has no peer id, IAMINE only dials the address and waits for normal
identify/protocol validation.

## Boundaries

This feature must not:

- define default public bootnodes;
- admit nodes to a testnet;
- infer operator trust, reputation, rewards, or Sybil resistance;
- define WAN discovery, NAT traversal, relay policy, or secure transport;
- change task format, result format, scheduler selection, model policy,
  inference, or worker startup semantics;
- log host secrets, key material, usernames, home directories, hostnames, MAC
  addresses, serial numbers, machine IDs, tokens, or private credentials.

Bootnodes are only an explicit discovery seed. Protocol compatibility remains
owned by P2P protocol versioning. Trust and admission remain owned by later
Milestone 2 gates.

## Validation

Required validation:

- bootnode args parse both repeated flag forms;
- invalid multiaddrs fail explicitly;
- missing flag values fail explicitly;
- duplicate bootnode addresses are deduplicated;
- bootnode set size is bounded;
- bootnodes carrying `/p2p/<peer_id>` expose peer id and routing address
  separately;
- runtime wiring registers peer-qualified bootnodes with Kademlia/Gossipsub
  before dialing;
- local quality gate passes before merge review;
- field QA is required because runtime discovery behavior is touched.
