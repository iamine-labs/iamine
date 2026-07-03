# WAN Peer Discovery

Feature:

```text
WAN-PEER-DISCOVERY-001
```

## Purpose

Private testnet candidates need a controlled way to discover peers outside the
local network after explicit bootnode support exists. WAN peer discovery lets an
operator provide bounded, peer-qualified seed addresses that IAMINE can route
through Kademlia and Gossipsub before later NAT traversal, admission, and secure
transport gates are introduced.

## Contract

Runtime modes that start networking may accept repeated WAN peer flags:

```text
--wan-peer ADDR
--wan-peer=ADDR
```

Each value must be a valid libp2p multiaddr ending in:

```text
/p2p/<peer_id>
```

Addresses without a trailing peer id fail startup explicitly. Duplicate peer ids
are ignored after the first occurrence. The WAN peer seed set is bounded by:

```text
32 peers
```

For each WAN peer seed, IAMINE:

- registers the peer-qualified routing address in Kademlia;
- registers the peer as an explicit Gossipsub peer;
- attempts to dial the configured multiaddr;
- starts one Kademlia bootstrap query when at least one WAN peer seed is present.

Runtime summaries report counts only. They must not print configured WAN peer
addresses.

## Boundaries

This feature must not:

- define default public peers;
- admit nodes to a private or public testnet;
- infer operator trust, reputation, rewards, Sybil resistance, or wallet policy;
- define NAT traversal, relay policy, secure transport, or remote API
  authentication;
- change task format, result format, scheduler selection, model policy,
  inference, worker startup, or model loading semantics;
- log host secrets, key material, usernames, home directories, hostnames, MAC
  addresses, serial numbers, machine IDs, tokens, or private credentials.

WAN peer seeds are only operator-provided discovery hints. Formal node admission
remains owned by `TESTNET-NODE-ADMISSION-001`. NAT traversal and relay behavior
remain owned by `NAT-TRAVERSAL-RELAY-001`.

## Validation

Required validation:

- WAN peer args parse both repeated flag forms;
- invalid multiaddrs fail explicitly;
- missing flag values fail explicitly;
- addresses without `/p2p/<peer_id>` fail explicitly;
- duplicate peer ids are deduplicated;
- WAN peer set size is bounded;
- runtime wiring registers peer-qualified WAN seeds with Kademlia/Gossipsub;
- runtime wiring starts Kademlia bootstrap only when WAN seeds exist;
- CLI help includes the WAN peer flag;
- local quality gate passes before merge review;
- field QA is required because runtime discovery behavior is touched.
