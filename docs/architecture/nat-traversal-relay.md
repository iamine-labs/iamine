# NAT Traversal Relay

Feature:

```text
NAT-TRAVERSAL-RELAY-001
```

## Purpose

Private testnet candidates need a bounded path for constrained nodes after
explicit bootnodes and WAN peer seeds exist. NAT relay policy lets an operator
enable relay-assisted discovery only with explicitly configured relay peers.

## Contract

Relay behavior is disabled by default. Runtime modes that start networking may
enable the operator-controlled policy with:

```text
--relay-policy operator-configured
--relay-policy=operator-configured
```

Accepted disabled values are:

```text
disabled
off
none
```

Relay peers are configured with repeated flags:

```text
--relay-peer ADDR
--relay-peer=ADDR
```

Each relay peer address must be a valid libp2p multiaddr ending in:

```text
/p2p/<peer_id>
```

Relay peer addresses must identify the relay node itself. Relayed destination
paths containing `p2p-circuit` are rejected in this gate. Duplicate relay peer
ids are ignored after the first occurrence. The relay peer set is bounded by:

```text
16 peers
```

For each operator-configured relay peer, IAMINE:

- registers the relay peer routing address in Kademlia;
- registers the relay peer as an explicit Gossipsub peer;
- attempts to dial the configured relay peer multiaddr.

Runtime summaries report policy state and counts only. They must not print
configured relay peer addresses.

## Boundaries

This feature must not:

- enable automatic public relays;
- make node admission decisions;
- infer operator trust, reputation, rewards, Sybil resistance, or wallet policy;
- define secure transport policy or remote API authentication;
- change task format, result format, scheduler selection, model policy,
  inference, worker startup, or model loading semantics;
- log host secrets, key material, usernames, home directories, hostnames, MAC
  addresses, serial numbers, machine IDs, tokens, or private credentials.

`TESTNET-NODE-ADMISSION-001` remains responsible for deciding which nodes are
authorized. `P2P-SECURE-TRANSPORT-POLICY-001` remains responsible for
authenticated transport and downgrade rejection.

## Validation

Required validation:

- relay policy defaults to disabled;
- relay policy parses disabled and operator-configured modes;
- invalid policy values fail explicitly;
- relay peers require operator-configured policy;
- relay peer args parse both repeated flag forms;
- invalid relay peer multiaddrs fail explicitly;
- missing relay peer values fail explicitly;
- relay peers without `/p2p/<peer_id>` fail explicitly;
- `p2p-circuit` destination paths fail explicitly in this gate;
- duplicate relay peer ids are deduplicated;
- relay peer set size is bounded;
- runtime wiring registers relay peers with Kademlia/Gossipsub before dialing;
- CLI help includes relay policy and relay peer flags;
- local quality gate passes before merge review;
- field QA is required because runtime discovery behavior is touched.
