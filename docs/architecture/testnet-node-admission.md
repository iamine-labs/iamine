# TESTNET-NODE-ADMISSION-001

State: IMPLEMENTATION IN PROGRESS

## Goal

Add an explicit private-testnet admission gate for operator-approved peers.
This closes the gap left by node identity registration, WAN peer discovery, and
NAT relay configuration: those features identify and reach peers, but they do
not decide whether a peer is authorized to participate in a private testnet.

## Scope

The admission policy is opt-in and defaults to open networking to preserve
existing LAN, WAN, relay, and single-node workflows.

Operators enable restricted admission with:

```bash
--testnet-admission allowlist --testnet-allow-peer <peer_id>
```

When allowlist mode is active:

- configured bootnodes must end with `/p2p/<peer_id>`;
- configured bootnodes, WAN peers, and relay peers must be present in the
  allowlist;
- mDNS-discovered peers outside the allowlist are ignored before registration
  or dialing;
- established connections outside the allowlist are disconnected;
- logs emit rejection reason and peer id, but do not emit local secrets.

## Non-Goals

This feature does not implement secure transport policy, remote API
authorization, Sybil resistance, reputation, rewards, scheduler policy,
wallet-based admission, model eligibility, or peer identity registration.

## Ownership

- Admission parsing and policy semantics live in `iamine-network`.
- Runtime application lives in `iamine-node`.
- `main.rs` only parses and wires the policy into existing network startup and
  event handlers.

## Compatibility

The default mode is `open`; without the new flags behavior is unchanged.
`allowlist` mode requires at least one `--testnet-allow-peer` value. Supplying
allowed peers without `--testnet-admission allowlist` is rejected to avoid a
false sense of admission enforcement.

## Privacy

The allowlist stores public libp2p peer IDs only. It must not collect or persist
usernames, home directories, hostnames, MAC addresses, IP addresses, serial
numbers, disk UUIDs, machine IDs, process lists, personal paths, keys, tokens,
or credentials.
