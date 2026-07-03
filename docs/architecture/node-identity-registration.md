# Node Identity Registration

Feature:

```text
NODE-IDENTITY-REGISTRATION-001
```

## Purpose

Private testnet nodes need a durable operator-controlled identity before
bootnodes, admission, secure transport, and remote APIs can make explicit trust
decisions. Identity registration must describe the local node identity without
collecting host fingerprints or deciding admission policy.

## Contract

IAMINE node identity is derived from the local libp2p keypair:

```text
key path: ~/.iamine/node_key
node_id:  libp2p peer id
peer_id:  libp2p peer id
wallet:   legacy derived iamine1 prefix
```

The key file remains local. CLI reports expose only public or derived values:

- node id;
- peer id;
- legacy wallet address;
- public key fingerprint;
- redacted key path label;
- private-permission status.

The CLI must not print key bytes, usernames, home directories, hostnames, IP
addresses, MAC addresses, serial numbers, machine IDs, process lists, tokens,
or other host secrets.

## CLI Behavior

Supported command:

```text
iamine-node node identity [status|init] [--path PATH] [--json]
```

Behavior:

- `status` inspects the key and never writes;
- `init` creates a durable key when missing;
- `init` refuses to overwrite an invalid existing key;
- existing keys keep their peer id;
- key files should be private to the current user when the platform supports
  file permissions;
- the command is pre-network control plane only and must not start workers,
  P2P, PubSub, model downloads, model loads, inference, or dynamic hardware
  probes.

## Boundaries

This feature must not:

- admit nodes to any private or public testnet;
- infer operator trust, reputation, rewards, or Sybil resistance;
- define bootnodes, WAN discovery, NAT traversal, relay behavior, secure
  transport policy, or remote API authentication;
- change scheduler, model policy, task format, result format, inference, or
  worker startup semantics beyond reusing the existing local identity.

Those responsibilities remain with later Milestone 2 features.

## Validation

Required validation:

- missing identity status is non-writing and explicit;
- init creates a durable identity in an isolated path;
- repeated status returns the same peer id and public key fingerprint;
- invalid key material is not overwritten;
- JSON output is parseable and privacy-safe;
- CLI parsing and help include `node identity`;
- local quality gate passes before merge review;
- field QA is required because runtime identity behavior is touched.
