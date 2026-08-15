# RUST-DEPENDENCY-SECURITY-REMEDIATION-001

## Objective

Reduce the supported Rust workspace dependency attack surface, remove obsolete
advisory chains where a compatible upgrade exists, and preserve unsupported
prototype code without presenting it as part of the production workspace.

## Scope

This feature:

- excludes `client-rust` from the supported Cargo workspace while preserving
  its files for later product and Architecture disposition;
- upgrades `libp2p` from `0.53` to `0.56` in its three direct IAMINE owners;
- refreshes `Cargo.lock` against the supported workspace;
- adapts request-response and identify event handling to the `libp2p 0.56`
  API;
- boxes identify events to preserve the existing clippy warning baseline.

The old `client-rust` prototype is not a supported IAMINE release surface. It
contains an early Solana-oriented experiment, has no production caller in the
current workspace, and belongs to roadmap work that remains deferred. This
feature does not delete or rewrite it.

## Ownership and Integration

Dependency declarations remain in their owner manifests:

```text
iamine-models/Cargo.toml
iamine-network/Cargo.toml
iamine-node/Cargo.toml
```

Compatibility changes remain in existing P2P owner modules. `main.rs` changes
are limited to event wiring required by the upstream API. No scheduler, model
selection, task format, inference, worker policy, reputation, reward, or
hardware-profiling behavior is introduced.

## Security Result

The exact base graph contained 863 packages, 13 vulnerabilities, and 18
warnings. The remediated supported graph contains 494 packages, 2
vulnerabilities, and 3 warnings.

The remaining vulnerabilities are both in `hickory-proto 0.25.2`, selected by
`libp2p-mdns 0.48.0` through the latest stable `libp2p 0.56.0` line:

| Advisory | Reachability and disposition |
| --- | --- |
| `RUSTSEC-2026-0119` | Message encoding is present in the active mDNS dependency path. The patched version is `hickory-proto >=0.26.1`, which is not selected by the latest stable `libp2p-mdns`. This remains an unaccepted upstream-blocked risk. |
| `RUSTSEC-2026-0118` | The vulnerable DNSSEC path requires `dnssec-ring` or `dnssec-aws-lc-rs`; neither feature is active in IAMINE. The advisory remains in the audit inventory and still requires Architecture disposition. |

The remaining warnings are unmaintained transitive crates:

```text
core2 0.4.0
paste 1.0.15
rustls-pemfile 1.0.4
```

No advisory or warning is silently accepted by this feature. Replacing mDNS,
pinning an unpublished upstream revision, or introducing a local fork would
change the network dependency ownership and requires a separate Architecture
decision and validation cycle.

## Field QA Boundary

The dependency upgrade touches P2P runtime behavior, so field QA remains
required before merge approval. Mac validation may establish local API,
build, regression, and two-node loopback behavior. It cannot replace TS140 or
Proxmox/R5500 evidence.

The current checkpoint intentionally records:

```text
Mac local validation: executed
TS140 field QA: deferred
Proxmox/R5500 field QA: deferred
security closure: blocked by unaccepted upstream advisory
```

## Risks and Controls

- A lockfile reduction can hide an accidentally removed product surface. The
  excluded prototype is preserved and explicitly documented.
- A P2P dependency upgrade can compile while changing discovery behavior. The
  Mac smoke exercises mDNS, Identify, Kademlia, PubSub, ping, heartbeat, and
  cluster status; remote field QA remains mandatory.
- Treating an unreachable feature path as a clean audit would overstate the
  result. Both Hickory advisories remain reported.
- Forcing Hickory through an incompatible override could create a less tested
  network stack. No fork, patch override, or unpublished dependency is added.
