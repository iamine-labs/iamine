# IAMINE Rust Dependency Security Remediation QA

Feature:

```text
RUST-DEPENDENCY-SECURITY-REMEDIATION-001
```

## Identity

```text
Branch: feature/rust-dependency-security-remediation-001
Base: 1409b6fa9cb780d00fb840503c16f83bd35c0405
Base tree: e55e88cbaf1f86a8b018c162a128ec7c2f13b5ef
Validation host: Mac development machine
Runtime behavior touched: P2P dependency and compatibility wiring
```

## Scope Checks

Expected changed paths:

```text
Cargo.toml
Cargo.lock
iamine-models/Cargo.toml
iamine-network/Cargo.toml
iamine-node/Cargo.toml
iamine-node/src/main.rs
iamine-node/src/network_config.rs
iamine-node/src/p2p_protocol_version_runtime.rs
docs/architecture/rust-dependency-security-remediation.md
docs/qa/rust-dependency-security-remediation.md
docs/roadmap/iamine-security-ci-track.md
```

No `client-rust` source file is deleted or modified. No workflow, secret-scan,
scheduler, model policy, task format, inference, hardware profiler, installer,
service, reputation, reward, wallet, or frontend behavior belongs to this
feature.

## Audit Evidence

`cargo-audit 0.22.2` executed with a locally cloned RustSec advisory database:

| Graph | Packages | Vulnerabilities | Warnings |
| --- | ---: | ---: | ---: |
| Exact base | 863 | 13 | 18 |
| Remediated workspace | 494 | 2 | 3 |
| Delta | -369 | -11 | -15 |

Current unresolved inventory:

```text
RUSTSEC-2026-0119: hickory-proto 0.25.2; active mDNS dependency path
RUSTSEC-2026-0118: hickory-proto 0.25.2; DNSSEC features not activated
core2 0.4.0: unmaintained
paste 1.0.15: unmaintained
rustls-pemfile 1.0.4: unmaintained
```

The audit exits nonzero because the two vulnerabilities remain. This result is
not reported as a passing security gate.

## Mac Validation Results

```text
cargo test -p iamine-network: PASS; 167 tests
cargo test -p iamine-models: PASS; 159 tests
cargo test -p iamine-node: PASS; 496 tests
cargo test --workspace -- --test-threads=1: PASS; 1,138 tests
cargo build -p iamine-node: PASS
cargo clippy --workspace --all-targets: PASS
cargo fmt --all -- --check: PASS
git diff --check: PASS
./scripts/quality-gate.sh: PASS WITH WARNINGS
```

Quality gate summary:

```text
required_failures=0
warnings=0
skipped=3
cargo audit: SKIPPED by gate PATH; executed separately as recorded above
cargo deny: SKIPPED; unavailable
gitleaks: SKIPPED; unavailable
```

Clippy produced the same `iamine-node` warning counts as the exact base:

```text
binary: 22 warnings
test target: 21 warnings
new warning regression: none
```

Architecture guards:

```text
iamine-node/src/main.rs: 4,935 -> 4,937 lines; delta +2
iamine-node/src/cluster_registry.rs: 862 lines; unchanged
new non-main Rust file above 900 lines: none
```

## Mac P2P Smoke

Two isolated local profiles were used with mock inference and model loading
disabled. The worker and controller established a local connection and
validated:

```text
TCP + Noise + Yamux listener: PASS
mDNS discovery: PASS
Identify exchange: PASS
Kademlia routing update: PASS
PubSub subscriptions: PASS
ping: PASS
heartbeat: PASS
cluster status --json reports one healthy mock worker: PASS
real inference remained disabled: PASS
```

Peer IDs and local network addresses are intentionally omitted from committed
evidence.

## Findings

- A default parallel `cargo test --workspace` run had two intermittent test
  failures. Both passed immediately in isolation, and the full workspace
  passed with one test thread. This is classified as existing harness
  concurrency sensitivity, not evidence of a product regression.
- A fresh isolated HOME selected the default first-run download path even with
  mock and skip-model environment variables. Selecting the manual setup path
  avoided the network request. This is a separate first-run UX finding and is
  not changed in this dependency feature.
- `RUSTSEC-2026-0119` remains reachable through the mDNS dependency graph and
  blocks dependency-security closure without an explicit Architecture
  disposition or a compatible upstream release.

## Deferred Field QA

Per the current Mac-only operating constraint:

```text
TS140: NOT EXECUTED / DEFERRED
Proxmox/R5500 guests: NOT EXECUTED / DEFERRED
multi-host LAN regression: NOT EXECUTED / DEFERRED
Linux platform coverage: NOT EXECUTED / DEFERRED
```

These checks remain mandatory before final Architecture merge approval because
the feature changes the P2P dependency graph.

## Recommendation

```text
implementation: IMPLEMENTATION COMPLETE
Mac regression validation: PASSED
security closure: BLOCKED
field QA: DEFERRED
canonical next state: ARCHITECTURE REVIEW REQUIRED
```

Do not report `QA PASS`, `READY FOR MERGE REVIEW`, `MERGE APPROVED`, or
`MERGED / VALIDATED / CLOSED` from this checkpoint.
