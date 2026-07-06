# IAMINE Product Roadmap

## Authority

This document is the canonical product roadmap for IAMINE. It translates the
Architecture plan into versioned repository state and supersedes milestone
interpretations inferred from the whitepaper or from individual feature
closeout documents.

The whitepaper remains strategic vision. This roadmap controls implementation
sequence, release meaning, and feature authorization.

Reconciled against:

```text
branch: origin/develop
commit: cc3084d51a9288224541088e9b4489c345946128
date: 2026-07-02
```

## State Vocabulary

Roadmap entries use these planning states:

```text
CLOSED
ACTIVE
APPROVED
PROPOSED
BLOCKED
DEFERRED
```

`CLOSED` requires merge and post-merge evidence under the canonical workflow.
`APPROVED` means Architecture has authorized the feature to enter its
development lifecycle. Only one next product feature should normally be
`APPROVED`.

## Release Definitions

| Release | Product meaning |
| --- | --- |
| v0.7 | Hardware and model foundations |
| v0.8 | Installable LAN beta |
| v0.9 | Private multi-operator testnet |
| v1.0 | Stable public testnet |
| v1.1 | Verification and reputation |
| v1.5 | Economic testnet using units with no real-world value |
| v2.0 | Mainnet |

`LAN-INFERENCE-BETA-001` is not IAMINE v1. Mainnet is not part of v1.

## Current Position

IAMINE has closed Milestone 0 and Milestone 1. Milestone 2 has started with
P2P protocol versioning, node identity registration, bootnode discovery, WAN
peer discovery, and NAT traversal relay policy closed. Hardware profiling,
model compatibility, model admission gates, LAN discovery, real legacy CPU
inference, LAN beta release-gate QA, explicit P2P compatibility checks, and
durable local node identity controls have reduced later product risk.

Roadmap incorporation is controlled by:

```text
PRODUCT-ROADMAP-CONSOLIDATION-001
```

Its state is closed by the roadmap document being present in `origin/develop`
with post-merge validation evidence. Later feature rows are updated by their
own feature lifecycle evidence.

## Milestone 0 - v0.7 Foundations

### Original Product Features

| Feature | State | Owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| NODE-HARDWARE-PROFILER-001 | CLOSED | `iamine-hardware`, `iamine-node` wiring | None | Describe local visible hardware without making scheduler or compatibility decisions. |
| MODEL-HARDWARE-COMPATIBILITY-001 | CLOSED | `iamine-models` | Hardware profile schema | Evaluate explicit model requirements against normalized hardware. |
| MODEL-TRUSTED-REGISTRY-INTEGRITY-001 | CLOSED | `iamine-models` | Model registry | Block new artifacts with missing, invalid, placeholder, or mismatched integrity metadata. |
| MODEL-BETA-REGISTRY-METADATA-001 | CLOSED | `iamine-models` | Integrity, license, network, and download gates | Add verified checksum, license, source, format, size, revision, and network metadata for the approved beta model set. |
| MODEL-CATALOG-SELECTION-CLI-001 | CLOSED | `iamine-models`; `iamine-node` wiring only | Beta registry metadata, hardware compatibility, all admission gates | Provide an explainable catalog, compatibility-aware selection, and controlled download flow. |

The approved v0.7 beta registry contains TinyLlama, Llama 3.2 3B, and Mistral
7B descriptors with verified artifact, license, source, format, size, revision,
and network metadata. The catalog explains compatibility and admission gates
before selection or download.

### Complementary Closed Gates

These features were added when Architecture or QA identified missing
responsibility boundaries. They strengthen Milestone 0 without replacing its
remaining product features.

| Feature | State | Owner | Evidence in `develop` |
| --- | --- | --- | --- |
| MODEL-DOWNLOAD-POLICY-001 | CLOSED | `iamine-models` | `65f51d3` |
| MODEL-LICENSE-GATE-001 | CLOSED | `iamine-models` | `16f60f0` |
| MODEL-LICENSE-ACCEPTANCE-001 | CLOSED | `iamine-models`; CLI wiring | `421632c` |
| MODEL-BACKEND-AVAILABILITY-GATE-001 | CLOSED | `iamine-node` owner module | `13907c2` |
| MODEL-NETWORK-POLICY-GATE-001 | CLOSED | `iamine-models` | `d981018` |
| MODEL-NETWORK-POLICY-RUNTIME-ENFORCEMENT-001 | CLOSED | `iamine-node` worker execution | `034d2b6` |
| MODEL-INFERENCE-ELIGIBILITY-GATE-001 | CLOSED | `iamine-models` | `0944028` |
| MODEL-INFERENCE-ELIGIBILITY-RUNTIME-WIRING-001 | CLOSED | `iamine-node` worker execution | `0686119` |
| MODEL-INFERENCE-ELIGIBILITY-REPORTING-001 | CLOSED | `iamine-models`; status wiring | `e390bef` |

### Milestone 0 Exit Gate

- approved beta models have verified immutable artifact metadata;
- new downloads fail closed when required metadata is absent or contradictory;
- the catalog explains compatibility and every admission gate;
- selection and download do not silently bypass policy;
- v0.7 local validation and required field QA pass.

Closeout evidence:

```text
docs/roadmap/v0.7-foundations-closeout.md
```

## Milestone 1 - v0.8 Installable LAN Beta

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| LAN-INFERENCE-BETA-CONTRACT-001 | CLOSED | Architecture; protocol owners | Milestone 0 closed | Define supported LAN topology, user flows, failure semantics, and release boundaries. |
| LAN-NODE-DOCTOR-001 | CLOSED | `iamine-node` diagnostics module | Beta contract, config schema | Diagnose hardware, model, backend, configuration, and LAN readiness without starting inference. |
| LAN-WORKER-LIFECYCLE-001 | CLOSED | `iamine-node` worker runtime | Beta contract | Provide explicit install, start, stop, restart, readiness, and recovery behavior. |
| NODE-CONFIG-SCHEMA-MIGRATION-001 | CLOSED | `iamine-node` configuration | Beta contract | Version node configuration and provide bounded migration and rollback. |
| WORKER-METRICS-PORT-ALLOCATION-001 | CLOSED | `iamine-node` metrics | Config schema | Allocate deterministic, non-conflicting metrics endpoints for multiple workers. |
| LAN-INFERENCE-CLI-001 | CLOSED | `iamine-node` CLI wiring | Catalog selection, beta contract | Expose the supported LAN inference workflow with clear errors and no hidden startup. |
| LAN-REAL-INFERENCE-RESILIENCE-001 | CLOSED | `iamine-node`, `iamine-network`, `iamine-models` owner modules | Worker lifecycle, LAN CLI | Recover safely from worker, network, backend, and model failures during real inference. |
| V1-OBSERVABILITY-001 (LAN phase) | CLOSED | `iamine-network`, `iamine-node` observability | Beta contract | Emit correlated operational evidence for setup, dispatch, execution, recovery, and result delivery. |
| LAN-INFERENCE-BETA-PACKAGING-001 | CLOSED | Packaging / operations | Doctor, config migration, lifecycle | Deliver clean install, upgrade, service integration, and rollback artifacts. |
| LAN-INFERENCE-BETA-001 | CLOSED | Architecture / QA release gate | All Milestone 1 features | Validate and publish the installable LAN beta. |

Milestone gate: clean installation, one to five workers, real inference,
diagnostics, reboot recovery, failure recovery, upgrade, and rollback.

Closed prework that reduces Milestone 1 risk:

| Feature | State | Evidence in `develop` |
| --- | --- | --- |
| CLUSTER-LAN-AUTO-DISCOVERY-001 | CLOSED | `c6ffe3b`, closeout `a973925` |
| LEGACY-BACKEND-REAL-INFERENCE-001 | CLOSED | `0df03f8`, hardening `4b82338` |
| LEGACY-BACKEND-WORKER-DAEMON-E2E-001 | CLOSED | merge `2882ce9` |

This prework is technical evidence, not formal completion of the LAN beta.

## v0.8 Beta Distribution Hardening

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| LAN-BETA-INSTALLER-POLISH-001 | CLOSED | Packaging / operations | v0.8 LAN beta closeout | Provide safer install and uninstall helpers for controlled beta testing on additional PCs. |
| LAN-BETA-FIRST-RUN-PREFLIGHT-001 | ACTIVE | Packaging / operations / diagnostics | Installer polish, node doctor, LAN beta closeout | Validate first-run configuration, model, backend, ports, permissions, diagnostics, worker readiness, and basic LAN smoke before broader beta testing on additional PCs. |

## Milestone 2 - v0.9 Private Testnet

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| P2P-PROTOCOL-VERSIONING-001 | CLOSED | `iamine-network` | v0.8 | Negotiate compatible protocol versions and reject unsupported peers explicitly. |
| NODE-IDENTITY-REGISTRATION-001 | CLOSED | Identity / `iamine-node` | Protocol versioning | Register durable operator-controlled node identities without exposing host secrets. |
| BOOTNODE-DISCOVERY-001 | CLOSED | `iamine-network` | Protocol versioning | Bootstrap peers from an explicit, replaceable bootnode set. |
| WAN-PEER-DISCOVERY-001 | CLOSED | `iamine-network` | Bootnodes | Discover authorized peers across physical networks. |
| NAT-TRAVERSAL-RELAY-001 | CLOSED | `iamine-network` | WAN discovery | Connect constrained nodes through bounded NAT traversal and relay policy. |
| TESTNET-NODE-ADMISSION-001 | CLOSED | Identity / network policy | Node registration | Admit only authorized private-testnet nodes. |
| P2P-SECURE-TRANSPORT-POLICY-001 | CLOSED | `iamine-network` | Node identity | Define authenticated transport and downgrade rejection. |
| REMOTE-INFERENCE-API-001 | PROPOSED | API boundary / `iamine-node` wiring | Secure transport, admission | Accept bounded remote inference requests with explicit authentication and policy checks. |
| TESTNET-OBSERVABILITY-001 | PROPOSED | Observability owner modules | Remote API, WAN network | Correlate cross-operator health, routing, execution, and failure evidence. |
| TESTNET-LOAD-RESILIENCE-001 | PROPOSED | Runtime, scheduler, QA | Testnet observability | Prove bounded behavior under concurrency, partial outages, retries, and recovery. |
| PRIVATE-TESTNET-RELEASE-001 | PROPOSED | Architecture / QA release gate | All Milestone 2 features | Operate and close the private multi-operator testnet gate. |

Milestone gate: 10-50 nodes, 3-10 operators, multiple physical networks, and
two to four weeks of stable operation.

## Milestone 3 - v1.0 Public Testnet

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| PUBLIC-TESTNET-ADMISSION-001 | PROPOSED | Identity / network policy | v0.9 | Define public operator admission, abuse controls, and removal policy. |
| SIGNED-AUTOUPDATE-001 | PROPOSED | Release engineering | Supply-chain policy | Distribute authenticated updates with explicit rollout controls. |
| USER-DIAGNOSTICS-SUPPORT-001 | PROPOSED | `iamine-node` diagnostics | v0.9 observability | Produce privacy-safe support bundles and actionable user diagnostics. |
| V1-SUPPLY-CHAIN-SECURITY-001 | PROPOSED | Security / release engineering | Signed artifacts | Secure source, dependency, build, artifact, and release provenance. |
| NODE-UPGRADE-ROLLBACK-001 | PROPOSED | Packaging / operations | Signed autoupdate | Recover nodes safely from failed or incompatible upgrades. |
| IAMINE-V1-RELEASE-GATE-001 | PROPOSED | Architecture / QA release gate | All Milestone 3 features | Prove stable public-testnet operation and publish IAMINE v1.0. |

Milestone gate: 50-200 initial nodes, 30-60 stable days, signed updates,
operational rollback, and zero unresolved critical defects.

## Milestone 4 - v1.1 Verification and Reputation

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| PROOF-OF-INFERENCE-BASELINE-001 | PROPOSED | Verification protocol | v1.0 evidence model | Define verifiable claims for completed inference. |
| RESULT-VALIDATOR-QUORUM-001 | PROPOSED | Verification protocol | Proof baseline | Reach bounded validator agreement on result claims. |
| CHALLENGE-PROTOCOL-001 | PROPOSED | `iamine-network`, verification | Proof baseline, validator quorum | Challenge suspicious claims with replay-safe protocol messages. |
| MISBEHAVIOR-EVIDENCE-001 | PROPOSED | Verification / observability | Challenge protocol | Record portable, privacy-safe evidence of protocol violations. |
| NODE-REPUTATION-001 | PROPOSED | Reputation domain | Misbehavior evidence | Derive explicit reputation state from accepted evidence. |
| SYBIL-RESISTANCE-001 | PROPOSED | Identity / reputation | Node reputation, public identity | Limit identity multiplication and reputation manipulation. |
| SCHEDULER-REPUTATION-WIRING-001 | PROPOSED | Scheduler wiring only | Reputation policy | Consume reputation without moving policy into the scheduler. |
| NODE-BENCHMARK-CERTIFICATION-001 | PROPOSED | Hardware certification | Stable hardware profiler | Certify bounded benchmark methods separately from profiling. |
| NODE-PERFORMANCE-ATTESTATION-001 | PROPOSED | Hardware certification | Benchmark certification | Attach verifiable performance attestations to node capability claims. |

Reputation, verification, and hardware certification remain separate domains.

## Milestone 5 - v1.5 Economic Testnet

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| COMPUTE-ACCOUNTING-001 | PROPOSED | Accounting domain | Verified task lifecycle | Account for accepted compute without assigning monetary value. |
| REWARD-POLICY-001 | PROPOSED | Rewards domain | Compute accounting, reputation | Define deterministic testnet reward rules. |
| TESTNET-WALLET-IDENTITY-001 | PROPOSED | Wallet / identity | Public node identity | Associate testnet-only wallet identity with an operator. |
| TESTNET-REWARD-LEDGER-001 | PROPOSED | Testnet ledger | Reward policy, wallet identity | Record auditable testnet reward units. |
| CERTIFIED-NODE-REWARD-BOOST-001 | PROPOSED | Rewards / certification wiring | Benchmark certification, reward policy | Apply explicit certification modifiers without changing benchmark policy. |
| STAKE-AND-SLASHING-BASELINE-001 | PROPOSED | Economic security | Wallet identity, misbehavior evidence | Simulate bounded stake and slashing behavior. |
| REWARD-DISPUTE-001 | PROPOSED | Dispute protocol | Reward ledger, evidence | Resolve disputed accounting and reward decisions. |

This milestone uses testnet units with no real-world value.

## Milestone 6 - v2.0 Mainnet

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| SOLANA-NODE-REGISTRY-001 | PROPOSED | On-chain integration | v1.5 identity model | Anchor approved node registration on Solana. |
| SOLANA-REPUTATION-ANCHOR-001 | PROPOSED | On-chain integration | Stable reputation protocol | Anchor agreed reputation state without moving all evidence on-chain. |
| SOLANA-REWARD-SETTLEMENT-001 | PROPOSED | On-chain settlement | Audited accounting and reward policy | Settle accepted rewards on-chain. |
| SOLANA-STAKING-SLASHING-001 | PROPOSED | On-chain economic security | Tested slashing baseline | Enforce audited staking and slashing rules. |
| IAMINE-TOKEN-CONTRACT-001 | PROPOSED | Token program | Economic design approval | Implement the audited token contract. |
| MAINNET-SECURITY-AUDIT-001 | PROPOSED | Independent security audit | Mainnet candidate complete | Audit protocol, contracts, release chain, and operational controls. |
| MAINNET-READINESS-GATE-001 | PROPOSED | Architecture / security / QA | Audit findings resolved | Decide whether the network is safe to launch. |
| MAINNET-GENESIS-001 | PROPOSED | Mainnet operations | Readiness approved | Execute controlled mainnet genesis. |

Existing experimental Solana code does not imply that any mainnet feature is
closed or authorized.

## Parallel or Later Product Lines

### Applications

| Feature | State | Primary owner | Activation dependency | Goal |
| --- | --- | --- | --- | --- |
| DESKTOP-NODE-APP-001 | DEFERRED | Desktop application | Stable node management APIs | Provide a desktop node operator experience. |
| USER-WALLET-UX-001 | DEFERRED | Wallet UX | Stable wallet identity | Provide understandable wallet operations and recovery. |
| NODE-RESOURCE-CONTROLS-001 | DEFERRED | Desktop / node configuration | Stable resource policy | Let operators bound node resource contribution. |
| MODEL-MANAGEMENT-UI-001 | DEFERRED | Desktop / model catalog | Stable catalog API | Manage approved local models visually. |
| MOBILE-CONTROL-APP-001 | DEFERRED | Mobile application | Remote management API | Monitor and control nodes from mobile devices. |
| MOBILE-COMPUTE-CONTRIBUTION-001 | DEFERRED | Mobile runtime | Mobile control, platform feasibility | Evaluate bounded mobile compute contribution. |

### Marketplace

| Feature | State | Primary owner | Activation dependency | Goal |
| --- | --- | --- | --- | --- |
| ENTERPRISE-API-GATEWAY-001 | DEFERRED | Enterprise API | Stable public inference API | Provide authenticated enterprise access. |
| USAGE-BILLING-001 | DEFERRED | Billing | Production accounting | Bill accepted enterprise usage. |
| SERVICE-TIER-SLA-001 | DEFERRED | Product / operations | Billing, reliability evidence | Define measurable service tiers. |
| PRIVATE-CLUSTER-001 | DEFERRED | Enterprise networking | Admission and secure transport | Operate isolated organization clusters. |
| MODEL-MARKETPLACE-001 | DEFERRED | Marketplace / model policy | Trusted registry maturity | Distribute policy-approved model offerings. |
| ENTERPRISE-AUDIT-LOG-001 | DEFERRED | Enterprise observability | Enterprise gateway | Provide immutable tenant-visible audit evidence. |
| TELEGRAM-INFERENCE-BOT-001 | DEFERRED | Integration application | Stable remote API | Offer a bounded messaging integration. |

### Advanced Compute

| Feature | State | Primary owner | Activation dependency | Goal |
| --- | --- | --- | --- | --- |
| MODEL-SHARD-STORAGE-001 | DEFERRED | `iamine-models` storage | Stable model metadata | Store model shards with explicit ownership and cleanup. |
| MODEL-SHARD-INTEGRITY-001 | DEFERRED | `iamine-models` integrity | Shard storage | Verify every shard and complete artifact composition. |
| DISTRIBUTED-INFERENCE-PLAN-001 | DEFERRED | Distributed inference architecture | Shard integrity, stable scheduler | Define supported partitioning and failure semantics. |
| DISTRIBUTED-TENSOR-TRANSPORT-001 | DEFERRED | `iamine-network` | Distributed inference plan | Transport bounded intermediate tensors securely. |
| DISTRIBUTED-INFERENCE-ASSEMBLY-001 | DEFERRED | Distributed inference runtime | Tensor transport | Execute and assemble a partitioned inference plan. |
| MIXTURE-OF-EXPERTS-ROUTING-001 | DEFERRED | Model routing | Distributed inference evidence | Route expert work without weakening admission gates. |
| DISTRIBUTED-TRAINING-BASELINE-001 | DEFERRED | Training architecture | Mature distributed runtime | Define a bounded training baseline and threat model. |
| CHECKPOINT-DISTRIBUTION-001 | DEFERRED | Training storage / network | Training baseline | Distribute verified training checkpoints. |

## Enabling and Corrective Work

Architecture may add scoped features when implementation or QA exposes a real
dependency. Added work must identify its owning module, dependency, release
impact, and whether it blocks the active product feature.

Examples already present in `develop` include:

- capability consistency and scheduler capability matching;
- task lifecycle and router scheduler baselines;
- PubSub readiness, subscriber tracking, and result-return corrections;
- cluster stress requirement transport and mixed-model validation;
- metrics fallback and unknown-mode closure;
- runtime extraction and quality-gate hardening.

These features preserve the roadmap objective; they do not silently redefine
release gates.

## Process Automation Backlog

Process features support delivery but do not count as product milestone
features.

| Feature | State | Owner | Scheduling |
| --- | --- | --- | --- |
| PROCESS-QA-LOCAL-GATE-001 | CLOSED | `scripts`, `docs/process` | Merged in `2dd9132` |
| PROCESS-QA-HARDWARE-SMOKE-001 | DEFERRED | `scripts`, hardware QA | Resume after the next product checkpoint unless Architecture promotes it as a blocker |
| PROCESS-QA-SIZE-GUARD-001 | DEFERRED | `scripts`, Architecture QA | Process backlog |
| PROCESS-QA-PROXMOX-PREFLIGHT-001 | DEFERRED | `scripts`, remote QA | Process backlog |
| PROCESS-QA-TS140-SMOKE-001 | DEFERRED | `scripts`, remote QA | Process backlog |
| PROCESS-MERGE-PRECHECK-001 | DEFERRED | `scripts`, merge owner | Process backlog |
| PROCESS-POST-MERGE-VALIDATION-001 | DEFERRED | `scripts`, merge owner | Process backlog |

## Immediate Sequence

```text
TESTNET-NODE-ADMISSION-001
-> P2P-SECURE-TRANSPORT-POLICY-001
-> LAN-BETA-FIRST-RUN-PREFLIGHT-001
-> REMOTE-INFERENCE-API-001
-> TESTNET-OBSERVABILITY-001
-> TESTNET-LOAD-RESILIENCE-001
-> PRIVATE-TESTNET-RELEASE-001
```

Each arrow still requires the complete canonical feature lifecycle. A roadmap
position is not development authorization.
