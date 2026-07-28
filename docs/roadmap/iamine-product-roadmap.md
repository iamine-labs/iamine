# IAMINE Product Roadmap

## Authority

This document is the canonical product roadmap index for IAMINE. It translates
the Architecture plan into versioned repository state and supersedes milestone
interpretations inferred from the whitepaper or from individual feature
closeout documents.

The whitepaper remains strategic vision. This roadmap controls implementation
sequence, release meaning, and feature authorization.

Operational status reconciled from:

```text
branch: origin/develop
baseline commit: c836d5c8f18fd95967b0114fbc0bd185c59158de
baseline tree: a351ba66c486975261ba1050f730a00ebe7f8aac
date: 2026-07-28
```

Canonical update:

```text
ROADMAP-OFFICIAL-AGENT-NETWORK-RECONCILIATION-001
IAMINE-CANONICAL-ROADMAP-RECONCILIATION-001
```

These updates incorporate the official Agent Network roadmap and its current
repository status. IAMINE v1.0 is the IAMINE Agent Network Public Beta, not an
inference-only public beta.

The detailed Agent Network roadmap lives in:

```text
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-gui-cli-product-track.md
docs/roadmap/iamine-security-ci-track.md
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
| v0.9.x | Remote inference, observability, load, and private testnet release candidate |
| v0.10.x | Pre-public infrastructure |
| v0.11.x | Agent research, architecture, scope, permissions, and runtime baseline |
| v0.12.x | P0 official agent skeletons, P0 official agents, and internal tools for official agent creation |
| v0.13.x | P1/P2 agents and Agent Network beta productization |
| v1.0.0 | IAMINE Agent Network Public Beta |
| v1.1.x | Validation, reputation, and trust |
| v1.2.x | Public agent developer platform |
| v1.3.x | Curated agent registry |
| v1.4.x | Curated agent marketplace |
| v1.5.x | Economic agent testnet using testnet units with no real-world value |
| v2.0.x | Advanced compute, distributed MoE, mainnet, settlement, open marketplace, and real economy |

`LAN-INFERENCE-BETA-001` is not IAMINE v1. An inference-only public beta is
not IAMINE v1. Mainnet is not part of v1.

## Current Position

IAMINE has closed Milestone 0 and Milestone 1. The v0.9 private-testnet
foundation has closed its protocol, identity, discovery, admission, secure
transport, remote inference, observability, load-resilience, and repository
launch-readiness feature gates. `PRIVATE-TESTNET-RELEASE-001` merged at
`92fdda4`; Mac local validation, TS140 field QA, Proxmox/R5500 field QA, and
post-merge validation passed with documented baseline warnings. The repository
fresh-install E2E gap also closed through
`V0.9-BETA-FRESH-INSTALL-E2E-001`, merge `a4cedc9`.

The official roadmap now requires IAMINE to continue through pre-public
infrastructure and Agent Network foundations before v1.0. The next product
line is not an inference-only public testnet. It is the IAMINE Agent Network.

All six v0.12.0 P0 skeleton contracts are closed, but they are non-executable
and not user available. Functional `NODE-DOCTOR-AGENT-001` development is
blocked while referenced metadata validation, trusted package/runtime
integration, sandbox implementation, remaining executable runtime gates,
compatibility decisions, and the dedicated redacted evidence provider remain
unavailable. The closed Scope, Permission, and Audit Events in-memory
boundaries do not by themselves authorize package loading or execution.

`AGENT-MANIFEST-PARSER-VALIDATOR-001` closed in v0.11.1 through merge
`c849d98`. It introduces the isolated `iamine-agents` root-manifest parser and
validation crate without package loading or runtime wiring. Its closure
satisfies only the parser prerequisite, not the remaining Node Doctor execution
gates.

`AGENT-PACKAGE-LOAD-GATE-001` closed in v0.11.2 through merge `d56cbce`. It adds
a typed in-memory assessment that remains structurally blocked while referenced
metadata validators, policy reviews, compatibility decisions, and runtime
enforcement gates are unavailable. It does not read package directories or
authorize execution.

`V0.11.2-EXECUTABLE-RUNTIME-PREREQUISITE-RECONCILIATION-001` closed in merge
`2380baa` after mapping those explicit blockers and remaining
architecture-only runtime claims to independently owned implementation
features. It changed roadmap state only.

Fifteen of the 19 executable v0.11.2 rows are closed. The last closed row is
`AGENT-AUDIT-EVENT-ENFORCEMENT-001`, merge `b9fe62d`.
`AGENT-EXECUTION-AUTHORIZATION-001` is the next sequential implementation
feature and is active on `feature/agent-execution-authorization-001` after
`IAMINE-CANONICAL-ROADMAP-RECONCILIATION-001` closed. The package-load evidence
integration, package loader, and runtime executor rows remain proposed and are
not authorized in bulk.

The broader v0.9 operational stability claim still requires the operational
target: 10-50 nodes, 3-10 operators, multiple physical networks, and two to
four weeks of stable operation.

Every IAMINE agent must be scope-bound. A specialized agent must only execute
tasks inside its declared scope. Out-of-scope tasks must be refused, clarified,
or handed off to the orchestrator.

## Canonical Reconciliation

| Feature | State | Owner | Goal |
| --- | --- | --- | --- |
| ROADMAP-OFFICIAL-AGENT-NETWORK-RECONCILIATION-001 | CLOSED | Architecture / product roadmap | Reconciled this repository roadmap with the official Agent Network roadmap while preserving closed feature evidence without reinterpreting it; merge `62761cb`, post-merge validation PASS. |
| ROADMAP-AGENT-NETWORK-COMPLETE-RECONCILIATION-001 | CLOSED | Architecture / product roadmap | Incorporated the complete Agent Network, agent creation architecture, developer platform, language policy, dependency policy, routing, and advanced compute roadmap update; merge `7769cb2`, focused post-merge validation PASS. |
| NODE-DOCTOR-AGENT-001-DEPENDENCY-RECONCILIATION-001 | CLOSED | Architecture / product roadmap | Kept functional Node Doctor implementation blocked and placed its executable prerequisite chain before development authorization; merge `7588e09`, focused post-merge validation PASS. |
| IAMINE-CANONICAL-ROADMAP-RECONCILIATION-001 | CLOSED | Architecture / product roadmap | Reconciled the current v0.11.2 status, preserved existing milestone numbering, registered proposed GUI/CLI and Security/CI tracks, and corrected superseded QA evidence without changing runtime behavior; merge `e761b0a`, post-merge quality gate PASS WITH WARNINGS. |

## Milestone 0 - v0.7 Foundations

| Feature | State | Owner | Goal |
| --- | --- | --- | --- |
| NODE-HARDWARE-PROFILER-001 | CLOSED | `iamine-hardware`, `iamine-node` wiring | Describe local visible hardware without making scheduler or compatibility decisions. |
| MODEL-HARDWARE-COMPATIBILITY-001 | CLOSED | `iamine-models` | Evaluate explicit model requirements against normalized hardware. |
| MODEL-TRUSTED-REGISTRY-INTEGRITY-001 | CLOSED | `iamine-models` | Block new artifacts with missing, invalid, placeholder, or mismatched integrity metadata. |
| MODEL-BETA-REGISTRY-METADATA-001 | CLOSED | `iamine-models` | Add verified checksum, license, source, format, size, revision, and network metadata for approved beta models. |
| MODEL-CATALOG-SELECTION-CLI-001 | CLOSED | `iamine-models`; `iamine-node` wiring only | Provide explainable catalog, compatibility-aware selection, and controlled download flow. |

Complementary closed gates:

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

Closeout evidence:

```text
docs/roadmap/v0.7-foundations-closeout.md
```

## Milestone 1 - v0.8 Installable LAN Beta

| Feature | State | Primary owner | Goal |
| --- | --- | --- | --- |
| LAN-INFERENCE-BETA-CONTRACT-001 | CLOSED | Architecture; protocol owners | Define supported LAN topology, user flows, failure semantics, and release boundaries. |
| LAN-NODE-DOCTOR-001 | CLOSED | `iamine-node` diagnostics module | Diagnose hardware, model, backend, configuration, and LAN readiness without starting inference. |
| LAN-WORKER-LIFECYCLE-001 | CLOSED | `iamine-node` worker runtime | Provide explicit install, start, stop, restart, readiness, and recovery behavior. |
| NODE-CONFIG-SCHEMA-MIGRATION-001 | CLOSED | `iamine-node` configuration | Version node configuration and provide bounded migration and rollback. |
| WORKER-METRICS-PORT-ALLOCATION-001 | CLOSED | `iamine-node` metrics | Allocate deterministic, non-conflicting metrics endpoints for multiple workers. |
| LAN-INFERENCE-CLI-001 | CLOSED | `iamine-node` CLI wiring | Expose the supported LAN inference workflow with clear errors and no hidden startup. |
| LAN-REAL-INFERENCE-RESILIENCE-001 | CLOSED | `iamine-node`, `iamine-network`, `iamine-models` owner modules | Recover safely from worker, network, backend, and model failures during real inference. |
| V1-OBSERVABILITY-001 (LAN phase) | CLOSED | `iamine-network`, `iamine-node` observability | Emit correlated operational evidence for setup, dispatch, execution, recovery, and result delivery. |
| LAN-INFERENCE-BETA-PACKAGING-001 | CLOSED | Packaging / operations | Deliver clean install, upgrade, service integration, and rollback artifacts. |
| LAN-INFERENCE-BETA-001 | CLOSED | Architecture / QA release gate | Validate and publish the installable LAN beta. |

Closed prework that reduces Milestone 1 risk:

| Feature | State | Evidence in `develop` |
| --- | --- | --- |
| CLUSTER-LAN-AUTO-DISCOVERY-001 | CLOSED | `c6ffe3b`, closeout `a973925` |
| LEGACY-BACKEND-REAL-INFERENCE-001 | CLOSED | `0df03f8`, hardening `4b82338` |
| LEGACY-BACKEND-WORKER-DAEMON-E2E-001 | CLOSED | merge `2882ce9` |

## v0.8 Beta Distribution Hardening

| Feature | State | Primary owner | Goal |
| --- | --- | --- | --- |
| LAN-BETA-INSTALLER-POLISH-001 | CLOSED | Packaging / operations | Provide safer install and uninstall helpers for controlled beta testing on additional PCs. |
| LAN-BETA-FIRST-RUN-PREFLIGHT-001 | CLOSED | Packaging / operations / diagnostics | Validate first-run configuration, model, backend, ports, permissions, diagnostics, worker readiness, and basic LAN smoke. |

## v0.9.x - Private Testnet Foundation

| Feature | State | Primary owner | Goal |
| --- | --- | --- | --- |
| P2P-PROTOCOL-VERSIONING-001 | CLOSED | `iamine-network` | Negotiate compatible protocol versions and reject unsupported peers explicitly. |
| NODE-IDENTITY-REGISTRATION-001 | CLOSED | Identity / `iamine-node` | Register durable operator-controlled node identities without exposing host secrets. |
| BOOTNODE-DISCOVERY-001 | CLOSED | `iamine-network` | Bootstrap peers from an explicit, replaceable bootnode set. |
| WAN-PEER-DISCOVERY-001 | CLOSED | `iamine-network` | Discover authorized peers across physical networks. |
| NAT-TRAVERSAL-RELAY-001 | CLOSED | `iamine-network` | Connect constrained nodes through bounded NAT traversal and relay policy. |
| TESTNET-NODE-ADMISSION-001 | CLOSED | Identity / network policy | Admit only authorized private-testnet nodes. |
| P2P-SECURE-TRANSPORT-POLICY-001 | CLOSED | `iamine-network` | Define authenticated transport and downgrade rejection. |
| REMOTE-INFERENCE-API-001 | CLOSED | API boundary / `iamine-node` wiring | Accept bounded remote inference requests with explicit authentication and policy checks. |
| TESTNET-OBSERVABILITY-001 | CLOSED | Observability owner modules | Correlate cross-operator health, routing, execution, and failure evidence. |
| TESTNET-LOAD-RESILIENCE-001 | CLOSED | Runtime, scheduler, QA | Prove bounded behavior under concurrency, partial outages, retries, and recovery. |
| PRIVATE-TESTNET-RELEASE-001 | CLOSED | Architecture / QA release gate | Close the repository launch-readiness gate while preserving the future multi-operator soak as a separate operational gate. |
| V0.9-BETA-FRESH-INSTALL-E2E-001 | CLOSED | Packaging / QA / operations | Prove that a fresh node install can join the private testnet and exercise bootnode, identity, remote inference, and observability; merge `a4cedc9`, post-merge validation PASS WITH WARNINGS. |

The repository fresh-install E2E gap is closed. The broader v0.9 operational
stability claim still requires the 10-50 node, 3-10 operator, multi-network,
two-to-four-week soak.

## v0.10.0 - Pre-Public Infrastructure

| Feature | State | Primary owner | Goal |
| --- | --- | --- | --- |
| PUBLIC-TESTNET-ADMISSION-001 | CLOSED | Identity / network policy | Define controlled public operator admission, abuse controls, and removal policy before public beta; merge `e0c125a`, post-merge validation PASS WITH WARNINGS. |
| SIGNED-AUTOUPDATE-001 | CLOSED | Release engineering | Distribute authenticated updates with explicit rollout controls; merge `39c6243`, post-merge validation PASS WITH WARNINGS. |
| USER-DIAGNOSTICS-SUPPORT-001 | CLOSED | `iamine-node` diagnostics | Produce privacy-safe support bundles and actionable user diagnostics; merge `8070963`, post-merge validation PASS WITH WARNINGS. |
| V1-SUPPLY-CHAIN-SECURITY-001 | CLOSED | Security / release engineering | Secure source, dependency, build, artifact, and release provenance; merge `a741699`, post-merge validation PASS WITH WARNINGS. |
| NODE-UPGRADE-ROLLBACK-001 | CLOSED | Packaging / operations | Recover nodes safely from failed or incompatible upgrades; merge `7dc1f11`, post-merge validation PASS. |
| PUBLIC-TESTNET-DOCUMENTATION-001 | CLOSED | Documentation / operations | Provide minimum public documentation without launching public beta; merge `2a3bd6d`, post-merge validation PASS WITH WARNINGS. |
| IAMINE-PREPUBLIC-READINESS-GATE-001 | CLOSED | Architecture / QA release gate | Decide whether IAMINE is ready to proceed into Agent Network foundations; merge `eb8db38`, focused post-merge validation PASS. |

Milestone gate: controlled public install, signed upgrade, rollback,
diagnostic export, public documentation baseline, release artifact validation,
and no public beta launch.

## Agent Network Roadmap Index

The detailed feature lists, scopes, restrictions, and QA expectations for
v0.11 through v2.0 are maintained in:

```text
docs/roadmap/iamine-agent-network-roadmap.md
```

Milestone closure is controlled by the named gate registry in that document.
Any milestone not already historically closed when
`AGENT-MILESTONE-QA-GATES-001` is adopted must keep its current state until its
registered exhaustive QA gate is merged, post-merge validated, and closed by
Architecture. Feature-local QA is insufficient for milestone closure.

| Release | State | Product focus |
| --- | --- | --- |
| v0.11.0 | CLOSED | Agent research and product fit |
| v0.11.1 | CLOSED | Agent architecture foundation; exhaustive gate merge `0bdff4b`, post-merge validation PASS |
| v0.11.2 | PROPOSED | Agent runtime baseline |
| v0.11.3 | CLOSED | Internal agent developer bootstrap contracts |
| v0.12.0 | ACTIVE | P0 skeletons closed; functional P0 agents blocked on executable runtime prerequisites |
| v0.12.1 | PROPOSED | P1 adoption agents |
| v0.12.2 | PROPOSED | P2 experimental and technical agents |
| v0.13.0 | PROPOSED | Agent beta productization |
| v1.0.0 | PROPOSED | IAMINE Agent Network Public Beta |
| v1.1.x | PROPOSED | Validation, reputation, and trust |
| v1.2.x | PROPOSED | Public agent developer platform |
| v1.3.x | PROPOSED | Curated agent registry |
| v1.4.x | PROPOSED | Curated agent marketplace |
| v1.5.x | PROPOSED | Economic agent testnet |
| v2.0.x | PROPOSED | Advanced compute, distributed MoE, mainnet, settlement, open marketplace, and real economy |

Mainnet, settlement, an open marketplace, arbitrary third-party agents, and
real payments remain blocked until the appropriate trust, registry,
validation, reputation, and economic-testnet layers exist.

## Parallel or Later Product Lines

Canonical parallel tracks:

| Track | State | Activation rule | Roadmap |
| --- | --- | --- | --- |
| IAMINE-GUI-CLI-PRODUCT-TRACK | PROPOSED | Visual mock work may proceed independently; real actions require shared contracts, local authorization, audit, and the Local Control API contract. | `docs/roadmap/iamine-gui-cli-product-track.md` |
| IAMINE-SECURITY-CI-TRACK | PROPOSED / OPEN | Does not replace the sequential runtime feature; unresolved findings block release closure and supply-chain readiness. | `docs/roadmap/iamine-security-ci-track.md` |

These lines remain deferred unless Architecture explicitly promotes them into
the active roadmap:

| Line | State | Activation dependency |
| --- | --- | --- |
| Desktop and mobile applications | DEFERRED | Stable node management APIs and resource policy |
| Enterprise API and billing | DEFERRED | Stable public inference API, accounting, and reliability evidence |
| Model marketplace | DEFERRED | Trusted registry maturity |
| Advanced distributed compute | DEFERRED | Mature distributed runtime and verified shard integrity |
| Advanced automation agents | DEFERRED | Agent safety, trust, and permission maturity |

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

Latest corrective feature:

| Feature | State | Owner | Release impact |
| --- | --- | --- | --- |
| V0.11.2-EXECUTABLE-RUNTIME-PREREQUISITE-RECONCILIATION-001 | CLOSED | Architecture / QA / roadmap | Registered the missing executable prerequisites and their canonical order; it did not authorize them in bulk. |

## Process Automation Backlog

Process features support delivery but do not count as product milestone
features.

| Feature | State | Owner | Scheduling |
| --- | --- | --- | --- |
| PROCESS-QA-LOCAL-GATE-001 | CLOSED | `scripts`, `docs/process` | Merged in `2dd9132` |
| AGENT-MILESTONE-QA-GATES-001 | CLOSED | Architecture / QA / roadmap | Named exhaustive QA gates formalized before Agent Network milestone closure; merge `03632e8`, docs-only post-merge validation PASS. |
| PROCESS-QA-HARDWARE-SMOKE-001 | DEFERRED | `scripts`, hardware QA | Required by the official v0.9.x QA/process plan; promote before any feature that depends on standardized hardware smoke evidence. |
| PROCESS-QA-SIZE-GUARD-001 | DEFERRED | `scripts`, Architecture QA | Required by the official v0.9.2 QA/process plan; promote before large or architecture-sensitive changes. |
| PROCESS-QA-PROXMOX-PREFLIGHT-001 | DEFERRED | `scripts`, remote QA | Required by the official v0.9.x QA/process plan; promote before remote field QA standardization. |
| PROCESS-QA-TS140-SMOKE-001 | DEFERRED | `scripts`, remote QA | Required by the official v0.9.x QA/process plan; promote before TS140 field QA standardization. |
| PROCESS-MERGE-PRECHECK-001 | DEFERRED | `scripts`, merge owner | Required by the official v0.9.x QA/process plan; promote before merge automation standardization. |
| PROCESS-POST-MERGE-VALIDATION-001 | DEFERRED | `scripts`, merge owner | Required by the official v0.9.x QA/process plan; promote before post-merge validation automation standardization. |

## Immediate Sequence

```text
AGENT-POLICY-METADATA-VALIDATORS-001
-> AGENT-DESCRIPTIVE-METADATA-VALIDATORS-001
-> AGENT-BOUNDARY-EVAL-VALIDATOR-001
-> AGENT-RUNTIME-CORE-001
-> AGENT-PACKAGE-REFERENCE-RESOLVER-001
-> AGENT-PACKAGE-REVIEW-EVIDENCE-001
-> AGENT-RUNTIME-COMPATIBILITY-GATE-001
-> AGENT-INPUT-OUTPUT-ENFORCEMENT-001
-> AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001
-> AGENT-EXECUTION-LIFECYCLE-ENGINE-001
-> AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001
-> AGENT-HANDOFF-ENFORCEMENT-001
-> AGENT-OUT-OF-SCOPE-RESPONSE-ENFORCEMENT-001
-> AGENT-ROUTING-CANDIDATE-SELECTOR-001
-> AGENT-AUDIT-EVENT-ENFORCEMENT-001
-> AGENT-EXECUTION-AUTHORIZATION-001
-> AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001
-> AGENT-PACKAGE-LOADER-001
-> AGENT-RUNTIME-EXECUTOR-001
-> V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001
-> NODE-DOCTOR-EVIDENCE-PROVIDER-001
-> NODE-DOCTOR-AGENT-001
-> remaining functional P0 official agents
-> V0.12.0-P0-OFFICIAL-AGENTS-MILESTONE-QA-001
```

`AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001` closed in merge `2dbb760` after
Architecture, implementation, local validation, exact-tree field QA, final
Architecture review, and post-merge validation. Fifteen of the 19 implementation
rows are now `CLOSED`. `AGENT-HANDOFF-ENFORCEMENT-001` closed in merge
`9e42136` after exact-tree six-role field QA and post-merge validation.
`AGENT-OUT-OF-SCOPE-RESPONSE-ENFORCEMENT-001` closed in merge `0b9bdf0`
after local validation, exact-tree six-role field QA, final Architecture
review, and post-merge validation.
`AGENT-ROUTING-CANDIDATE-SELECTOR-001` closed in merge `1efa9cf`; its bounded
in-memory implementation, local regression, exact-tree field QA on six
platform roles, final Architecture review, and unrestricted post-merge gate
passed without scheduler, transport, model-selection, distributed-MoE, or
execution side effects. `AGENT-AUDIT-EVENT-ENFORCEMENT-001` closed in merge
`b9fe62d` after its bounded audit-owner integration passed focused, runtime,
agents, strict-clippy, full quality-gate, privacy, size, exact-tree field QA
on six platform roles, final Architecture validation, and unrestricted
post-merge validation. `AGENT-EXECUTION-AUTHORIZATION-001` is the next
sequential feature. The other four rows stay `PROPOSED`, and no later row is
authorized in bulk.

Each arrow still requires the complete canonical feature lifecycle. A roadmap
position is not development authorization.
