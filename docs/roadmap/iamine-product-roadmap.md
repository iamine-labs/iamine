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
commit: 2e03f5984abdadfb3eedb12ab6ea7cfbdd7710a1
date: 2026-07-09
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
| v0.9.1 | Remote inference operational |
| v0.9.2 | Private testnet release candidate |
| v0.10 | Pre-public infrastructure |
| v0.11 | Agent research, architecture, runtime, and internal developer bootstrap |
| v0.12 | Official agent pack |
| v0.13 | Agent beta productization |
| v1.0 | IAMINE Agent Network Public Beta |
| v1.1 | Validation, reputation, and trust |
| v1.2 | Public Agent Developer Platform |
| v1.3 | Curated agent registry |
| v1.4 | Curated agent marketplace |
| v1.5 | Economic agent testnet using units with no real-world value |
| v2.0 | Mainnet |

`LAN-INFERENCE-BETA-001` is not IAMINE v1. IAMINE v1.0 is not an
inference-only public beta; it is the IAMINE Agent Network Public Beta.
Mainnet is not part of v1.

## Agent Network Public Beta Decision

IAMINE public beta must launch as an agent network, not only as a remote
inference network. Before v1.0, IAMINE must close private testnet readiness,
pre-public infrastructure, agent package contracts, permissions, audit logs,
runtime controls, sandbox baseline, internal agent developer bootstrap, P0
official agents, catalog preview, onboarding, and feedback loop.

The public developer platform is intentionally after v1.0. IAMINE may use
internal agent developer tools before v1.0 to build official agents, but
third-party agent creation, open publication, real payments, and mainnet remain
blocked until their registry, validation, reputation, and economic gates exist.

## Current Position

IAMINE has closed Milestone 0, Milestone 1, beta distribution hardening,
remote inference, and private-testnet observability. Milestone 2 has closed
P2P protocol versioning, node identity registration, bootnode discovery, WAN
peer discovery, NAT traversal relay policy, testnet admission, secure transport
policy, remote inference API, and testnet observability.

The next product gap before load-resilience work is a fresh-install private
testnet E2E. That gate validates that a clean machine can install IAMINE, join
the private testnet, and execute the first bounded test inference with the
current remote inference and observability contracts.

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
| LAN-BETA-FIRST-RUN-PREFLIGHT-001 | CLOSED | Packaging / operations / diagnostics | merge `b7accd3`; post-merge `quality-gate.sh` PASS WITH WARNINGS | Validate first-run configuration, model, backend, ports, permissions, diagnostics, worker readiness, and basic LAN smoke before broader beta testing on additional PCs. |

## Milestone 2 - v0.9 Private Testnet

### v0.9.1 - Remote Inference Operational

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| P2P-PROTOCOL-VERSIONING-001 | CLOSED | `iamine-network` | v0.8 | Negotiate compatible protocol versions and reject unsupported peers explicitly. |
| NODE-IDENTITY-REGISTRATION-001 | CLOSED | Identity / `iamine-node` | Protocol versioning | Register durable operator-controlled node identities without exposing host secrets. |
| BOOTNODE-DISCOVERY-001 | CLOSED | `iamine-network` | Protocol versioning | Bootstrap peers from an explicit, replaceable bootnode set. |
| WAN-PEER-DISCOVERY-001 | CLOSED | `iamine-network` | Bootnodes | Discover authorized peers across physical networks. |
| NAT-TRAVERSAL-RELAY-001 | CLOSED | `iamine-network` | WAN discovery | Connect constrained nodes through bounded NAT traversal and relay policy. |
| TESTNET-NODE-ADMISSION-001 | CLOSED | Identity / network policy | Node registration | Admit only authorized private-testnet nodes. |
| P2P-SECURE-TRANSPORT-POLICY-001 | CLOSED | `iamine-network` | Node identity | Define authenticated transport and downgrade rejection. |
| REMOTE-INFERENCE-API-001 | CLOSED | API boundary / `iamine-node` wiring | merge `c4046068`; Mac/TS140/Proxmox QA PASS; post-merge `quality-gate.sh` PASS WITH WARNINGS | Accept bounded remote inference requests with explicit authentication and policy checks. |
| TESTNET-OBSERVABILITY-001 | CLOSED | Observability owner modules | merge `d6068ce`; Mac/TS140/Proxmox QA PASS; post-merge `quality-gate.sh` PASS WITH WARNINGS | Correlate cross-operator health, routing, execution, and failure evidence. |
| V0.9-BETA-FRESH-INSTALL-E2E-001 | PROPOSED | Architecture / QA release gate | Remote inference, testnet observability, beta packaging | Validate a clean Proxmox VM user journey from installation through first private-testnet inference. |

Close criteria:

- a fresh install node can join the private testnet;
- bootnode discovery and durable identity work on the clean node;
- remote inference works with explicit policy checks;
- errors carry trace IDs and are visible in observability output;
- Mac, TS140, and Proxmox QA pass for the supported path.

### v0.9.2 - Private Testnet Release Candidate

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| TESTNET-LOAD-RESILIENCE-001 | PROPOSED | Runtime, scheduler, QA | Testnet observability, fresh-install E2E | Prove bounded behavior under concurrency, partial outages, retries, and recovery. |
| PRIVATE-TESTNET-RELEASE-001 | PROPOSED | Architecture / QA release gate | All v0.9 features | Operate and close the private multi-operator testnet gate. |

`V0.9-BETA-FRESH-INSTALL-E2E-001` blocks v0.9.2 if it has not closed.

Milestone gate: 10-50 nodes, 3-10 operators, multiple physical networks,
remote inference across nodes, basic fault tolerance, diagnosable logs, private
installation documentation, and two to four weeks of stable operation.

## v0.10 - Pre-Public Infrastructure

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| PUBLIC-TESTNET-ADMISSION-001 | PROPOSED | Identity / network policy | v0.9 | Define public operator admission, abuse controls, and removal policy. |
| SIGNED-AUTOUPDATE-001 | PROPOSED | Release engineering | Supply-chain policy | Distribute authenticated updates with explicit rollout controls. |
| USER-DIAGNOSTICS-SUPPORT-001 | PROPOSED | `iamine-node` diagnostics | v0.9 observability | Produce privacy-safe support bundles and actionable user diagnostics. |
| V1-SUPPLY-CHAIN-SECURITY-001 | PROPOSED | Security / release engineering | Signed artifacts | Secure source, dependency, build, artifact, and release provenance. |
| NODE-UPGRADE-ROLLBACK-001 | PROPOSED | Packaging / operations | Signed autoupdate | Recover nodes safely from failed or incompatible upgrades. |
| PUBLIC-TESTNET-DOCUMENTATION-001 | PROPOSED | Documentation / operations | Pre-public install path | Publish minimum controlled public-testnet documentation. |
| IAMINE-PREPUBLIC-READINESS-GATE-001 | PROPOSED | Architecture / QA release gate | All v0.10 features | Confirm IAMINE is ready for external users without launching public beta. |

Close criteria: controlled public-style install works, signed upgrade and
rollback work, diagnostics export is available, documentation exists, and beta
public launch remains blocked until the agent network gates close.

## v0.11 - Agent Foundation

### v0.11.0 - Agent Research and Product Fit

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-MARKET-FIT-RESEARCH-001 | PROPOSED | Product / research | v0.10 planning | Identify agent use cases by real adoption signals, not technical intuition alone. |
| AGENT-USER-PERSONA-MAPPING-001 | PROPOSED | Product / research | Market-fit research | Map target users, pain points, hardware reality, and safety expectations. |
| AGENT-BETA-PACK-SELECTION-001 | PROPOSED | Product / Architecture | Persona mapping | Choose the official beta agent pack by value, safety, demo quality, and feasibility. |

### v0.11.1 - Agent Architecture Foundation

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-PACKAGE-MANIFEST-001 | PROPOSED | Agent package schema | Agent beta pack selection | Define the agent package manifest and validation boundary. |
| AGENT-CAPABILITY-METADATA-001 | PROPOSED | Capability model | Agent manifest | Let agents declare capability inputs, outputs, and compatibility metadata. |
| AGENT-RESOURCE-REQUIREMENTS-001 | PROPOSED | Resource policy | Capability metadata | Declare CPU, memory, storage, network, and backend requirements explicitly. |
| AGENT-PERMISSION-MODEL-001 | PROPOSED | Permission policy | Agent manifest | Define visible, deny-by-default permissions for agent execution. |
| AGENT-AUDIT-LOG-001 | PROPOSED | Observability / audit | Permission model | Record privacy-safe execution, permission, and failure evidence. |
| AGENT-REGISTRY-LOCAL-001 | PROPOSED | Local registry | Agent manifest | Provide a local official-agent registry without opening third-party publication. |

### v0.11.2 - Agent Runtime Baseline

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-RUNTIME-BASELINE-001 | PROPOSED | Agent runtime | Architecture foundation | Execute official agents through a controlled runtime. |
| AGENT-RUNTIME-SANDBOX-001 | PROPOSED | Agent runtime / security | Runtime baseline | Enforce sandbox boundaries for official agents. |
| AGENT-EXECUTION-LIFECYCLE-001 | PROPOSED | Agent runtime | Runtime baseline | Support queued, permission_pending, running, completed, failed, cancelled, timeout, and blocked states. |
| AGENT-INPUT-OUTPUT-CONTRACT-001 | PROPOSED | Agent runtime / schema | Runtime baseline | Define structured inputs, outputs, and errors. |
| AGENT-TIMEOUT-CANCEL-001 | PROPOSED | Agent runtime | Execution lifecycle | Bound long-running agents with timeout and cancellation. |

### v0.11.3 - Internal Agent Developer Bootstrap

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-FRAMEWORK-BASELINE-001 | PROPOSED | Internal agent framework | Runtime baseline | Provide the internal framework used to build official IAMINE agents. |
| AGENT-TEMPLATE-DIAGNOSTIC-001 | PROPOSED | Internal templates | Framework baseline | Generate diagnostic agent skeletons. |
| AGENT-TEMPLATE-FILE-READONLY-001 | PROPOSED | Internal templates | Framework baseline | Generate file-readonly agent skeletons with scoped permissions. |
| AGENT-TEMPLATE-NETWORK-DIAGNOSTIC-001 | PROPOSED | Internal templates | Framework baseline | Generate LAN/network diagnostic agent skeletons. |
| AGENT-TEMPLATE-REPORTER-001 | PROPOSED | Internal templates | Framework baseline | Generate reporter agent skeletons. |
| AGENT-TEMPLATE-TEXT-ASSISTANT-001 | PROPOSED | Internal templates | Framework baseline | Generate text-assistant agent skeletons. |
| AGENT-TEMPLATE-OS-DIAGNOSTIC-001 | PROPOSED | Internal templates | Framework baseline | Generate OS diagnostic agent skeletons. |
| IAMINE-DEV-SETUP-AGENT-001-INTERNAL | PROPOSED | Internal developer agents | Framework baseline | Help IAMINE maintainers install and validate internal agent tooling with confirmation. |
| AGENT-BUILDER-ASSISTANT-AGENT-001-INTERNAL | PROPOSED | Internal developer agents | Templates | Assist maintainers in creating official agents from approved templates. |
| AGENT-MANIFEST-WIZARD-AGENT-001-INTERNAL | PROPOSED | Internal developer agents | Manifest schema | Generate and correct manifests for official agents. |
| AGENT-PERMISSION-REVIEW-AGENT-001-INTERNAL | PROPOSED | Internal developer agents | Permission model | Detect excessive permissions before official agents proceed. |

Internal bootstrap must not publish tools to third parties, auto-publish
agents, generate destructive permissions by default, or grant unrestricted
shell, filesystem, or network access.

## v0.12 - Official Agents

### v0.12.0 - P0 Official Agents

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| NODE-DOCTOR-AGENT-001 | PROPOSED | Official agents / diagnostics | Agent runtime | Diagnose IAMINE node health, identity, peers, models, readiness, logs, config, and warnings. |
| REPORTER-AGENT-001 | PROPOSED | Official agents / reporting | Agent runtime | Convert technical results into clear technical and user-friendly reports. |
| LAN-FILE-SHARE-ASSISTANT-AGENT-001 | PROPOSED | Official agents / LAN files | Permission model, local-only mode | Copy allowed files or folders between IAMINE LAN nodes with hashing, progress, cancel, and overwrite protection. |
| PHOTO-LIBRARY-ORGANIZER-AGENT-001 | PROPOSED | Official agents / local files | Permission model | Detect exact duplicates, large media, metadata dates, and proposed folder organization without auto-delete. |
| HOME-NETWORK-ASSISTANT-AGENT-001 | PROPOSED | Official agents / networking | Network diagnostic template | Diagnose gateway, DNS, latency, ports, peer reachability, and connectivity without changing router settings. |
| WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001 | PROPOSED | Official agents / OS diagnostics | OS diagnostic template | Produce safe Windows performance recommendations without destructive changes or silent uninstall. |

### v0.12.1 - P1 Adoption Agents

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| PRINTER-DOCTOR-AGENT-001 | PROPOSED | Official agents / LAN diagnostics | Agent runtime | Diagnose LAN printers, queues, reachability, and platform print services without unsafe driver or firewall changes. |
| DOCUMENT-ORGANIZER-AGENT-001 | PROPOSED | Official agents / local files | File-readonly template | Classify permitted documents, detect duplicates, infer metadata, and propose organization without moving files automatically. |
| CONTENT-POST-DRAFT-AGENT-001 | PROPOSED | Official agents / content | Text assistant template | Draft captions, hashtags, tone variants, and platform-specific text without publishing. |
| CONTENT-CALENDAR-AGENT-001 | PROPOSED | Official agents / content | Text assistant template | Create weekly content calendars, topic suggestions, campaign grouping, and posting checklists. |
| RECIPE-TEXT-AGENT-001 | PROPOSED | Official agents / text assistant | Text assistant template | Generate simple recipes, substitutions, steps, and timing from user-provided ingredients. |
| HOMELAB-DOCTOR-AGENT-001 | PROPOSED | Official agents / homelab diagnostics | Diagnostic templates | Report IAMINE nodes, services, resources, network, containers when available, warnings, and health. |

### v0.12.2 - P2 Experimental and Technical Agents

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| PROXMOX-READONLY-AGENT-001 | PROPOSED | Experimental agents | Permission model | Inspect Proxmox environments in read-only mode where available. |
| DOCKER-READONLY-AGENT-001 | PROPOSED | Experimental agents | Permission model | Inspect Docker environments in read-only mode where available. |
| SMART-HOME-DOCTOR-AGENT-001 | PROPOSED | Experimental agents | Permission model | Diagnose smart-home state without modifying devices. |
| HOME-ASSISTANT-YAML-INSPECTOR-AGENT-001 | PROPOSED | Experimental agents | File-readonly template | Inspect Home Assistant YAML for local issues without applying changes. |
| CODE-REPO-INSPECTOR-AGENT-001 | PROPOSED | Experimental agents | File-readonly template | Inspect repositories and produce local findings without committing or publishing changes. |

P2 agents remain read-only by default, local-only or LAN-only for private
credentials, and cannot restart services, mutate infrastructure, or publish
changes automatically.

## v0.13 - Agent Beta Productization

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| IAMINE-AGENT-NETWORK-BETA-001 | PROPOSED | Product / Architecture | Official agents | Assemble the installable agent-network beta experience. |
| OFFICIAL-AGENT-PACK-001 | PROPOSED | Product / official agents | P0 agents | Ship the initial official agent pack. |
| AGENT-CATALOG-PREVIEW-001 | PROPOSED | Catalog | Local registry | Show official, beta, experimental, and disabled catalog states. |
| BETA-ONBOARDING-FLOW-001 | PROPOSED | Product / UX | Catalog preview | Guide users from install to first agent run. |
| BETA-FEEDBACK-LOOP-001 | PROPOSED | Product / operations | Onboarding flow | Collect actionable feedback without leaking private data. |
| AGENT-PERMISSION-DISPLAY-001 | PROPOSED | Permission UX | Permission model | Show permissions before execution. |
| AGENT-RISK-LABELING-001 | PROPOSED | Security / product | Permission display | Label agent risk consistently. |
| AGENT-LOCAL-ONLY-MODE-001 | PROPOSED | Runtime / security | Permission model | Enforce local-only operation for agents that must not reach public networks. |

Close criteria: a user installs IAMINE, joins beta, runs Node Doctor, sees the
agent catalog, executes at least one domestic agent and one technical agent,
reviews permissions before execution, submits feedback, and sees understandable
errors. Open marketplace remains blocked.

## v1.0 - IAMINE Agent Network Public Beta

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| IAMINE-V1-RELEASE-GATE-001 | PROPOSED | Architecture / QA release gate | v0.13 beta productization | Decide whether IAMINE can launch the Agent Network Public Beta. |
| IAMINE-AGENT-NETWORK-PUBLIC-BETA-001 | PROPOSED | Product / operations | Release gate | Launch the public beta as an agent network. |
| OFFICIAL-AGENT-PACK-v1.0 | PROPOSED | Product / official agents | P0 agents, selected P1 agents | Publish the supported official agent pack for v1.0. |
| PUBLIC-BETA-DOCUMENTATION-001 | PROPOSED | Documentation | Public beta release gate | Publish public beta docs, install flow, known limits, and safety guidance. |
| PUBLIC-BETA-WEBSITE-ALIGNMENT-001 | PROPOSED | Product / website | Documentation | Align public web messaging with the agent-network beta scope. |
| PUBLIC-BETA-SUPPORT-FLOW-001 | PROPOSED | Support / diagnostics | User diagnostics | Provide a support flow tied to diagnostics and feedback evidence. |
| PUBLIC-BETA-KNOWN-LIMITATIONS-001 | PROPOSED | Product / QA | Release gate | Publish explicit known limitations before beta launch. |

Minimum v1.0 official agents: Node Doctor, Reporter, LAN File Share Assistant,
Photo Library Organizer, Home Network Assistant, and Windows Optimizer
Assistant.

v1.0 does not include real payments, open marketplace, arbitrary third-party
agents, or mainnet.

## v1.1 - Validation, Reputation, and Trust

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
| AGENT-RESULT-VALIDATION-001 | PROPOSED | Agent validation | Agent audit logs | Validate accepted agent outputs separately from node performance. |
| AGENT-QUALITY-SIGNAL-001 | PROPOSED | Agent reputation | Agent result validation | Track agent quality signals without conflating them with node reputation. |

Performance certification, behavioral reputation, reward eligibility, and agent
quality remain separate domains.

## v1.2 - Public Agent Developer Platform

### v1.2.0 - Developer Platform Foundation

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-SDK-PYTHON-001 | PROPOSED | Developer platform | v1.0 beta evidence | Provide a supported Python SDK for external agent creators. |
| AGENT-SDK-TYPESCRIPT-001 | PROPOSED | Developer platform | v1.0 beta evidence | Provide a supported TypeScript SDK for external agent creators. |
| AGENT-CLI-DEVTOOLS-001 | PROPOSED | Developer platform CLI | SDK baseline | Provide CLI tools for creating, validating, testing, simulating, and packaging agents. |
| AGENT-TEMPLATE-BASELINE-001 | PROPOSED | Developer platform templates | SDK baseline | Publish the base public agent template set. |
| AGENT-TEST-HARNESS-001 | PROPOSED | Developer platform QA | SDK baseline | Provide a public test harness for agent packages. |
| AGENT-PACKAGE-VALIDATOR-CLI-001 | PROPOSED | Developer platform CLI | Manifest schema | Validate manifests, permissions, dependencies, and package structure. |
| AGENT-LOCAL-SIMULATOR-001 | PROPOSED | Developer platform runtime | Runtime baseline | Simulate local agent execution, permissions, fixtures, and timeouts. |
| AGENT-DEVELOPER-DOCS-001 | PROPOSED | Documentation | Developer tools | Publish developer quickstart and package guidance. |

### v1.2.1 - AI-Assisted Developer Experience

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| IAMINE-DEV-SETUP-AGENT-001 | PROPOSED | Developer assistant agents | Developer platform foundation | Install and validate developer prerequisites with explicit confirmation. |
| AGENT-BUILDER-ASSISTANT-AGENT-001 | PROPOSED | Developer assistant agents | Templates, SDKs | Help creators shape an agent idea into template, category, inputs, outputs, permissions, risk, tests, and manifest. |
| AGENT-MANIFEST-WIZARD-AGENT-001 | PROPOSED | Developer assistant agents | Manifest schema | Generate and correct manifests. |
| AGENT-PERMISSION-REVIEW-AGENT-001 | PROPOSED | Developer assistant agents | Permission model | Detect excessive permissions and unsafe access requests. |
| AGENT-TEST-GENERATOR-AGENT-001 | PROPOSED | Developer assistant agents | Test harness | Generate minimum manifest, permission, validation, timeout, sandbox, privacy, and mock-tool tests. |
| AGENT-LOCAL-SIMULATION-AGENT-001 | PROPOSED | Developer assistant agents | Local simulator | Simulate local execution, fixtures, mock permissions, compatible nodes, and timeout or permission failures. |
| AGENT-PACKAGE-REVIEW-AGENT-001 | PROPOSED | Developer assistant agents | Package validator | Review package structure, manifest, checksums, dependencies, license, README, permissions, resources, and sandbox profile. |

### v1.2.2 - Developer Onboarding E2E

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-DEVELOPER-ONBOARDING-FLOW-001 | PROPOSED | Developer platform / UX | Developer assistant agents | Close the complete creator journey from install through dry-run submission. |
| AGENT-CREATOR-QUICKSTART-001 | PROPOSED | Documentation | Developer onboarding | Publish the first creator quickstart. |
| AGENT-SAMPLE-PACK-001 | PROPOSED | Developer samples | Templates | Publish sample agent packages. |
| AGENT-SDK-DOCS-SITE-001 | PROPOSED | Documentation | SDKs | Publish the SDK documentation site. |
| AGENT-SUBMISSION-DRY-RUN-001 | PROPOSED | Registry / developer platform | Package validator | Let creators dry-run submission without automatic publication. |

## v1.3 - Curated Agent Registry

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-PACKAGE-REGISTRY-001 | PROPOSED | Agent registry | Developer platform | Accept reviewed external agent packages without opening marketplace. |
| AGENT-VALIDATION-PIPELINE-001 | PROPOSED | Agent registry / QA | Package registry | Validate manifests, permissions, dependencies, signatures, tests, and malicious package signals. |
| AGENT-SIGNING-REVOCATION-001 | PROPOSED | Agent registry / security | Validation pipeline | Sign accepted agents and revoke unsafe or obsolete packages. |
| AGENT-TRUST-SCORE-001 | PROPOSED | Agent registry / trust | Validation pipeline | Calculate explicit trust signals for curated agents. |
| AGENT-REPUTATION-REVIEWS-001 | PROPOSED | Agent registry / reputation | Trust score | Collect bounded reviews and reputation evidence. |
| AGENT-CREATOR-PROFILE-001 | PROPOSED | Agent registry / identity | Wallet or registry identity | Represent reviewed creator identity and package ownership. |

## v1.4 - Curated Agent Marketplace

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| AGENT-MARKETPLACE-CURATED-001 | PROPOSED | Marketplace | Curated registry | Present reviewed agents through a limited, curated marketplace. |
| AGENT-DISCOVERY-001 | PROPOSED | Marketplace / catalog | Curated marketplace | Support search and discovery for curated agents. |
| AGENT-CATEGORY-TAXONOMY-001 | PROPOSED | Marketplace / product | Agent discovery | Define agent categories consistently. |
| AGENT-MARKETPLACE-PERMISSION-DISPLAY-001 | PROPOSED | Marketplace / permission UX | Permission model | Show permissions in marketplace and install flows. |
| AGENT-COMPATIBILITY-DISPLAY-001 | PROPOSED | Marketplace / compatibility | Capability metadata | Show node and resource compatibility before install or run. |
| AGENT-RATING-REVIEWS-001 | PROPOSED | Marketplace / reputation | Agent reputation reviews | Provide review and rating flows with abuse controls. |
| AGENT-MARKETPLACE-CREATOR-PROFILE-DISPLAY-001 | PROPOSED | Marketplace / identity | Creator profile | Display reviewed creator identity and package history. |

## v1.5 - Economic Agent Testnet

| Feature | State | Primary owner | Dependencies | Goal |
| --- | --- | --- | --- | --- |
| COMPUTE-ACCOUNTING-001 | PROPOSED | Accounting domain | Verified task lifecycle | Account for accepted compute without assigning monetary value. |
| REWARD-POLICY-001 | PROPOSED | Rewards domain | Compute accounting, reputation | Define deterministic testnet reward rules. |
| TESTNET-WALLET-IDENTITY-001 | PROPOSED | Wallet / identity | Public node identity | Associate testnet-only wallet identity with an operator. |
| TESTNET-REWARD-LEDGER-001 | PROPOSED | Testnet ledger | Reward policy, wallet identity | Record auditable testnet reward units. |
| CERTIFIED-NODE-REWARD-BOOST-001 | PROPOSED | Rewards / certification wiring | Benchmark certification, reward policy | Apply explicit certification modifiers without changing benchmark policy. |
| STAKE-AND-SLASHING-BASELINE-001 | PROPOSED | Economic security | Wallet identity, misbehavior evidence | Simulate bounded stake and slashing behavior. |
| REWARD-DISPUTE-001 | PROPOSED | Dispute protocol | Reward ledger, evidence | Resolve disputed accounting and reward decisions. |
| AGENT-USAGE-METERING-001 | PROPOSED | Agent accounting | Curated marketplace | Meter agent usage accurately in testnet units. |
| AGENT-REVENUE-SHARE-001 | PROPOSED | Agent economics | Usage metering | Simulate creator share in testnet units. |
| NODE-COMPUTE-PAYOUT-001 | PROPOSED | Node economics | Compute accounting | Simulate node payout in testnet units. |
| BILLING-DISPUTE-001 | PROPOSED | Disputes | Usage and reward ledgers | Resolve disputed usage and billing evidence. |
| AGENT-COMMISSION-POLICY-001 | PROPOSED | Agent economics | Revenue share | Define IAMINE commission rules for testnet simulation. |
| AGENT-CREATOR-DASHBOARD-001 | PROPOSED | Product / creator UX | Agent revenue share | Show creator testnet usage and credit evidence. |
| NODE-OPERATOR-EARNINGS-DASHBOARD-001 | PROPOSED | Product / operator UX | Node payout | Show node operator testnet earnings evidence. |

This milestone uses testnet units with no real-world value. There is no mainnet
and no irreversible real-money settlement.

## v2.0 - Mainnet

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

### Enterprise and Integrations

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
V0.9-BETA-FRESH-INSTALL-E2E-001
-> TESTNET-LOAD-RESILIENCE-001
-> PRIVATE-TESTNET-RELEASE-001
-> PUBLIC-TESTNET-ADMISSION-001
-> SIGNED-AUTOUPDATE-001
-> USER-DIAGNOSTICS-SUPPORT-001
-> V1-SUPPLY-CHAIN-SECURITY-001
-> NODE-UPGRADE-ROLLBACK-001
-> PUBLIC-TESTNET-DOCUMENTATION-001
-> IAMINE-PREPUBLIC-READINESS-GATE-001
-> AGENT-MARKET-FIT-RESEARCH-001
```

Each arrow still requires the complete canonical feature lifecycle. A roadmap
position is not development authorization.
