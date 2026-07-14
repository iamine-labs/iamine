# IAMINE Agent Network Roadmap

## Authority

This document records the official Agent Network roadmap incorporated by
`ROADMAP-OFFICIAL-AGENT-NETWORK-RECONCILIATION-001`.

Decision:

```text
IAMINE public beta must launch as IAMINE Agent Network Public Beta, not as an
inference-only public beta.
```

Before v1.0, IAMINE must complete private testnet readiness, pre-public
infrastructure, agent research, agent manifests, scope manifests, permissions,
runtime, handoff policy, out-of-scope response policy, sandbox baseline, audit
logs, scope boundary evals, internal developer bootstrap, official agent
registry, P0 official agents, beta onboarding, feedback loop, QA, and release
gates.

## Required Sequence

```text
v0.9.x   -> remote inference, observability, load, private testnet
v0.10.x  -> pre-public infrastructure
v0.11.x  -> agent research, architecture, scope, permissions, runtime baseline
v0.12.x  -> P0 official agents and internal tools for official agent creation
v0.13.x  -> P1/P2 agents and beta productization
v1.0.0   -> IAMINE Agent Network Public Beta
v1.1.x   -> validation, reputation, trust
v1.2.x   -> public agent developer platform
v1.3.x   -> curated agent registry
v1.4.x   -> curated marketplace
v1.5.x   -> economic agent testnet
v2.0.x   -> mainnet, settlement, open marketplace, real economy
```

## Scope-Bound Agent Rule

Every IAMINE agent must be scope-bound.

A specialized agent must only execute tasks inside its declared scope. If a
task is outside its domain, the agent must refuse, ask for clarification, or
hand off to the orchestrator.

Every agent feature must define:

```text
manifest
scope
permissions
blocked_actions
handoff_targets where applicable
resource requirements
local/LAN/remote mode
audit logs
positive tests
negative tests
scope boundary evals
permission boundary tests
unsafe action tests
prompt injection tests
role confusion tests
```

An agent fails review if it tries to solve tasks outside scope, asks for
excessive permissions without justification, executes destructive actions by
default, accepts role confusion, ignores blocked actions, cannot hand off, or
cannot explain its limits.

## v0.10.0 - Pre-Public Infrastructure

```text
PUBLIC-TESTNET-ADMISSION-001
SIGNED-AUTOUPDATE-001
USER-DIAGNOSTICS-SUPPORT-001
V1-SUPPLY-CHAIN-SECURITY-001
NODE-UPGRADE-ROLLBACK-001
PUBLIC-TESTNET-DOCUMENTATION-001
IAMINE-PREPUBLIC-READINESS-GATE-001
```

Closure requires controlled public install, signed update, rollback,
diagnostic export, public documentation baseline, checksum/signature
verification, release artifact validation, and no public beta launch.

## v0.11.0 - Agent Research and Product Fit

```text
AGENT-MARKET-FIT-RESEARCH-001
AGENT-USER-PERSONA-MAPPING-001
AGENT-BETA-PACK-SELECTION-001
```

| Feature | State | Goal |
| --- | --- | --- |
| AGENT-MARKET-FIT-RESEARCH-001 | CLOSED | Define the research baseline, candidate segments, safety criteria, and exclusion rules for early IAMINE agents; merge `9931f1d`, focused post-merge validation PASS. |
| AGENT-USER-PERSONA-MAPPING-001 | CLOSED | Convert research segments into explicit user personas and task contexts; merge `6c84f6c`, focused post-merge validation PASS. |
| AGENT-BETA-PACK-SELECTION-001 | CLOSED | Select the first official beta agent pack from validated personas and constraints; merge `15949da`, focused post-merge validation PASS. |

Research segments include home users, non-technical users, homelabs,
self-hosted users, content creators, small businesses, users with several PCs,
users with network or printer problems, and future agent developers.

Deliverables:

```text
docs/agents/agent-market-fit-research.md
docs/agents/agent-user-personas.md
docs/agents/official-beta-agent-pack-selection.md
```

## v0.11.1 - Agent Architecture Foundation

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-SKELETON-STANDARD-001
AGENT-PACKAGE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-SCOPE-MANIFEST-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-LANGUAGE-POLICY-001
AGENT-DEPENDENCY-POLICY-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
```

| Feature | State | Goal |
| --- | --- | --- |
| AGENT-CREATION-ARCHITECTURE-001 | CLOSED | Define the end-to-end architecture for creating, reviewing, packaging, validating, and later executing IAMINE agents; merge `bc6242b`, focused post-merge validation PASS. |
| AGENT-SKELETON-STANDARD-001 | CLOSED | Define the canonical agent skeleton layout before generating or implementing agent code; merge `be57a4c`, focused post-merge validation PASS. |
| AGENT-PACKAGE-MANIFEST-001 | CLOSED | Define the agent package manifest contract and required references before execution; merge `453b1b6`, focused post-merge validation PASS. |
| AGENT-CAPABILITY-METADATA-001 | CLOSED | Define agent capability metadata without scheduler or reputation side effects. |
| AGENT-EXPERTISE-METADATA-001 | CLOSED | Define expertise metadata for agent selection without claiming distributed model MoE. |
| AGENT-SCOPE-MANIFEST-001 | CLOSED | Define agent scope boundaries, blocked actions, handoff targets, and supported task types; merge `ca37818`, focused post-merge validation PASS. |
| AGENT-RESOURCE-REQUIREMENTS-001 | CLOSED | Define agent resource requirements before runtime placement or scheduling. |
| AGENT-PERMISSION-MODEL-001 | CLOSED | Define explicit permission categories and denial behavior. |
| AGENT-AUDIT-LOG-001 | CLOSED | Define privacy-safe audit evidence for agent review and execution. |
| AGENT-REGISTRY-LOCAL-001 | CLOSED | Define local registry behavior before public marketplace behavior. |
| AGENT-SCOPE-BOUNDARY-EVALS-001 | CLOSED | Define positive and negative boundary evals for scope enforcement. |
| AGENT-LANGUAGE-POLICY-001 | CLOSED | Define allowed implementation languages by layer and release phase. |
| AGENT-DEPENDENCY-POLICY-001 | CLOSED | Define dependency classes that are allowed, optional, deferred, or blocked for agent work. |
| AGENT-RUNTIME-LANGUAGE-MATRIX-001 | CLOSED | Define supported runtime language modes before execution features. |
| AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001 | CLOSED | Define the source of truth for agent manifest schemas and generated validation artifacts. |

`AGENT-SCOPE-MANIFEST-001` must define what an agent does, what it does not
do, required permissions, blocked actions, supported task types, handoff
targets, confirmation boundaries, and when the task must return to the
orchestrator.

`AGENT-SCOPE-BOUNDARY-EVALS-001` must cover in-scope tasks, out-of-scope
tasks, ambiguous tasks, dangerous tasks, cross-domain tasks, permission
escalation attempts, prompt injection attempts, role confusion attempts, and
handoff to orchestrator.

## Language and Dependency Policy

Recommended language placement:

```text
Rust:
- IAMINE core
- node
- runtime
- CLI
- contracts
- validators
- official P0 agents
- audit
- registry
- file/network/system agents

Python:
- public SDK later
- AI/dev tooling later
- prototypes
- OCR/classification future
- heavy model integrations under sandbox

TypeScript:
- public SDK later
- web/API integrations
- dashboard/tooling
- content connectors

WASM/WASI:
- preferred future sandbox for third-party lightweight agents

Containers:
- future heavy agents only after registry, sandbox, and permission model mature
```

Manifest format policy:

```text
Authoring: YAML
Internal representation: Rust structs
Validation: generated JSON Schema
Runtime/API payloads: JSON
Source of truth: Rust types
```

Expected minimal Rust dependency set for `AGENT-CAPABILITY-METADATA-001`:

```text
serde
serde_json
serde_yaml
schemars
jsonschema
thiserror
```

Optional only if needed:

```text
clap
anyhow
tracing
```

Do not introduce yet:

```text
wasmtime
python SDK
typescript SDK
containers
LLM frameworks
OCR frameworks
social APIs
router APIs
Windows automation advanced dependencies
```

## v0.11.2 - Agent Runtime Baseline

```text
AGENT-RUNTIME-BASELINE-001
AGENT-RUNTIME-SANDBOX-001
AGENT-EXECUTION-LIFECYCLE-001
AGENT-INPUT-OUTPUT-CONTRACT-001
AGENT-TIMEOUT-CANCEL-001
AGENT-HANDOFF-POLICY-001
AGENT-OUT-OF-SCOPE-RESPONSE-001
AGENT-ROUTING-CANDIDATE-SELECTION-001
```

Minimum execution states:

```text
queued
permission_pending
scope_check
handoff_required
running
completed
failed
cancelled
timeout
blocked
```

`AGENT-ROUTING-CANDIDATE-SELECTION-001` must select candidate agents from
declared task type, scope, permissions, resources, risk, execution mode, and
node compatibility. It must not implement distributed model MoE.

## v0.11.3 - Internal Agent Developer Bootstrap

```text
AGENT-SKELETON-GENERATOR-001
AGENT-TEMPLATE-VALIDATION-001
AGENT-FRAMEWORK-BASELINE-001
AGENT-TEMPLATE-DIAGNOSTIC-001
AGENT-TEMPLATE-FILE-READONLY-001
AGENT-TEMPLATE-NETWORK-DIAGNOSTIC-001
AGENT-TEMPLATE-REPORTER-001
AGENT-TEMPLATE-TEXT-ASSISTANT-001
AGENT-TEMPLATE-OS-DIAGNOSTIC-001
IAMINE-DEV-SETUP-AGENT-001-INTERNAL
AGENT-BUILDER-ASSISTANT-AGENT-001-INTERNAL
AGENT-MANIFEST-WIZARD-AGENT-001-INTERNAL
AGENT-PERMISSION-REVIEW-AGENT-001-INTERNAL
AGENT-SCOPE-REVIEW-AGENT-001-INTERNAL
AGENT-BOUNDARY-TEST-GENERATOR-AGENT-001-INTERNAL
```

Restrictions:

```text
no public third-party publishing yet
no auto-publication
no destructive permissions by default
no generic do_anything scope
no arbitrary shell
no unrestricted filesystem
no unrestricted network
no bypassing manual validation
```

## v0.12.0 - P0 Official Agents

First, IAMINE must define official P0 agent skeletons:

```text
NODE-DOCTOR-AGENT-001-SKELETON
REPORTER-AGENT-001-SKELETON
LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON
PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON
HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON
```

Then IAMINE may implement functional P0 official agents:

```text
NODE-DOCTOR-AGENT-001
REPORTER-AGENT-001
LAN-FILE-SHARE-ASSISTANT-AGENT-001
PHOTO-LIBRARY-ORGANIZER-AGENT-001
HOME-NETWORK-ASSISTANT-AGENT-001
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001
```

Not all functional P0 agents should be implemented in parallel at the start.
`NODE-DOCTOR-AGENT-001` is the recommended complete reference vertical. After
that, `REPORTER-AGENT-001` should be the next functional agent, followed by
P0 skeletons and implementation waves.

Each P0 agent must pass positive capability tests, negative capability tests,
scope boundary tests, permission boundary tests, handoff tests, unsafe action
tests, prompt injection tests, role confusion tests, privacy redaction tests,
and local-only tests where applicable.

## v0.12.1 - P1 Adoption Agents

```text
PRINTER-DOCTOR-AGENT-001
DOCUMENT-ORGANIZER-AGENT-001
CONTENT-POST-DRAFT-AGENT-001
CONTENT-CALENDAR-AGENT-001
RECIPE-TEXT-AGENT-001
HOMELAB-DOCTOR-AGENT-001
```

## v0.12.2 - P2 Experimental and Technical Agents

```text
PROXMOX-READONLY-AGENT-001
DOCKER-READONLY-AGENT-001
SMART-HOME-DOCTOR-AGENT-001
HOME-ASSISTANT-YAML-INSPECTOR-AGENT-001
CODE-REPO-INSPECTOR-AGENT-001
```

Rules:

```text
read-only by default
local-only or LAN-only for private credentials
no destructive actions
no service restarts
no infrastructure mutation
no automatic publication
```

## v0.13.0 - Agent Beta Productization

```text
IAMINE-AGENT-NETWORK-BETA-001
OFFICIAL-AGENT-PACK-001
AGENT-CATALOG-PREVIEW-001
BETA-ONBOARDING-FLOW-001
BETA-FEEDBACK-LOOP-001
AGENT-PERMISSION-DISPLAY-001
AGENT-SCOPE-DISPLAY-001
AGENT-RISK-LABELING-001
AGENT-LOCAL-ONLY-MODE-001
AGENT-EXPERT-ROUTING-001
```

`AGENT-EXPERT-ROUTING-001` is the practical MoE concept for IAMINE v1:
the orchestrator selects the specialized agent by task type, scope,
permissions, resources, risk, execution mode, and node compatibility.

This is not distributed model MoE or model-expert sharding.

Catalog states:

```text
Official
Beta
Experimental
Disabled
```

## v1.0.0 - IAMINE Agent Network Public Beta

```text
IAMINE-V1-RELEASE-GATE-001
IAMINE-AGENT-NETWORK-PUBLIC-BETA-001
OFFICIAL-AGENT-PACK-v1.0
PUBLIC-BETA-DOCUMENTATION-001
PUBLIC-BETA-WEBSITE-ALIGNMENT-001
PUBLIC-BETA-SUPPORT-FLOW-001
PUBLIC-BETA-KNOWN-LIMITATIONS-001
```

Minimum v1.0 official agents:

```text
NODE-DOCTOR-AGENT-001
REPORTER-AGENT-001
LAN-FILE-SHARE-ASSISTANT-AGENT-001
PHOTO-LIBRARY-ORGANIZER-AGENT-001
HOME-NETWORK-ASSISTANT-AGENT-001
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001
```

Ideal v1.0 official pack:

```text
NODE-DOCTOR-AGENT-001
REPORTER-AGENT-001
LAN-FILE-SHARE-ASSISTANT-AGENT-001
PHOTO-LIBRARY-ORGANIZER-AGENT-001
HOME-NETWORK-ASSISTANT-AGENT-001
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001
PRINTER-DOCTOR-AGENT-001
DOCUMENT-ORGANIZER-AGENT-001
CONTENT-POST-DRAFT-AGENT-001
CONTENT-CALENDAR-AGENT-001
RECIPE-TEXT-AGENT-001
HOMELAB-DOCTOR-AGENT-001
```

v1.0 must not include real payments, mainnet, an open marketplace, or arbitrary
third-party agents.

## v1.1.x - Validation, Reputation, and Trust

```text
PROOF-OF-INFERENCE-BASELINE-001
RESULT-VALIDATOR-QUORUM-001
CHALLENGE-PROTOCOL-001
MISBEHAVIOR-EVIDENCE-001
NODE-REPUTATION-001
SYBIL-RESISTANCE-001
SCHEDULER-REPUTATION-WIRING-001
NODE-BENCHMARK-CERTIFICATION-001
NODE-PERFORMANCE-ATTESTATION-001
AGENT-RESULT-VALIDATION-001
AGENT-QUALITY-SIGNAL-001
AGENT-SCOPE-ADHERENCE-SCORE-001
AGENT-ROUTING-QUALITY-SCORE-001
AGENT-ROUTING-FEEDBACK-LOOP-001
```

Minimum trust metrics:

```text
unsafe_action_block_rate = 100%
permission_violation_rate = 0%
scope_adherence_rate tracked
wrong_handoff_rate tracked
```

Rule:

```text
Performance certification != behavioral reputation != reward eligibility.
```

## v1.2.x - Public Agent Developer Platform

Public developer tools must not allow automatic publication or bypass manual
validation.

## v1.2.0 - Developer Platform Foundation

```text
AGENT-SDK-PYTHON-001
AGENT-SDK-TYPESCRIPT-001
AGENT-CLI-DEVTOOLS-001
AGENT-TEMPLATE-BASELINE-001
AGENT-TEST-HARNESS-001
AGENT-PACKAGE-VALIDATOR-CLI-001
AGENT-LOCAL-SIMULATOR-001
AGENT-DEVELOPER-DOCS-001
AGENT-FRAMEWORK-BASELINE-001
AGENT-SCOPE-TEST-HARNESS-001
AGENT-EXPERTISE-TEMPLATE-001
```

Initial public templates:

```text
AGENT-TEMPLATE-DIAGNOSTIC-001
AGENT-TEMPLATE-FILE-READONLY-001
AGENT-TEMPLATE-NETWORK-DIAGNOSTIC-001
AGENT-TEMPLATE-REPORTER-001
AGENT-TEMPLATE-TEXT-ASSISTANT-001
AGENT-TEMPLATE-DOCUMENT-LOCAL-001
AGENT-TEMPLATE-CONTENT-DRAFT-001
AGENT-TEMPLATE-OS-DIAGNOSTIC-001
AGENT-TEMPLATE-CONNECTOR-READONLY-001
```

## v1.2.1 - AI-Assisted Developer Experience

```text
IAMINE-DEV-SETUP-AGENT-001
AGENT-BUILDER-ASSISTANT-AGENT-001
AGENT-MANIFEST-WIZARD-AGENT-001
AGENT-PERMISSION-REVIEW-AGENT-001
AGENT-SCOPE-REVIEW-AGENT-001
AGENT-TEST-GENERATOR-AGENT-001
AGENT-BOUNDARY-TEST-GENERATOR-AGENT-001
AGENT-LOCAL-SIMULATION-AGENT-001
AGENT-PACKAGE-REVIEW-AGENT-001
```

Restrictions:

```text
no auto-publication
no validation bypass
no destructive permissions by default
no generic scope
no unrestricted shell
no unrestricted filesystem
no unrestricted network
no agents that handle secrets without manual review
```

## v1.2.2 - Developer Onboarding E2E

```text
AGENT-DEVELOPER-ONBOARDING-FLOW-001
AGENT-CREATOR-QUICKSTART-001
AGENT-SAMPLE-PACK-001
AGENT-SDK-DOCS-SITE-001
AGENT-SUBMISSION-DRY-RUN-001
```

Closeout requires a new creator to install tools, create an agent from a
template, run positive and negative tests, simulate execution, validate
manifest, validate permissions, validate scope, package, and execute a dry-run
submission without automatic publication.

## v1.3.x - Curated Agent Registry

```text
AGENT-PACKAGE-REGISTRY-001
AGENT-VALIDATION-PIPELINE-001
AGENT-SIGNING-REVOCATION-001
AGENT-TRUST-SCORE-001
AGENT-REPUTATION-REVIEWS-001
AGENT-CREATOR-PROFILE-001
AGENT-SCOPE-CERTIFICATION-001
```

## v1.4.x - Curated Agent Marketplace

```text
AGENT-MARKETPLACE-CURATED-001
AGENT-DISCOVERY-001
AGENT-CATEGORY-TAXONOMY-001
AGENT-PERMISSION-DISPLAY-001
AGENT-SCOPE-DISPLAY-001
AGENT-COMPATIBILITY-DISPLAY-001
AGENT-RATING-REVIEWS-001
AGENT-CREATOR-PROFILE-001
```

## v1.5.x - Economic Agent Testnet

```text
COMPUTE-ACCOUNTING-001
REWARD-POLICY-001
TESTNET-WALLET-IDENTITY-001
TESTNET-REWARD-LEDGER-001
CERTIFIED-NODE-REWARD-BOOST-001
STAKE-AND-SLASHING-BASELINE-001
REWARD-DISPUTE-001
AGENT-USAGE-METERING-001
AGENT-REVENUE-SHARE-001
NODE-COMPUTE-PAYOUT-001
BILLING-DISPUTE-001
AGENT-COMMISSION-POLICY-001
AGENT-CREATOR-DASHBOARD-001
NODE-OPERATOR-EARNINGS-DASHBOARD-001
AGENT-SCOPE-VIOLATION-PENALTY-001
```

If an agent violates scope, it does not receive payout. If it violates
permissions, it is blocked or revoked. If it causes harm or evasion, it enters
dispute/review.

## Deferred Advanced Automation

```text
HOME-NETWORK-CONFIG-AGENT-001
SMB-SHARE-CONFIG-ASSISTANT-AGENT-001
PRINTER-SETUP-AUTOMATION-AGENT-001
ASSISTED-PUBLISHING-AGENT-001
SOCIAL-PUBLISHING-AGENT-001
TELEGRAM-PUBLISHING-AGENT-001
INSTAGRAM-PUBLISHING-AGENT-001
FACEBOOK-PUBLISHING-AGENT-001
PERSONAL-FINANCE-COACH-AGENT-001
DAILY-LIFE-COACH-AGENT-001
MACOS-OPTIMIZER-ASSISTANT-AGENT-001
LINUX-OPTIMIZER-ASSISTANT-AGENT-001
```

## Advanced Compute and Distributed MoE

```text
MIXTURE-OF-EXPERTS-ROUTING-001
DISTRIBUTED-MOE-INFERENCE-001
MODEL-EXPERT-SHARDING-001
EXPERT-ROUTER-NODE-PLACEMENT-001
MODEL-SHARD-STORAGE-001
MODEL-SHARD-INTEGRITY-001
DISTRIBUTED-INFERENCE-PLAN-001
DISTRIBUTED-TENSOR-TRANSPORT-001
DISTRIBUTED-INFERENCE-ASSEMBLY-001
DISTRIBUTED-TRAINING-BASELINE-001
CHECKPOINT-DISTRIBUTION-001
```

MoE in v1 means Agent Expert Routing. Distributed model MoE, model-expert
sharding, tensor transport, distributed training, and checkpoint distribution
are deferred to v2.x / Advanced Compute.

## Mainnet and Solana

```text
SOLANA-NODE-REGISTRY-001
SOLANA-REPUTATION-ANCHOR-001
SOLANA-REWARD-SETTLEMENT-001
SOLANA-STAKING-SLASHING-001
IAMINE-TOKEN-CONTRACT-001
MAINNET-SECURITY-AUDIT-001
MAINNET-READINESS-GATE-001
MAINNET-GENESIS-001
```

Mainnet, settlement, an open marketplace, real payments, and arbitrary
third-party agents remain blocked until the appropriate trust, registry,
validation, reputation, and economic-testnet layers exist.
