# IAMINE Agent Network Roadmap

## Authority

This document records the official Agent Network roadmap incorporated by
`ROADMAP-OFFICIAL-AGENT-NETWORK-RECONCILIATION-001` and corrected to the
current repository state by `IAMINE-CANONICAL-ROADMAP-RECONCILIATION-001`.

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

Current operational baseline:

```text
validated develop merge: c8a0ecc3a9bdee09c59130232c74ab7724b352b5
tree: 7fab6e20fc798c8cf9c7b5af74b1e25fe39141e3
v0.11.2 executable rows: 17 of 19 CLOSED
last closed: AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001
next sequential feature: AGENT-PACKAGE-LOADER-001
runtime regression baseline: 128/128
agents regression baseline: 109/109
```

The earlier 62-test runtime result remains historical evidence for its exact
snapshot. It is not the current regression baseline.

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

## Canonical Milestone Closure Gate Registry

Every milestone that was not historically closed before this registry must
have a named QA gate before it can transition to `CLOSED`. Gate registration
does not authorize product work or change the required sequence above.

The gate is executed after the final in-scope feature merges. Architecture may
close the milestone only after exhaustive evidence for the exact milestone
HEAD and tree is merged and post-merge validated.

| Milestone | Closure gate | Gate state |
| --- | --- | --- |
| v0.10.0 | IAMINE-PREPUBLIC-READINESS-GATE-001 | CLOSED / historical release gate |
| v0.11.0 | Historical closure predating this registry | CLOSED / not reopened |
| v0.11.1 | V0.11.1-AGENT-ARCHITECTURE-FOUNDATION-MILESTONE-QA-001 | CLOSED / merge `0bdff4b` / post-merge PASS |
| v0.11.2 | V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001 | PROPOSED / blocked on milestone features and executable evidence |
| v0.11.3 | V0.11.3-AGENT-CREATION-ASSISTANTS-MILESTONE-QA-001 | CLOSED / documentation-only scope |
| v0.12.0 | V0.12.0-P0-OFFICIAL-AGENTS-MILESTONE-QA-001 | PROPOSED / blocked on functional P0 agents |
| v0.12.1 | V0.12.1-P1-ADOPTION-AGENTS-MILESTONE-QA-001 | PROPOSED |
| v0.12.2 | V0.12.2-P2-EXPERIMENTAL-AGENTS-MILESTONE-QA-001 | PROPOSED |
| v0.13.0 | V0.13.0-AGENT-BETA-PRODUCTIZATION-MILESTONE-QA-001 | PROPOSED |
| v1.0.0 | IAMINE-V1-RELEASE-GATE-001 | PROPOSED / must satisfy exhaustive milestone policy |
| v1.1.x | V1.1-VALIDATION-REPUTATION-TRUST-MILESTONE-QA-001 | PROPOSED |
| v1.2.0 | V1.2.0-DEVELOPER-PLATFORM-FOUNDATION-MILESTONE-QA-001 | PROPOSED |
| v1.2.1 | V1.2.1-AI-DEVELOPER-EXPERIENCE-MILESTONE-QA-001 | PROPOSED |
| v1.2.2 | V1.2.2-DEVELOPER-ONBOARDING-E2E-MILESTONE-QA-001 | PROPOSED |
| v1.3.x | V1.3-CURATED-AGENT-REGISTRY-MILESTONE-QA-001 | PROPOSED |
| v1.4.x | V1.4-CURATED-AGENT-MARKETPLACE-MILESTONE-QA-001 | PROPOSED |
| v1.5.x | V1.5-ECONOMIC-AGENT-TESTNET-MILESTONE-QA-001 | PROPOSED |
| v2.0.x | V2.0-ADVANCED-COMPUTE-MAINNET-MILESTONE-QA-001 | PROPOSED; MAINNET-READINESS-GATE-001 remains a required security prerequisite |

The v1.2.x product line remains open until the v1.2.0, v1.2.1, and v1.2.2
gates are each closed. A sub-milestone gate cannot substitute for another.

Future milestone QA documents must consume:

```text
docs/process/iamine-canonical-workflow.md#phase-15a---milestone-qa-gate
docs/qa/agent-milestone-qa-gates.md
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
AGENT-MANIFEST-PARSER-VALIDATOR-001
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
| AGENT-MANIFEST-PARSER-VALIDATOR-001 | CLOSED | Canonical root manifest types, generated JSON Schema, bounded YAML parsing, semantic validation, fixtures, and negative tests are implemented in `iamine-agents` without package loading or execution; merge `c849d98`, focused post-merge validation PASS with accepted baseline/environment exception in real Metal inference tests. |
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

Closure gate:

```text
V0.11.1-AGENT-ARCHITECTURE-FOUNDATION-MILESTONE-QA-001
state: CLOSED
QA evidence: 412e093a9beabb9861d40fc30febfe7a1755e68e
develop merge: 0bdff4b46adad82b094c8106669194425e4a24ab
tree: e7109bca0a4ec4f35968660f3a253725b57dbec2
```

All feature rows and the exhaustive QA gate are closed. Architecture closed
v0.11.1 only after the QA evidence merged and post-merge validation passed on
the exact remote commit and tree. v0.11.2 remains `PROPOSED`; closing this
milestone does not independently authorize a runtime feature.

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

Root manifest parser ownership:

```text
Crate: iamine-agents
Input: agent.yaml content in memory
Filesystem access: none
Referenced metadata loading: none
Agent execution: none
```

Expected minimal Rust dependency set for `AGENT-CAPABILITY-METADATA-001`:

```text
serde
serde_json
serde_yaml
schemars
jsonschema
thiserror
semver
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
AGENT-PACKAGE-LOAD-GATE-001
AGENT-EXECUTION-LIFECYCLE-001
AGENT-INPUT-OUTPUT-CONTRACT-001
AGENT-SCOPE-ENFORCEMENT-001
AGENT-PERMISSION-ENFORCEMENT-001
AGENT-AUDIT-EVENTS-001
AGENT-RUNTIME-SANDBOX-001
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

| Feature | State | Goal |
| --- | --- | --- |
| AGENT-RUNTIME-BASELINE-001 | CLOSED | Define the minimum runtime state vocabulary and prerequisite gates before execution. |
| AGENT-PACKAGE-LOAD-GATE-001 | CLOSED | Consume canonical root parsing and emit a typed blocked package-load assessment while scope, permission, audit, policy, compatibility, and enforcement prerequisites remain unavailable; merge `d56cbce`, no filesystem or runtime loading. |
| AGENT-RUNTIME-SANDBOX-001 | CLOSED | Define sandbox requirements before agent code can run. |
| AGENT-EXECUTION-LIFECYCLE-001 | CLOSED | Define runtime transition rules without worker side effects. |
| AGENT-INPUT-OUTPUT-CONTRACT-001 | CLOSED | Define privacy-safe input and output boundaries. |
| AGENT-SCOPE-ENFORCEMENT-001 | CLOSED | Implemented a typed fail-closed scope decision engine without package/runtime integration or execution authorization; merge `48cb6b2`, exact-tree field QA PASS on Mac, TS140, and four Proxmox guests, post-merge PASS WITH ACCEPTED BASELINE / ENVIRONMENT EXCEPTIONS. |
| AGENT-PERMISSION-ENFORCEMENT-001 | CLOSED | Implemented a typed deny-by-default permission gate after Scope without package/runtime integration or execution authorization; implementation `11a1dfb`, merge `2a84543`, exact-tree field QA PASS on Mac, TS140, and four Proxmox guests, unrestricted post-merge quality gate PASS WITH WARNINGS. |
| AGENT-AUDIT-EVENTS-001 | CLOSED | Implemented bounded, redacted in-memory lifecycle, scope, permission, refusal, and handoff event projections without persistence or runtime authorization; implementation `df80dad`, merge `5a505d8`, exact-commit field QA PASS on Mac, TS140, and four Proxmox guests, post-merge quality gate PASS WITH WARNINGS. |
| AGENT-TIMEOUT-CANCEL-001 | CLOSED | Define timeout, cancellation, and cleanup expectations. |
| AGENT-HANDOFF-POLICY-001 | CLOSED | Define handoff behavior to orchestrator or human operator. |
| AGENT-OUT-OF-SCOPE-RESPONSE-001 | CLOSED | Define safe refusal, clarification, and out-of-scope responses. |
| AGENT-ROUTING-CANDIDATE-SELECTION-001 | CLOSED | Define candidate selection inputs without distributed model MoE. |

### Executable Completion Registry

`V0.11.2-EXECUTABLE-RUNTIME-PREREQUISITE-RECONCILIATION-001` records the
implementation work still required by the closed contracts and the 19 static
package-load blockers. It closed in merge `2380baa`; it does not reopen those
contracts or authorize any row.

Required implementation order:

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
```

| Feature | State | Owner boundary |
| --- | --- | --- |
| AGENT-POLICY-METADATA-VALIDATORS-001 | CLOSED | Scope, Permission, and Audit child-policy validators in separate `iamine-agents` modules; merged in `238dfe2`. |
| AGENT-DESCRIPTIVE-METADATA-VALIDATORS-001 | CLOSED | Capability, Expertise, and Resource child-metadata validators in separate `iamine-agents` modules; merged in `b2ae7f2`. |
| AGENT-BOUNDARY-EVAL-VALIDATOR-001 | CLOSED | Typed fail-closed boundary eval declarations in dedicated `iamine-agents` modules; merged in `329d1da`. |
| AGENT-RUNTIME-CORE-001 | CLOSED | Dedicated fail-closed `iamine-agent-runtime` foundation and typed owner boundaries; merged in `5bcbcf4`. |
| AGENT-PACKAGE-REFERENCE-RESOLVER-001 | CLOSED | Bounded capability-relative package I/O with traversal, symlink, hardlink, size, race, and privacy controls; merged in `c013f10`. |
| AGENT-PACKAGE-REVIEW-EVIDENCE-001 | CLOSED | Typed authority-bound local-registry, language, dependency, and human-review evidence; merged in `ad1d281`. |
| AGENT-RUNTIME-COMPATIBILITY-GATE-001 | CLOSED | Typed authority-bound runtime-language and resource compatibility evidence; merged in `40a9a80`, exact-tree field QA PASS on Mac, TS140, and four Proxmox guests. |
| AGENT-INPUT-OUTPUT-ENFORCEMENT-001 | CLOSED | Typed authority-bound, per-record operator-attested bounded input/output enforcement; merged in `1ec2938`, local and post-merge QA PASS; field QA not required for this in-memory boundary. |
| AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001 | CLOSED | Typed platform-bound sandbox restriction and cleanup plan; merged in `54e4721`, exact-tree field QA PASS on Mac, TS140, and four Proxmox guests. No active OS sandbox is claimed. |
| AGENT-EXECUTION-LIFECYCLE-ENGINE-001 | CLOSED | Authority-bound canonical-state transition engine; merged in `827ceb7`, exact-tree field QA PASS across six platform roles. It does not authorize or execute agents. |
| AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001 | CLOSED | Authority-bound monotonic timers, one-shot cancellation handles, canonical terminal transitions, and sandbox-owned cleanup-pending evidence; merged in `2dbb760`, exact-tree field QA and post-merge validation passed. |
| AGENT-HANDOFF-ENFORCEMENT-001 | CLOSED | Typed authority-bound handoff controls and local dispatch evidence without permission expansion, target selection, transport, or implicit execution; merged in `9e42136`, exact-tree six-role field QA and post-merge validation passed. |
| AGENT-OUT-OF-SCOPE-RESPONSE-ENFORCEMENT-001 | CLOSED | Authority-bound deterministic refusal, clarification, blocked, and handoff response evidence; merged in `0b9bdf0`, exact-tree six-role field QA and post-merge validation passed. |
| AGENT-ROUTING-CANDIDATE-SELECTOR-001 | CLOSED | Authority-bound bounded candidate-selection evidence without scoring, execution, scheduler mutation, transport, model selection, or distributed model MoE; merged in `1efa9cf`, exact-tree field QA passed on six platform roles, and unrestricted post-merge quality gate passed. |
| AGENT-AUDIT-EVENT-ENFORCEMENT-001 | CLOSED | Bounded audit-owner evidence wraps typed Scope/Permission projections and authority-bound lifecycle state without authorization semantics; merged in `b9fe62d`, exact-tree six-role field QA and post-merge validation passed. |
| AGENT-EXECUTION-AUTHORIZATION-001 | CLOSED | Final authority-bound typed decision recomputes package-bound Scope/Permission and verifies the exact review, compatibility, I/O, sandbox, lifecycle, timeout/cancel, routing, and audit chain; merged in `22adc69`, exact-tree six-role field QA and unrestricted post-merge validation passed with no package load or runtime side effects. |
| AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001 | CLOSED | Typed authority-bound package-load eligibility evidence consumes the exact execution authorization and canonically validated reviewed references; merged in `c8a0ecc`, six-role field QA and post-merge validation passed without loading or execution. |
| AGENT-PACKAGE-LOADER-001 | PROPOSED | Load an eligible package through the bounded resolver; no execution. |
| AGENT-RUNTIME-EXECUTOR-001 | PROPOSED | Execute only authorized loaded packages through every runtime owner. |

Closure gate:

```text
V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001
state: PROPOSED / blocked
```

The historical documentation-only v0.11.2 QA snapshot does not close the
current expanded milestone. Scope enforcement, permission enforcement, and
audit event boundaries now have executable validation evidence. Every row in
the executable completion registry must close independently before Architecture
may authorize the exhaustive milestone gate.

`AGENT-EXECUTION-AUTHORIZATION-001` closed in merge `22adc69`.
`AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001` closed in merge `c8a0ecc` after
local validation, exact-tree six-role field QA, final Architecture review, and
post-merge validation. `AGENT-PACKAGE-LOADER-001` is the next sequential
feature but remains `PROPOSED`; this closure does not authorize it or the
runtime executor.

## v0.11.3 - Internal Agent Developer Bootstrap

Closure gate:

```text
V0.11.3-AGENT-CREATION-ASSISTANTS-MILESTONE-QA-001
state: CLOSED
evidence: docs/qa/v0.11.3-agent-creation-assistants-milestone.md
```

This closure covers documentation-only internal assistant contracts. It does
not validate functional agents or agent runtime execution.

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

The v0.11.3 milestone remains closed and must not be reopened. Identifiers in
this section are internal documentation contracts. A later public maturity
stage may consume them as dependencies, but it must not reuse a closed feature
ID as if it represented new implementation work.

| Feature | State | Goal |
| --- | --- | --- |
| AGENT-SKELETON-GENERATOR-001 | CLOSED | Define the bounded generated skeleton shape without generating files. |
| AGENT-TEMPLATE-VALIDATION-001 | CLOSED | Define validation rules for generated templates. |
| AGENT-FRAMEWORK-BASELINE-001 | CLOSED | Define the internal framework baseline for official agent templates. |
| AGENT-TEMPLATE-DIAGNOSTIC-001 | CLOSED | Define diagnostic template boundaries. |
| AGENT-TEMPLATE-FILE-READONLY-001 | CLOSED | Define read-only file template boundaries. |
| AGENT-TEMPLATE-NETWORK-DIAGNOSTIC-001 | CLOSED | Define network diagnostic template boundaries. |
| AGENT-TEMPLATE-REPORTER-001 | CLOSED | Define reporter template boundaries. |
| AGENT-TEMPLATE-TEXT-ASSISTANT-001 | CLOSED | Define text assistant template boundaries. |
| AGENT-TEMPLATE-OS-DIAGNOSTIC-001 | CLOSED | Define OS diagnostic template boundaries. |
| IAMINE-DEV-SETUP-AGENT-001-INTERNAL | CLOSED | Define the internal development setup assistant. |
| AGENT-BUILDER-ASSISTANT-AGENT-001-INTERNAL | CLOSED | Define the internal agent builder assistant. |
| AGENT-MANIFEST-WIZARD-AGENT-001-INTERNAL | CLOSED | Define the internal manifest wizard assistant. |
| AGENT-PERMISSION-REVIEW-AGENT-001-INTERNAL | CLOSED | Define the internal permission review assistant. |
| AGENT-SCOPE-REVIEW-AGENT-001-INTERNAL | CLOSED | Define the internal scope review assistant. |
| AGENT-BOUNDARY-TEST-GENERATOR-AGENT-001-INTERNAL | CLOSED | Define the internal boundary-test generator assistant. |

## v0.12.0 - P0 Official Agents

| Feature | State | Goal |
| --- | --- | --- |
| NODE-DOCTOR-AGENT-001-SKELETON | CLOSED | Defined the official P0 Node Doctor skeleton as a local-readonly, privacy-safe, scope-bound, non-executable planning contract that is not user available; merge `9b058bc`, post-merge quality gate PASS WITH WARNINGS. |
| REPORTER-AGENT-001-SKELETON | CLOSED | Defined the official P0 Privacy-Safe Support Reporter skeleton as a local-readonly, evidence-limited, privacy-safe planning contract without collection, export, or execution; merge `ca163d6`, focused post-merge validation PASS. |
| LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON | CLOSED | Defined the official P0 LAN File Share Assistant skeleton as a local-planning, privacy-safe contract without discovery, credentials, file access, or execution; merge `1fd6709`, focused post-merge validation PASS. |
| PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON | CLOSED | Defined the official P0 Photo Library Organizer skeleton as a local-planning, privacy-safe contract without library access, media analysis, or filesystem execution; merge `e379ced`, focused post-merge validation PASS. |
| HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON | CLOSED | Defined the official P0 Home Network Assistant skeleton as a local-planning, privacy-safe contract without discovery, router access, or network execution; merge `aa3ec2a`, focused post-merge validation PASS. |
| WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON | CLOSED | Defined the official P0 Windows Optimizer Assistant skeleton as a local-planning, privacy-safe contract without system inspection, configuration change, or execution; merge `f81072a`, focused post-merge validation PASS. |
| NODE-DOCTOR-AGENT-001-DEPENDENCY-RECONCILIATION-001 | CLOSED | Recorded the executable prerequisite chain and kept functional Node Doctor development blocked until every gate has implementation and validation evidence; merge `7588e09`, focused post-merge validation PASS. |
| NODE-DOCTOR-EVIDENCE-PROVIDER-001 | PROPOSED | Expose bounded, structured, redacted, read-only node evidence through a stable non-agent interface. |
| NODE-DOCTOR-AGENT-001 | PROPOSED | Implement the first functional P0 reference agent only after the full executable prerequisite chain is implemented and validated. |

Closure gate:

```text
V0.12.0-P0-OFFICIAL-AGENTS-MILESTONE-QA-001
state: PROPOSED / blocked
```

Closed skeletons do not satisfy this gate. It requires exhaustive per-agent,
cross-agent, runtime, privacy, safety, regression, and field evidence for the
functional P0 official agents promised by the milestone.

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

The closed skeletons have the following product status:

```text
skeleton
non_executable
not_user_available
```

Closing an architecture contract does not prove that its runtime or
enforcement behavior exists. `NODE-DOCTOR-AGENT-001` remains in `PROPOSED`
and development authorization is blocked until all of the following have
executable implementation and validation evidence:

```text
AGENT-MANIFEST-PARSER-VALIDATOR-001
AGENT-PACKAGE-LOAD-GATE-001
AGENT-RUNTIME-BASELINE-001
AGENT-EXECUTION-LIFECYCLE-001
AGENT-SCOPE-ENFORCEMENT-001
AGENT-PERMISSION-ENFORCEMENT-001
AGENT-AUDIT-EVENTS-001
AGENT-RUNTIME-SANDBOX-001
AGENT-OUT-OF-SCOPE-RESPONSE-001
AGENT-HANDOFF-POLICY-001
NODE-DOCTOR-EVIDENCE-PROVIDER-001
```

`NODE-DOCTOR-EVIDENCE-PROVIDER-001` is not an agent. It may expose structured,
redacted, read-only node status, hardware profile, configuration status, model
readiness, peer/network status, and remote-inference readiness. It must not
execute commands, invoke a shell, dump raw logs, expose private identifiers,
modify node state, or produce user-facing agent behavior.

The existing `iamine-node lan doctor` command is not an agent adapter. The
Node Doctor skeleton must not invoke or wrap it. A later evidence provider may
reuse only owner-module data behind a dedicated typed and redacted interface.

`AGENT-MANIFEST-PARSER-VALIDATOR-001` closed in merge `c849d98`. It reconciles
the root format as YAML with Rust types as source of truth, generated JSON
Schema, bounded parsing, fixtures, and semantic validation. It does not load or
execute packages.

The package-load assessment closed in merge `d56cbce`:

```text
AGENT-PACKAGE-LOAD-GATE-001
```

It consumes the root parser and emits only a typed blocked report while
referenced metadata validators or enforcement gates remain unavailable. It
does not authorize package loading or agent execution.

`AGENT-EXECUTION-AUTHORIZATION-001` closed in merge `22adc69`. The next
executable feature registered by the v0.11.2 reconciliation is:

```text
AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001
```

Seventeen of the 19 implementation rows are `CLOSED`.
`AGENT-HANDOFF-ENFORCEMENT-001` closed in merge `9e42136` after implementation,
local validation, Architecture checkpoints, exact-tree field QA on six
required platform roles, and post-merge validation.
`AGENT-OUT-OF-SCOPE-RESPONSE-ENFORCEMENT-001` closed in merge `0b9bdf0`
after local validation, final Architecture review, exact-tree field QA on six
platform roles, and post-merge validation.
`AGENT-ROUTING-CANDIDATE-SELECTOR-001` closed in merge `1efa9cf` after local
implementation, 10 focused tests, the 93-test runtime regression, strict
crate clippy, scope review, size review, exact-tree field QA on six platform
roles, final Architecture review, and unrestricted post-merge validation
passed. `AGENT-AUDIT-EVENT-ENFORCEMENT-001` closed in merge `b9fe62d` after
10 focused tests, the 103-test runtime regression, the 109-test agents
regression, strict crate clippy, the full quality gate, privacy review, size
review, exact-tree field QA with 60/60 focused and 24/24 library tests across
six platform roles, final Architecture review, and unrestricted post-merge
validation passed. `AGENT-EXECUTION-AUTHORIZATION-001` closed in merge
`22adc69` after 14 focused tests, the 117-test runtime regression, the
109-test agents regression, strict crate clippy, the full quality gate,
privacy and size review, exact-tree field QA with 84/84 focused and 24/24
library tests across six platform roles, final Architecture review, and
unrestricted post-merge validation passed.
`AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001` closed in merge `c8a0ecc` after
11 focused tests, the 128-test runtime regression, the 109-test agents
regression, strict crate Clippy, full quality gate, exact-tree six-role field
QA, and post-merge validation without package loading or execution. The
package loader and runtime executor rows remain `PROPOSED`.

Not all functional P0 agents should be implemented in parallel at the start.
After every prerequisite gate above passes, `NODE-DOCTOR-AGENT-001` remains
the recommended complete reference vertical. `REPORTER-AGENT-001` should then
be the next functional agent, followed by the remaining P0 implementation
waves.

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

## Parallel Product Tracks

The following tracks are strategically accepted but remain `PROPOSED`. They do
not renumber v0.12.0, v0.12.1, v0.12.2, or v0.13.0 and do not authorize product
implementation:

```text
IAMINE-GUI-CLI-PRODUCT-TRACK
  -> docs/roadmap/iamine-gui-cli-product-track.md

IAMINE-SECURITY-CI-TRACK
  -> docs/roadmap/iamine-security-ci-track.md

IAMINE-INTERNAL-QUALITY-SECURITY-AUTOMATION-TRACK
  -> docs/roadmap/iamine-internal-quality-security-automation-track.md

IAMINE-PUBLIC-INTERNAL-ARCHITECTURE-GOVERNANCE-TRACK
  -> docs/roadmap/iamine-public-internal-governance-track.md

IAMINE-FUTURE-PRODUCT-EXPERIENCE-TRACKS
  -> docs/roadmap/iamine-future-product-experience-tracks.md
```

Visual dashboard work may use typed mocks. Real mutating actions remain blocked
until shared contracts, local authorization, audit requirements, and the Local
Control API contract close. Security/CI remediation may proceed as an
independent maintenance track but must preserve the active runtime sequence.
Internal QA and Security agent implementation begins only after the v0.11.2
runtime milestone, remains subordinate to human authority, and is not a new
implicit prerequisite for P0 agents. Future desktop, mobile, memory,
cross-device, family, and education implementation remains deferred until
Architecture promotes its dependency-complete groups.

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
AGENT-SCOPE-TEST-HARNESS-001
AGENT-EXPERTISE-TEMPLATE-001
```

Closed internal contracts consumed by this milestone:

```text
AGENT-FRAMEWORK-BASELINE-001
AGENT-TEMPLATE-DIAGNOSTIC-001
AGENT-TEMPLATE-FILE-READONLY-001
AGENT-TEMPLATE-NETWORK-DIAGNOSTIC-001
AGENT-TEMPLATE-REPORTER-001
AGENT-TEMPLATE-TEXT-ASSISTANT-001
AGENT-TEMPLATE-OS-DIAGNOSTIC-001
```

These closed identifiers are dependencies, not reopened v1.2 feature rows.
Public implementation or distribution work must receive distinct feature IDs
only when Architecture defines genuinely different deliverables.

Proposed public template additions:

```text
AGENT-TEMPLATE-DOCUMENT-LOCAL-001
AGENT-TEMPLATE-CONTENT-DRAFT-001
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

The public assistant identifiers are distinct from their closed `-INTERNAL`
contracts because they require executable public developer experience,
distribution, and QA deliverables. They must consume the internal contracts
without reinterpreting those contracts as executable evidence.

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
