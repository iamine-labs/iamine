# ROADMAP-AGENT-NETWORK-COMPLETE-RECONCILIATION-001

## Objective

Reconcile the repository roadmap with the updated official IAMINE Agent Network,
Agent Creation Architecture, Developer Platform, routing, and advanced compute
roadmap.

The central decision remains:

```text
IAMINE v1.0 is IAMINE Agent Network Public Beta, not an inference-only public
beta.
```

## Scope

This feature may update:

```text
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
docs/architecture/roadmap-agent-network-complete-reconciliation.md
docs/qa/roadmap-agent-network-complete-reconciliation.md
```

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- runtime code;
- scheduler behavior;
- P2P behavior;
- worker behavior;
- model policy;
- install scripts;
- release scripts;
- QA scripts;
- agent runtime;
- functional agents;
- package parser;
- scope parser;
- permission enforcement;
- sandboxing;
- audit logging;
- routing implementation;
- marketplace behavior;
- rewards, settlement, mainnet, or token behavior.

## Reconciliation Rules

Closed repository evidence remains closed. This feature must not reopen,
rewrite, or reinterpret closed v0.7, v0.8, v0.9, v0.10, or closed v0.11 Agent
Network feature evidence.

New roadmap entries are `PROPOSED` unless explicit merge and post-merge
validation evidence already exists in `develop`.

Current closed Agent Network entries stay closed:

```text
AGENT-MARKET-FIT-RESEARCH-001
AGENT-USER-PERSONA-MAPPING-001
AGENT-BETA-PACK-SELECTION-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
```

The active product phase after this reconciliation is:

```text
v0.11.1 - Agent Architecture Foundation
```

## Added Roadmap Coverage

This feature adds or clarifies roadmap coverage for:

- agent creation architecture;
- agent skeleton standard;
- expertise metadata;
- language policy;
- dependency policy;
- runtime language matrix;
- manifest schema source of truth;
- routing candidate selection;
- internal skeleton generator;
- template validation;
- P0 agent skeletons before P0 functional agents;
- Agent Expert Routing as the practical v1 MoE concept;
- routing quality and feedback signals;
- public developer platform subdivision;
- public templates;
- developer onboarding E2E;
- advanced compute and distributed MoE deferred to v2.x.

## Version Placement

`AGENT-EXPERT-ROUTING-001` belongs to v0.13/v1 productization as agent
selection, not distributed model MoE.

Distributed model MoE, expert sharding, tensor transport, distributed training,
and checkpoint distribution are deferred to v2.x / Advanced Compute.

Public SDKs, public templates, and public developer submission tooling remain
after v1.0 under v1.2.x. Internal developer tools may exist before v1.0 only to
accelerate official IAMINE agent creation.

## Language and Dependency Boundary

The roadmap records policy only. It does not add dependencies.

Rust remains the source of truth for core contracts, validators, runtime, CLI,
audit, registry, and official P0 agents. Python and TypeScript SDKs are later
public developer platform work. WASM/WASI is a future sandbox direction for
third-party lightweight agents. Containers are deferred for mature heavy-agent
sandboxing.

## Risks

- Marking proposed roadmap entries as closed without merge evidence would
  corrupt the feature state model.
- Treating Agent Expert Routing as distributed MoE would move advanced compute
  into the wrong release phase.
- Treating internal developer tools as public platform tooling before v1.0
  would weaken manual validation and publication controls.
- Adding dependencies or runtime behavior in this reconciliation would bypass
  the agent architecture gates.

## Recommendation

After this docs-only reconciliation closes, Architecture should re-evaluate the
next v0.11.1 feature. The updated roadmap indicates that the missing
architecture-first entries should be considered before continuing directly into
capability metadata:

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-SKELETON-STANDARD-001
```
