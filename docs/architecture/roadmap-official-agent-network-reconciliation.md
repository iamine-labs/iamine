# ROADMAP-OFFICIAL-AGENT-NETWORK-RECONCILIATION-001

## Objective

Reconcile the repository roadmap with the official IAMINE Agent Network
roadmap. This is a documentation and planning feature only.

The central architecture decision is:

```text
IAMINE v1.0 is IAMINE Agent Network Public Beta, not an inference-only public
testnet.
```

## Scope

This feature may update:

- `docs/roadmap/iamine-product-roadmap.md`
- `docs/roadmap/iamine-agent-network-roadmap.md`
- `docs/architecture/roadmap-official-agent-network-reconciliation.md`
- `docs/qa/roadmap-official-agent-network-reconciliation.md`

This feature must not change runtime code, scheduler behavior, P2P behavior,
worker behavior, model policy, install scripts, release scripts, QA scripts, or
field QA harnesses.

## Reconciliation Rules

Closed repository evidence remains closed. This feature must not reopen,
rewrite, or reinterpret closed v0.7, v0.8, or v0.9 feature evidence.

New official roadmap features are added as `PROPOSED` unless there is explicit
merge and post-merge validation evidence in `develop`.

`PUBLIC-TESTNET-ADMISSION-001` moves from the previous v1.0 public-testnet
interpretation into v0.10.0 pre-public infrastructure. The feature remains
`PROPOSED`.

The roadmap must include the official rule that every IAMINE agent is
scope-bound. Agent features must explicitly define scope, permissions,
blocked actions, handoff behavior, resource requirements, audit logging, and
positive and negative boundary tests.

## Architecture Boundaries

This reconciliation does not authorize:

- arbitrary third-party agents;
- open marketplace behavior;
- real payments;
- mainnet;
- public beta launch;
- execution of agents without manifests, scope, permissions, sandboxing, audit
  logs, and boundary tests;
- scheduler, P2P, runtime, reputation, reward, or model-policy behavior changes.

## Expected Product Sequence

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

## QA Expectations

Validation is documentation-focused:

- confirm no runtime source files changed;
- confirm v1.0 is no longer described as an inference-only public testnet;
- confirm `PUBLIC-TESTNET-ADMISSION-001` is under v0.10.0;
- confirm new official roadmap features are not marked `CLOSED`;
- confirm the scope-bound agent rule is present;
- confirm `git diff --check` passes;
- confirm no private local paths, IP addresses, credentials, or secrets are
  added.

Field QA is not required for this feature because it changes only roadmap and
architecture documentation. Any future feature that touches runtime, network,
worker behavior, scheduler behavior, inference behavior, install behavior, or
agent execution must follow the normal field QA rules.

## Recommendation

After this feature closes, the next product feature should be selected from the
official immediate sequence:

```text
V0.9-BETA-FRESH-INSTALL-E2E-001
```

`PUBLIC-TESTNET-ADMISSION-001` remains next in v0.10 after the fresh-install
E2E gap is addressed or Architecture explicitly accepts its scheduling.
