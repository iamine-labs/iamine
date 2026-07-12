# AGENT-MARKET-FIT-RESEARCH-001

## Objective

Define the research baseline for IAMINE Agent Network product fit before any
agent runtime, scope manifest, permission model, or official beta agent pack is
implemented.

## Scope

This feature adds:

```text
docs/agents/agent-market-fit-research.md
docs/architecture/agent-market-fit-research.md
docs/qa/agent-market-fit-research.md
```

It also updates roadmap state for the active v0.11 research phase.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- agent manifests;
- scope manifests;
- permission enforcement;
- sandboxing;
- audit logs;
- P2P, PubSub, worker, scheduler, model, inference, installer, updater,
  rollback, reputation, reward, wallet, marketplace, or public beta behavior;
- Rust source, scripts, service definitions, release artifacts, or package
  generation.

## Research Contract

The research baseline must:

- preserve the scope-bound agent rule;
- describe candidate user segments without claiming completed user validation;
- define evaluation criteria for safe first agents;
- define exclusion criteria for unsafe early agents;
- keep public beta blocked;
- keep arbitrary third-party agents blocked;
- keep agent runtime execution blocked;
- keep payments, rewards, marketplace, settlement, and mainnet blocked.

## Integration

This feature feeds later v0.11 features:

```text
AGENT-USER-PERSONA-MAPPING-001
AGENT-BETA-PACK-SELECTION-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
AGENT-PERMISSION-MODEL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
```

The output of this feature is not enough to select the final beta pack. It only
defines the research frame and initial hypotheses.

## Risks

- Treating hypotheses as validated market evidence would be incorrect.
- Selecting official P0 agents before persona and beta-pack features would
  bypass the roadmap.
- Allowing broad or generic agents would violate the scope-bound rule.
- Runtime implementation before manifests, permissions, audit logs, and boundary
  tests would create an unsafe execution path.
- Field QA is not meaningful for this documentation-only feature, but later
  runtime features must run field QA.

## Recommendation

If QA confirms this feature remains documentation-only and aligned with the
canonical roadmap, proceed to:

```text
AGENT-USER-PERSONA-MAPPING-001
```
