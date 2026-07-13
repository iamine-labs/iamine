# AGENT-BETA-PACK-SELECTION-001

## Objective

Select the first official IAMINE beta agent pack from the market-fit research
and persona mapping while preserving the scope-bound agent rule.

## Scope

This feature adds:

```text
docs/agents/official-beta-agent-pack-selection.md
docs/architecture/agent-beta-pack-selection.md
docs/qa/agent-beta-pack-selection.md
```

It also updates the Agent Network roadmap state for the active v0.11 research
phase.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- package manifests;
- scope manifests;
- capability metadata;
- resource requirements;
- permission enforcement;
- sandboxing;
- audit logs;
- agent registry;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, worker, scheduler, model, inference, installer, updater,
  rollback, reputation, reward, wallet, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts, or
  package generation.

## Selected Pack

The selected pack is:

```text
IAMINE Local Readiness Beta Pack
```

Selected candidate agents:

```text
Node Doctor
Privacy-Safe Support Reporter
LAN Readiness Reporter
Agent Manifest Wizard
```

The selected agents are product targets only. They are not executable until
later manifest, permission, audit, boundary-eval, and runtime features define
their contracts.

## Integration

This feature consumes:

```text
AGENT-MARKET-FIT-RESEARCH-001
AGENT-USER-PERSONA-MAPPING-001
```

It feeds:

```text
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-RUNTIME-BASELINE-001
```

## Selection Constraints

The selected pack must remain:

- scope-bound;
- read-only by default;
- local-only or bounded LAN-only in the earliest mode;
- privacy-safe;
- testable without credentials;
- blocked from destructive mutation;
- blocked from public marketplace behavior;
- blocked from third-party agent publication;
- blocked from mainnet, wallet, reward, or settlement behavior.

## Risks

- Treating this product selection as runtime authorization would bypass
  required manifest, permission, audit, and sandbox work.
- Selecting broad agents would weaken the scope-bound agent rule.
- Including Proxmox, Docker, router, filesystem-write, or OS-wide agents too
  early would increase unsafe permission pressure.
- Claiming external validation would overstate evidence; this is repository
  planning based on prior research artifacts.

## Recommendation

If QA confirms this remains documentation-only and roadmap-aligned, proceed to:

```text
AGENT-PACKAGE-MANIFEST-001
```
