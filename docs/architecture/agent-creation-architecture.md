# AGENT-CREATION-ARCHITECTURE-001

## Objective

Define the end-to-end IAMINE architecture for creating, reviewing, packaging,
validating, and later executing scope-bound agents.

## Scope

This feature adds:

```text
docs/agents/agent-creation-architecture.md
docs/architecture/agent-creation-architecture.md
docs/qa/agent-creation-architecture.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-CREATION-ARCHITECTURE-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- agent skeleton generator;
- package manifest parser;
- scope manifest parser;
- capability metadata parser;
- expertise metadata parser;
- permission enforcement;
- resource enforcement;
- sandboxing;
- audit log implementation;
- agent registry;
- router or scheduler behavior;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, worker, model, inference, installer, updater, rollback,
  reputation, reward, wallet, settlement, token, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts, or
  package generation.

## Creation Pipeline

The canonical agent creation pipeline is:

```text
idea
-> persona and task fit
-> package manifest
-> skeleton standard
-> scope manifest
-> capability metadata
-> expertise metadata
-> resource requirements
-> permission model
-> audit policy
-> boundary evals
-> local registry review
-> runtime eligibility review
-> execution lifecycle
-> beta pack inclusion
```

Each step must be independently reviewable. A later step may consume earlier
metadata, but it must not silently own that earlier step's responsibility.

## Required Gates

An IAMINE agent cannot become executable unless these gates exist and pass:

- package manifest is valid;
- skeleton layout matches the standard;
- scope manifest is narrow and explicit;
- capabilities are declared without reputation or scheduler side effects;
- expertise metadata is explicit and non-promissory;
- resource requirements are bounded;
- permissions are explicit and denied by default;
- audit policy is privacy-safe;
- boundary evals cover positive, negative, ambiguous, dangerous, cross-domain,
  prompt-injection, role-confusion, permission-escalation, and handoff cases;
- local registry review has accepted the package for its intended phase;
- runtime eligibility review has not been bypassed.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Package identity and references | `AGENT-PACKAGE-MANIFEST-001` | no |
| Skeleton layout | `AGENT-SKELETON-STANDARD-001` | no |
| Scope boundary | `AGENT-SCOPE-MANIFEST-001` | no |
| Capability metadata | `AGENT-CAPABILITY-METADATA-001` | no |
| Expertise metadata | `AGENT-EXPERTISE-METADATA-001` | no |
| Resource requirements | `AGENT-RESOURCE-REQUIREMENTS-001` | no |
| Permission categories | `AGENT-PERMISSION-MODEL-001` | no |
| Audit evidence | `AGENT-AUDIT-LOG-001` | no |
| Local registry review | `AGENT-REGISTRY-LOCAL-001` | no |
| Boundary evals | `AGENT-SCOPE-BOUNDARY-EVALS-001` | no |
| Runtime execution | `AGENT-RUNTIME-BASELINE-001` and later | no |

This feature owns only the architecture ordering, handoff boundaries, review
responsibilities, and non-bypass rules for agent creation.

## Non-Bypass Rules

- A package manifest cannot authorize execution.
- A scope manifest cannot grant permissions.
- A permission model cannot expand scope.
- Capability metadata cannot imply expertise, reputation, or scheduler
  priority.
- Expertise metadata cannot claim distributed model MoE.
- Resource requirements cannot select a node by themselves.
- Audit logging cannot make unsafe actions safe.
- Boundary evals cannot be skipped by manual approval.
- Local registry presence cannot imply public marketplace publication.
- Runtime eligibility cannot exist before scope, permission, audit, and eval
  gates are defined.

## Review States

Agent creation review uses these architecture states:

```text
draft
manifest_ready
skeleton_ready
scope_ready
metadata_ready
permissions_ready
audit_ready
boundary_eval_ready
registry_review_ready
runtime_review_ready
blocked
deprecated
```

These states are planning and review states only. They do not start runtime
services or authorize agent execution.

## Failure Policy

Missing, unknown, contradictory, broad, or unsafe metadata must block install,
registry admission, and execution by default.

Examples:

- missing scope manifest;
- generic `do_anything` scope;
- missing blocked actions;
- missing permission denial behavior;
- unrestricted filesystem request;
- arbitrary shell request;
- unrestricted network request;
- destructive action without a later explicit permission gate;
- credential, key, host identifier, private path, or secret collection;
- public marketplace channel before public registry gates;
- runtime execution requested before runtime features exist.

## Integration

This feature consumes:

```text
AGENT-MARKET-FIT-RESEARCH-001
AGENT-USER-PERSONA-MAPPING-001
AGENT-BETA-PACK-SELECTION-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
```

It feeds:

```text
AGENT-SKELETON-STANDARD-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-RUNTIME-BASELINE-001
```

## Risks

- Treating this architecture as runtime authorization would bypass later
  enforcement gates.
- Letting package, scope, permission, or audit contracts overlap would create
  hidden responsibility shifts.
- Allowing generic agents would weaken the scope-bound agent rule.
- Adding parser or runtime behavior here would jump ahead of the v0.11.1
  architecture foundation sequence.
- Marking public registry or marketplace behavior available here would
  redefine v1.0 and later platform gates.

## Recommendation

If QA confirms this feature remains documentation-only, roadmap-aligned, and
non-executable, proceed to:

```text
AGENT-SKELETON-STANDARD-001
```
