# AGENT-FRAMEWORK-BASELINE-001

## Objective

Define the shared non-runtime baseline for future official IAMINE agent
templates without implementing framework code, SDKs, generators, validators,
file writes, package installation, registry publication, marketplace
publication, runtime startup, workers, schedulers, model loading, or inference.

## Scope

This feature adds:

```text
docs/agents/agent-framework-baseline.md
docs/architecture/agent-framework-baseline.md
docs/qa/agent-framework-baseline.md
```

It updates the v0.11.3 roadmap state for `AGENT-FRAMEWORK-BASELINE-001`.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, runtime
startup, workers, schedulers, queues, state machines, persistence, service
definitions, Cargo dependencies, package managers, registry storage, model
loading, inference, installer, updater, rewards, wallet, marketplace, public
beta, or mainnet behavior.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Skeleton shape | `AGENT-SKELETON-GENERATOR-001` | no |
| Template validation | `AGENT-TEMPLATE-VALIDATION-001` | no |
| Framework baseline contract | `AGENT-FRAMEWORK-BASELINE-001` | yes |
| Diagnostic template | `AGENT-TEMPLATE-DIAGNOSTIC-001` | no |
| File read-only template | `AGENT-TEMPLATE-FILE-READONLY-001` | no |

## Non-Bypass Rules

- Framework baseline cannot authorize runtime execution.
- Framework baseline cannot implement SDKs, generators, or validators.
- Framework baseline cannot grant permissions.
- Framework baseline cannot approve scope.
- Framework baseline cannot publish to registry or marketplace.
- Framework baseline cannot install packages or dependencies.
- Framework baseline cannot skip template validation, boundary tests, manual
  validation, audit, or local registry review.
- Framework baseline cannot imply trust, reputation, rewards, settlement,
  token, mainnet behavior, or distributed model MoE.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive framework metadata must block validation, install, execution,
persistence, or publication by default.

## Integration

This feature consumes skeleton generator and template validation. It feeds all
official v0.11.3 template definitions and internal assistant definitions.

## Recommendation

Keep this feature documentation-only. Later implementation must own any SDK,
template engine, validation hooks, audit records, or packaging behavior in
dedicated modules.
