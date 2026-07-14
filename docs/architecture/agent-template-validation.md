# AGENT-TEMPLATE-VALIDATION-001

## Objective

Define future IAMINE agent template validation requirements without
implementing validators, CLI commands, file writes, template rendering,
package installation, dependency resolution, registry publication, marketplace
publication, runtime startup, workers, schedulers, model loading, or inference
behavior.

## Scope

This feature adds:

```text
docs/agents/agent-template-validation.md
docs/architecture/agent-template-validation.md
docs/qa/agent-template-validation.md
```

It updates the v0.11.3 roadmap state for `AGENT-TEMPLATE-VALIDATION-001`.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, runtime
startup, workers, schedulers, queues, state machines, persistence, service
definitions, Cargo dependencies, package managers, registry storage, model
loading, inference, installer, updater, rewards, wallet, marketplace, public
beta, or mainnet behavior.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Skeleton file shape | `AGENT-SKELETON-GENERATOR-001` | no |
| Template validation gates | `AGENT-TEMPLATE-VALIDATION-001` | yes |
| Framework baseline | `AGENT-FRAMEWORK-BASELINE-001` | no |
| Permission review | `AGENT-PERMISSION-REVIEW-AGENT-001-INTERNAL` | no |
| Scope review | `AGENT-SCOPE-REVIEW-AGENT-001-INTERNAL` | no |

## Non-Bypass Rules

- Template validation policy cannot authorize runtime execution.
- Template validation policy cannot implement validators.
- Template validation policy cannot grant permissions.
- Template validation policy cannot publish to registry or marketplace.
- Template validation policy cannot install packages or dependencies.
- Template validation policy cannot skip skeleton shape, scope review,
  permission review, boundary tests, manual validation, audit, or local registry
  review.
- Template validation policy cannot imply trust, reputation, rewards,
  settlement, token, mainnet behavior, or distributed model MoE.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive template metadata must block validation, install, execution,
persistence, or publication by default.

## Integration

This feature consumes skeleton generator, manifest source-of-truth, scope,
permission, runtime, input/output, timeout, handoff, out-of-scope, and routing
contracts. It feeds framework baseline and official template definitions.

## Recommendation

Keep this feature documentation-only. A later implementation must own concrete
validator code, error models, audit records, CLI surfaces, and packaging hooks
in dedicated modules.
