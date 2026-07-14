# AGENT-SKELETON-GENERATOR-001

## Objective

Define future IAMINE agent skeleton generator requirements without
implementing generator code, CLI commands, file writes, template rendering,
package installation, validation enforcement, registry publication, marketplace
publication, runtime startup, workers, schedulers, model loading, or inference
behavior.

## Scope

This feature adds:

```text
docs/agents/agent-skeleton-generator.md
docs/architecture/agent-skeleton-generator.md
docs/qa/agent-skeleton-generator.md
```

It creates the v0.11.3 roadmap state table and marks
`AGENT-SKELETON-GENERATOR-001` as active.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, runtime
startup, workers, schedulers, queues, state machines, persistence, service
definitions, Cargo dependencies, package managers, registry storage, model
loading, inference, installer, updater, rewards, wallet, marketplace, public
beta, or mainnet behavior.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Manifest source of truth | `AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001` | no |
| Runtime baseline | `AGENT-RUNTIME-BASELINE-001` | no |
| Skeleton file shape | `AGENT-SKELETON-GENERATOR-001` | yes |
| Template validation | `AGENT-TEMPLATE-VALIDATION-001` | no |
| Framework baseline | `AGENT-FRAMEWORK-BASELINE-001` | no |

## Non-Bypass Rules

- Skeleton generator policy cannot authorize runtime execution.
- Skeleton generator policy cannot implement file writes.
- Skeleton generator policy cannot implement package or dependency installs.
- Skeleton generator policy cannot grant permissions.
- Skeleton generator policy cannot publish to registry or marketplace.
- Skeleton generator policy cannot skip template validation, scope review,
  permission review, boundary tests, manual validation, or audit gates.
- Skeleton generator policy cannot imply trust, reputation, rewards,
  settlement, token, mainnet behavior, or distributed model MoE.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive skeleton metadata must block validation and publication by
default.

Examples:

- `declared_scope` missing;
- permission set includes unrestricted filesystem;
- runtime language missing;
- output contract missing;
- skeleton includes auto-publication defaults;
- skeleton writes outside package root;
- boundary tests omitted;
- skeleton claims approval without review.

## Integration

This feature starts v0.11.3 internal agent developer bootstrap. It consumes
manifest, runtime baseline, scope, permission, input/output, timeout, handoff,
out-of-scope, and routing contracts. It feeds template validation and framework
baseline work.

## Recommendation

Keep this feature documentation-only. A later implementation must own concrete
CLI behavior, file generation, template rendering, validation hooks, audit
records, and packaging in dedicated modules.
