# AGENT-TEMPLATE-REPORTER-001

## Objective

Define the future reporter template boundary without implementing report
generation, file readers, network probes, runtime execution, exports,
persistence, publication, package installation, workers, schedulers, model
loading, or inference behavior.

## Scope

This feature adds:

```text
docs/agents/agent-template-reporter.md
docs/architecture/agent-template-reporter.md
docs/qa/agent-template-reporter.md
```

It updates the v0.11.3 roadmap state for `AGENT-TEMPLATE-REPORTER-001`.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, runtime
startup, workers, schedulers, queues, state machines, persistence, service
definitions, Cargo dependencies, package managers, registry storage, model
loading, inference, installer, updater, rewards, wallet, marketplace, public
beta, or mainnet behavior.

## Non-Bypass Rules

- Reporter template policy cannot authorize runtime execution.
- Reporter template policy cannot implement report generation.
- Reporter template policy cannot collect evidence.
- Reporter template policy cannot read arbitrary files or probe networks.
- Reporter template policy cannot export or publish reports.
- Reporter template policy cannot grant permissions or approve scope.
- Reporter template policy cannot publish to registry or marketplace.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive reporter metadata must block validation, execution,
persistence, export, or publication by default.

## Integration

This feature consumes framework baseline, template validation, diagnostic, and
file read-only boundaries. It feeds the P0 reporter agent skeleton and future
operator-facing report workflows.

## Recommendation

Keep this feature documentation-only. Later implementation must own rendering,
export, source citation, redaction, audit records, and permission enforcement
in dedicated modules.
