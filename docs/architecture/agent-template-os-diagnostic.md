# AGENT-TEMPLATE-OS-DIAGNOSTIC-001

## Objective

Define the future OS diagnostic template boundary without implementing system
probes, command execution, file readers, process inspection, network probes,
exports, persistence, publication, package installation, workers, schedulers,
model loading, or inference behavior.

## Scope

This feature adds:

```text
docs/agents/agent-template-os-diagnostic.md
docs/architecture/agent-template-os-diagnostic.md
docs/qa/agent-template-os-diagnostic.md
```

It updates the v0.11.3 roadmap state for
`AGENT-TEMPLATE-OS-DIAGNOSTIC-001`.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, runtime
startup, workers, schedulers, queues, state machines, persistence, service
definitions, Cargo dependencies, package managers, registry storage, model
loading, inference, installer, updater, rewards, wallet, marketplace, public
beta, or mainnet behavior.

## Non-Bypass Rules

- OS diagnostic template policy cannot authorize runtime execution.
- OS diagnostic template policy cannot implement probes or shell adapters.
- OS diagnostic template policy cannot collect host identity.
- OS diagnostic template policy cannot read arbitrary files or inspect
  processes.
- OS diagnostic template policy cannot probe networks or mutate state.
- OS diagnostic template policy cannot approve scope or permissions.
- OS diagnostic template policy cannot publish to registry or marketplace.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive OS diagnostic metadata must block validation, execution,
persistence, export, or publication by default.

## Integration

This feature consumes framework baseline, template validation, diagnostic,
file read-only, network diagnostic, reporter, and text assistant boundaries. It
feeds the P0 OS diagnostic agent skeleton and future operator-approved platform
diagnostic workflows.

## Recommendation

Keep this feature documentation-only. Later implementation must own probe
authorization, platform adapters, identity redaction, process data handling,
network metadata handling, audit records, and permission enforcement in
dedicated modules.
