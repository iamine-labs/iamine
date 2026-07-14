# AGENT-RUNTIME-SANDBOX-001

## Objective

Define sandbox requirements for future IAMINE agent execution without
implementing sandbox startup, process isolation, runtime execution, scheduler
integration, worker startup, package installation, or dependency installation.

## Scope

This feature adds:

```text
docs/agents/agent-runtime-sandbox.md
docs/architecture/agent-runtime-sandbox.md
docs/qa/agent-runtime-sandbox.md
```

It also updates the v0.11.2 roadmap state for `AGENT-RUNTIME-SANDBOX-001`.

## Architecture Boundary

This feature is documentation-only. It does not modify Rust source, runtime
startup, workers, schedulers, process management, filesystem access, network
access, containers, WASM runtime, interpreters, service definitions, Cargo
dependencies, lockfiles, package managers, registry storage, model loading,
inference, installer, updater, rewards, wallet, marketplace, public beta, or
mainnet behavior.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Runtime state vocabulary | `AGENT-RUNTIME-BASELINE-001` | no |
| Sandbox requirements | `AGENT-RUNTIME-SANDBOX-001` | yes |
| Execution lifecycle transitions | `AGENT-EXECUTION-LIFECYCLE-001` | no |
| Timeout and cancellation | `AGENT-TIMEOUT-CANCEL-001` | no |
| Handoff policy | `AGENT-HANDOFF-POLICY-001` | no |

## Non-Bypass Rules

- Sandbox policy cannot authorize runtime execution.
- Sandbox policy cannot implement sandbox enforcement.
- Sandbox policy cannot grant permissions.
- Sandbox policy cannot allow arbitrary shell.
- Sandbox policy cannot allow unrestricted filesystem access.
- Sandbox policy cannot allow unrestricted network access.
- Sandbox policy cannot start workers.
- Sandbox policy cannot select nodes or models.
- Sandbox policy cannot replace audit evidence, boundary evals, or local
  registry review.
- Sandbox policy cannot imply trust, reputation, rewards, marketplace,
  settlement, token, mainnet behavior, or distributed model MoE.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive sandbox metadata must block install and execution by default.

Examples:

- sandbox availability claimed before implementation;
- arbitrary shell allowed;
- unrestricted filesystem or network allowed;
- private paths allowed;
- missing cleanup policy;
- missing resource limits;
- missing timeout compatibility;
- runtime execution requested before lifecycle gates.

## Integration

This feature consumes `AGENT-RUNTIME-BASELINE-001` and feeds
`AGENT-EXECUTION-LIFECYCLE-001`, `AGENT-TIMEOUT-CANCEL-001`, and
`AGENT-ROUTING-CANDIDATE-SELECTION-001`.

## Recommendation

Keep this feature documentation-only. Future implementation must live in a
dedicated runtime/sandbox owner module, not in `iamine-node/src/main.rs`.
