# AGENT-RUNTIME-BASELINE-001

## Objective

Define the IAMINE agent runtime baseline state vocabulary and prerequisite
gates before real runtime execution, sandbox startup, package installation,
scheduler integration, worker startup, trust, reputation, reward, marketplace,
or distributed model MoE behavior exists.

## Scope

This feature adds:

```text
docs/agents/agent-runtime-baseline.md
docs/architecture/agent-runtime-baseline.md
docs/qa/agent-runtime-baseline.md
```

It also updates the v0.11.2 Agent Runtime Baseline roadmap state for
`AGENT-RUNTIME-BASELINE-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify Rust
source, runtime startup, schedulers, workers, task queues, state machines,
service definitions, Cargo dependencies, lockfiles, package managers,
interpreters, WASM runtime, containers, agent execution, dependency
installation, sandboxing, registry storage, model loading, inference,
installer, updater, rewards, wallet, marketplace, public beta, or mainnet.

## Runtime Baseline Role

The baseline defines required state labels and prerequisite gates. It cannot
execute agents, persist execution records, start workers, evaluate prompts,
load models, or route tasks.

## State Rules

- `queued` means accepted for future review, not execution.
- `permission_pending` means permission review is required.
- `scope_check` means scope review is required.
- `handoff_required` means control must return to orchestrator or human.
- `running` is reserved for later execution lifecycle features.
- `completed` and `failed` are reserved terminal states.
- `cancelled` and `timeout` require later cancellation/cleanup contracts.
- `blocked` must be used for unsafe, contradictory, or missing gates.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Runtime state vocabulary | `AGENT-RUNTIME-BASELINE-001` | yes |
| Sandbox behavior | `AGENT-RUNTIME-SANDBOX-001` | no |
| Execution lifecycle transitions | `AGENT-EXECUTION-LIFECYCLE-001` | no |
| Input/output contract | `AGENT-INPUT-OUTPUT-CONTRACT-001` | no |
| Timeout and cancellation | `AGENT-TIMEOUT-CANCEL-001` | no |
| Handoff policy | `AGENT-HANDOFF-POLICY-001` | no |
| Routing candidate selection | `AGENT-ROUTING-CANDIDATE-SELECTION-001` | no |

## Non-Bypass Rules

- Runtime baseline cannot authorize package installation.
- Runtime baseline cannot authorize runtime execution.
- Runtime baseline cannot implement state transitions.
- Runtime baseline cannot start workers.
- Runtime baseline cannot load models.
- Runtime baseline cannot create sandbox availability.
- Runtime baseline cannot grant permissions.
- Runtime baseline cannot replace audit evidence.
- Runtime baseline cannot replace boundary evals.
- Runtime baseline cannot replace local registry review.
- Runtime baseline cannot expand scope.
- Runtime baseline cannot create capabilities.
- Runtime baseline cannot select nodes or models.
- Runtime baseline cannot imply public registry or marketplace publication.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive runtime baseline metadata must block local registry review
advancement, install, and execution by default.

Examples:

- unknown runtime baseline schema;
- missing `blocked`;
- missing `handoff_required`;
- `running` treated as available execution;
- timeout without cleanup policy;
- cancellation without cleanup policy;
- runtime execution requested before sandbox and lifecycle gates;
- credential, key, host identifier, private path, or secret collection.

## Integration

This feature consumes all closed v0.11.1 architecture foundation contracts and
feeds every remaining v0.11.2 runtime contract.

## Risks

- Treating state vocabulary as implementation would create hidden runtime
  behavior.
- Treating `running` as available would bypass sandbox and lifecycle gates.
- Adding code in `iamine-node` here would violate the docs-only boundary.

## Recommendation

Keep this feature documentation-only. Runtime implementation must remain in
later owner modules and gates.
