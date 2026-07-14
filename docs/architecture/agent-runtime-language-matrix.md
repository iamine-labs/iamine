# AGENT-RUNTIME-LANGUAGE-MATRIX-001

## Objective

Define the IAMINE agent runtime language matrix before runtime execution,
interpreter startup, dependency installation, package manager execution,
sandboxing, registry admission, scheduler integration, worker startup, trust,
reputation, reward, marketplace, or distributed model MoE behavior exists.

## Scope

This feature adds:

```text
docs/agents/agent-runtime-language-matrix.md
docs/architecture/agent-runtime-language-matrix.md
docs/qa/agent-runtime-language-matrix.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-RUNTIME-LANGUAGE-MATRIX-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify Rust
source, Cargo dependencies, lockfiles, package managers, runtime startup,
interpreter startup, WASM execution, container execution, agent execution,
dependency installation, sandboxing, registry storage, scheduler behavior,
worker startup, model loading, inference, installer, updater, rewards, wallet,
marketplace, public beta, mainnet, or service definitions.

## Runtime Matrix Role

The runtime language matrix is a planning contract. It records runtime modes
that later implementation features may design, defer, or block.

It cannot start interpreters, execute code, run containers, run WASM modules,
install packages, choose workers, or make packages executable.

## Status Model

Allowed statuses are:

```text
planned
deferred
blocked
```

Rules:

- `planned` means a later runtime feature may design the mode;
- `deferred` means the mode belongs to a later roadmap phase;
- `blocked` means the mode must not be implemented.

No status implies execution availability.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Language placement | `AGENT-LANGUAGE-POLICY-001` | no |
| Dependency class policy | `AGENT-DEPENDENCY-POLICY-001` | no |
| Runtime language availability planning | `AGENT-RUNTIME-LANGUAGE-MATRIX-001` | yes |
| Manifest schema source of truth | `AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001` | no |
| Runtime execution | `AGENT-RUNTIME-BASELINE-001` and later | no |
| Sandbox behavior | `AGENT-RUNTIME-SANDBOX-001` | no |

## Non-Bypass Rules

- Runtime matrix cannot authorize package installation.
- Runtime matrix cannot authorize runtime execution.
- Runtime matrix cannot start interpreters.
- Runtime matrix cannot run package managers.
- Runtime matrix cannot install dependencies.
- Runtime matrix cannot create sandbox availability.
- Runtime matrix cannot grant permissions.
- Runtime matrix cannot replace audit evidence.
- Runtime matrix cannot replace boundary evals.
- Runtime matrix cannot replace local registry review.
- Runtime matrix cannot expand scope.
- Runtime matrix cannot create capabilities.
- Runtime matrix cannot select nodes or models.
- Runtime matrix cannot imply public registry or marketplace publication.
- Runtime matrix cannot imply trust, reputation, certification, reward
  eligibility, wallet behavior, settlement, token, or mainnet behavior.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive runtime matrix metadata must block local registry review
advancement, install, and execution by default.

Examples:

- unknown runtime matrix schema;
- Python listed as current executable runtime;
- TypeScript listed as current executable runtime;
- WASM/WASI listed as available before sandbox gates;
- containers listed as available before sandbox and registry gates;
- arbitrary shell listed as runtime mode;
- unrestricted filesystem listed as runtime mode;
- wallet or mainnet runtime listed before economic and mainnet gates;
- interpreter startup requested;
- package manager execution requested;
- credential, key, host identifier, private path, or secret collection.

## Integration

This feature consumes:

```text
AGENT-LANGUAGE-POLICY-001
AGENT-DEPENDENCY-POLICY-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
```

It feeds:

```text
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-RUNTIME-BASELINE-001
AGENT-RUNTIME-SANDBOX-001
```

## Risks

- Treating a planned runtime mode as executable would bypass runtime baseline
  and sandbox gates.
- Treating Python, TypeScript, WASM/WASI, or containers as current runtime
  availability would jump ahead of dependency and sandbox policy.
- Adding runtime code or dependencies here would violate the docs-only
  boundary.

## Recommendation

Keep this feature documentation-only. Runtime implementation must remain in
later owner modules and gates.
