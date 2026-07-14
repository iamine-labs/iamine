# AGENT-DEPENDENCY-POLICY-001

## Objective

Define the IAMINE agent dependency policy by dependency class, ownership layer,
and roadmap phase before dependency installation, package manager execution,
runtime execution, sandboxing, registry admission, scheduler integration,
worker startup, trust, reputation, reward, marketplace, or distributed model
MoE behavior exists.

## Scope

This feature adds:

```text
docs/agents/agent-dependency-policy.md
docs/architecture/agent-dependency-policy.md
docs/qa/agent-dependency-policy.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-DEPENDENCY-POLICY-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify Rust
source, Cargo dependencies, lockfiles, package managers, runtime startup,
agent execution, dependency installation, sandboxing, registry storage,
scheduler behavior, worker startup, model loading, inference, installer,
updater, rewards, wallet, marketplace, public beta, mainnet, or service
definitions.

## Dependency Policy Role

The dependency policy is a review contract for dependency classes. It cannot
install dependencies, run package managers, validate lockfiles, scan licenses,
scan vulnerabilities, choose a runtime, or make a package executable.

## Status Model

Allowed statuses are:

```text
allowed
optional
deferred
blocked
```

Rules:

- `allowed` means a later implementation feature may propose the dependency;
- `optional` means a later feature must justify the dependency;
- `deferred` means the dependency belongs to a later roadmap phase;
- `blocked` means the dependency must not be introduced in this phase.

No status implies runtime availability or install authorization.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Language placement | `AGENT-LANGUAGE-POLICY-001` | no |
| Dependency class policy | `AGENT-DEPENDENCY-POLICY-001` | yes |
| Runtime language availability | `AGENT-RUNTIME-LANGUAGE-MATRIX-001` | no |
| Manifest schema source of truth | `AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001` | no |
| Runtime execution | `AGENT-RUNTIME-BASELINE-001` and later | no |
| Sandbox behavior | `AGENT-RUNTIME-SANDBOX-001` | no |

## Non-Bypass Rules

- Dependency policy cannot authorize package installation.
- Dependency policy cannot authorize runtime execution.
- Dependency policy cannot run package managers.
- Dependency policy cannot install dependencies.
- Dependency policy cannot create sandbox availability.
- Dependency policy cannot grant permissions.
- Dependency policy cannot replace audit evidence.
- Dependency policy cannot replace boundary evals.
- Dependency policy cannot replace local registry review.
- Dependency policy cannot expand scope.
- Dependency policy cannot create capabilities.
- Dependency policy cannot select nodes or models.
- Dependency policy cannot imply public registry or marketplace publication.
- Dependency policy cannot imply trust, reputation, certification, reward
  eligibility, wallet behavior, settlement, token, or mainnet behavior.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive dependency metadata must block local registry review
advancement, install, and execution by default.

Examples:

- unknown dependency policy schema;
- package manager execution requested;
- dependency installation requested;
- Python SDK dependency added before v1.2.x;
- TypeScript SDK dependency added before v1.2.x;
- WASM/WASI runtime dependency added before sandbox and runtime matrix gates;
- container runtime dependency added before sandbox and registry gates;
- external LLM framework dependency in v0.11.x;
- social API or router API client dependency in v0.11.x;
- credential, key, host identifier, private path, or secret collection.

## Integration

This feature consumes:

```text
AGENT-LANGUAGE-POLICY-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-REGISTRY-LOCAL-001
```

It feeds:

```text
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-RUNTIME-BASELINE-001
```

## Risks

- Treating an allowed dependency class as install authorization would bypass
  future installer and runtime gates.
- Adding Cargo or package-manager changes in this feature would violate its
  documentation-only boundary.
- Introducing SDK, WASM, container, LLM framework, OCR, social API, or router
  dependencies now would jump ahead of the roadmap.

## Recommendation

Keep this feature documentation-only. Later implementation features must add
dependencies through their own owner modules, validation, review evidence, and
roadmap gates.
