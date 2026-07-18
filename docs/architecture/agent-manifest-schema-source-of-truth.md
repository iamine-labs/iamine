# AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001

## Objective

Define IAMINE's agent manifest schema source of truth before schema generation,
validator implementation, runtime execution, dependency installation, package
manager execution, sandboxing, registry admission, scheduler integration,
worker startup, trust, reputation, reward, marketplace, or distributed model
MoE behavior exists.

## Scope

This feature adds:

```text
docs/agents/agent-manifest-schema-source-of-truth.md
docs/architecture/agent-manifest-schema-source-of-truth.md
docs/qa/agent-manifest-schema-source-of-truth.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify Rust
source, Cargo dependencies, lockfiles, package managers, generated schemas,
validators, runtime startup, agent execution, dependency installation,
sandboxing, registry storage, scheduler behavior, worker startup, model
loading, inference, installer, updater, rewards, wallet, marketplace, public
beta, mainnet, or service definitions.

`AGENT-MANIFEST-PARSER-VALIDATOR-001` later implements the root
`package_manifest` source chain in `iamine-agents`. This historical feature
remains documentation-only, and every non-root schema family remains separately
owned.

## Source Of Truth Role

The source-of-truth contract defines ownership and derivation order:

```text
Rust types -> generated JSON Schema -> YAML authoring validation -> JSON payloads
```

Docs describe the policy but are not the executable source of truth. YAML files
are human-authored inputs, not schema authority. JSON payloads are exchange
formats, not source definitions.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Language placement | `AGENT-LANGUAGE-POLICY-001` | no |
| Dependency class policy | `AGENT-DEPENDENCY-POLICY-001` | no |
| Runtime language availability | `AGENT-RUNTIME-LANGUAGE-MATRIX-001` | no |
| Manifest schema source policy | `AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001` | yes |
| Runtime execution | `AGENT-RUNTIME-BASELINE-001` and later | no |
| Sandbox behavior | `AGENT-RUNTIME-SANDBOX-001` | no |

## Non-Bypass Rules

- Schema source policy cannot authorize package installation.
- Schema source policy cannot authorize runtime execution.
- Schema source policy cannot generate schemas in this feature.
- Schema source policy cannot run validators in this feature.
- Schema source policy cannot run package managers.
- Schema source policy cannot install dependencies.
- Schema source policy cannot create sandbox availability.
- Schema source policy cannot grant permissions.
- Schema source policy cannot replace audit evidence.
- Schema source policy cannot replace boundary evals.
- Schema source policy cannot replace local registry review.
- Schema source policy cannot expand scope.
- Schema source policy cannot create capabilities.
- Schema source policy cannot select nodes or models.
- Schema source policy cannot imply public registry or marketplace publication.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, divergent,
or privacy-invasive schema source metadata must block local registry review
advancement, install, and execution by default.

Examples:

- unknown schema source policy;
- YAML treated as source of truth;
- hand-maintained JSON Schema treated as source of truth;
- generated schema divergence from Rust types;
- runtime payloads used before runtime gates;
- generator execution requested;
- validator execution requested;
- package manager execution requested;
- credential, key, host identifier, private path, or secret collection.

## Integration

This feature consumes:

```text
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-REGISTRY-LOCAL-001
AGENT-LANGUAGE-POLICY-001
AGENT-DEPENDENCY-POLICY-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
```

It feeds:

```text
AGENT-RUNTIME-BASELINE-001
AGENT-SKELETON-GENERATOR-001
AGENT-TEMPLATE-VALIDATION-001
```

## Risks

- Treating docs or YAML as source of truth would create schema drift.
- Hand-maintaining JSON Schema without generated artifacts would make
  validation inconsistent.
- Adding generator code or dependencies in this feature would violate the
  docs-only boundary.

## Recommendation

Keep this feature documentation-only. Later implementation must introduce Rust
types, generated JSON Schema, validation commands, and drift checks through
their own owner modules and gates.
