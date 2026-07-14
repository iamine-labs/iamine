# AGENT-LANGUAGE-POLICY-001

## Objective

Define the IAMINE agent language policy by layer and release phase before
runtime language execution, dependency installation, package installation,
sandboxing, registry admission, scheduler integration, worker startup, trust,
reputation, reward, marketplace, or distributed model MoE behavior exists.

## Scope

This feature adds:

```text
docs/agents/agent-language-policy.md
docs/architecture/agent-language-policy.md
docs/qa/agent-language-policy.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-LANGUAGE-POLICY-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- package installation;
- package manager integration;
- dependency installation;
- agent skeleton generator;
- package manifest parser;
- scope manifest parser;
- capability metadata parser;
- expertise metadata parser;
- resource requirement parser;
- permission parser or runtime permission enforcement;
- audit parser or runtime audit logging;
- boundary eval parser or runner;
- registry storage implementation;
- registry synchronization;
- registry publication;
- sandboxing;
- router or scheduler behavior;
- worker startup or worker capability advertisement;
- model routing, model loading, backend selection, inference execution, or
  distributed inference behavior;
- hardware profiler behavior or hardware compatibility gates;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, installer, updater, rollback, reputation, reward, wallet,
  settlement, token, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts,
  dependencies, package generation, or schema generation.

## Language Policy Role

The language policy is a review contract for which languages may be considered
by which IAMINE layer and roadmap phase.

It cannot run code. It cannot install dependencies. It cannot choose a runtime
for a package. It cannot replace dependency policy, sandbox policy, runtime
language matrix, local registry review, human review, QA, or runtime
eligibility gates.

## Layer Placement

| Layer | Allowed language policy |
| --- | --- |
| IAMINE core protocol and node behavior | Rust only. |
| Runtime, scheduler, worker, CLI, validators, contracts | Rust only. |
| Official P0 agent implementation | Rust default once later runtime gates authorize execution. |
| Audit, local registry, file/network/system agents | Rust default for IAMINE-owned implementation. |
| Public SDKs | Python and TypeScript deferred to developer-platform phases. |
| Web/API/dashboard/tooling/connectors | TypeScript deferred to tooling phases. |
| AI/dev tooling, prototypes, OCR/classification, heavy integrations | Python deferred; sandbox and dependency policy required. |
| Third-party lightweight agents | WASM/WASI preferred future sandbox direction. |
| Heavy third-party agents | Containers deferred until registry, sandbox, permission, dependency, and runtime matrix gates mature. |

## Release Phase Rules

- v0.11.x is documentation-only for agent contracts.
- v0.12.x may consider Rust for official P0 agents only after separate runtime,
  dependency, sandbox, permission, audit, boundary-eval, and registry gates
  authorize it.
- v0.13.x may refine additional official agents, but language support still
  depends on dependency policy and runtime language matrix.
- v1.0.0 Agent Network Public Beta cannot infer language support from this
  document alone.
- v1.2.x may introduce Python and TypeScript public SDK surfaces for developer
  tooling, not automatic runtime execution.
- v1.3.x curated registry must validate language, dependency, sandbox, review,
  and runtime policy.
- v1.4.x marketplace publication remains a separate gate.
- v2.0.x mainnet, wallet, settlement, token, open marketplace, and real economy
  behavior remain separate gates.

## Manifest Format Boundary

The metadata format policy remains:

```text
Authoring: YAML
Internal representation: Rust structs
Validation: generated JSON Schema
Runtime/API payloads: JSON
Source of truth: Rust types
```

This feature does not add `serde`, `serde_json`, `serde_yaml`, `schemars`,
`jsonschema`, or any other dependency. Dependency changes must wait for the
dependency-policy and schema source-of-truth features.

## Ownership

| Area | Owner | This feature owns it? |
| --- | --- | --- |
| Package identity and references | `AGENT-PACKAGE-MANIFEST-001` | no |
| Skeleton layout | `AGENT-SKELETON-STANDARD-001` | no |
| Scope boundary | `AGENT-SCOPE-MANIFEST-001` | no |
| Capability metadata | `AGENT-CAPABILITY-METADATA-001` | no |
| Expertise metadata | `AGENT-EXPERTISE-METADATA-001` | no |
| Resource requirements | `AGENT-RESOURCE-REQUIREMENTS-001` | no |
| Permission categories | `AGENT-PERMISSION-MODEL-001` | no |
| Audit evidence | `AGENT-AUDIT-LOG-001` | no |
| Boundary eval schema | `AGENT-SCOPE-BOUNDARY-EVALS-001` | no |
| Local registry review states | `AGENT-REGISTRY-LOCAL-001` | no |
| Language placement by layer and phase | `AGENT-LANGUAGE-POLICY-001` | yes |
| Dependency classes | `AGENT-DEPENDENCY-POLICY-001` | no |
| Runtime language availability | `AGENT-RUNTIME-LANGUAGE-MATRIX-001` | no |
| Manifest schema source of truth | `AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001` | no |
| Runtime execution | `AGENT-RUNTIME-BASELINE-001` and later | no |

## Non-Bypass Rules

- Language policy cannot authorize package installation.
- Language policy cannot authorize runtime execution.
- Language policy cannot select a runtime mode.
- Language policy cannot install dependencies.
- Language policy cannot authorize package managers.
- Language policy cannot create sandbox availability.
- Language policy cannot grant permissions.
- Language policy cannot replace audit evidence.
- Language policy cannot replace boundary evals.
- Language policy cannot replace local registry review.
- Language policy cannot expand scope.
- Language policy cannot create capabilities.
- Language policy cannot select nodes or models.
- Language policy cannot imply public registry or marketplace publication.
- Language policy cannot imply trust, reputation, certification, reward
  eligibility, wallet behavior, settlement, token, or mainnet behavior.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive language metadata must block local registry review
advancement, install, and execution by default.

Examples:

- unknown language policy schema;
- Python listed as current core runtime language;
- TypeScript listed as current worker runtime language;
- WASM/WASI listed as current sandbox availability;
- containers listed as current runtime availability;
- package manager execution requested before dependency policy;
- dependency installation requested before dependency policy;
- sandbox claim before sandbox policy;
- runtime execution request before runtime language matrix;
- public marketplace publication request;
- credential, key, host identifier, private path, or secret collection.

## Privacy Boundary

Language policy metadata must be product and architecture metadata only. It
must not store usernames, full hostnames, IP addresses, MAC addresses, serial
numbers, disk UUIDs, machine IDs, private paths, credentials, wallet keys, raw
user prompts, raw outputs, raw process lists, unredacted logs, or permanent
hardware fingerprints.

## Integration

This feature consumes:

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-SKELETON-STANDARD-001
AGENT-PACKAGE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-SCOPE-MANIFEST-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
```

It feeds:

```text
AGENT-DEPENDENCY-POLICY-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-RUNTIME-BASELINE-001
```

## Risks

- Treating language allowance as runtime availability would bypass the runtime
  language matrix.
- Treating Python, TypeScript, WASM/WASI, or containers as current runtime
  modes would jump ahead of dependency, sandbox, registry, and runtime gates.
- Adding dependencies in this feature would violate the docs-only boundary.
- Allowing package manager execution before dependency policy would create an
  install surface without review.
- Letting language policy override scope, permission, audit, or eval gates
  would weaken the scope-bound agent rule.

## Recommendation

Keep this feature documentation-only. Later implementation must introduce
language parsing, dependency validation, runtime language availability, and
sandbox behavior through their own owner modules and roadmap gates.
