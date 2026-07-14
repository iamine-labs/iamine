# AGENT-REGISTRY-LOCAL-001

## Objective

Define the local IAMINE agent registry review contract before any public
registry, marketplace, runtime loading, sandboxing, permission enforcement,
scheduler integration, worker startup, trust, reputation, reward, or
distributed model MoE behavior exists.

## Scope

This feature adds:

```text
docs/agents/agent-registry-local.md
docs/architecture/agent-registry-local.md
docs/qa/agent-registry-local.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-REGISTRY-LOCAL-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- package installation;
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

## Local Registry Role

The local registry is an operator-local review ledger. It can record whether a
package is a candidate, under review, blocked, deprecated, or ready for local
registry review once all required contracts pass.

It cannot make a package executable. It cannot publish a package. It cannot
replace scope, permission, audit, boundary eval, human review, QA, or runtime
eligibility gates.

## Required State Model

Allowed review states are:

```text
candidate
under_review
blocked
registry_review_ready
deprecated
```

State rules:

- new packages start as `candidate`;
- incomplete packages move to `under_review` only for evidence gathering;
- unsafe, broad, contradictory, missing, or privacy-invasive metadata moves to
  `blocked`;
- `registry_review_ready` requires all prerequisite contracts to exist and pass;
- `deprecated` records package retirement and cannot imply deletion.

This roadmap currently defines the local registry before the boundary eval
contract. Therefore, this feature can define the `registry_review_ready` state
but cannot allow any package to reach it until
`AGENT-SCOPE-BOUNDARY-EVALS-001` defines the boundary eval contract.

## Required Gates

Before a package can become `registry_review_ready`, local review must confirm:

- package manifest is valid;
- skeleton layout matches the standard;
- scope manifest is narrow and explicit;
- capability metadata is present and non-promissory;
- expertise metadata is present and does not claim distributed model MoE;
- resource requirements are bounded;
- permission model is deny-by-default;
- audit policy is privacy-safe;
- boundary evals exist and pass;
- human review exists;
- QA evidence exists;
- distribution policy remains local and manual;
- public beta, marketplace, third-party publication, and network publication
  remain blocked.

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
| Boundary evals | `AGENT-SCOPE-BOUNDARY-EVALS-001` | no |
| Local registry review states | `AGENT-REGISTRY-LOCAL-001` | yes |
| Runtime execution | `AGENT-RUNTIME-BASELINE-001` and later | no |
| Public registry | `v1.3.x` curated registry features | no |
| Public marketplace | `v1.4.x` marketplace features | no |

## Non-Bypass Rules

- Local registry presence cannot imply public marketplace publication.
- Local registry presence cannot imply package installation.
- Local registry presence cannot imply runtime execution.
- Local registry presence cannot bypass boundary evals.
- Local registry presence cannot bypass human review or QA evidence.
- Local registry presence cannot grant permissions.
- Local registry presence cannot expand scope.
- Local registry presence cannot create capabilities.
- Local registry presence cannot select nodes or models.
- Local registry presence cannot imply trust, reputation, certification,
  reward eligibility, wallet behavior, settlement, token, or mainnet behavior.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive local registry metadata must block registry review
advancement, install, and execution by default.

Examples:

- unknown registry schema;
- missing package manifest result;
- missing scope result;
- missing permission result;
- missing audit result;
- missing boundary eval result;
- public marketplace channel;
- third-party publication request;
- public beta request;
- arbitrary shell request;
- unrestricted filesystem request;
- unrestricted network request;
- credential, key, host identifier, private path, or secret collection;
- raw prompt or raw output storage;
- runtime execution request before runtime features exist.

## Privacy Boundary

Local registry metadata must be operator-local and review-focused. It must not
store usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, credentials, wallet keys, raw prompts,
raw outputs, raw process lists, unredacted logs, or permanent hardware
fingerprints.

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
```

It feeds:

```text
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-LANGUAGE-POLICY-001
AGENT-DEPENDENCY-POLICY-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-RUNTIME-BASELINE-001
```

## Risks

- Treating a local registry entry as execution approval would bypass runtime
  gates.
- Treating a local registry entry as publication approval would bypass public
  registry and marketplace phases.
- Allowing `registry_review_ready` before boundary evals exist would contradict
  the agent creation state model.
- Persisting local machine identifiers in registry metadata would violate the
  privacy boundary.
- Storing review state in package artifacts could make operator-local decisions
  look portable or public.

## Recommendation

Keep this feature documentation-only. Later implementation must introduce a
separate owner module for registry storage and validation instead of wiring
registry behavior into `iamine-node/src/main.rs` or agent runtime startup.
