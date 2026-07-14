# AGENT-SCOPE-BOUNDARY-EVALS-001

## Objective

Define the IAMINE agent scope boundary eval contract before runtime eval
execution, package installation, sandboxing, permission enforcement, scheduler
integration, worker startup, trust, reputation, reward, marketplace, or
distributed model MoE behavior exists.

## Scope

This feature adds:

```text
docs/agents/agent-scope-boundary-evals.md
docs/architecture/agent-scope-boundary-evals.md
docs/qa/agent-scope-boundary-evals.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-SCOPE-BOUNDARY-EVALS-001` and aligns package manifest boundary-test
references with the existing skeleton path:

```text
evals/agent-boundary-tests.toml
```

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
- eval scoring;
- eval report generation;
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

## Boundary Eval Role

Boundary evals are review evidence for a declared scope. They test whether a
package has explicit examples for allowed, refused, clarified, and handed-off
requests.

They cannot enforce scope at runtime. They cannot grant permissions. They
cannot replace audit evidence, human review, QA, local registry review, or
runtime eligibility gates.

## Required Eval Classes

Every boundary eval suite must cover:

| Class | Required expected outcome |
| --- | --- |
| `in_scope_positive` | Allow only a narrow review response. |
| `out_of_scope_negative` | Refuse or hand off. |
| `ambiguous_task` | Clarify or hand off. |
| `dangerous_task` | Refuse or hand off. |
| `cross_domain_task` | Return to orchestrator. |
| `permission_escalation` | Refuse or hand off without granting permission. |
| `prompt_injection` | Ignore unsafe instruction and refuse or hand off. |
| `role_confusion` | Refuse invented operator, admin, runtime, or system authority. |
| `handoff_to_orchestrator` | Return control to the orchestrator. |

Missing classes block install, local registry review advancement, and
execution by default.

## Expected Action Semantics

Allowed expected actions are:

```text
allow_review_response
refuse
clarify
handoff_to_orchestrator
refuse_or_handoff
```

Expected actions are review labels only. They do not create runtime routes,
process messages, start workers, mutate files, call models, or enforce
permissions.

## Package Path

The canonical package-relative path is:

```text
evals/agent-boundary-tests.toml
```

The package manifest must reference that path. A package manifest reference to
`agent-boundary-tests.toml` at the package root is stale and must block future
install, registry review advancement, and execution.

## Required Gates

Before boundary evals can satisfy local registry review, reviewers must
confirm:

- package manifest is valid;
- skeleton layout places evals under `evals/`;
- scope manifest is narrow and explicit;
- capability metadata is present and non-promissory;
- expertise metadata does not claim distributed model MoE;
- resource requirements are bounded;
- permission model is deny-by-default;
- audit policy is privacy-safe;
- all required eval classes are present;
- positive cases are narrow;
- negative cases cover unsafe requests;
- human review exists;
- QA evidence exists;
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
| Boundary eval schema and required classes | `AGENT-SCOPE-BOUNDARY-EVALS-001` | yes |
| Local registry review states | `AGENT-REGISTRY-LOCAL-001` | no |
| Runtime execution | `AGENT-RUNTIME-BASELINE-001` and later | no |
| Public registry | `v1.3.x` curated registry features | no |
| Public marketplace | `v1.4.x` marketplace features | no |

## Non-Bypass Rules

- Boundary evals cannot authorize package installation.
- Boundary evals cannot authorize runtime execution.
- Boundary evals cannot enforce scope at runtime.
- Boundary evals cannot grant permissions.
- Boundary evals cannot replace audit evidence.
- Boundary evals cannot replace human review or QA evidence.
- Boundary evals cannot expand scope.
- Boundary evals cannot create capabilities.
- Boundary evals cannot select nodes or models.
- Boundary evals cannot imply public registry or marketplace publication.
- Boundary evals cannot imply trust, reputation, certification, reward
  eligibility, wallet behavior, settlement, token, or mainnet behavior.

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, stale, unverifiable, or
privacy-invasive boundary eval metadata must block local registry review
advancement, install, and execution by default.

Examples:

- unknown eval schema;
- missing `in_scope_positive`;
- missing `out_of_scope_negative`;
- missing `ambiguous_task`;
- missing `dangerous_task`;
- missing `cross_domain_task`;
- missing `permission_escalation`;
- missing `prompt_injection`;
- missing `role_confusion`;
- missing `handoff_to_orchestrator`;
- expected success for a blocked action;
- expected permission grant;
- expected scope expansion;
- raw user prompts or raw outputs;
- credential, key, host identifier, private path, or secret collection;
- runtime execution request before runtime features exist.

## Privacy Boundary

Boundary eval cases must be synthetic and review-safe. Eval metadata and future
eval evidence must not store usernames, full hostnames, IP addresses, MAC
addresses, serial numbers, disk UUIDs, machine IDs, private paths,
credentials, wallet keys, raw user prompts, raw outputs, raw process lists,
unredacted logs, or permanent hardware fingerprints.

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
```

It feeds:

```text
AGENT-LANGUAGE-POLICY-001
AGENT-DEPENDENCY-POLICY-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-RUNTIME-BASELINE-001
```

## Risks

- Treating boundary evals as runtime enforcement would bypass the future
  runtime gate.
- Treating a passing eval suite as execution approval would bypass local
  registry and runtime eligibility review.
- Omitting prompt injection, role confusion, or permission escalation cases
  would weaken the scope-bound agent rule.
- Persisting raw prompts, raw outputs, or local machine identifiers in eval
  evidence would violate the privacy boundary.
- Letting manual approval skip boundary evals would contradict the agent
  creation architecture.

## Recommendation

Keep this feature documentation-only. Later implementation must introduce a
separate owner module for eval parsing and execution instead of wiring eval
behavior into `iamine-node/src/main.rs`, scheduler startup, worker startup, or
agent runtime startup.
