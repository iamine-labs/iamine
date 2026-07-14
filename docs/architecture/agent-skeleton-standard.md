# AGENT-SKELETON-STANDARD-001

## Objective

Define the canonical IAMINE agent skeleton layout before generating,
packaging, validating, or executing agent code.

## Scope

This feature adds:

```text
docs/agents/agent-skeleton-standard.md
docs/architecture/agent-skeleton-standard.md
docs/qa/agent-skeleton-standard.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-SKELETON-STANDARD-001`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- agent skeleton generator;
- package manifest parser;
- scope manifest parser;
- capability metadata parser;
- expertise metadata parser;
- resource requirement parser;
- permission enforcement;
- sandboxing;
- audit log implementation;
- agent registry;
- router or scheduler behavior;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, worker, model, inference, installer, updater, rollback,
  reputation, reward, wallet, settlement, token, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts, or
  package generation.

## Skeleton Role

The skeleton standard defines where reviewable agent metadata belongs. It does
not define whether an agent is safe, executable, trusted, installable, or
publishable.

The skeleton must keep these concerns separate:

- package identity;
- scope boundary;
- capability metadata;
- expertise metadata;
- resource requirements;
- permission requirements;
- audit policy;
- boundary evals;
- implementation source;
- local registry review;
- runtime eligibility.

No skeleton path may replace a later contract.

## Canonical Layout

The canonical future package layout is:

```text
<agent-package>/
  iamine-agent-package.toml
  agent-scope.toml
  README.md
  metadata/
    agent-capabilities.toml
    agent-expertise.toml
    agent-resources.toml
    agent-permissions.toml
    agent-audit.toml
  evals/
    agent-boundary-tests.toml
    README.md
  src/
    README.md
  review/
    human-review.md
    qa-evidence.md
```

During this documentation phase, the layout is a contract only. This feature
does not create executable packages or validate real package directories.

## Required Files

Future skeleton instances must include these package-relative files before
registry or runtime review:

| Path | Owner contract | Required before execution |
| --- | --- | --- |
| `iamine-agent-package.toml` | `AGENT-PACKAGE-MANIFEST-001` | yes |
| `agent-scope.toml` | `AGENT-SCOPE-MANIFEST-001` | yes |
| `metadata/agent-capabilities.toml` | `AGENT-CAPABILITY-METADATA-001` | yes |
| `metadata/agent-expertise.toml` | `AGENT-EXPERTISE-METADATA-001` | yes |
| `metadata/agent-resources.toml` | `AGENT-RESOURCE-REQUIREMENTS-001` | yes |
| `metadata/agent-permissions.toml` | `AGENT-PERMISSION-MODEL-001` | yes |
| `metadata/agent-audit.toml` | `AGENT-AUDIT-LOG-001` | yes |
| `evals/agent-boundary-tests.toml` | `AGENT-SCOPE-BOUNDARY-EVALS-001` | yes |
| `review/human-review.md` | Architecture / QA | yes |
| `review/qa-evidence.md` | QA | yes |

If any required file is missing, unknown, contradictory, broad, unsafe, or
unparseable by its future owning feature, install, registry admission, and
execution must remain blocked by default.

## Reserved Implementation Area

The `src/` directory is reserved for later runtime and language policy work.
It must not be interpreted as executable in this phase.

Until later features define language support and runtime behavior:

- `src/` may contain only review notes or placeholders;
- no script in `src/` may be run automatically;
- no package may request arbitrary shell execution;
- no package may request unrestricted filesystem or network access;
- no package may mutate services, routers, VMs, containers, wallets, rewards,
  settlement state, or mainnet state.

## Naming Rules

Skeleton paths must be package-relative and stable. They must not include:

- usernames;
- full hostnames;
- IP addresses;
- MAC addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- private paths;
- credentials;
- private keys;
- wallet keys;
- secrets.

Package roots should use lowercase, product-scoped names. Broad names such as
`general-assistant`, `do-anything`, `system-admin`, `all-files`, or
`all-networks` are blocked.

## Non-Bypass Rules

- A skeleton cannot authorize execution.
- A skeleton cannot grant permissions.
- A skeleton cannot expand scope.
- A skeleton cannot make missing metadata optional.
- A skeleton cannot replace boundary evals.
- A skeleton cannot imply registry admission.
- A skeleton cannot imply public marketplace publication.
- A skeleton cannot select a runtime language.
- A skeleton cannot bypass human review.

## Integration

This feature consumes:

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
```

It feeds:

```text
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-SKELETON-GENERATOR-001
```

## Risks

- Treating the skeleton as a generator would add implementation behavior too
  early.
- Treating placeholder files as valid metadata would bypass future schemas.
- Letting `src/` imply executable code would jump ahead of runtime and sandbox
  gates.
- Allowing broad package names would weaken the scope-bound agent rule.
- Allowing absolute paths would create privacy and portability risk.

## Recommendation

If QA confirms this feature remains documentation-only, roadmap-aligned, and
non-executable, proceed to:

```text
AGENT-CAPABILITY-METADATA-001
```
