# AGENT-PACKAGE-MANIFEST-001

## Objective

Define the first IAMINE agent package manifest contract without enabling agent
execution.

## Scope

This feature adds:

```text
docs/agents/agent-package-manifest.md
docs/architecture/agent-package-manifest.md
docs/qa/agent-package-manifest.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- package manifest parser;
- package installer;
- scope manifest parser;
- capability metadata parser;
- resource requirement enforcement;
- permission enforcement;
- sandboxing;
- audit logs;
- agent registry;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, worker, scheduler, model, inference, installer, updater,
  rollback, reputation, reward, wallet, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts, or
  package generation.

## Manifest Boundary

The package manifest defines package identity and required references only. It
must not own:

- scope semantics;
- permission grants;
- runtime states;
- sandbox policy;
- audit behavior;
- capability scoring;
- hardware scheduling;
- registry admission;
- marketplace publication.

Each of those remains owned by later roadmap features.

## Draft Schema

The initial documentation schema is:

```text
iamine.agent.package.draft-0.1
```

Default file name:

```text
agent.yaml
```

`AGENT-MANIFEST-PARSER-VALIDATOR-001` now provides the root YAML parser and
metadata validation in `iamine-agents`. The schema remains non-executable:
package loading, referenced-contract validation, and runtime integration are
still separate blocked features.

## Integration

This feature consumes:

```text
AGENT-BETA-PACK-SELECTION-001
```

It feeds:

```text
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
```

## Required Manifest Guarantees

The manifest contract must guarantee:

- stable package identity without local host identity;
- explicit package version;
- explicit official pack membership;
- `execution_authorized = false` during this phase;
- bounded agent family and earliest mode;
- required package-relative references to future scope, capability, expertise,
  resource, permission, audit, and boundary-test contracts;
- child reference extensions and formats remain owned by their respective
  parser features and are not inferred by the root manifest or load gate;
- public beta, marketplace, and third-party publication disabled;
- no credentials, destructive actions, arbitrary shell, or unrestricted
  filesystem access;
- human review cannot be bypassed.

## Failure Policy

Missing, unknown, contradictory, or unsafe manifest metadata must block install
and execution by default.

Examples:

- unknown schema;
- missing `scope_manifest`;
- missing `expertise_metadata`;
- missing `resource_requirements`;
- missing `permission_model`;
- missing `boundary_tests`;
- `execution_authorized = true`;
- public marketplace channel;
- broad assistant family;
- secret-bearing package ID;
- private local path reference.

## Risks

- Treating the package manifest as a permission model would bypass later
  permission review.
- Treating references as optional would allow broad unreviewed packages.
- Adding executable parser behavior in this feature would jump ahead of scope,
  permission, audit, and boundary-eval gates.
- Allowing broad agent families would weaken the scope-bound agent rule.
- Putting host identity or private paths into package IDs would violate privacy
  rules.

## Recommendation

If QA confirms this remains documentation-only and roadmap-aligned, proceed to:

```text
AGENT-SCOPE-MANIFEST-001
```
