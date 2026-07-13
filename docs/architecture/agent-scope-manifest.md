# AGENT-SCOPE-MANIFEST-001

## Objective

Define the first IAMINE agent scope manifest contract without enabling runtime
scope enforcement.

## Scope

This feature adds:

```text
docs/agents/agent-scope-manifest.md
docs/architecture/agent-scope-manifest.md
docs/qa/agent-scope-manifest.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- package manifest parser;
- scope manifest parser;
- scope enforcement;
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

## Scope Boundary

The scope manifest defines:

- in-scope tasks;
- out-of-scope tasks;
- supported task types;
- allowed and forbidden inputs;
- allowed operations;
- blocked actions;
- future permission categories;
- confirmation boundaries;
- handoff targets;
- orchestrator return conditions;
- future eval requirements.

It must not own:

- permission grants;
- sandbox behavior;
- audit behavior;
- runtime state transitions;
- capability scoring;
- hardware scheduling;
- registry admission;
- marketplace publication.

Each of those remains owned by later roadmap features.

## Draft Schema

The initial documentation schema is:

```text
iamine.agent.scope.draft-0.1
```

Default file name:

```text
agent-scope.toml
```

This schema is not executable until a later implementation feature adds parser,
validation, and runtime integration.

## Integration

This feature consumes:

```text
AGENT-PACKAGE-MANIFEST-001
```

It feeds:

```text
AGENT-CAPABILITY-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
AGENT-AUDIT-LOG-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-RUNTIME-BASELINE-001
```

## Required Scope Guarantees

The scope manifest contract must guarantee:

- explicit in-scope tasks;
- explicit out-of-scope tasks;
- explicit blocked actions;
- explicit supported task types;
- explicit allowed and forbidden inputs;
- no credentials, secrets, host identifiers, or private paths;
- no arbitrary shell, unrestricted filesystem, service mutation, network
  mutation, marketplace publication, wallet, reward, settlement, or mainnet
  behavior;
- explicit handoff targets;
- explicit orchestrator return conditions;
- positive and negative boundary eval classes required before execution;
- scope cannot self-approve execution.

## Failure Policy

Missing, unknown, contradictory, broad, or unsafe scope metadata must block
install and execution by default.

Examples:

- missing `out_of_scope`;
- missing `blocked_actions`;
- missing `handoff`;
- missing `orchestrator_return`;
- missing `eval_requirements`;
- broad `scope_id`;
- broad `task_types`;
- allowed shell operation;
- credential or private data collection;
- `scope_can_self_approve = true`.

## Risks

- Treating the scope manifest as a permission grant would bypass later
  permission review.
- Treating handoff as optional would allow silent scope expansion.
- Allowing generic task types would weaken the scope-bound agent rule.
- Adding parser or enforcement behavior in this feature would jump ahead of
  permission, audit, boundary-eval, and runtime gates.
- Letting confirmation override blocked actions would create unsafe behavior.

## Recommendation

If QA confirms this remains documentation-only and roadmap-aligned, proceed to:

```text
AGENT-CAPABILITY-METADATA-001
```
