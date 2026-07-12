# AGENT-USER-PERSONA-MAPPING-001

## Objective

Convert the agent market-fit research segments into explicit user personas and
task contexts that can guide later beta-pack selection.

## Scope

This feature adds:

```text
docs/agents/agent-user-personas.md
docs/architecture/agent-user-persona-mapping.md
docs/qa/agent-user-persona-mapping.md
```

It also updates the Agent Network roadmap state for the active v0.11 research
phase.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- agent manifests;
- scope manifests;
- permission enforcement;
- sandboxing;
- audit logs;
- agent registry;
- beta-pack selection;
- public beta behavior;
- P2P, PubSub, worker, scheduler, model, inference, installer, updater,
  rollback, reputation, reward, wallet, marketplace, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts, or
  package generation.

## Persona Contract

Each persona must define:

- repeated problem;
- likely task context;
- safe first-agent value;
- allowed inspection surface;
- forbidden data;
- blocked actions;
- handoff or refusal triggers;
- beta signal.

Personas are research inputs. They do not select official agents by
themselves.

## Integration

This feature consumes:

```text
AGENT-MARKET-FIT-RESEARCH-001
```

It feeds:

```text
AGENT-BETA-PACK-SELECTION-001
AGENT-SCOPE-MANIFEST-001
AGENT-PERMISSION-MODEL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
```

The next feature may select an official beta pack only after preserving the
scope-bound rule and blocked-action requirements.

## Risks

- Treating personas as validated external research would overstate evidence.
- Treating the persona-to-agent mapping as final beta-pack selection would
  bypass the roadmap.
- Introducing broad personas without blocked actions would weaken the
  scope-bound agent rule.
- Agent runtime implementation before manifests, permissions, audit logs, and
  boundary tests would create unsafe execution paths.

## Recommendation

If QA confirms this remains documentation-only and roadmap-aligned, proceed to:

```text
AGENT-BETA-PACK-SELECTION-001
```
