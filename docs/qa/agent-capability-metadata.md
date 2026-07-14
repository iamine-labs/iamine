# IAMINE Agent Capability Metadata QA

Feature:

```text
AGENT-CAPABILITY-METADATA-001
```

## Objective

Validate that the agent capability metadata contract is roadmap-aligned,
documentation-only, scope-bound, privacy-safe, and does not authorize runtime,
permission, scheduler, reputation, reward, or routing behavior.

## Identity

Record before QA:

```text
Branch: feature/agent-capability-metadata-001
HEAD before implementation: 0a4f41c678c1c715a1e19e44b4f405b1c8754d8d
Tree before implementation: 5def1148ec4582adaf4a9fa08afac6d6b39cc8a5
Base: origin/develop
origin/develop: 0a4f41c678c1c715a1e19e44b4f405b1c8754d8d
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: expected new feature docs only
untracked files:
- docs/agents/agent-capability-metadata.md
- docs/architecture/agent-capability-metadata.md
- docs/qa/agent-capability-metadata.md
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-capability-metadata.md
docs/architecture/agent-capability-metadata.md
docs/qa/agent-capability-metadata.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, executable
agent packages, package parser, scope parser, capability parser, permission
enforcement, sandboxing, audit logging, registry runtime, router, scheduler,
worker behavior, model policy, inference execution, installer, updater,
rollback, reputation, rewards, wallet, marketplace, public beta, or mainnet
behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-CAPABILITY-METADATA-001" docs/agents/agent-capability-metadata.md docs/architecture/agent-capability-metadata.md docs/qa/agent-capability-metadata.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "documentation-only|does not authorize executable|does not modify|runtime behavior change" docs/agents/agent-capability-metadata.md docs/architecture/agent-capability-metadata.md docs/qa/agent-capability-metadata.md
rg -n "iamine.agent.capabilities.draft-0.1|metadata/agent-capabilities.toml|declared_task_types|operations|input_classes|output_classes|execution_modes|limitations|risk_profile" docs/agents/agent-capability-metadata.md docs/architecture/agent-capability-metadata.md
rg -n "cannot expand scope|cannot grant permissions|cannot authorize execution|cannot imply scheduler priority|cannot imply trust|cannot claim distributed model MoE" docs/architecture/agent-capability-metadata.md
rg -n "credentials|private keys|wallet keys|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|unrestricted filesystem|arbitrary shell|unrestricted network" docs/agents/agent-capability-metadata.md docs/architecture/agent-capability-metadata.md
rg -n "AGENT-CAPABILITY-METADATA-001 \\| ACTIVE|AGENT-EXPERTISE-METADATA-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
git diff --name-only origin/develop..HEAD
```

Expected:

- roadmap marks this feature active while implementation evidence is being
  prepared;
- capability metadata draft schema and skeleton path are documented;
- capability metadata remains separate from scope, permissions, resources,
  expertise, audit, evals, scheduler, reputation, and runtime;
- the feature does not authorize execution, parsing, generation, validation,
  routing, or runtime integration;
- privacy-sensitive identifiers and secrets remain prohibited;
- no source code or dependency changes are present.

## Quality Gate Policy

This feature is documentation-only. Use focused documentation checks plus
`cargo fmt` unless Architecture asks for the full workspace gate.

## Observed Local Results

```text
git diff --check: PASS
git diff --cached --check: PASS after final staging
cargo fmt --all -- --check: PASS
feature presence scan: PASS
runtime boundary scan: PASS
capability schema and field scan: PASS
non-bypass scan: PASS
privacy and blocked-mode scan: PASS
roadmap ACTIVE state scan: PASS
file size guard: PASS; no Rust source growth
main.rs: unchanged
cluster_registry.rs: unchanged
```

## Field QA Decision

Field QA is not required for this documentation-only capability metadata
feature because no runtime, agent execution, installer, updater, P2P, worker,
scheduler, inference, model, service-manager, marketplace, reward, or
public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the agent capability metadata contract is documented;
- capability declarations remain separate from scope, permissions, expertise,
  resources, audit, evals, scheduler, reputation, and runtime contracts;
- execution, parsing, generation, validation, routing, and runtime integration
  stay unauthorized;
- unsafe, broad, missing, or contradictory capability metadata blocks install,
  registry admission, and execution by default;
- next feature remains `AGENT-EXPERTISE-METADATA-001`;
- agent runtime remains blocked;
- public beta remains blocked.
