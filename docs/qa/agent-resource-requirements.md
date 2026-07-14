# IAMINE Agent Resource Requirements QA

Feature:

```text
AGENT-RESOURCE-REQUIREMENTS-001
```

## Objective

Validate that the agent resource requirements contract is roadmap-aligned,
documentation-only, scope-bound, privacy-safe, bounded, and does not authorize
runtime, scheduler placement, worker startup, hardware profiling, model loading,
backend selection, reputation, reward, marketplace, or distributed model MoE
behavior.

## Identity

Record before QA:

```text
Branch: feature/agent-resource-requirements-001
HEAD before implementation: 0a7ef9ad482a315bcfe134cab3a7e4db13847309
Tree before implementation: 90105de1374443caa01ca7c7ec8ca4159d0f5212
Base: origin/develop
origin/develop: 0a7ef9ad482a315bcfe134cab3a7e4db13847309
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: expected new feature docs only
untracked files:
- docs/agents/agent-resource-requirements.md
- docs/architecture/agent-resource-requirements.md
- docs/qa/agent-resource-requirements.md
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-resource-requirements.md
docs/architecture/agent-resource-requirements.md
docs/qa/agent-resource-requirements.md
docs/agents/agent-package-manifest.md
docs/architecture/agent-package-manifest.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, executable
agent packages, package parser, scope parser, capability parser, expertise
parser, resource parser, permission enforcement, sandboxing, audit logging,
registry runtime, router, scheduler, worker behavior, hardware profiler, model
policy, inference execution, installer, updater, rollback, reputation, rewards,
wallet, marketplace, public beta, or mainnet behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-RESOURCE-REQUIREMENTS-001" docs/agents/agent-resource-requirements.md docs/architecture/agent-resource-requirements.md docs/qa/agent-resource-requirements.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "documentation-only|does not authorize executable|does not modify|runtime behavior change" docs/agents/agent-resource-requirements.md docs/architecture/agent-resource-requirements.md docs/qa/agent-resource-requirements.md
rg -n "iamine.agent.resources.draft-0.1|metadata/agent-resources.toml|operating_modes|cpu|memory|storage|network|model_dependencies|accelerators|constraints|degradation" docs/agents/agent-resource-requirements.md docs/architecture/agent-resource-requirements.md
rg -n "cannot expand scope|cannot grant permissions|cannot authorize execution|cannot start workers|cannot run hardware probes|cannot load or download models|cannot imply scheduler priority|cannot imply node compatibility" docs/architecture/agent-resource-requirements.md
rg -n "credentials|private keys|wallet keys|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|permanent hardware fingerprints|distributed model MoE" docs/agents/agent-resource-requirements.md docs/architecture/agent-resource-requirements.md
rg -n "resource_requirements = \"metadata/agent-resources.toml\"|metadata/agent-resources.toml" docs/agents/agent-package-manifest.md docs/architecture/agent-package-manifest.md docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md
rg -n "AGENT-RESOURCE-REQUIREMENTS-001 \\| ACTIVE|AGENT-PERMISSION-MODEL-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
git diff --name-only origin/develop..HEAD
```

Expected:

- roadmap marks this feature active while implementation evidence is being
  prepared;
- resource requirements draft schema and skeleton path are documented;
- package manifest references use `metadata/agent-resources.toml`;
- resource requirements remain separate from scope, capabilities, expertise,
  permissions, audit, evals, hardware profiling, scheduler, placement, model
  gates, reputation, routing, and runtime;
- the feature does not authorize execution, parsing, generation, validation,
  scheduling, hardware probing, model loading, backend selection, or runtime
  integration;
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
resource schema and field scan: PASS
non-bypass scan: PASS
privacy and blocked-claim scan: PASS
package manifest reference scan: PASS
roadmap ACTIVE state scan: PASS
file size guard: PASS; no Rust source growth
main.rs: unchanged
cluster_registry.rs: unchanged
```

## Field QA Decision

Field QA is not required for this documentation-only resource requirements
feature because no runtime, agent execution, installer, updater, P2P, worker,
scheduler, hardware profiler, inference, model, service-manager, marketplace,
reward, or public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the agent resource requirements contract is documented;
- resource declarations remain separate from scope, capabilities, expertise,
  permissions, audit, evals, hardware profiling, scheduler, model gates,
  reputation, routing, and runtime contracts;
- execution, parsing, generation, validation, scheduling, hardware probing,
  model loading, backend selection, and runtime integration stay unauthorized;
- unsafe, broad, missing, stale, contradictory, or unbounded resource metadata
  blocks install, registry admission, and execution by default;
- next feature remains `AGENT-PERMISSION-MODEL-001`;
- agent runtime remains blocked;
- public beta remains blocked.
