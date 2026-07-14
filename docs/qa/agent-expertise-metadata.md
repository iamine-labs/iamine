# IAMINE Agent Expertise Metadata QA

Feature:

```text
AGENT-EXPERTISE-METADATA-001
```

## Objective

Validate that the agent expertise metadata contract is roadmap-aligned,
documentation-only, scope-bound, privacy-safe, non-promissory, and does not
authorize runtime, permission, scheduler, reputation, reward, certification,
routing, model backend, or distributed model MoE behavior.

## Identity

Record before QA:

```text
Branch: feature/agent-expertise-metadata-001
HEAD before implementation: 69295db12e6012bc205e07e3556143d91d1cace8
Tree before implementation: 06cc8ef827d71e49a1b2fb06af85fcceb5a3c7a7
Base: origin/develop
origin/develop: 69295db12e6012bc205e07e3556143d91d1cace8
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: expected new feature docs only
untracked files:
- docs/agents/agent-expertise-metadata.md
- docs/architecture/agent-expertise-metadata.md
- docs/qa/agent-expertise-metadata.md
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-expertise-metadata.md
docs/architecture/agent-expertise-metadata.md
docs/qa/agent-expertise-metadata.md
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
parser, permission enforcement, sandboxing, audit logging, registry runtime,
router, scheduler, worker behavior, model policy, inference execution,
installer, updater, rollback, reputation, rewards, wallet, marketplace, public
beta, or mainnet behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-EXPERTISE-METADATA-001" docs/agents/agent-expertise-metadata.md docs/architecture/agent-expertise-metadata.md docs/qa/agent-expertise-metadata.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "documentation-only|does not authorize executable|does not modify|runtime behavior change" docs/agents/agent-expertise-metadata.md docs/architecture/agent-expertise-metadata.md docs/qa/agent-expertise-metadata.md
rg -n "iamine.agent.expertise.draft-0.1|metadata/agent-expertise.toml|task_families|supported_capabilities|expertise_claims|evidence|evaluation_requirements|limitations|freshness" docs/agents/agent-expertise-metadata.md docs/architecture/agent-expertise-metadata.md
rg -n "cannot expand scope|cannot create capabilities|cannot grant permissions|cannot authorize execution|cannot imply scheduler priority|cannot imply trust|cannot claim distributed model MoE" docs/architecture/agent-expertise-metadata.md
rg -n "credentials|private keys|wallet keys|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|unrestricted filesystem|distributed model MoE" docs/agents/agent-expertise-metadata.md docs/architecture/agent-expertise-metadata.md
rg -n "expertise_metadata = \"agent-expertise.toml\"|metadata/agent-expertise.toml" docs/agents/agent-package-manifest.md docs/architecture/agent-package-manifest.md docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md
rg -n "AGENT-EXPERTISE-METADATA-001 \\| ACTIVE|AGENT-RESOURCE-REQUIREMENTS-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
git diff --name-only origin/develop..HEAD
```

Expected:

- roadmap marks this feature active while implementation evidence is being
  prepared;
- expertise metadata draft schema and skeleton path are documented;
- package manifest references include expertise metadata;
- expertise metadata remains separate from scope, capabilities, permissions,
  resources, audit, evals, scheduler, reputation, routing, and runtime;
- the feature does not authorize execution, parsing, generation, validation,
  routing, model backend selection, or runtime integration;
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
expertise schema and field scan: PASS
non-bypass scan: PASS
privacy and blocked-claim scan: PASS
package manifest reference scan: PASS
roadmap ACTIVE state scan: PASS
file size guard: PASS; no Rust source growth
main.rs: unchanged
cluster_registry.rs: unchanged
```

## Field QA Decision

Field QA is not required for this documentation-only expertise metadata feature
because no runtime, agent execution, installer, updater, P2P, worker,
scheduler, inference, model, service-manager, marketplace, reward, or
public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the agent expertise metadata contract is documented;
- expertise declarations remain separate from scope, capabilities, permissions,
  resources, audit, evals, scheduler, reputation, routing, and runtime
  contracts;
- execution, parsing, generation, validation, routing, model backend selection,
  and runtime integration stay unauthorized;
- unsafe, broad, missing, stale, or contradictory expertise metadata blocks
  install, registry admission, and execution by default;
- next feature remains `AGENT-RESOURCE-REQUIREMENTS-001`;
- agent runtime remains blocked;
- public beta remains blocked.
