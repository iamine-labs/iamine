# IAMINE Agent Skeleton Standard QA

Feature:

```text
AGENT-SKELETON-STANDARD-001
```

## Objective

Validate that the agent skeleton standard is roadmap-aligned,
documentation-only, scope-bound, privacy-safe, and does not authorize runtime
behavior.

## Identity

Record before QA:

```text
Branch: feature/agent-skeleton-standard-001
HEAD before implementation: b3887001ebabdefbeb1acd2abc1444ead114f47f
Tree before implementation: b7ff5984bc916a6d695a38b6e1908b341a3dde14
Base: origin/develop
origin/develop: b3887001ebabdefbeb1acd2abc1444ead114f47f
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: expected new feature docs only
untracked files:
- docs/agents/agent-skeleton-standard.md
- docs/architecture/agent-skeleton-standard.md
- docs/qa/agent-skeleton-standard.md
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-skeleton-standard.md
docs/architecture/agent-skeleton-standard.md
docs/qa/agent-skeleton-standard.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, executable
agent packages, skeleton generator, package parser, scope parser, permission
enforcement, sandboxing, audit logging, registry runtime, P2P, PubSub,
scheduler, worker behavior, model policy, inference execution, installer,
updater, rollback, reputation, rewards, wallet, marketplace, public beta, or
mainnet behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-SKELETON-STANDARD-001" docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md docs/qa/agent-skeleton-standard.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "documentation-only|does not authorize executable|does not modify|runtime behavior change" docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md docs/qa/agent-skeleton-standard.md
rg -n "iamine-agent-package.toml|agent-scope.toml|metadata/agent-capabilities.toml|metadata/agent-expertise.toml|metadata/agent-resources.toml|metadata/agent-permissions.toml|metadata/agent-audit.toml|evals/agent-boundary-tests.toml" docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md
rg -n "package-relative|absolute local paths|src/|non-executable placeholders|reserved" docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md
rg -n "credentials|private keys|wallet keys|host identifiers|private paths|arbitrary shell|unrestricted filesystem|unrestricted network|service mutation" docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md
rg -n "AGENT-SKELETON-STANDARD-001 \\| ACTIVE|AGENT-CAPABILITY-METADATA-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
git diff --name-only origin/develop..HEAD
```

Expected:

- roadmap marks this feature active while implementation evidence is being
  prepared;
- skeleton layout is documented;
- package, scope, metadata, permission, audit, eval, review, and runtime
  responsibilities remain separate;
- the `src/` directory is reserved and non-executable in this phase;
- the feature does not authorize execution, generation, validation, or runtime
  integration;
- privacy-sensitive identifiers and secrets remain prohibited;
- no source code changes are present.

## Quality Gate Policy

This feature is documentation-only. Use focused documentation checks plus
`cargo fmt` unless Architecture asks for the full workspace gate.

## Observed Local Results

```text
git diff --check: PASS
cargo fmt --all -- --check: PASS
feature presence scan: PASS
runtime boundary scan: PASS
skeleton layout scan: PASS
package-relative and src policy scan: PASS
privacy and blocked-mode scan: PASS
roadmap ACTIVE state scan: PASS
```

File-size check:

```text
docs/architecture/agent-skeleton-standard.md: 206 lines
docs/agents/agent-skeleton-standard.md: 150 lines
docs/qa/agent-skeleton-standard.md: 133 lines after result entry
iamine-node/src/main.rs: 4929 lines
iamine-node/src/cluster_registry.rs: 862 lines
```

## Field QA Decision

Field QA is not required for this documentation-only skeleton standard feature
because no runtime, agent execution, installer, updater, P2P, worker,
scheduler, inference, model, service-manager, marketplace, reward, or
public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the agent skeleton layout is documented;
- skeleton placement remains separate from package, scope, permission, audit,
  eval, registry, and runtime contracts;
- execution, generation, validation, and runtime integration stay unauthorized;
- unsafe, broad, missing, or contradictory skeleton metadata blocks install,
  registry admission, and execution by default;
- next feature remains `AGENT-CAPABILITY-METADATA-001`;
- agent runtime remains blocked;
- public beta remains blocked.
