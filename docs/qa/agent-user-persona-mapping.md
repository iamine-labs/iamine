# IAMINE Agent User Persona Mapping QA

Feature:

```text
AGENT-USER-PERSONA-MAPPING-001
```

## Objective

Validate that the user-persona mapping is scoped, roadmap-aligned, privacy-safe,
and does not select the final beta pack or authorize agent runtime.

## Identity

Record before QA:

```text
Branch: feature/agent-user-persona-mapping-001
HEAD: 10f3385b92f484b80133670e8283ba99b3469fe7
Tree: edee82539275c8297f19c54dba61dac033bebcb7
Base: origin/develop
origin/develop: 10f3385b92f484b80133670e8283ba99b3469fe7
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: none expected in feature worktree
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-user-personas.md
docs/architecture/agent-user-persona-mapping.md
docs/qa/agent-user-persona-mapping.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, agent
execution, agent manifests, permission enforcement, P2P, PubSub, scheduler,
worker behavior, model policy, inference execution, installer, updater,
rollback, reputation, rewards, wallet, marketplace, public beta, or mainnet
behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-USER-PERSONA-MAPPING-001" docs/agents/agent-user-personas.md docs/architecture/agent-user-persona-mapping.md docs/qa/agent-user-persona-mapping.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "research artifact|does not claim completed external user research|not beta-pack selection|not as validated external market evidence" docs/agents/agent-user-personas.md docs/architecture/agent-user-persona-mapping.md
rg -n "scope-bound|Blocked actions|blocked actions|refusal|handoff" docs/agents/agent-user-personas.md docs/architecture/agent-user-persona-mapping.md
rg -n "AGENT-USER-PERSONA-MAPPING-001 \\| ACTIVE|AGENT-BETA-PACK-SELECTION-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
```

Expected:

- roadmap marks this feature active;
- beta-pack selection remains proposed;
- personas are documented as research inputs;
- blocked actions and refusal/handoff triggers are present;
- no source code changes are present.

## Quality Gate Policy

This feature is documentation-only. Use focused documentation checks plus
`cargo fmt` unless Architecture asks for the full workspace gate.

The local environment remains disk constrained from recent full validation
cycles. Re-running `./scripts/quality-gate.sh` for a docs-only persona feature
is not expected to add product evidence unless local capacity is first
remediated.

## Observed Local Results

```text
git diff --check: PASS
cargo fmt --all -- --check: PASS
feature presence scan: PASS
research-artifact / no-external-validation wording scan: PASS
scope-bound, blocked-action, refusal, and handoff scan: PASS
roadmap state scan: PASS
```

File-size check:

```text
docs/agents/agent-user-personas.md: 288 lines
docs/architecture/agent-user-persona-mapping.md: 91 lines
docs/qa/agent-user-persona-mapping.md: 98 lines before result entry
iamine-node/src/main.rs: 4929 lines
iamine-node/src/cluster_registry.rs: 862 lines
```

Full quality gate:

```text
./scripts/quality-gate.sh: NOT RERUN FOR THIS DOCS-ONLY ITERATION
classification: environment risk / redundant broad gate
```

Reason:

```text
The recent full gate already passed all required checks before docs-only agent
research changes. The local environment remains disk constrained, and this
feature does not alter the Rust workspace.
```

## Field QA Decision

Field QA is not required for this documentation-only persona feature because no
runtime, agent execution, installer, updater, P2P, worker, scheduler, inference,
model, service-manager, marketplace, reward, or public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- user personas are mapped from market-fit research segments;
- personas are research inputs, not validated market evidence;
- blocked actions are explicit;
- handoff or refusal triggers are present;
- next feature remains `AGENT-BETA-PACK-SELECTION-001`;
- agent runtime remains blocked;
- public beta remains blocked.
