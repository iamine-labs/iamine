# IAMINE Agent Scope Manifest QA

Feature:

```text
AGENT-SCOPE-MANIFEST-001
```

## Objective

Validate that the scope manifest contract is roadmap-aligned, scope-bound,
privacy-safe, and does not authorize runtime enforcement.

## Identity

Record before QA:

```text
Branch: feature/agent-scope-manifest-001
HEAD: 682bf7f18f7d3f0e6065cdc6b57d8990efa27bdc
Tree: 1a183e269da4cbf55db40823d44ab9210b98b450
Base: origin/develop
origin/develop: 682bf7f18f7d3f0e6065cdc6b57d8990efa27bdc
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: none expected in feature worktree
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-scope-manifest.md
docs/architecture/agent-scope-manifest.md
docs/qa/agent-scope-manifest.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, executable
agent manifests, scope parser, scope enforcement, permission enforcement, P2P,
PubSub, scheduler, worker behavior, model policy, inference execution,
installer, updater, rollback, reputation, rewards, wallet, marketplace, public
beta, or mainnet behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-SCOPE-MANIFEST-001" docs/agents/agent-scope-manifest.md docs/architecture/agent-scope-manifest.md docs/qa/agent-scope-manifest.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "iamine.agent.scope.draft-0.1|agent-scope.toml|scope_can_self_approve = false" docs/agents/agent-scope-manifest.md docs/architecture/agent-scope-manifest.md
rg -n "in_scope|out_of_scope|task_types|allowed_inputs|forbidden_inputs|allowed_operations|blocked_actions" docs/agents/agent-scope-manifest.md docs/architecture/agent-scope-manifest.md
rg -n "permission_requirements|confirmation_boundary|handoff|orchestrator_return|eval_requirements" docs/agents/agent-scope-manifest.md docs/architecture/agent-scope-manifest.md
rg -n "does not authorize executable|documentation-only|does not implement TOML parsing|not executable|block install and execution" docs/agents/agent-scope-manifest.md docs/architecture/agent-scope-manifest.md
rg -n "credentials|private_keys|wallet_keys|host identifiers|ip_addresses|mac_addresses|serial_numbers|machine_ids|private_paths|arbitrary_shell|unrestricted_filesystem|scope-bound" docs/agents/agent-scope-manifest.md docs/architecture/agent-scope-manifest.md
rg -n "AGENT-SCOPE-MANIFEST-001 \\| ACTIVE|AGENT-CAPABILITY-METADATA-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
```

Expected:

- roadmap marks this feature active before merge closeout;
- scope manifest draft schema is documented;
- scope manifest does not authorize execution or enforcement;
- in-scope, out-of-scope, blocked actions, handoff, orchestrator return, and
  eval requirements are present;
- privacy-sensitive identifiers and secrets remain prohibited;
- no source code changes are present.

## Quality Gate Policy

This feature is documentation-only. Use focused documentation checks plus
`cargo fmt` unless Architecture asks for the full workspace gate.

The local environment remains disk constrained from recent full validation
cycles. Re-running `./scripts/quality-gate.sh` for a docs-only scope contract
is not expected to add product evidence unless local capacity is first
remediated.

## Observed Local Results

```text
git diff --check: PASS
cargo fmt --all -- --check: PASS
feature presence scan: PASS
draft schema and scope filename scan: PASS
task and input boundary scan: PASS
permission, confirmation, handoff, orchestrator return, and eval scan: PASS
runtime boundary scan: PASS
privacy and blocked-mode scan: PASS
roadmap ACTIVE state scan: PASS before closeout
```

File-size check:

```text
docs/agents/agent-scope-manifest.md: 510 lines
docs/architecture/agent-scope-manifest.md: 157 lines
docs/qa/agent-scope-manifest.md: 105 lines before result entry
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

Field QA is not required for this documentation-only scope manifest feature
because no runtime, agent execution, installer, updater, P2P, worker,
scheduler, inference, model, service-manager, marketplace, reward, or
public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the scope manifest contract is documented;
- the manifest defines scope boundaries without enforcing them;
- execution and runtime enforcement stay unauthorized;
- missing or unsafe scope metadata blocks install and execution by default;
- scope cannot self-approve execution;
- next feature remains `AGENT-CAPABILITY-METADATA-001`;
- agent runtime remains blocked;
- public beta remains blocked.
