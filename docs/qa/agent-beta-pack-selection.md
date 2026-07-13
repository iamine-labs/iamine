# IAMINE Agent Beta Pack Selection QA

Feature:

```text
AGENT-BETA-PACK-SELECTION-001
```

## Objective

Validate that the official beta pack selection is roadmap-aligned,
scope-bound, privacy-safe, and does not authorize runtime execution.

## Identity

Record before QA:

```text
Branch: feature/agent-beta-pack-selection-001
HEAD: a91a27e84c3ae42f2a561f41b1d4b87e0ab6709b
Tree: ffdb983270860eda4e14faf0311a3407f03d6594
Base: origin/develop
origin/develop: a91a27e84c3ae42f2a561f41b1d4b87e0ab6709b
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: none expected in feature worktree
```

## Scope Checks

Expected changed paths:

```text
docs/agents/official-beta-agent-pack-selection.md
docs/architecture/agent-beta-pack-selection.md
docs/qa/agent-beta-pack-selection.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, executable
agent manifests, permission enforcement, P2P, PubSub, scheduler, worker
behavior, model policy, inference execution, installer, updater, rollback,
reputation, rewards, wallet, marketplace, public beta, or mainnet behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-BETA-PACK-SELECTION-001" docs/agents/official-beta-agent-pack-selection.md docs/architecture/agent-beta-pack-selection.md docs/qa/agent-beta-pack-selection.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "IAMINE Local Readiness Beta Pack|Node Doctor|Privacy-Safe Support Reporter|LAN Readiness Reporter|Agent Manifest Wizard" docs/agents/official-beta-agent-pack-selection.md docs/architecture/agent-beta-pack-selection.md
rg -n "does not authorize agent runtime|documentation-only|not executable|planning contract only|Do not implement runtime execution" docs/agents/official-beta-agent-pack-selection.md docs/architecture/agent-beta-pack-selection.md
rg -n "credentials|destructive|host identifiers|MAC addresses|serials|machine IDs|scope-bound|blocked actions|refusal|handoff" docs/agents/official-beta-agent-pack-selection.md
rg -n "AGENT-BETA-PACK-SELECTION-001 \\| ACTIVE|AGENT-PACKAGE-MANIFEST-001" docs/roadmap/iamine-agent-network-roadmap.md
```

Expected:

- roadmap marks this feature active before merge closeout;
- first official beta pack is selected;
- selected agents remain product targets, not executable agents;
- selected agents include explicit blocked actions and required later gates;
- unsafe candidates are deferred with reasons;
- no source code changes are present.

## Quality Gate Policy

This feature is documentation-only. Use focused documentation checks plus
`cargo fmt` unless Architecture asks for the full workspace gate.

The local environment remains disk constrained from recent full validation
cycles. Re-running `./scripts/quality-gate.sh` for a docs-only beta-pack
selection is not expected to add product evidence unless local capacity is
first remediated.

## Observed Local Results

```text
git diff --check: PASS
cargo fmt --all -- --check: PASS
feature presence scan: PASS
official beta pack selection scan: PASS
runtime boundary scan: PASS
privacy and blocked-action scan: PASS
roadmap ACTIVE state scan: PASS before closeout
```

File-size check:

```text
docs/agents/official-beta-agent-pack-selection.md: 256 lines
docs/architecture/agent-beta-pack-selection.md: 115 lines
docs/qa/agent-beta-pack-selection.md: 102 lines before result entry
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

Field QA is not required for this documentation-only selection feature because
no runtime, agent execution, installer, updater, P2P, worker, scheduler,
inference, model, service-manager, marketplace, reward, or public-beta behavior
changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the first official beta pack is selected from existing personas and
  constraints;
- selected agents are narrow, read-only, and scope-bound;
- selected agents do not collect credentials or permanent hardware
  identifiers;
- broad or mutation-heavy candidates remain deferred;
- next feature remains `AGENT-PACKAGE-MANIFEST-001`;
- agent runtime remains blocked;
- public beta remains blocked.
