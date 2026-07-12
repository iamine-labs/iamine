# IAMINE Pre-Public Readiness Gate QA

Feature:

```text
IAMINE-PREPUBLIC-READINESS-GATE-001
```

## Objective

Validate that the v0.10 pre-public readiness gate is evidence-backed,
privacy-safe, aligned with the canonical roadmap, and does not launch public
beta or change runtime behavior.

## Identity

Record before QA:

```text
Branch: feature/iamine-prepublic-readiness-gate-001
HEAD: 847b4bb09ac9fe99f4c811d619b924b5d353be54
Tree: a5e227587e329867fb6ea7ef3089945620a26f24
Base: origin/develop
origin/develop: 847b4bb09ac9fe99f4c811d619b924b5d353be54
tracked clean: no; feature delta is limited to expected documentation paths
staging clean: yes before final staging
untracked baseline: none expected in feature worktree
```

## Scope Checks

Expected changed paths:

```text
docs/architecture/iamine-prepublic-readiness-gate.md
docs/qa/iamine-prepublic-readiness-gate.md
docs/roadmap/iamine-product-roadmap.md
docs/roadmap/v0.10-prepublic-readiness-gate.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, packaging scripts, release
scripts, service templates, runtime startup, P2P, PubSub, worker behavior,
scheduler behavior, model policy, inference execution, updater execution,
rollback execution, public onboarding, public bootnodes, support intake, or
agent execution.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
rg -n "IAMINE-PREPUBLIC-READINESS-GATE-001 \\| ACTIVE" docs/roadmap/iamine-product-roadmap.md
rg -n "READY TO PROCEED TO v0.11 AGENT NETWORK FOUNDATIONS" docs/architecture/iamine-prepublic-readiness-gate.md docs/roadmap/v0.10-prepublic-readiness-gate.md
rg -n "NOT READY FOR PUBLIC BETA|NOT READY FOR PUBLIC ONBOARDING" docs/architecture/iamine-prepublic-readiness-gate.md docs/roadmap/v0.10-prepublic-readiness-gate.md
rg -n "PUBLIC-TESTNET-ADMISSION-001|SIGNED-AUTOUPDATE-001|USER-DIAGNOSTICS-SUPPORT-001|V1-SUPPLY-CHAIN-SECURITY-001|NODE-UPGRADE-ROLLBACK-001|PUBLIC-TESTNET-DOCUMENTATION-001" docs/roadmap/v0.10-prepublic-readiness-gate.md
rg -n "Q2 2026|Quick Start \\(Testnet\\)|ganas|recibe .*token" README.md
```

Expected:

- roadmap marks the readiness gate `ACTIVE` during feature implementation;
- v0.10 dependencies are present and closed in the readiness package;
- the decision allows v0.11 Agent Network foundations only;
- public beta and public onboarding remain explicitly blocked;
- no stale public-testnet launch or public reward claims reappear in README;
- no runtime files change.

## Quality Gate Policy

This feature is documentation-only. Run targeted documentation checks first.

Full `./scripts/quality-gate.sh` is useful before merge review only when local
disk capacity is sufficient. The immediately preceding post-merge gate on
`origin/develop` passed all required checks and classified the optional clippy
warning as environmental disk exhaustion. If local capacity remains below a
safe threshold, do not intentionally recreate the same environmental failure;
record the limitation and use focused checks for this docs-only gate.

## Observed Local Results

```text
git diff --check: PASS
cargo fmt --all -- --check: PASS
roadmap ACTIVE state scan: PASS
v0.11 readiness decision scan: PASS
public beta/public onboarding blocked scan: PASS
v0.10 dependency coverage scan: PASS
README stale public-testnet/reward claims scan: PASS; no matches
```

Local disk state:

```text
/private/tmp: 228Gi total, 186Gi used, 5.1Gi available, 98% capacity
```

Full quality gate:

```text
./scripts/quality-gate.sh: NOT RERUN FOR THIS DOCS-ONLY ITERATION
classification: environment risk / redundant broad gate
```

Reason:

```text
The immediately preceding post-merge validation on origin/develop ran the full
quality gate. All required checks passed. The only warning was optional clippy
failing while creating target/debug/.fingerprint files because /private/tmp was
at 100% capacity. This feature changes documentation only and does not alter
the Rust workspace, so repeating the same full gate with /private/tmp still at
98% would primarily retest the local disk limitation.
```

## Post-Merge Validation

Merge:

```text
origin/develop: eb8db38c3999c3c4d369c32e211e7adf834af401
feature commit: c317f9c999d77700e472f7a4bf68f239e82c6fa0
tree: 6f529a27ba3b5b9e933fb66647161f3635886572
```

Focused post-merge checks:

```text
git diff --check origin/develop~1..origin/develop: PASS
cargo fmt --all -- --check: PASS
roadmap ACTIVE state scan: PASS before closeout
v0.11 readiness decision scan: PASS
public beta/public onboarding blocked scan: PASS
README stale public-testnet/reward claims scan: PASS; no matches
```

Full post-merge quality gate:

```text
./scripts/quality-gate.sh: NOT RERUN
classification: environment risk / redundant broad gate
```

Reason:

```text
This feature is documentation-only and the immediately preceding origin/develop
quality gate already passed all required checks. The known local disk
limitation remained: /private/tmp was at 98% capacity with 5.1Gi available.
```

## Field QA Decision

Field QA is not required for this documentation-only feature because no runtime,
installer, updater, rollback executor, P2P, worker, scheduler, inference,
model, service-manager, public onboarding, public bootnode, or agent execution
behavior changes.

Proxmox/R5500 is available, but not used by this feature unless Architecture
expands the scope into runtime or operational validation. Later public
onboarding, release-package, updater, rollback, bootnode, or agent-runtime
features must run their own field QA.

## Expected Results

- v0.10 repository pre-public infrastructure can close;
- v0.11 Agent Network foundations may begin;
- public beta remains blocked;
- public onboarding remains blocked;
- public release packages remain blocked;
- public bootnodes remain blocked;
- public reward and settlement claims remain blocked;
- arbitrary or third-party agent execution remains blocked;
- no private host identifiers, local paths, credentials, or secrets are added.
