# AGENT-PACKAGE-REVIEW-EVIDENCE-001 QA

## State

```text
MERGED / VALIDATED / CLOSED
branch: feature/agent-package-review-evidence-001
base: cfeec6f83e80b9a34a224cb1863d3e260d9f1e20
base tree: 3e8fa823d9e06e80a4e49ead8d442e35bc271f39
source commit: fbf1a8428f8095e80f37c22c293d0cbc2524602c
source tree: f1c577bf14343207c29b2932c305711305f4f4ad
QA evidence commit: 63f9e8d73c82066be3d1c04093b8d67fd636e109
QA evidence tree: fe77ce05e2ed4ebb626c3f0cb7aea15b074b636c
merge commit: ad1d2816f17e6d725e153ab38b3107eb810c1431
merge tree: fe77ce05e2ed4ebb626c3f0cb7aea15b074b636c
```

## Scope

Expected executable changes are limited to:

```text
iamine-agent-runtime/src/contract.rs
iamine-agent-runtime/src/lib.rs
iamine-agent-runtime/src/review_evidence/
iamine-agent-runtime/tests/package_review_evidence.rs
```

Expected documentation:

```text
docs/architecture/agent-package-review-evidence.md
docs/qa/agent-package-review-evidence.md
```

No `iamine-node`, scheduler, worker, network, model, inference, service, or
package-load integration file may change.

## Required Local Checks

1. Verify exact branch, base, tree, tracked state, staging, and untracked state.
2. Run `cargo fmt --all -- --check`.
3. Run `cargo test -p iamine-agent-runtime`.
4. Run strict crate Clippy with `-D warnings`.
5. Run `cargo test -p iamine-agents`.
6. Confirm the four package-load blockers remain present.
7. Run `./scripts/quality-gate.sh`.
8. Run diff and size guards.

## Required Adversarial Assertions

- all four positive decisions establish evidence;
- each non-positive owner decision fails closed;
- another authority cannot verify evidence;
- an equivalent reparsed manifest cannot reuse evidence;
- a cloned resolution cannot reuse evidence;
- package-controlled claims cannot replace operator decisions;
- Debug and errors redact package and review values;
- evidence never allows loading or execution;
- package-load blockers remain unchanged.

## Field Matrix

Required because the runtime crate changes:

| Host | Identity | Focused build/tests | Side effects | Result |
| --- | --- | --- | --- | --- |
| Mac | exact commit/tree/base | 18/18 | process count 0 -> 0; clean | PASS |
| TS140 | exact commit/tree/base | 18/18 | process count 0 -> 0; canonical staged/untracked preserved | PASS |
| iamine-ctrl | exact commit/tree/base | 18/18 | process count 0 -> 0; CANDIDATE_1 clean | PASS |
| iamine-wrk1 | exact commit/tree/base | 18/18 | process count 0 -> 0; CANDIDATE_1 clean | PASS |
| iamine-wrk2 | exact commit/tree/base | 18/18 | process count 0 -> 0; CANDIDATE_1 clean | PASS |
| iamine-heavy | exact commit/tree/base | 18/18 | process count 0 -> 0; CANDIDATE_1 clean | PASS |

QA must use the exact authorized source commit and stop on the first
unclassified failure. Successful checks are not repeated unless commit, tree,
scope, or Architecture direction changes.

## Local Validation Results

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-agent-runtime: PASS, 18/18
new package review tests: PASS, 6 test functions / 13 negative decision variants
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
cargo test -p iamine-agents: PASS, 109/109
scripts/quality-gate.sh: PASS WITH WARNINGS
cargo test --workspace: PASS, 990/990
cargo test -p iamine-models: PASS, 158/158
cargo test -p iamine-network: PASS, 167/167
cargo test -p iamine-node: PASS, 480/480
cargo build -p iamine-node: PASS
workspace clippy: PASS with historical warnings
git diff --check: PASS
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new production module: 92 lines
required failures: 0
```

Optional tools:

```text
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

The first implementation compile found one private-module import error. It was
corrected before the validation ladder; no product behavior or contract changed
as part of that correction.

## Field QA Results

```text
hosts: 6/6 PASS
runtime test executions: 108/108 PASS
feature test executions: 36/36 PASS
negative decision variant executions: 78/78 PASS
product failures: 0
environment failures: 0
harness failures: 0
iamine-node process changes: 0
tracked/staged contamination: 0
```

TS140 canonical state was recorded before QA. Its existing eight staged feature
files and every untracked artifact hash remained unchanged after QA. The
feature ran from `/tmp/iamine-agent-package-review-evidence-qa` with an explicit
Cargo path.

Each Proxmox guest used the previously authorized clean `CANDIDATE_1` only to
fetch the exact source ref and create an isolated detached worktree. The
candidate remained clean. `CANDIDATE_2` was not inspected or modified.

No test started or stopped `iamine-node`, loaded a model, opened a network
runtime, installed a package, or changed package-load blockers.

## QA Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not authorize merge. Architecture owns the final merge decision.

## Post-Merge Validation

The controlled merge completed without conflicts. `origin/develop`, the local
integration HEAD, and merge commit `ad1d281` resolved to tree
`fe77ce05e2ed4ebb626c3f0cb7aea15b074b636c`.

```text
scripts/quality-gate.sh: PASS WITH WARNINGS
cargo test --workspace: PASS, 990/990
cargo test -p iamine-models: PASS, 158/158
cargo test -p iamine-network: PASS, 167/167
cargo test -p iamine-node: PASS, 480/480
cargo build -p iamine-node: PASS
cargo clippy --workspace --all-targets: PASS with historical warnings
git diff --check: PASS
architecture and repository guards: PASS
required failures: 0
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

The feature is `MERGED / VALIDATED / CLOSED`. Its evidence does not authorize
package loading or execution.
