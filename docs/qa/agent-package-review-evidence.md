# AGENT-PACKAGE-REVIEW-EVIDENCE-001 QA

## State

```text
FIELD QA AUTHORIZED
branch: feature/agent-package-review-evidence-001
base: cfeec6f83e80b9a34a224cb1863d3e260d9f1e20
base tree: 3e8fa823d9e06e80a4e49ead8d442e35bc271f39
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
| Mac | pending | pending | pending | pending |
| TS140 | pending | pending | pending | pending |
| iamine-ctrl | pending | pending | pending | pending |
| iamine-wrk1 | pending | pending | pending | pending |
| iamine-wrk2 | pending | pending | pending | pending |
| iamine-heavy | pending | pending | pending | pending |

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
