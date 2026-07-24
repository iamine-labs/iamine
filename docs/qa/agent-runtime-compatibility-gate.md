# AGENT-RUNTIME-COMPATIBILITY-GATE-001 QA

## State

```text
FIELD QA AUTHORIZED
branch: feature/agent-runtime-compatibility-gate-001
base: a83e08effdb5c67ec8a0ac411f7c489fb44f466e
base tree: 884b6f921094fbc4e41fad5484ae304b11437311
source commit: pending
source tree: pending
field QA: pending
```

## Scope

Expected executable changes are limited to:

```text
iamine-agent-runtime/src/contract.rs
iamine-agent-runtime/src/lib.rs
iamine-agent-runtime/src/review_evidence/subject.rs
iamine-agent-runtime/src/runtime_compatibility/
iamine-agent-runtime/tests/runtime_compatibility.rs
iamine-agents/src/descriptive_metadata/resource.rs
```

Expected documentation:

```text
docs/architecture/agent-runtime-compatibility-gate.md
docs/qa/agent-runtime-compatibility-gate.md
```

No node, hardware profiler, scheduler, worker, network, model, inference,
package-load, sandbox, loader, executor, or Cargo dependency file may change.

## Required Local Checks

1. Verify exact branch, base, tree, tracked state, staging, and untracked state.
2. Run `cargo fmt --all -- --check`.
3. Run `cargo test -p iamine-agent-runtime`.
4. Run strict crate Clippy with `-D warnings`.
5. Run `cargo test -p iamine-agents`.
6. Confirm runtime-language and resource package-load blockers remain present.
7. Run `./scripts/quality-gate.sh`.
8. Run diff and size guards.

## Required Adversarial Assertions

- reviewed Rust official mode with sufficient resources establishes evidence;
- every other runtime mode fails even if labeled available;
- unavailable, deferred, and blocked Rust states fail independently;
- another review or compatibility authority cannot reuse evidence;
- another manifest or cloned resolution cannot reuse evidence;
- package ID and operating mode must match reviewed declarations;
- CPU, memory, storage, and network insufficiency fail independently;
- malformed resource metadata fails without echoing its content;
- zero resource-envelope dimensions are rejected;
- Debug and errors redact package, resource, review, and host values;
- compatibility never allows loading or execution;
- package-load blockers remain unchanged.

## Local Validation Results

```text
baseline cargo test -p iamine-agent-runtime: PASS, 18/18
cargo fmt --all: PASS
cargo test -p iamine-agent-runtime: PASS, 25/25
new runtime compatibility tests: PASS, 7/7
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
cargo test -p iamine-agents: PASS, 109/109
scripts/quality-gate.sh: PASS WITH WARNINGS
cargo test --workspace: PASS, 997/997
cargo test -p iamine-models: PASS, 158/158
cargo test -p iamine-network: PASS, 167/167
cargo test -p iamine-node: PASS, 480/480
cargo build -p iamine-node: PASS
workspace clippy: WARN, environmental disk exhaustion after required checks
git diff --check: PASS
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new production module: 184 lines
required failures: 0
```

Optional tools:

```text
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

The Mac filesystem had 117 MiB free after the required workspace gate. The
optional workspace Clippy pass stopped with `No space left on device` while
writing generated target artifacts. This is classified as an environmental
warning: strict Clippy for the changed crate passed, the workspace test passed,
and no Clippy diagnostic from changed source preceded the storage error.

## Field Matrix

| Host | Identity | Focused tests | Side effects | Result |
| --- | --- | --- | --- | --- |
| Mac | exact commit/tree/base | pending | process count and worktree | pending |
| TS140 | exact commit/tree/base | pending | canonical state preservation | pending |
| iamine-ctrl | exact commit/tree/base | pending | CANDIDATE_1 preservation | pending |
| iamine-wrk1 | exact commit/tree/base | pending | CANDIDATE_1 preservation | pending |
| iamine-wrk2 | exact commit/tree/base | pending | CANDIDATE_1 preservation | pending |
| iamine-heavy | exact commit/tree/base | pending | CANDIDATE_1 preservation | pending |

QA must use the exact source commit and stop at the first unclassified failure.
Successful checks are not repeated unless commit, tree, scope, or Architecture
direction changes.

## QA Recommendation

```text
FIELD QA AUTHORIZED
```

QA does not authorize merge. Architecture owns the final merge decision.
