# AGENT-RUNTIME-COMPATIBILITY-GATE-001 QA

## State

```text
MERGED / VALIDATED / CLOSED
branch: feature/agent-runtime-compatibility-gate-001
base: a83e08effdb5c67ec8a0ac411f7c489fb44f466e
base tree: 884b6f921094fbc4e41fad5484ae304b11437311
source commit: 933f15fa41395fe4d18bd8cc4b4c7a3fe95dea7e
source tree: 5182dd9faab77d4b943d1b20b18f9536b2f34c3f
QA evidence commit: b4f50b12dcc1e3030a2700f990ffa4265785c068
QA evidence tree: 5f6f15eee580875fadd1e6862116b5431e8fea18
merge commit: 40a9a8074d8138204b6f4c1dd4c787d1d97fb219
merge tree: 5f6f15eee580875fadd1e6862116b5431e8fea18
field QA: passed, 6/6 hosts
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
| Mac | exact commit/tree/base | 25/25 | clean; current process count 0 | PASS |
| TS140 | exact commit/tree/base | 25/25 | process 0 -> 0; canonical staged/untracked preserved | PASS |
| iamine-ctrl | exact commit/tree/base | 25/25 | process 0 -> 0; CANDIDATE_1 clean | PASS |
| iamine-wrk1 | exact commit/tree/base | 25/25 | process 0 -> 0; CANDIDATE_1 clean | PASS |
| iamine-wrk2 | exact commit/tree/base | 25/25 | process 0 -> 0; CANDIDATE_1 clean | PASS |
| iamine-heavy | exact commit/tree/base | 25/25 | process 0 -> 0; CANDIDATE_1 clean | PASS |

QA must use the exact source commit and stop at the first unclassified failure.
Successful checks are not repeated unless commit, tree, scope, or Architecture
direction changes.

## Field QA Results

```text
hosts: 6/6 PASS
runtime test executions: 150/150 PASS
feature test executions: 42/42 PASS
product failures: 0
environment findings: 1 classified
harness findings: 2 classified
iamine-node process changes on remotes: 0
tracked/staged contamination: 0
```

Mac used the exact clean source commit with a shared temporary Cargo target.
The app sandbox prevented the initial process-list read, then an escalated
read confirmed zero `iamine-node` processes after the test. The focused tests
do not spawn the node binary.

TS140 QA used
`/tmp/iamine-agent-runtime-compatibility-gate-qa-933f15f`. The first command
stopped before tests because the non-login SSH shell did not load Cargo.
Loading `$HOME/.cargo/env` corrected the harness and the exact checkout passed.
The canonical `/home/ts140/iamine` branch, HEAD, tree, eight staged files, and
every untracked artifact hash remained unchanged.

Each Proxmox guest used an isolated checkout with the exact source commit.
The authorized `CANDIDATE_1` remained clean at its original commit and tree.
`CANDIDATE_2` was not inspected or modified.

No test started or stopped `iamine-node`, inspected real hardware, loaded a
model, opened a network runtime, installed a package, or changed package-load
blockers.

## QA Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not authorize merge. Architecture owns the final merge decision.

## Post-Merge Validation

The controlled merge completed without conflicts. `origin/develop`, the local
integration HEAD, and merge commit `40a9a80` resolved to tree
`5f6f15eee580875fadd1e6862116b5431e8fea18`.

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-agent-runtime: PASS, 25/25
cargo test -p iamine-agents: PASS, 109/109
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
git diff --check: PASS
scripts/quality-gate.sh: FAIL, required_failures=3
cargo clippy --workspace --all-targets: PASS with historical warnings
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

The broad script failures are classified as accepted baseline and environment
exceptions:

1. `iamine-models` has the same subtree object
   `0bfec98804f2cb81fca6e9597e383e0825738ad6` in base and merge. Its
   `Cargo.toml` and the workspace `Cargo.lock` are also identical.
2. The merge produced one complete `158/158` package pass. The exact base then
   reproduced failures in `test_concurrency_limit`, `test_inference_queue`,
   `test_real_inference`, and `test_token_streaming` during a long Metal suite.
3. Each of those four tests passed in an isolated process on the merge. This
   proves a historical real-backend harness instability, not a feature diff.
4. The node package failed only
   `daemon_runtime::tests::test_daemon_start_stop` because the Codex sandbox
   denied its Unix socket. The exact test passed outside the sandbox.
5. The workspace command repeated the same four unchanged Metal failures.

No runtime-compatibility, package-review, scope, permission, package-load,
node, network, scheduler, model, or execution behavior failed because of the
feature. The final Architecture decision is:

```text
PASS WITH ACCEPTED BASELINE / ENVIRONMENT EXCEPTIONS
MERGED / VALIDATED / CLOSED
```
