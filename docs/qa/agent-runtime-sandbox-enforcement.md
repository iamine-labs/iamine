# AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001 QA

## State

```text
MERGED / VALIDATED / CLOSED
branch: feature/agent-runtime-sandbox-enforcement-001
base: c97dcf66047683e99937a05ebd2b63b8349a5195
base tree: c118fe9a35fc589d186d3dd1e55b9158b47b748f
source commit: 0a57870873adfef716a56904aa84e92913bc3dbb
source tree: 41f7dc8c7e5f78c91204878130dd89412325f675
QA evidence commit: 875be7f5665d1423f311352397c22e30f3fb9861
QA evidence tree: fee769843370d8e760ec8fd8f65cd53c5dff4fbc
merge commit: 54e4721f89b4b5cc8bf697c8c29834ccaf3a26a4
merge tree: fee769843370d8e760ec8fd8f65cd53c5dff4fbc
field QA: passed, 6/6 hosts
post-merge: PASS WITH ACCEPTED BASELINE / ENVIRONMENT EXCEPTIONS
```

## Scope

Expected executable changes are limited to:

```text
iamine-agent-runtime/src/input_output_enforcement/authority.rs
iamine-agent-runtime/src/input_output_enforcement/evidence.rs
iamine-agent-runtime/src/input_output_enforcement/mod.rs
iamine-agent-runtime/src/lib.rs
iamine-agent-runtime/src/runtime_compatibility/authority.rs
iamine-agent-runtime/src/runtime_compatibility/evaluation.rs
iamine-agent-runtime/src/runtime_compatibility/evidence.rs
iamine-agent-runtime/src/runtime_compatibility/mod.rs
iamine-agent-runtime/src/sandbox_enforcement/
iamine-agent-runtime/tests/sandbox_enforcement.rs
```

Expected documentation:

```text
docs/architecture/agent-runtime-sandbox-enforcement.md
docs/qa/agent-runtime-sandbox-enforcement.md
```

No node, scheduler, worker, network, model, inference, package-load, active
sandbox adapter, process-launch, Cargo dependency, or static blocker behavior
may change.

## Required Checks

1. Verify exact branch, base, commit, tree, tracked state, staging, and
   aggregate untracked state.
2. Run formatting, focused runtime tests, dependency crate tests, workspace
   tests, strict crate Clippy, the quality gate, and diff checks.
3. Confirm the static sandbox-unavailable package-load blocker remains.
4. Confirm prepared evidence cannot claim an active sandbox, registered
   cleanup, load, execution, persistence, or transport authorization.
5. Run the focused sandbox suite on Mac, TS140, and four Proxmox guests.
6. Confirm no `iamine-node` process, persistent state, or repository residue is
   created by field QA.

## Local Validation

```text
baseline cargo test -p iamine-agent-runtime: PASS, 33/33
cargo fmt --all -- --check: PASS
cargo test -p iamine-agent-runtime: PASS, 42/42
new sandbox enforcement tests: PASS, 9/9
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
cargo test -p iamine-agents: PASS, 109/109
cargo test -p iamine-models: PASS, 158/158
cargo test -p iamine-network: PASS, 167/167
cargo test -p iamine-node: PASS, 480/480
cargo build -p iamine-node: PASS
cargo test --workspace: PASS, 1014/1014
scripts/quality-gate.sh: PASS WITH WARNINGS
git diff --check: PASS
git diff --cached --check: PASS
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new production module: 168 lines
required failures: 0
```

Optional tools:

```text
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

Workspace Clippy emitted only historical warnings from unchanged
`client-rust`, `iamine-models`, `iamine-network`, and `iamine-node` sources.
Strict Clippy for the changed crate passed without warnings.

## Field Matrix

| Host | Exact identity | Focused tests | Side effects | Result |
| --- | --- | --- | --- | --- |
| Mac | source commit/tree/base | 9/9 | no product process or persistent state | PASS |
| TS140 | source commit/tree/base | 9/9 | process 0 -> 0; isolated clean worktree | PASS |
| iamine-ctrl | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |
| iamine-wrk1 | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |
| iamine-wrk2 | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |
| iamine-heavy | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |

```text
hosts: 6/6 PASS
feature test executions: 54/54 PASS
product failures: 0
environment findings: 0
harness findings: 2 classified
iamine-node process changes on Linux remotes: 0
tracked/staged contamination in selected QA copies: 0
```

The Mac app sandbox denied process-list inspection. The focused tests and
source diff contain no process-spawn path, and no product process or persistent
state was observed.

The canonical TS140 copy contained historical staged work and 34 preserved
untracked artifacts. QA stopped at that preflight, classified the condition as
a harness isolation issue, and created a clean worktree under `/tmp` without
altering the canonical branch, staging, or artifacts. The first non-login SSH
test invocation also lacked Cargo in `PATH`; using the installed Cargo binary
by explicit path corrected the harness before compilation.

Each Proxmox guest exposed two candidate copies. `HOME/work/iamine` contained
historical staged work and two preserved untracked artifacts, so it was not
modified. QA selected the clean `HOME/code/iamine` copy, synchronized it only
through explicit feature fetch plus switch and fast-forward merge, and
validated the exact source identity before testing.

No field test started or stopped `iamine-node`, opened a network runtime,
loaded a package or model, created a sandbox, registered cleanup, executed
agent code, or changed package-load blockers. The feature prepares typed
enforcement evidence only; it does not prove that OS isolation is active.

## Architecture Checkpoint

The implementation remains inside `iamine-agent-runtime`. Resource parsing is
reused from the compatibility owner, evidence identities bind the exact
authority chain, and the new owner is split into seven focused modules. There
is no production growth in either protected node file and no new dependency.

The corrected input/output evidence binding prevents a later consumer from
combining equivalent-subject evidence created by a different compatibility
authority. The adversarial suite covers this mismatch and fails closed.

## QA Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not authorize merge. Final Architecture review owns the merge
decision.

## Post-Merge Validation

The controlled merge completed without conflicts. The merge tree is identical
to the approved QA evidence tree.

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-agent-runtime: PASS, 42/42
cargo test -p iamine-agents: PASS, 109/109
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
git diff --check: PASS
scripts/quality-gate.sh: FAIL, required_failures=3
cargo test -p iamine-network: PASS, 167/167
cargo build -p iamine-node: PASS
cargo clippy --workspace --all-targets: PASS with historical warnings
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

The three broad command failures are accepted baseline or environment
exceptions:

1. `iamine-models` failed `test_concurrency_limit`, `test_inference_queue`,
   `test_real_inference`, and `test_token_streaming` under the real Metal
   backend. The exact base reproduced the same four failures with `55/59`
   integration tests passing.
2. The `iamine-models` subtree object, workspace `Cargo.toml`, and `Cargo.lock`
   are identical between base and merge. `iamine-node` does not depend on
   `iamine-agent-runtime`.
3. The workspace command repeated the same four model failures and stopped
   before completing the remaining workspace binaries.
4. `iamine-node` passed `479/480` tests inside the app sandbox. Its only
   failure was `daemon_runtime::tests::test_daemon_start_stop`, where creation
   of the temporary Unix socket was denied. The exact test passed `1/1`
   outside the sandbox.
5. The daemon source blob is identical between base and merge. No changed
   source participated in either failure family.

No sandbox-enforcement, runtime-compatibility, input/output, package-review,
scope, permission, package-load, node, network, scheduler, model, or execution
behavior regressed because of this feature. Final Architecture classification:

```text
PASS WITH ACCEPTED BASELINE / ENVIRONMENT EXCEPTIONS
MERGED / VALIDATED / CLOSED
```
