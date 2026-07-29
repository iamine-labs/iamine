# AGENT-PACKAGE-LOADER-001 QA

## Identity

```text
feature: AGENT-PACKAGE-LOADER-001
branch: feature/agent-package-loader-001
base: 7455f30193bcb53c5362690206b3fb79aba92bbd
base tree: 84a53698274ab1d5d71b001a313f961b7ce5d8ae
source commit: 22fc428def790eb74db23f6aa11fe8e247df25d3
source tree: 9d760b1831206a5d72a8c6b878e50d3c8ded98bd
feature tip: fc2bb2fc4b580815f9853f04022b3c3b051097a5
feature tree: d787a6fa958d4e1571cfd64268be56947dce78f3
merge commit: 0e8e2db37ba55f14729e0d4c10f1e3e34898b172
merge tree: d787a6fa958d4e1571cfd64268be56947dce78f3
canonical remote: origin
runtime behavior changed: bounded in-memory package loading
field QA required: yes
field QA result: PASS
post-merge validation: PASS
```

QA must record the exact source commit and tree before running any test. It
must not modify code, repeat successful roles after identity remains stable, or
continue after an unclassified first failure.

## Checks

1. Verify branch, full HEAD, tree, merge base, origin, tracked/staged state,
   and untracked baseline.
2. Run the nine focused package-loader tests.
3. Run the complete `iamine-agent-runtime` regression.
4. Confirm exact evidence loads seven bounded references.
5. Confirm foreign evidence authorities fail closed.
6. Confirm cancellation makes the prior evidence stale.
7. Confirm a loaded package is bound to the exact evidence instance.
8. Confirm loader authorities are isolated.
9. Confirm loading retains the resolved snapshot without reopening paths.
10. Confirm Debug and errors do not expose package IDs, paths, host values, or
    private data.
11. Confirm loaded state does not permit execution, activate runtime or
    sandbox, mutate scheduler, start transport, persist, or emit events.
12. Confirm the static package-load gate and runtime executor remain blocked.
13. Confirm no daemon, worker, agent, sandbox, network, model, or inference
    process is started by the test.
14. Run formatting, strict crate Clippy, diff checks, size guards, privacy
    scan, and the complete local quality gate.
15. Execute exact-tree field QA on Mac, TS140, and four Proxmox roles.

## Local Commands

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test package_loader
cargo test -p iamine-agent-runtime
cargo test -p iamine-agents
cargo build -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

## Field Matrix

```text
Mac development machine
TS140
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

Each role must use the exact source commit and tree and run:

```bash
cargo test -p iamine-agent-runtime --test package_loader
cargo test -p iamine-agent-runtime
```

Remote QA must use a clean tracked/staged working copy. Local untracked
artifacts must be recorded and preserved. No destructive Git, Cargo cleanup,
process termination, package installation, or source modification is
authorized.

## Local Results

The source commit passed:

```text
focused package loader: 9/9 PASS
iamine-agent-runtime: 137/137 PASS
iamine-agents: 109/109 PASS
workspace: 1109/1109 PASS
iamine-agent-runtime strict clippy: PASS
quality gate required failures: 0
quality gate warnings: 0
quality gate optional checks skipped: 3
quality gate result: PASS WITH WARNINGS
```

The workspace Clippy warnings are established findings outside the feature
diff. The modified runtime crate passed `clippy -D warnings`. Optional
`cargo audit`, `cargo deny`, and `gitleaks` checks were unavailable and were
reported as skipped.

## Field Results

QA transferred a complete Git bundle with SHA-256:

```text
be15ccda8d845de6f7e61c91086a80672c9bbf8d0ae8bffde2cb329e347dd62d
```

Git verified that the bundle contained complete history and the exact feature
ref. Every Linux role cloned it into an isolated `/tmp` directory and
validated the source commit, tree, parent, branch, tracked state, staging, and
untracked state before testing. Canonical remote working copies were not
modified.

| Platform role | Focused loader | Runtime regression | Final state |
| --- | --- | --- | --- |
| macOS development | 9/9 PASS | 137/137 PASS | clean |
| physical Linux, TS140 | 9/9 PASS | 137/137 PASS | clean |
| Linux VM control, iamine-ctrl | 9/9 PASS | 137/137 PASS | clean |
| Linux VM worker A, iamine-wrk1 | 9/9 PASS | 137/137 PASS | clean |
| Linux VM worker B, iamine-wrk2 | 9/9 PASS | 137/137 PASS | clean |
| Linux VM heavy, iamine-heavy | 9/9 PASS | 137/137 PASS | clean |

Aggregate:

```text
roles passed: 6/6
focused tests: 54/54 PASS
runtime regression: 822/822 PASS
product failures: 0
runtime side effects observed: 0
source changes during QA: 0
```

## Environmental Findings

The initial complete-runtime link failed on `iamine-ctrl` with linker
`SIGBUS`; `/` had 58 MB free and was at 100% usage. The first `iamine-wrk1`
attempt failed with explicit `No space left on device`; `/` had no free space.
Redirecting only the Cargo target then exposed the same environmental limit
when tests attempted to create `tempfile` directories under `/tmp`.

QA classified these as environmental harness failures, stopped at each first
failure, and retried only after redirecting both `CARGO_TARGET_DIR` and
`TMPDIR` to available tmpfs storage. The exact unchanged source then passed
137/137 on both guests. Preflight found 386 MB free on `iamine-wrk2` and
1.1 GB on `iamine-heavy`, so the complete harness was applied before their
regressions; both passed 137/137.

No files were deleted, no package was installed, no process was terminated,
and no system or repository configuration was changed. Proxmox root
filesystem maintenance remains a separate operational follow-up.

TS140 did not expose Cargo through the default non-interactive SSH path. The
existing `/home/ts140/.cargo/bin/cargo` executable was used without changing
the host.

## Post-Merge Validation

The controlled merge preserved the exact validated feature tree. Validation
ran against merge `0e8e2db37ba55f14729e0d4c10f1e3e34898b172` and tree
`d787a6fa958d4e1571cfd64268be56947dce78f3`.

```text
focused package loader: 9/9 PASS
workspace: 1109/1109 PASS
workspace clippy: PASS WITH BASELINE WARNINGS
quality gate required failures: 0
quality gate warnings: 0
quality gate optional checks skipped: 3
quality gate result: PASS WITH WARNINGS
```

The first post-merge workspace attempt exhausted the Mac data volume while
compiling generated Cargo artifacts. No product test failed. After removing
only the disposable feature-worktree `target/` directory, the unchanged merge
passed the workspace regression, workspace Clippy, and complete quality gate.
This was an environmental capacity finding, not a product exception.

## Recommendation

```text
MERGED / VALIDATED / CLOSED
next feature: AGENT-RUNTIME-EXECUTOR-001 remains PROPOSED
```

QA does not approve or authorize merge.
