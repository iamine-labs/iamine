# AGENT-PACKAGE-LOADER-001 QA

## Identity

```text
feature: AGENT-PACKAGE-LOADER-001
branch: feature/agent-package-loader-001
base: 7455f30193bcb53c5362690206b3fb79aba92bbd
base tree: 84a53698274ab1d5d71b001a313f961b7ce5d8ae
canonical remote: origin
runtime behavior changed: bounded in-memory package loading
field QA required: yes
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

## Expected Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not approve or authorize merge.
