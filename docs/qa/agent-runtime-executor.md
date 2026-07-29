# AGENT-RUNTIME-EXECUTOR-001 QA

## Identity

```text
feature: AGENT-RUNTIME-EXECUTOR-001
branch: feature/agent-runtime-executor-001
base: b5aaf292f71cf7a3b243fc2780bac5f95c8223d6
base tree: a3085fafb2e9f28d26b1a0430aa5e3ffd287ce8f
source commit: df6b9037994822db3677e13175184e81a9dcff58
source tree: 4a37be4da2e42f4f8cc48004346e034377eb3856
canonical remote: origin
runtime behavior changed: registered official Rust execution
field QA required: yes
field QA result: pending
```

QA must record the exact source commit and tree before testing. It must not
modify code, continue after an unclassified first failure, or change canonical
remote working copies.

## Checks

1. Verify branch, full HEAD, tree, merge base, origin, tracked/staged state,
   and untracked baseline.
2. Run all 12 focused runtime-executor tests.
3. Run the complete `iamine-agent-runtime` regression.
4. Confirm only the exact loaded and authorized package can produce a permit.
5. Confirm the permit is bound to one executor, program, subject, execution,
   sandbox evidence, and lifecycle revision.
6. Confirm program substitution, foreign authorities, stale revisions, and
   foreign input evidence fail closed.
7. Confirm direct public lifecycle transition to `Running` remains blocked.
8. Confirm pending cancellation blocks before execution.
9. Confirm deadline expiry records `Timeout`.
10. Confirm program and output failures record `Failed`.
11. Confirm successful execution records `Running -> Completed` and both audit
    projections.
12. Confirm result verification rejects mismatched identity chains.
13. Confirm package bytes and paths are not reopened or interpreted.
14. Confirm Debug and errors do not expose package, input, output, path, host,
    credential, or secret values.
15. Confirm no scheduler, transport, persistence, external event, model,
    inference, node, or worker side effect.
16. Confirm the adapter does not claim OS isolation.
17. Run formatting, strict crate Clippy, diff checks, size guards, privacy
    scan, and the complete quality gate.
18. Execute exact-tree field QA on Mac, TS140, and four Proxmox roles.

## Local Commands

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test runtime_executor
cargo test -p iamine-agent-runtime
cargo test -p iamine-agents
cargo build -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

## Local Results

```text
focused runtime executor: 12/12 PASS
iamine-agent-runtime: 149/149 PASS
iamine-agents: 109/109 PASS
iamine-agent-runtime strict clippy: PASS
format: PASS
runtime crate build: PASS
diff check: PASS
workspace clippy: PASS WITH BASELINE WARNINGS
quality gate script raw result: FAIL
quality gate classification: PASS WITH ACCEPTED BASELINE EXCEPTION
optional checks skipped: 3
```

The complete gate passed formatting, `iamine-models`, `iamine-network`,
`iamine-node`, the node build, and both diff checks. Its first workspace
attempt failed while writing Cargo artifacts with `No space left on device`;
the Mac data volume initially had 116 MiB available. QA removed only the
discardable feature-worktree `target/` directory and retained every source,
commit, and evidence file.

The workspace replay reached the real-inference integration and reported four
failures:

```text
test_concurrency_limit
test_inference_queue
test_real_inference
test_token_streaming
```

The exact same `55/59` result and test list reproduced at base commit
`b5aaf292f71cf7a3b243fc2780bac5f95c8223d6`. The candidate had also passed
that same 59-test suite earlier in the gate. Architecture therefore classifies
the failures as an existing intermittent Metal real-inference baseline, not a
runtime-executor regression. No `iamine-models` source changed in this
feature.

Workspace Clippy completed successfully with existing warnings in
`iamine-models`, `iamine-network`, `iamine-node`, and `client-rust`. The
modified `iamine-agent-runtime` crate passes Clippy with `-D warnings`.
`cargo audit`, `cargo deny`, and `gitleaks` were unavailable and reported as
skipped.

## Field Matrix

```text
Mac development machine
TS140
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

Each role must use source commit
`df6b9037994822db3677e13175184e81a9dcff58`, tree
`4a37be4da2e42f4f8cc48004346e034377eb3856`, and run:

```bash
cargo test -p iamine-agent-runtime --test runtime_executor
cargo test -p iamine-agent-runtime
```

Remote QA must use a disposable, clean tracked/staged copy. Local untracked
artifacts must be recorded and preserved. No destructive Git/Cargo cleanup,
process termination, package installation, or source modification is
authorized.

Proxmox availability must be checked before use. When guest root capacity is
insufficient, QA may place only disposable Cargo targets and temporary test
files in `/dev/shm`; it must not delete canonical source or evidence.

## Required Side-Effect Review

Before and after the focused test, record whether IAMINE node or worker
processes appeared. The test must not:

```text
start a node daemon
start a controller or worker
open IAMINE transport
load or download a model
start inference
execute package bytes
write a package or runtime profile
mutate scheduler state
```

## Known Boundaries

- The current handler is a trusted function compiled into the operator binary.
- The sandbox adapter validates a restriction contract but does not enforce or
  claim OS isolation.
- Timeout and cancellation are cooperative for in-process functions; a
  non-cooperative function cannot be preempted by this synchronous baseline.
- Functional official agents and node integration remain separate features.

## Recommendation

```text
FIELD QA REQUIRED
MERGE REVIEW NOT YET AUTHORIZED
```

QA does not approve or authorize merge.
