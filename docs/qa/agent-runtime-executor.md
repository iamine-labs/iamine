# AGENT-RUNTIME-EXECUTOR-001 QA

## Identity

```text
feature: AGENT-RUNTIME-EXECUTOR-001
branch: feature/agent-runtime-executor-001
base: b5aaf292f71cf7a3b243fc2780bac5f95c8223d6
base tree: a3085fafb2e9f28d26b1a0430aa5e3ffd287ce8f
field QA source commit: df6b9037994822db3677e13175184e81a9dcff58
field QA source tree: 4a37be4da2e42f4f8cc48004346e034377eb3856
feature tip: 4c070f2f4d64508a817334c0cd967ccf56097bfc
feature tip tree: 7db36dc65c06e560b8dfe82e14393c41f7fc276b
merge commit: 612d5cd84d0c79a3a7909e1b2d1aafb29fd40440
merge tree: 7db36dc65c06e560b8dfe82e14393c41f7fc276b
canonical remote: origin
runtime behavior changed: registered official Rust execution
field QA required: yes
field QA result: PASS
post-merge validation: PASS WITH WARNINGS
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

## Field Results

QA transferred a complete Git bundle with SHA-256:

```text
fc81fae549e45911b69f29547f6a0a9005933204e754c473c819f9e3116d39b7
```

Every role validated source commit
`df6b9037994822db3677e13175184e81a9dcff58`, tree
`4a37be4da2e42f4f8cc48004346e034377eb3856`, and base
`b5aaf292f71cf7a3b243fc2780bac5f95c8223d6` before testing.

| Platform role | Focused executor | Runtime regression | Final state |
| --- | --- | --- | --- |
| macOS development | 12/12 PASS | 149/149 PASS | clean |
| physical Linux, TS140 | 12/12 PASS | 149/149 PASS | clean |
| Linux VM control, iamine-ctrl | 12/12 PASS | 149/149 PASS | clean |
| Linux VM worker A, iamine-wrk1 | 12/12 PASS | 149/149 PASS | clean |
| Linux VM worker B, iamine-wrk2 | 12/12 PASS | 149/149 PASS | clean |
| Linux VM heavy, iamine-heavy | 12/12 PASS | 149/149 PASS | clean |

Aggregate:

```text
roles passed: 6/6
focused tests: 72/72 PASS
runtime regression: 894/894 PASS
product failures: 0
runtime side effects observed: 0
source changes during QA: 0
node processes started: 0
worker processes started: 0
```

TS140 used the existing `/home/ts140/.cargo/bin/cargo` executable. The Mac
used an exact detached worktree and shared only generated Cargo cache.
Canonical remote working copies were not modified.

## Environmental Findings

The Mac data volume initially had 116 MiB available. The first quality-gate
workspace build failed with `No space left on device`. Removing only the
discardable feature-worktree `target/` restored 3.5 GiB; source and Git
evidence were unchanged.

Proxmox preflight recorded:

```text
iamine-ctrl: / 100% used, /dev/shm 4.1 GB available
iamine-wrk1: / 100% used, /dev/shm 2.2 GB available
iamine-wrk2: / 99% used, /dev/shm 4.1 GB available
iamine-heavy: / 97% used, /dev/shm 12.3 GB available
```

The guests expose session-isolated `/dev/shm`. Initial SFTP transfers returned
success but were not visible to the following SSH session. QA stopped,
classified the harness condition, and used one SSH session to receive the
bundle, clone, verify, compile, test, and inspect final state in tmpfs.

`iamine-wrk1` emitted `database or disk is full` while its root filesystem was
saturated. Both required commands nevertheless exited successfully with
12/12 and 149/149. No source was changed, no package was installed, no process
was terminated, and no canonical artifact was deleted.

## Known Boundaries

- The current handler is a trusted function compiled into the operator binary.
- The sandbox adapter validates a restriction contract but does not enforce or
  claim OS isolation.
- Timeout and cancellation are cooperative for in-process functions; a
  non-cooperative function cannot be preempted by this synchronous baseline.
- Functional official agents and node integration remain separate features.

## Post-Merge Validation

The merge owner integrated feature tip
`4c070f2f4d64508a817334c0cd967ccf56097bfc` into `develop` as merge
`612d5cd84d0c79a3a7909e1b2d1aafb29fd40440`. Both resolve to tree
`7db36dc65c06e560b8dfe82e14393c41f7fc276b`.

Mac post-merge checks:

```text
focused executor: 12/12 PASS
iamine-agent-runtime: 149/149 PASS
iamine-agents: 109/109 PASS
strict runtime Clippy: PASS
format and diff checks: PASS
```

A clean TS140 clone from complete bundle SHA-256
`0d3bdb07182eb3fb1d9ad05b750a088ff1bb2f5b1fae47ea95ebe3ee5778c7b5`
validated the exact merge and tree. The full quality gate passed formatting,
models, network, node, node build, the complete workspace test inventory,
workspace Clippy, and both diff checks:

```text
workspace tests listed: 1118
required_failures=0
warnings=0
skipped=3
QUALITY GATE RESULT: PASS WITH WARNINGS
```

All four Metal/model tests that intermittently failed on Mac passed in this
post-merge Linux gate. `cargo audit`, `cargo deny`, and `gitleaks` were
unavailable. TS140 lacked `rg`, so its early text guards were non-authoritative;
the corresponding Mac guards passed. The TS140 checkout was clean after the
gate and no IAMINE node or client process remained active.

## Recommendation

```text
MERGED / VALIDATED / CLOSED
```

The next action is the exhaustive
`V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001` closure gate.
