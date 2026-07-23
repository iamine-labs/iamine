# AGENT-PACKAGE-REFERENCE-RESOLVER-001 QA

## Identity

```text
branch: feature/agent-package-reference-resolver-001
base: c018e4a25aa054c23f2f5818f0f946eace47922f
base tree: 94a3a734e8d79b287af093765ccd0f9043487d0d
tracked clean before implementation: yes
staging clean before implementation: yes
untracked baseline before implementation: empty
expected runtime behavior change: bounded package filesystem reads
```

## Expected Scope

```text
Cargo.lock
iamine-agent-runtime/Cargo.toml
iamine-agent-runtime/src/lib.rs
iamine-agent-runtime/src/error.rs
iamine-agent-runtime/src/limits.rs
iamine-agent-runtime/src/reference.rs
iamine-agent-runtime/src/resolver.rs
iamine-agent-runtime/tests/package_reference_resolver.rs
docs/architecture/agent-package-reference-resolver.md
docs/qa/agent-package-reference-resolver.md
```

No existing `iamine-agents` blocker, node wiring, worker, scheduler, P2P,
hardware, model, inference, installer, service, reward, reputation, wallet,
marketplace, public beta, or mainnet behavior may change.

## Required Assertions

- Package paths are relative to an open directory capability.
- Parent, absolute, Windows-prefix, backslash, and empty path forms fail.
- Parent and final symlinks fail.
- Hard links and non-regular files fail.
- Exactly seven unique references are bounded by count.
- Per-file and aggregate limits are enforced before returning bytes.
- Two reads from the same handle must agree.
- Error and Debug output do not contain roots, declared paths, contents, or raw
  host errors.
- Resolved bytes do not change package-load status or authorize execution.
- `main.rs` and `cluster_registry.rs` remain unchanged.

## Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test package_reference_resolver
cargo test -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
cargo test -p iamine-agents
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
wc -l iamine-agent-runtime/src/*.rs
wc -l iamine-node/src/main.rs iamine-node/src/cluster_registry.rs
```

## Field QA Matrix

This feature performs filesystem access and requires:

```text
Mac development machine
TS140
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

On each environment:

1. verify exact branch, commit, tree, base, tracked state, staging, and
   untracked baseline;
2. build `iamine-agent-runtime`;
3. run `cargo test -p iamine-agent-runtime --test package_reference_resolver`;
4. confirm traversal, symlink, hardlink, size, and privacy assertions pass;
5. confirm temporary fixture cleanup;
6. confirm no IAMINE process, package store, model store, profile, or service
   state changed.

QA must stop on the first environment that cannot validate the exact commit.
No code changes are allowed during field QA.

## Observed Results

```text
implementation: complete
local validation: passed
field QA: passed
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

Local evidence before the source commit:

- `cargo fmt --all -- --check`: PASS
- resolver adversarial integration tests: 8 passed
- `cargo test -p iamine-agent-runtime`: 12 passed
- `cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings`: PASS
- `cargo test -p iamine-agents`: 109 passed
- `./scripts/quality-gate.sh`: PASS WITH WARNINGS
- unique workspace tests: 984 passed
- required failures: 0
- new feature warnings: 0
- optional tools skipped: `cargo audit`, `cargo deny`, `gitleaks`
- `main.rs`: 4,929 lines, delta 0
- `cluster_registry.rs`: 862 lines, delta 0
- largest new production file: `resolver.rs`, 361 lines

Historical `dead_code`, unused Solana client, `too_many_arguments`, and
`type_complexity` warnings remain outside this feature. Strict clippy for the
new runtime crate is clean.

Field identity:

```text
commit: 47b0d3ecbb599b81cc8e97129f275028a8d87176
tree: 7f6c42373df9781046ae1fefceddee293bcaec74
base: c018e4a25aa054c23f2f5818f0f946eace47922f
branch: feature/agent-package-reference-resolver-001
```

Field results:

| Environment | Build | Adversarial tests | Side effects | Result |
| --- | --- | ---: | --- | --- |
| Mac | PASS | 8/8 | none observed | PASS |
| TS140 | PASS | 8/8 | none observed | PASS |
| iamine-ctrl | PASS | 8/8 | none observed | PASS |
| iamine-wrk1 | PASS | 8/8 | none observed | PASS |
| iamine-wrk2 | PASS | 8/8 | none observed | PASS |
| iamine-heavy | PASS | 8/8 | none observed | PASS |

Every environment preserved tracked/staged/untracked state after testing,
cleaned its isolated temporary fixtures, and kept IAMINE process, service, and
state snapshots unchanged.

Field harness findings:

- The TS140 canonical copy contained staged work for another feature. QA
  stopped before synchronization and resumed in an isolated worktree; all 8
  staged and 34 untracked artifacts remained untouched.
- TS140 non-login SSH did not expose Cargo on `PATH`. QA stopped before build,
  confirmed the existing toolchain, and resumed with `~/.cargo/bin` explicit.
- The initial assumed Proxmox repository path was absent. QA stopped before
  fetch, rediscovered the previously authorized clean CANDIDATE_1, and used it
  on all four guests. CANDIDATE_2 was not touched.

These were environment or harness blockers. No product failure was observed
and no source code changed during field QA.

## Controlled Merge And Post-Merge Validation

```text
source closeout: 77342969c7561ecd461d83c8a51396e51ab1c9a1
target: develop
merge: c013f10f267ea13451ea205b8cb3a56b9ac12246
merged tree: 6084e7b3ea05df19471ec96292d7b7bc0e75a35f
merge conflicts: none
runtime behavior changed after field QA: no
```

Post-merge results:

- `cargo test -p iamine-agent-runtime`: PASS, 12 tests
- strict `iamine-agent-runtime` clippy: PASS
- repository and architecture guards: PASS
- format and diff checks: PASS
- `cargo test -p iamine-network`: PASS
- `cargo build -p iamine-node`: PASS
- workspace clippy: PASS with historical warnings
- `cargo audit`, `cargo deny`, and `gitleaks`: SKIPPED, unavailable
- full quality gate: FAIL with accepted baseline/environment exceptions

Failure classification:

| Failure | Base comparison | Classification |
| --- | --- | --- |
| Four TinyLlama integration assertions returned unsuccessful generated output | Exact base `c018e4a` reproduced `test_real_inference`; `iamine-models` diff is empty | stochastic baseline |
| Daemon Unix socket returned `Operation not permitted` | Exact base `c018e4a` reproduced it inside the sandbox; merge passed outside the sandbox | harness restriction |
| Workspace command repeated the four TinyLlama failures | same unchanged model test surface | stochastic baseline |

No resolver failure occurred. Architecture accepted these explicit exceptions
without changing unrelated model, daemon, or test code.

Final state:

```text
MERGED / VALIDATED / CLOSED
next feature: AGENT-PACKAGE-REVIEW-EVIDENCE-001 (PROPOSED)
```
