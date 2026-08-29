# Reporter Agent QA

Feature:

```text
REPORTER-AGENT-001
```

Current state:

```text
FIELD QA PASSED
READY FOR ARCHITECTURE MERGE REVIEW
MERGED
POST-MERGE VALIDATION PASSED
MERGED / VALIDATED / CLOSED
```

## Authorized Identity

```text
branch: feature/reporter-agent-001
authorized base: 27c4d5fb7a6f4315546a5897c5e136c3748940ad
implementation commit: bd72f6baf53444d03e5d68eb83471e8704f28c2b
implementation tree: 1ef3a6d042ffca65ed85ba0e41a9ce3570e7906b
timeout fix commit: 4a10d2912819592b6c5f0f7eef0b6ca6eb1a926c
reconciled develop: 27c4d5fb7a6f4315546a5897c5e136c3748940ad
reconciled develop tree: 299e7becdcdb8f3c1557ad43ca57571e4c185aa9
QA candidate commit: 4a10d2912819592b6c5f0f7eef0b6ca6eb1a926c
QA candidate tree: e416330a672270474bb99a55240075c72862d22d
feature tip: 2b83c4fc6cdde0438551134c33ab5ece5a9d6c07
feature tip tree: c97274f3ace6af6735e349257f05ed1607f2a132
merge commit: 8f5d4fb2470406b946e76da585e2ea4a55199f70
merge tree: c97274f3ace6af6735e349257f05ed1607f2a132
origin: https://github.com/iamine-labs/iamine
```

## Scope

The Reporter implementation commit adds the official local-readonly package,
typed bounded input and output, CLI dispatch, and a shared official-agent
execution composition used by Reporter and Node Doctor. Its Reporter-owned
diff does not modify `iamine-core`, `iamine-models`, `iamine-network`,
dashboard code, scheduler, P2P, PubSub, worker lifecycle, model execution, or
inference behavior. Dashboard changes present in the reconciled candidate came
unchanged from canonical `develop`.

## Canonical Develop Reconciliation

The clean feature branch was reconciled without conflicts through merge
`11a55531d6e421cbd42947d893163e2fb30abaa3` with the fetched canonical
`origin/develop` identity above. The incoming develop delta was limited to
dashboard readiness tests and documentation and did not overlap Reporter,
agent runtime, Node Doctor, models, network, scheduler, worker, or inference
source. A disposable merge simulation also completed without conflicts.

Local QA then found a timing regression at the shared official-agent boundary:
Node Doctor's hardware-backed report could exceed the inherited 1,000 ms
execution budget on macOS. Commit `4a10d2912819592b6c5f0f7eef0b6ca6eb1a926c`
made the execution timeout explicit per immutable agent spec, preserved
Reporter at 1,000 ms, and assigned Node Doctor 5,000 ms. No other timeout class
or subsystem changed.

## Mac Frontend Regression

```text
CHECK MAC-FRONTEND-REGRESSION: PASS
validated reconciled tip: 11a55531d6e421cbd42947d893163e2fb30abaa3
Node.js: 24.19.0
npm: 11.17.0
format: PASS
lint: PASS
typecheck: PASS
unit tests: 51/51 PASS
production build: PASS
E2E matrix: 20/20 PASS
browser matrix: Chromium 1440, Firefox 1024, WebKit 390, Chromium 360
```

The inherited dashboard from reconciled `develop` was validated on Mac without
changing dashboard, Reporter, runtime, or core source. All six implemented
routes (`overview`, `agents`, `nodes`, `models`, `activity`, and `diagnostics`)
were inspected. The 1024- and 390-pixel route matrices had no horizontal
overflow or interactive elements outside the viewport. The reviewed desktop
and mobile views had no visible clipping or overlap, all referenced images
loaded, and the browser console contained no warnings or errors.

The final multi-browser matrix passed all 20 cases. The first in-sandbox E2E
attempt could not bind `127.0.0.1:4173` and was classified as an environmental
`EPERM`; the exact rerun outside the filesystem sandbox passed. No dashboard,
Reporter, runtime, or core source was modified during this QA check.

The production preview was stopped after inspection and port 4173 had no
remaining listener. Generated Playwright results remain local QA artifacts and
are not staged for commit.

## Current Local Results

```text
focused Reporter tests: 10/10 PASS
focused Node Doctor regression group: 21/21 PASS
iamine-agent-runtime input/output enforcement: 8/8 PASS
iamine-models with isolated HOME: 100/100 unit + 59/59 integration PASS
iamine-node outside sandbox: 506/506 PASS
cargo test --workspace outside sandbox: PASS
cargo build -p iamine-node: PASS
cargo fmt --all -- --check: PASS
supported report CLI smoke: PASS
missing-evidence blocked report CLI smoke: PASS
unsupported-claim handoff CLI smoke: PASS
eight-evidence boundary CLI smoke: PASS
ninth-evidence rejection CLI smoke: PASS
duplicate-evidence rejection CLI smoke: PASS
contradictory-evidence rejection CLI smoke: PASS
private-shaped input rejection and no-echo smoke: PASS
git diff --check: PASS
git diff --cached --check: PASS
scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
architecture warnings: 1 historical size warning
optional tools skipped: 3
```

The conclusive quality gate ran outside the filesystem sandbox with an isolated
HOME and isolated Cargo target. Every required check and clippy passed. The
gate reported the existing 914-line `iamine-node/src/cli.rs` size warning and
historical clippy warnings. `main.rs` remained within the 5,000-line threshold
and grew by two lines over `origin/develop`. `cargo audit`, `cargo deny`, and
`gitleaks` were unavailable and were reported as skipped.

Two earlier gate attempts identified environmental contamination rather than
product failures. The operator HOME exposed an installed TinyLlama model to
four otherwise conditional Metal integration tests; all 159 model tests passed
with an isolated HOME. The filesystem sandbox rejected the daemon's temporary
Unix socket with `EPERM`; the exact test passed outside the sandbox. A separate
intermittent Node Doctor timeout remained after both environmental conditions
were corrected and was fixed by the per-agent timeout commit above. The final
gate then passed both `iamine-node` and workspace without retries.

## Mac Field Result

```text
MAC FIELD QA: PASS
REMOTE FIELD QA: PASS
```

The exact QA candidate and tree matched the identity above in a detached,
disposable checkout. A fresh isolated Cargo target was used after a shared
target exposed stale dependency metadata. The fresh build passed, followed by
Reporter 10/10, Node Doctor 21/21, and input/output enforcement 8/8.

Field executions ran from a new empty working directory with a new empty HOME.
The human output and the three valid JSON paths returned the expected
`support_report`, `blocked_action_report`, and `handoff_request`
classifications. Explicit `missing` evidence also returned the bounded blocked
report. Eight distinct evidence records were accepted, while the ninth was
rejected. Duplicate and contradictory evidence were rejected with typed
errors.

Each valid result reported package load, runtime authorization, sandbox
adapter use, cleanup, and audit evidence, with scheduler mutation, transport
startup, persistence, and OS-isolation claims disabled. A private-shaped claim
failed with exit code 1 and did not echo the private token. A copied package
with altered capability metadata failed closed with `PackageMismatch` in the
earlier detached-source run. The exact-candidate focused suite also proved that
altered capability metadata and an altered manifest fail closed.

```text
IAMINE processes: 0 -> 0
fresh HOME entries: 0 -> 0
fresh working-directory entries: 0 -> 0
exact branch status after QA: clean
logs created: 0
profiles created: 0
model-store entries created: 0
```

## Remote Field Results

On 2026-08-29 UTC, package v6 used a complete-history bundle to validate the
exact post-fix QA candidate in detached, disposable checkouts. The bundle had
SHA-256
`fdb14863e03c2d530413126d55a5964097b01c7a39241962358aa749d8eebc71`.
Every role independently verified the expected candidate, tree, authorized
base, bundle checksum, and clean source state.

```text
TS140: PASS
Proxmox/R5500 host preflight: PASS
iamine-ctrl: PASS
iamine-wrk1: PASS
iamine-wrk2: PASS
iamine-heavy: PASS
```

TS140 ran Linux x86_64 with AVX2 and FMA available and Rust/Cargo 1.94.0. The
four Proxmox guests ran Linux x86_64 under KVM without exposed AVX2 or FMA and
with Rust/Cargo 1.95.0. Proxmox host `pve-manager/9.0.3` reported the mapped
controller, two workers, and heavy worker VMs running before guest transfer.

Each of the five execution roles passed:

```text
cargo fmt --all -- --check: PASS
Reporter focused tests: 10/10 PASS
Node Doctor regression group: 21/21 PASS
runtime input/output enforcement: 8/8 PASS
iamine-node build: PASS
supported human report: PASS
supported JSON report: PASS
missing evidence blocked report: PASS
explicit missing evidence report: PASS
unsupported claim handoff: PASS
eight-evidence boundary: PASS
ninth-evidence rejection: PASS
duplicate evidence rejection: PASS
contradictory evidence rejection: PASS
private-shaped input rejection and no echo: PASS
```

The 25 positive remote JSON outputs were validated again after retrieval on the
Mac.
All five roles preserved empty runtime HOME, work, and temporary directories,
kept the detached source clean, started no model download or real model load,
and left the IAMINE process count unchanged. TS140 remained at one pre-existing
IAMINE process before and after the run; every guest remained at zero. All
preflight and runner stderr files were empty.

Each build reported the same five historical `dead_code` warnings in
`task_cache.rs`, `task_protocol.rs`, `wallet.rs`, and `worker_pool.rs`. None of
those files belongs to the Reporter diff, and every build completed.

## Environment Findings

The existing shared workspace target selected stale agent-runtime incremental
metadata and initially failed to compile Reporter because it could not see the
current `SupportReport` classification. The source contained the variant and
the reconciliation changed no Reporter or runtime file. A fresh isolated
target compiled the same candidate and passed every focused Reporter check.

The initial gate inherited the operator's installed TinyLlama model; its four
conditional Metal tests passed under an isolated HOME. The daemon socket test
passed outside the filesystem sandbox. These two findings are environmental
and do not belong to the Reporter diff.

After those conditions were removed, Node Doctor intermittently exceeded the
shared 1,000 ms execution timeout on macOS. A focused post-fix run completed in
1.08 seconds, proving that the old budget was insufficient. The explicit
5,000 ms Node Doctor budget removed the failure while Reporter retained its
1,000 ms limit. The conclusive quality gate and exact-candidate Field QA then
passed.

The first remote attempt stopped read-only because non-interactive TS140 SSH
omitted the installed `~/.cargo/bin` toolchain from PATH. Package version 4
added that standard per-user path when present without installing or changing
the toolchain. The next attempt passed TS140 and then stopped in the Proxmox
inventory check because SSH aliases differ from VM names. Package version 5
added the observed alias-to-VM-name mapping and required exact prior TS140 PASS
evidence before resuming at Proxmox. No product source was changed during those
attempts, and successful TS140 execution was not repeated until candidate
identity changed for the timeout fix. Package v6 then reran every role as
required for the new commit and tree.

## QA Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not authorize merge or closure. Architecture must review this evidence,
reconcile the feature against current canonical `develop`, and require final
pre-merge and post-merge validation.

## Post-Merge Validation

Architecture approved the exact QA candidate and the Merge Owner integrated
feature tip `2b83c4fc6cdde0438551134c33ab5ece5a9d6c07` into canonical `develop` as
merge `8f5d4fb2470406b946e76da585e2ea4a55199f70`. The feature tip and merge both
resolve to tree `c97274f3ace6af6735e349257f05ed1607f2a132`; the difference from the
Field QA candidate is review and QA documentation only.

The exact merge tree passed the complete Mac quality gate:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-models: PASS
cargo test -p iamine-network: PASS
cargo test -p iamine-node: 506/506 PASS
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
cargo clippy --workspace --all-targets: PASS
git diff --check: PASS
git diff --cached --check: PASS
required failures: 0
warnings: 1
optional tools skipped: 3
QUALITY GATE RESULT: PASS WITH WARNINGS
```

The warning is the existing 914-line `iamine-node/src/cli.rs` architecture
warning. `cargo audit`, `cargo deny`, and `gitleaks` were unavailable. The first
isolated-HOME invocation did not expose the installed rustup default and was
classified as an environmental setup failure before tests ran. The corrected
invocation used the existing Rust toolchain explicitly, retained isolated
runtime HOME and Cargo target directories, and passed without product changes.

Architecture and the Merge Owner therefore record:

```text
MERGED / VALIDATED / CLOSED
```
