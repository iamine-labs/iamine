# Reporter Agent QA

Feature:

```text
REPORTER-AGENT-001
```

Current state:

```text
FIELD QA PASSED
READY FOR ARCHITECTURE MERGE REVIEW
```

## Authorized Identity

```text
branch: feature/reporter-agent-001
authorized base: 65f12dc3c7b6a67489fe54e691dd30778bd6a183
implementation commit: bd72f6baf53444d03e5d68eb83471e8704f28c2b
implementation tree: 1ef3a6d042ffca65ed85ba0e41a9ce3570e7906b
reconciled develop: 1d8b0aeb0e3254b915765d865ce572e448428c98
reconciled develop tree: 7ad0375a7a4e5387b7a92efd7e7a4b080aa82b71
QA candidate commit: b69fd9e85c5d18a287d4c6160b6b4b27c5c5134f
QA candidate tree: 4f21ffb6d30ade3f05f63afb0d3e88c447a871e8
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

On 2026-08-24, the clean feature branch was reconciled with the fetched
`origin/develop` identity above through a non-conflicting `--no-ff` merge. The
branch moved from 31 commits behind and four ahead to zero behind and five
ahead. The merge introduced 95 dashboard and GUI-documentation files from
`develop`; the Reporter side changed 35 files before reconciliation, and the
two name sets had zero overlap.

A direct comparison from the pre-reconciliation tip
`a26695e8cc4ceb754d9486259cae9b25c273ce14` to the QA candidate found no
content change under `agents/official/reporter`, `iamine-agent-runtime`, or
`iamine-node`. The Reporter implementation and its runtime contracts are
therefore unchanged by the merge. The branch was clean after reconciliation.

## Mac Frontend Regression

```text
CHECK MAC-FRONTEND-REGRESSION: PASS WITH RECORDED TEST-SYNCHRONIZATION FINDING
validated branch tip: e4328e087155cc554a0022a6a8f9cb8451506fae
Node.js: 24.19.0
npm: 11.16.0
format: PASS
lint: PASS
typecheck: PASS
unit tests: 51/51 PASS
production build: PASS
npm audit: 0 vulnerabilities
E2E initial matrix: 15/16 PASS
exact failed E2E rerun: 1/1 PASS
visual matrix: 1440x900, 1024x768, and 390x844 PASS
```

The inherited dashboard from reconciled `develop` was validated on Mac without
changing dashboard, Reporter, runtime, or core source. All six implemented
routes (`overview`, `agents`, `nodes`, `models`, `activity`, and `diagnostics`)
were inspected. The 1024- and 390-pixel route matrices had no horizontal
overflow or interactive elements outside the viewport. The reviewed desktop
and mobile views had no visible clipping or overlap, all referenced images
loaded, and the browser console contained no warnings or errors.

The initial multi-browser E2E run passed 15 of 16 cases. The Firefox 1024 shell
case timed out while waiting for `networkidle`, although its captured page was
fully rendered, all HTTP responses had succeeded, and the Vite development
server's HMR WebSocket remained open. The exact failed case passed when rerun
once in isolation. This is recorded as a nondeterministic test-synchronization
finding rather than a dashboard product failure or a pristine initial E2E
pass. Future test maintenance should replace dev-server `networkidle` waits
with explicit UI-readiness assertions. No product or test code was modified
during this QA check.

The production preview was stopped after inspection and port 4173 had no
remaining listener. Generated Playwright results remain local QA artifacts and
are not staged for commit.

## Current Local Results

```text
focused Reporter tests: 10/10 PASS
focused Node Doctor regression group: 21/21 PASS
iamine-agent-runtime input/output enforcement: 8/8 PASS
iamine-models outside sandbox: 100/100 unit + 59/59 integration PASS
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
scripts/quality-gate.sh raw result: FAIL
required failures: 3, classified as sandbox-only after isolated reruns
architecture warnings: 1 historical size warning
optional tools skipped: 3
```

The canonical quality gate passed format, network tests, node build, clippy,
diff checks, repository guards, and the hard architecture guards. Its three
required failures were `cargo test -p iamine-models`, `cargo test -p
iamine-node`, and `cargo test --workspace`. The model and workspace failures
were the same four TinyLlama/Metal assertions: `test_concurrency_limit`,
`test_inference_queue`, `test_real_inference`, and `test_token_streaming`. The
node failure was `daemon_runtime::tests::test_daemon_start_stop`, which could
not create its socket inside the filesystem sandbox.

Each of the four Metal cases passed individually with serial execution outside
the sandbox. The daemon socket case also passed outside the sandbox. The raw
gate remains reported as `FAIL`; the five isolated passes classify its failures
as environmental rather than Reporter regressions. The gate also reported the
existing 914-line `iamine-node/src/cli.rs` architecture warning and historical
clippy warnings. `main.rs` remained within the 5,000-line warning threshold and
grew by two lines over `origin/develop`. `cargo audit`, `cargo deny`, and
`gitleaks` were unavailable and were reported as skipped.

The complete affected suites were then repeated outside the filesystem
sandbox with `RUST_TEST_THREADS=1`. `iamine-models` passed all 100 unit and 59
integration tests, `iamine-node` passed all 506 tests, and `cargo test
--workspace` passed across the complete repository. This closes the three raw
gate failures for the Mac host while preserving the gate's original result as
executed.

## Mac Field Result

```text
MAC FIELD QA: PASS
REMOTE FIELD QA: PASS
```

The reconciled QA candidate commit and tree matched the identity above and the
working tree was clean. A fresh isolated Cargo target was used after a shared
target exposed stale dependency metadata. The fresh build passed, followed by
Reporter 10/10, the filtered Node Doctor regression group 21/21, and
input/output enforcement 8/8 focused tests.

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

On 2026-08-29 UTC, a complete-history bundle was used to validate the exact QA
candidate in detached, disposable checkouts. The reconstructed package bundle
had SHA-256
`16926a2269253ab5e05917f7b1b256a5fd3ddff174854ccc390c1df67de78d7d`.
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

The 25 positive JSON outputs were validated again after retrieval on the Mac.
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

The canonical gate's serial real-model run still failed four TinyLlama/Metal
generation assertions inside the sandbox. All four passed when repeated one at
a time outside it and as part of both the complete `iamine-models` suite and
the complete workspace suite. The daemon socket test likewise passed outside
the sandbox and in the complete `iamine-node` and workspace suites. Neither
finding belongs to the Reporter source diff.

The first remote attempt stopped read-only because non-interactive TS140 SSH
omitted the installed `~/.cargo/bin` toolchain from PATH. Package version 4
added that standard per-user path when present without installing or changing
the toolchain. The next attempt passed TS140 and then stopped in the Proxmox
inventory check because SSH aliases differ from VM names. Package version 5
added the observed alias-to-VM-name mapping and required exact prior TS140 PASS
evidence before resuming at Proxmox. No product source was changed during QA,
and successful TS140 execution was not repeated.

## QA Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not authorize merge or closure. Architecture must review this evidence,
reconcile the feature against current canonical `develop`, and require final
pre-merge and post-merge validation.
