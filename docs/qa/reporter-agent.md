# Reporter Agent QA

Feature:

```text
REPORTER-AGENT-001
```

Current state:

```text
QA BLOCKED
```

## Authorized Identity

```text
branch: feature/reporter-agent-001
base: 65f12dc3c7b6a67489fe54e691dd30778bd6a183
source commit: bd72f6baf53444d03e5d68eb83471e8704f28c2b
source tree: 1ef3a6d042ffca65ed85ba0e41a9ce3570e7906b
origin: https://github.com/iamine-labs/iamine
```

## Scope

The candidate adds the official local-readonly Reporter package, typed bounded
input and output, CLI dispatch, and a shared official-agent execution
composition used by Reporter and Node Doctor. The source diff does not modify
`iamine-core`, `iamine-models`, `iamine-network`, dashboard code, scheduler,
P2P, PubSub, worker lifecycle, model execution, or inference behavior.

## Local Results

```text
focused Reporter tests: 9/9 PASS
iamine-agent-runtime input/output enforcement: 8/8 PASS
iamine-node: 506/506 PASS
iamine-core direct suites: 73/73 PASS
cargo build -p iamine-node: PASS
supported report CLI smoke: PASS
missing-evidence blocked report CLI smoke: PASS
unsupported-claim handoff CLI smoke: PASS
private-shaped input rejection and no-echo smoke: PASS
git diff --check: PASS
git diff --cached --check: PASS
scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
architecture warnings: 1 historical size warning
optional tools skipped: 3
```

The isolated quality gate passed format, package tests, node build, workspace
tests, clippy, repository guards, and architecture guards. It reported the
existing `iamine-node/src/cli.rs` size warning and historical clippy warnings.
No Reporter warning remained after the focused clippy correction. `cargo
audit`, `cargo deny`, and `gitleaks` were unavailable and were reported as
skipped.

## Mac Field Result

The exact source commit was checked out detached in a disposable worktree. Its
commit, tree, base, and clean status matched the authorized identity above. A
build from that checkout passed, followed by Reporter 9/9, Node Doctor 8/8,
and input/output enforcement 8/8 focused tests.

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
with altered capability metadata failed closed with `PackageMismatch`.

```text
IAMINE processes: 0 -> 0
fresh HOME entries: 0 -> 0
fresh working-directory entries: 0 -> 0
exact worktree status after QA: clean
logs created: 0
profiles created: 0
model-store entries created: 0
```

## Remote Field Blocker

Reachability was checked before attempting remote QA. No bundle was created or
transferred, and no remote working copy was modified.

```text
TS140 / 192.168.2.200: SSH timeout
iamine-ctrl / 192.168.2.220: SSH timeout
iamine-wrk1 / 192.168.2.221: SSH timeout
iamine-wrk2 / 192.168.2.222: SSH timeout
iamine-heavy / 192.168.2.223: SSH timeout
```

Architecture requires the exact commit to pass on Mac, TS140, and all four
Proxmox/R5500 roles. Therefore this evidence does not claim Field QA PASS,
merge readiness, merge approval, or closure.

## Environment Findings

The existing workspace target intermittently selected stale `iamine-core`
incremental metadata during broad workspace validation. A fresh isolated
target compiled the complete workspace and dashboard authorization target,
and the canonical quality gate then passed.

A manually parallelized real-model integration run failed four TinyLlama/Metal
generation assertions. The canonical gate sets `RUST_TEST_THREADS=1`; under
that required serial policy all 59 model integration tests passed twice. The
daemon socket test also failed inside the filesystem sandbox with `Operation
not permitted` and passed outside it. Neither finding belongs to the Reporter
source diff.

## Resume Condition

Resume Field QA when TS140 and the four Proxmox guests are reachable. Validate
the exact source commit and tree above in disposable checkouts, then update
this evidence before Architecture merge review.
