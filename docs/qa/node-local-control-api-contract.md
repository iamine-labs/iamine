# NODE-LOCAL-CONTROL-API-CONTRACT-001 QA

## Identity

```text
branch: feature/node-local-control-api-contract-001
base: 1170c4a67996d97f757fc18950bfebe4f2ea24e5
target: develop
feature commit: ae53c9be565b1ba9904442e598c81af8a84cfad1
merge commit: 4bb90fdd56874655c484c52a18781890097e4767
validated tree: 9c7f79fd5f22d170a0563ae27ca628062846a824
platform: Mac development machine
runtime behavior changed: no
field QA: not required for contract-only behavior
```

No historical local or remote feature branch existed. Development used an
isolated worktree from current `origin/develop`; the user's dirty primary
checkout and all untracked historical artifacts remained untouched.

## Scope

Validate a strict, privacy-bounded Local Control API transport contract over
the existing shared GUI/CLI types. It must define local ingress, request and
response envelopes, explicit authorization/replay/audit handoffs, and a threat
model without binding a server or granting authority.

## Required Checks

```text
CHECK 1 identity, develop base, history, and isolated worktree
CHECK 2 architecture, roadmap, and iamine-core ownership
CHECK 3 loopback transport, limits, handoffs, and threat model
CHECK 4 typed implementation and negative tests
CHECK 5 iamine-core format, tests, and strict Clippy
CHECK 6 workspace quality gate and size guards
CHECK 7 fresh develop reconciliation and Core Safety
CHECK 8 architecture handoff and controlled push authorization
```

## Results

```text
CHECK 1: PASS
CHECK 2: PASS
CHECK 3: PASS
CHECK 4: PASS WITH HARDENING
CHECK 5: PASS
CHECK 6: PASS WITH BASELINE WARNINGS
CHECK 7: PASS
CHECK 8: PASS, CONTROLLED MERGE VALIDATED
```

Focused evidence:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-core: PASS, 43 unit + 10 shared + 9 local API tests
cargo clippy -p iamine-core --all-targets -- -D warnings: PASS
git diff --check: PASS
```

Repository evidence:

```text
./scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
new warnings: 0
workspace tests: 1157 passed
cargo clippy --workspace --all-targets: PASS with baseline warnings
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
main.rs: 4935 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
local_control_api_contract.rs: 378 lines
local_control_api_contract tests: 321 lines
new Rust TODO/FIXME/unwrap/expect/panic markers: 0
```

## Contract Coverage

The 9 new integration tests verify:

1. exact request envelope and correlated response shape;
2. current API schema and generic operation endpoint;
3. incompatible schema, invalid/null UUID, and unknown-field rejection;
4. IPv4 browser and IPv6 native loopback admission;
5. non-loopback transport/peer and missing/cross-origin rejection;
6. POST, route, JSON media type, and 64 KiB request limit;
7. distinct authorization/replay/audit requirements for every operation class;
8. validation and handoffs never authorize actions;
9. operation correlation, 512 KiB response limit, and redacted problem mapping.

## Findings

1. The shared contract already owns operation identity and outcome semantics.
   The new module wraps those types instead of creating a second policy model.
2. An initial draft left route selection to the future adapter. Architecture
   review added one generic `/api/v1/operations` endpoint so URL paths cannot
   become a second operation registry.
3. UUID parsing initially accepted the null UUID. It now rejects null and
   non-canonical IDs to preserve meaningful request correlation.
4. Four fixture `expect()` calls were removed before the broad gate. The final
   feature introduces no `unwrap`, `expect`, `panic`, TODO, or FIXME marker.
5. Workspace Clippy warnings in `client-rust`, `iamine-models`,
   `iamine-network`, and `iamine-node` are historical and outside the diff.
   Focused strict Clippy for `iamine-core` is clean.
6. Optional security tools are unavailable. Their checks were skipped and are
   not represented as executed.
7. A sandboxed post-merge run blocked the daemon Unix socket and four
   Metal-backed inference cases. The same failures reproduced on the exact
   base. On the host, the focused failures and the complete merge quality gate
   passed, classifying the finding as an environment/harness limitation.

## Core Safety

The feature changes only an additive `iamine-core` module/export, focused
tests, architecture/QA evidence, and the GUI/CLI roadmap row. It does not
change `iamine-node`, `main.rs`, `cluster_registry.rs`, CLI behavior, P2P,
PubSub, scheduler, workers, hardware, models, inference, agents, or dashboard
code. No dependency or lockfile changed.

Fresh `git fetch origin --prune` confirmed `origin/develop` remains at the
exact base `1170c4a67996d97f757fc18950bfebe4f2ea24e5`. There is no remote
divergence to merge before handoff.

The controlled merge has parents `1170c4a` and `ae53c9b`; its tree exactly
matches the validated feature tree. Post-merge `./scripts/quality-gate.sh`
passed with `required_failures=0`, `warnings=0`, and the three unavailable
optional tools reported as skipped.

## Field QA Classification

```text
Mac runtime smoke: not required, no runtime exists
TS140: not required
Proxmox/R5500: not required
future real Local Control API field QA: required
```

## Recommendation

```text
MERGED / VALIDATED / CLOSED
```

This recommendation approves only the contract boundary. It does not approve
a listener, local session, authorization decision, audit persistence,
dashboard connectivity, owner dispatch, mutation, or agent execution.
