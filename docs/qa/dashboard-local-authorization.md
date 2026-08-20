# DASHBOARD-LOCAL-AUTHORIZATION-001 QA

## Identity

```text
branch: feature/dashboard-local-authorization-001
base: 0ecf6d16d6078923a07964d477692eae5e67b756
target: develop
platform: Mac development machine
runtime behavior changed: no
field QA: not required for in-process contract behavior
```

Development uses an isolated worktree from current `origin/develop`. The
user's dirty primary checkout and historical untracked artifacts remain
untouched.

## Checks

```text
CHECK 1 identity, develop base, roadmap order, and isolated worktree
CHECK 2 owner architecture and canonical handoff reuse
CHECK 3 bounded policy, opaque issuer, and session lifecycle
CHECK 4 explicit decisions, denial mapping, and audit handoffs
CHECK 5 single-use replay and evidence-consumption enforcement
CHECK 6 agent-runtime non-bypass and privacy redaction
CHECK 7 focused format, tests, strict Clippy, and size guards
CHECK 8 workspace quality gate and baseline warning classification
CHECK 9 fresh develop reconciliation and Core Safety
CHECK 10 architecture handoff and controlled push authorization
```

## Focused Results

```text
CHECK 1: PASS
CHECK 2: PASS
CHECK 3: PASS
CHECK 4: PASS WITH AUDIT HARDENING
CHECK 5: PASS
CHECK 6: PASS
CHECK 7: PASS
CHECK 8: PASS WITH BASELINE WARNINGS
CHECK 9: PASS
CHECK 10: MERGED / VALIDATED / CLOSED
```

Focused evidence:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-core: PASS, 43 unit + 11 authorization + 10 shared + 9 local API
cargo clippy -p iamine-core --all-targets -- -D warnings: PASS
```

Repository evidence:

```text
./scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
new warnings: 0
workspace tests: 1168 passed
cargo clippy --workspace --all-targets: PASS with historical warnings
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
main.rs: 4935 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
authority.rs: 392 lines
types.rs: 648 lines
new Rust TODO/FIXME/unwrap/expect/panic markers: 0
```

## New Coverage

The 11 authorization integration tests verify:

1. zero, unbounded, and contradictory policy values fail closed;
2. session issuer authority, capacity, expiry, and audit behavior;
3. read-only session evidence still requires owner dispatch;
4. mutation confirmation and request-ID replay rejection;
5. explicit denial remains denied and cannot be retried with the same ID;
6. foreign, revoked, and expired sessions are rejected;
7. clock regression and lifetime overflow fail closed;
8. evidence is bound to session, request, operation, and expiry;
9. contradictory transport handoffs are rejected;
10. local agent approval cannot replace agent-runtime authority;
11. debug and audit surfaces redact capabilities and request IDs.

## Findings

1. The Local Control API handoff calculation originally lived only inside
   ingress validation. It is now one canonical function reused by ingress and
   local authorization, avoiding duplicated operation-class policy.
2. The first implementation allowed evidence to be detached from its decision
   audit handoff. Architecture review changed `consume` to accept the complete
   approved decision and retain both decision and consumption audit handoffs.
3. Browser/session wire transport remains intentionally undefined. Exposing
   the opaque Rust evidence in JSON would turn an internal capability into a
   bearer credential and is prohibited.
4. Replay state is bounded and memory-only. Capacity exhaustion denies instead
   of evicting a live record to allow a mutation.

## Core Safety

The feature changes `iamine-core` contract owners, focused tests, architecture
and QA evidence, the Local Control API catalog, and the GUI/CLI roadmap row. It
does not change `iamine-node`, `main.rs`, `cluster_registry.rs`, dashboard
TypeScript, CLI behavior, P2P, PubSub, scheduler, workers, hardware, models,
inference, agent policy, or agent runtime.

Pre-merge `git fetch origin --prune` confirmed the exact base
`0ecf6d16d6078923a07964d477692eae5e67b756`. The controlled merge commit is
`ee0f074b6eaf95e7e7aa31d6d086ea6f4967b0cf`; its first parent is the validated
base and its second parent is the feature commit. `origin/main` has no
commits absent from `origin/develop`; no reconciliation merge was required.

## Field QA Classification

```text
Mac runtime smoke: not required, no runtime adapter exists
TS140: not required
Proxmox/R5500: not required
future NODE-LOCAL-CONTROL-API-001 field QA: required
```

## Recommendation

```text
MERGED / VALIDATED / CLOSED
```
