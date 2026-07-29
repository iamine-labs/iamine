# AGENT-EXECUTION-AUTHORIZATION-001 QA

## State

```text
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
ARCHITECTURE CHECKPOINT PASSED
FIELD QA PASSED
FINAL ARCHITECTURE REVIEW PASSED
MERGED
POST-MERGE VALIDATION PASSED
MERGED / VALIDATED / CLOSED
focused integration tests: PASS, 14/14
runtime regression: PASS, 117/117
agents regression: PASS, 109/109
strict crate clippy: PASS
quality gate: PASS WITH WARNINGS
field QA: PASS, 6/6 roles
```

## Identity

```text
branch: feature/agent-execution-authorization-001
base: ff7ba1668ffdf61a71294ac3fa1921baf426ce43
base tree: f38b05e44c635989fa1594803eee8d97ea45ec5a
source commit: 125264ef77e9fad63a79474c6834be63ae86e5bf
source tree: bbf11787fe76a4f24e20a287f5e7125830bb6a3b
feature tip: 30c84aff1dd12d81a7cdc1f084f819814a8afb1f
merge commit: 22adc690f3b8d9704783d7f8304680d3ea677404
```

## Expected Scope

```text
iamine-agent-runtime/src/execution_authorization/
iamine-agent-runtime/src/audit_event_enforcement/authority.rs
iamine-agent-runtime/src/review_evidence/subject.rs
iamine-agent-runtime/src/routing_candidate_selector/
iamine-agent-runtime/src/lib.rs
iamine-agent-runtime/tests/execution_authorization.rs
iamine-agent-runtime/tests/support/execution_authorization_chain.rs
iamine-agent-runtime/tests/support/routing_policy.rs
iamine-agents/src/scope_enforcement/request.rs
iamine-agents/src/permission_enforcement/request.rs
docs/architecture/agent-execution-authorization.md
docs/qa/agent-execution-authorization.md
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
```

Forbidden:

```text
iamine-node/src/main.rs
iamine-node/src/cluster_registry.rs
Cargo.toml
Cargo.lock
package-load blocker removal
RuntimeOwner availability changes
lifecycle transition behavior
handoff or out-of-scope behavior
package loading or runtime execution
filesystem, process, scheduler, network, model, or inference wiring
```

## Check 1: Identity And Scope

Verify full branch, HEAD, tree, merge base, origin, author identity, tracked
state, staging, untracked baseline, and exact changed files.

## Check 2: Positive Authorization

Confirm an exact valid chain produces:

```text
status = Authorized
schema = iamine.agent.execution_authorization.decision-0.1
lifecycle_state = scope_check
lifecycle_revision = 2
selected_candidate_id = candidate-local
authorization_recorded = true
execution_authorized = true
```

The evidence must verify only under the issuing authorization authority and
against the current complete request.

## Check 3: Independent Owner Verification

Reject independently:

- missing or foreign package review evidence;
- foreign compatibility evidence;
- foreign input/output enforcement evidence;
- foreign sandbox evidence;
- foreign lifecycle authority or record;
- non-`scope_check` lifecycle state;
- foreign timeout/cancel control;
- cancellation requested before authorization;
- foreign routing authority;
- missing or non-unique selected candidate;
- routing selected for another sandbox evidence;
- foreign or mismatched Scope, Permission, or lifecycle audit evidence.

Each failure must return the stable owner-specific code and requirement without
producing authorization evidence.

## Check 4: Scope And Permission Binding

Validate that the owner:

- rejects request package IDs different from the reviewed manifest;
- recomputes Scope from the supplied policy and request;
- recomputes Permission from that exact Scope result;
- rejects dangerous, injected, ambiguous, refused, handoff, or confirmation
  outcomes;
- compares audit projections with the recomputed evaluations;
- never treats audit evidence as a replacement for Scope or Permission.

## Check 5: Replay, Privacy, And Side Effects

Validate:

- a foreign authorization authority cannot verify evidence;
- cancellation invalidates previously issued evidence;
- lifecycle revision or execution changes invalidate replay;
- Debug output redacts package, candidate, policy, authority, execution, and
  sandbox values;
- errors contain fixed messages only.

Every authorized result must report:

```text
package_load_allowed = false
package_loaded = false
runtime_active = false
sandbox_active = false
scheduler_mutated = false
transport_started = false
persisted = false
external_event_emitted = false
```

`RuntimeOwner::ExecutionAuthorization` remains `Unavailable`, the runtime
foundation remains blocked, and
`PackageLoadBlockerCode::ExecutionAuthorizationUnavailable` remains present.

## Check 6: Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test execution_authorization
cargo test -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
cargo test -p iamine-agents
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Current focused results:

```text
authorization integration: PASS, 14/14
runtime total: PASS, 117/117
agents total: PASS, 109/109
strict crate clippy: PASS
quality gate required checks: PASS
quality gate required failures: 0
workspace clippy: PASS
optional cargo audit: SKIPPED / unavailable
optional cargo deny: SKIPPED / unavailable
optional gitleaks: SKIPPED / unavailable
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new production file: 270 lines
```

Warnings emitted by workspace validation are established findings outside the
feature diff: unused and deprecated client/runtime items plus existing
`too_many_arguments` and `type_complexity` findings. The strict owner-crate
Clippy run passes with `-D warnings`; no feature warning was introduced.

## Check 7: Field QA

Run on the exact source commit and tree:

| Platform role | Required | Result |
| --- | --- | --- |
| macOS development | yes | PASS, 14/14 + 4/4 |
| physical Linux, TS140 | yes | PASS, 14/14 + 4/4 |
| Linux VM control, iamine-ctrl | yes | PASS, 14/14 + 4/4 |
| Linux VM worker A, iamine-wrk1 | yes | PASS, 14/14 + 4/4 |
| Linux VM worker B, iamine-wrk2 | yes | PASS, 14/14 + 4/4 |
| Linux VM heavy, iamine-heavy | yes | PASS, 14/14 + 4/4 |

For each role:

```bash
cargo test -p iamine-agent-runtime --test execution_authorization
cargo test -p iamine-agent-runtime --lib
```

Expected:

```text
integration: 14/14 PASS
library: 4/4 PASS
tracked worktree: clean
staging: clean
runtime side effects: none
```

Stop at the first failure, classify product/environment/harness/baseline, and
do not modify code during QA.

Actual field evidence:

```text
HEAD: 125264ef77e9fad63a79474c6834be63ae86e5bf
TREE: bbf11787fe76a4f24e20a287f5e7125830bb6a3b
BASE: ff7ba1668ffdf61a71294ac3fa1921baf426ce43
ORIGIN: https://github.com/iamine-labs/iamine
focused integration per role: 14/14 PASS
runtime library per role: 4/4 PASS
tracked worktree per role: clean
staging per role: clean
runtime side effects observed: none
product failures: none
```

TS140's canonical checkout contained preserved staged changes from an older
feature plus local log artifacts. QA did not modify that checkout. The exact
source commit was transferred as a verified complete Git bundle and tested in
an isolated clean clone under `/tmp`, as were the four Proxmox roles.

The first TS140 invocation did not find `cargo` in the non-interactive SSH
`PATH`; no test had started. This was classified as a harness issue and the
sequence was restarted with the existing user-local Cargo directory in
`PATH`. Both required checks then passed without source changes.

The first post-merge quality-gate invocation inherited the Codex filesystem
sandbox after a custom target-directory prefix. Four real-inference assertions
and one daemon socket test failed with restricted Metal/socket access. The
merge was not published at that point. Re-executing the same gate with normal
OS access passed every required check, including both inference passes and the
daemon test. This is classified as an environment/harness finding, not a
product or baseline exception.

```text
controlled merge: PASS, 22adc690f3b8d9704783d7f8304680d3ea677404
post-merge quality gate: PASS WITH WARNINGS
post-merge required failures: 0
post-merge workspace clippy: PASS
post-merge optional tools skipped: cargo-audit, cargo-deny, gitleaks
new product failures: 0
Recommendation: MERGED / VALIDATED / CLOSED
```
