# AGENT-EXECUTION-LIFECYCLE-ENGINE-001 QA

## State

```text
READY FOR MERGE REVIEW
branch: feature/agent-execution-lifecycle-engine-001
base: 0db1c0930c73655a75c2111599d32b24f08f58ef
base tree: 10f5845272a4f4558f3dfaccf0d7abbf03f21e4a
source commit: 9520bf244695daa790a4c0119f75312a64dc8379
source tree: 23648aa6264866ea4927d960eba3c2977fe9fa91
field QA: passed, 6/6 hosts
```

## Scope

Expected executable changes are limited to:

```text
iamine-agent-runtime/src/execution_lifecycle/
iamine-agent-runtime/src/sandbox_enforcement/evidence.rs
iamine-agent-runtime/src/sandbox_enforcement/mod.rs
iamine-agent-runtime/src/lib.rs
iamine-agent-runtime/tests/execution_lifecycle.rs
iamine-agent-runtime/tests/support/
```

Expected documentation:

```text
docs/agents/agent-execution-lifecycle.md
docs/architecture/agent-execution-lifecycle-engine.md
docs/qa/agent-execution-lifecycle-engine.md
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
```

No node, scheduler, worker, network, model, inference, package-load, active
sandbox adapter, process-launch, timer, cancellation, handoff, persistence,
audit-emission, Cargo dependency, or static blocker behavior may change.

## Required Checks

1. Verify exact branch, base, source commit, tree, tracked state, staging, and
   aggregate untracked state.
2. Run formatting, focused runtime tests, audit-vocabulary tests, strict crate
   Clippy, the workspace quality gate, and diff checks.
3. Confirm all ten canonical states and every allowed transition edge.
4. Confirm entry to `running` requires future execution authorization and
   leaves state and revision unchanged.
5. Confirm terminal, invalid, skipped, stale, and foreign transitions fail
   closed without mutation.
6. Confirm records bind the exact lifecycle authority, package subject,
   sandbox authority, sandbox evidence, and execution identity.
7. Run the focused lifecycle suite on macOS, one physical Linux host, and four
   Linux VM guests.
8. Confirm no `iamine-node` process, socket, timer, persistent state, package
   load, model load, worker, or network runtime is created by field QA.

## Local Validation

```text
baseline cargo test -p iamine-agent-runtime: PASS, 42/42
cargo fmt --all -- --check: PASS
cargo test -p iamine-agent-runtime: PASS, 51/51
new execution lifecycle tests: PASS, 9/9
cargo test -p iamine-agents: PASS, 109/109
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
scripts/quality-gate.sh: PASS WITH WARNINGS
cargo test -p iamine-models: PASS, 158/158
cargo test -p iamine-network: PASS, 167/167
cargo test -p iamine-node: PASS, 480/480
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
cargo clippy --workspace --all-targets: PASS with historical warnings
git diff --check: PASS
git diff --cached --check: PASS
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new production module: 140 lines
required failures: 0
```

Optional tools:

```text
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

Workspace Clippy emitted only historical warnings from unchanged
`client-rust`, `iamine-models`, `iamine-network`, and `iamine-node` sources.
Strict Clippy for the changed crate passed without warnings.

## Architecture Finding

The initial implementation bound a lifecycle record to the sandbox authority
and package subject but not to the exact prepared sandbox evidence object.
That would have allowed replacement evidence from the same authority and
subject to satisfy record verification.

The checkpoint corrected the issue by adding a private identity to each
prepared sandbox evidence object and binding that identity into the lifecycle
record. The adversarial test now creates replacement evidence from the exact
same authority and subject and confirms that verification fails closed.

The correction initially exposed one compile-time visibility error because
the new crate-private identity was not reexported by the sandbox owner module.
The reexport was added; focused tests, full crate tests, strict Clippy, diff
checks, and the full quality gate all passed afterward.

## Field Matrix

| Host | Exact identity | Focused tests | Side effects | Result |
| --- | --- | --- | --- | --- |
| macOS development host | source commit/tree/base | 9/9 | process 0 -> 0; worktree clean | PASS |
| physical Linux host | source commit/tree/base | 9/9 | process 0 -> 0; isolated worktree clean | PASS |
| Linux VM guest 1 | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |
| Linux VM guest 2 | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |
| Linux VM guest 3 | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |
| Linux VM guest 4 | source commit/tree/base | 9/9 | process 0 -> 0; selected copy clean | PASS |

```text
hosts: 6/6 PASS
feature test executions: 54/54 PASS
product failures: 0
environment findings: 0
harness findings: 2 classified
iamine-node process changes: 0
tracked/staged/untracked contamination in QA copies: 0
```

The macOS app sandbox initially denied process-list inspection. The same check
ran outside the app sandbox before and after the focused suite and confirmed
zero matching `iamine-node` processes.

The physical Linux host's canonical copy retained historical staged work and
local artifacts, so QA preserved it unchanged. A detached temporary worktree
was created from the exact remote source commit with an isolated Cargo target.
It remained clean after the focused suite.

Each Linux VM guest used its preselected clean QA copy. The feature ref was
fetched explicitly, a tracking branch was created without force, and the
exact source commit, tree, and base were verified before testing. Historical
copies with preserved work were not modified.

The focused suite creates only isolated Cargo build output. It does not start
`iamine-node`, open a product runtime, register a timer, load a package or
model, activate a sandbox, dispatch a handoff, persist lifecycle state, or
emit product audit events.

## QA Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not authorize merge. Final Architecture review owns the merge
decision.
