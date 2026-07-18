# Agent Network Milestone QA Gates

Feature:

```text
AGENT-MILESTONE-QA-GATES-001
```

## Purpose

Define the reusable minimum evidence for every Agent Network milestone closure
gate. Milestone-specific QA documents may add checks but may not remove this
baseline silently.

## CHECK 0 - Authorization And Identity

Record before running tests:

```text
gate feature
milestone
authorized branch
authorized HEAD
authorized tree
prior milestone baseline
origin/develop
tracked state
staging state
untracked baseline
required environments
```

Stop if identity, tree, scope, or authorization differs.

## CHECK 1 - Feature Closure Matrix

For every feature assigned to the milestone, record:

```text
feature
state
implementation commit
merge commit
resulting tree
QA result
field QA result or explicit not-required decision
accepted exceptions
```

Block the gate if a feature is proposed, active, unmerged, only locally
validated, missing post-merge validation, or represented only by architecture
when the milestone claims executable behavior.

## CHECK 2 - Scope And Deliverables

Compare the current roadmap, architecture contracts, package schemas, runtime
interfaces, CLI surface, and user-visible claims. Confirm every promised
deliverable is present and every deferred item remains explicitly unavailable.

Do not infer a runtime capability from a closed planning contract.

## CHECK 3 - Local Regression

Run the current required local gate and all milestone-owned focused suites:

```bash
./scripts/quality-gate.sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets
cargo build -p iamine-node
git diff --check
git diff --cached --check
```

Add parser, schema, package, agent, runtime, CLI, installer, or network tests
owned by the milestone. Record exact test counts and compare failures and
warnings with the prior milestone baseline.

## CHECK 4 - Agent Safety Matrix

For every executable agent, cover:

```text
positive capability
negative capability
scope boundary
permission boundary
blocked actions
unsafe action attempts
prompt injection
role confusion
privacy redaction
handoff
timeout and cancellation
resource limits
local/LAN/remote restrictions
audit evidence
```

A missing category is a test gap and blocks milestone closure unless the
category is provably inapplicable and Architecture records that decision.

## CHECK 5 - E2E And Cross-Feature Regression

Exercise each user journey promised by the milestone from admission through
final outcome. Include failure, cleanup, restart, and retry paths. Confirm new
agent behavior does not regress existing node, model, inference, network,
installer, diagnostics, or security behavior.

## CHECK 6 - Field QA

Use Mac, TS140, and Proxmox/R5500 when the milestone includes executable
runtime, worker, scheduler, network, inference, hardware, packaging, service,
or operational claims.

For each environment record exact Git identity, toolchain, configuration,
isolated state paths, commands, results, side effects, and cleanup. Preserve
existing processes, models, profiles, credentials, and untracked artifacts.

## CHECK 7 - Privacy, Security, And Resource Bounds

Confirm:

- no secrets, personal paths, host fingerprints, or raw private evidence;
- deny-by-default scope and permissions;
- no unrestricted shell, filesystem, network, or credentials;
- bounded input, output, concurrency, duration, memory, disk, and logs;
- no partial state after failure, timeout, or cancellation;
- no unintended download, model load, worker, P2P, or persistent mutation.

## CHECK 8 - Final Gate And Closure Evidence

Rerun checks affected by field QA, confirm the exact final HEAD and tree, and
publish a milestone report containing failures, warnings, gaps, exceptions,
and residual risk.

Allowed QA results:

```text
PASS COMPLETO
PASS WITH ACCEPTED BASELINE EXCEPTION
FAIL
RERUN
MILESTONE QA BLOCKED
TEST GAP
```

The only positive QA recommendation is:

```text
READY FOR ARCHITECTURE MILESTONE CLOSURE REVIEW
```

QA must not declare the milestone closed. Architecture may close it only after
the gate evidence is merged and post-merge validation passes.

## Field QA For This Policy Feature

This policy feature is documentation-only. Mac, TS140, and Proxmox execution
is not required to merge the policy itself.

## Policy Feature Validation

Validation date:

```text
2026-07-18
```

Identity:

```text
branch: feature/agent-milestone-qa-gates-001
base: 23a170548c18b62a30bea6d156c158a7c6c7ead9
base tree: 51f689d539a146ac014b71159dbafd4e56c704b0
runtime behavior changed: no
```

Validation executed:

- `cargo fmt --all -- --check`: pass;
- quality-gate repository and architecture guards: pass;
- roadmap near-milestone state reconciliation: pass;
- named closure gate registry: pass, 18 milestone rows;
- required near-term gate documents: pass, all present and non-empty;
- docs-only scope check: pass;
- `git diff --check`: pass;
- `main.rs`: 4,929 lines, delta 0;
- no non-main Rust file above the 900-line warning threshold;
- no tracked generated artifacts or sensitive files.

The full Cargo workspace and runtime field suites were not repeated because the
feature changes only documentation and leaves the exact Rust tree unchanged.
They remain mandatory when executing a milestone gate according to its
authorized scope.

Policy feature QA result:

```text
PASS - DOCUMENTATION / PROCESS SCOPE
```
