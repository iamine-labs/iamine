# Agent Scope Enforcement QA

Feature:

```text
AGENT-SCOPE-ENFORCEMENT-001
```

## Objective

Validate that the in-memory scope engine allows only exact declared requests,
fails closed for every required boundary class, preserves privacy, and cannot
authorize package loading or runtime execution.

## Expected Scope

```text
iamine-agents/src/identifiers.rs
iamine-agents/src/lib.rs
iamine-agents/src/manifest/validation.rs
iamine-agents/src/scope_enforcement/
iamine-agents/tests/scope_enforcement.rs
iamine-agents/README.md
docs/architecture/agent-scope-enforcement.md
docs/qa/agent-scope-enforcement.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime integration change:

```text
none
```

## Required Behavior

- exact package, task type, task, operation, and input matches allow;
- ambiguous requests clarify;
- dangerous, permission-escalation, prompt-injection, and role-confusion
  requests refuse;
- cross-domain requests return to the orchestrator;
- unknown or out-of-scope tasks return to the orchestrator;
- blocked actions and forbidden inputs refuse;
- unsupported operations and inputs return to the orchestrator;
- malformed and oversized requests fail closed;
- broad, contradictory, duplicate, empty, oversized, or unsafe policies fail;
- mandatory privacy and mutation denies cannot be omitted;
- decisions are deterministic and expose stable bounded codes;
- reports do not retain or echo declaration or request values;
- package load remains blocked on scope-manifest and scope-integration evidence;
- no filesystem, process, network, persistence, runtime, model, inference,
  worker, scheduler, hardware, or CLI side effect occurs.

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
wc -l iamine-agents/src/scope_enforcement/*.rs
wc -l iamine-node/src/main.rs
wc -l iamine-node/src/cluster_registry.rs
rg -n "std::fs|std::process|std::net|File::|read_to_string|Command::" \
  iamine-agents/src/scope_enforcement
```

The final `rg` command must return no matches.

## Field QA Matrix

The exact implementation commit and tree must be tested on:

- Mac development machine;
- Dell TS140;
- Proxmox/R5500 guests `iamine-ctrl`, `iamine-wrk1`, `iamine-wrk2`, and
  `iamine-heavy`.

Required remote check:

```bash
cargo test -p iamine-agents --test scope_enforcement
cargo clippy -p iamine-agents --all-targets
```

Field QA validates deterministic cross-platform behavior only. It must not
start an agent runtime, worker, P2P, model load, download, or inference path.

## Evidence Status

Baseline before implementation:

```text
base: 435b391ccf9b3fd71c914426c09c4148f54252c7
tree: 53a7efc8a334b8cd07399d31ed2ccd973889bb86
cargo test -p iamine-agents: PASS, 31 tests
```

Implementation and pre-merge checks:

```text
cargo test -p iamine-agents --test scope_enforcement: PASS, 15 tests
cargo test -p iamine-agents: PASS, 46 tests
cargo clippy -p iamine-agents --all-targets -- -D warnings: PASS
cargo fmt --all -- --check: PASS
cargo test --workspace: PASS, 909 tests
cargo clippy --workspace --all-targets: PASS with historical warnings
scripts/quality-gate.sh: PASS WITH WARNINGS, required_failures=0
git diff --check: PASS
side-effect API scan: PASS, no matches
```

Quality gate detail:

- `iamine-models`: 158 tests passed, including real inference, queue,
  concurrency, cache, model load, and streaming;
- `iamine-network`: 167 tests passed;
- `iamine-node`: 480 tests passed and build passed;
- repository and architecture guards passed;
- `main.rs`: 4,929 lines, delta 0;
- `cluster_registry.rs`: 862 lines, delta 0;
- largest new production Rust file: `policy.rs`, 329 lines;
- focused `iamine-agents` Clippy passed with warnings denied;
- workspace Clippy warnings are historical and occur only in unchanged crates;
- `cargo audit`, `cargo deny`, and `gitleaks`: skipped, unavailable.

Exact implementation identity:

```text
branch: feature/agent-scope-enforcement-001
commit: 0a7201912b49da76b539a4b08158490c8a796320
tree: c7f81846cfaa5a79d24c3542e397fb909cb1e744
base: 435b391ccf9b3fd71c914426c09c4148f54252c7
```

Field QA results:

- Mac, TS140, `iamine-ctrl`, `iamine-wrk1`, `iamine-wrk2`, and
  `iamine-heavy` validated the exact implementation commit and tree;
- `cargo test -p iamine-agents --test scope_enforcement`: PASS, 15 tests on
  every platform;
- `cargo clippy -p iamine-agents --all-targets -- -D warnings`: PASS on every
  platform;
- tracked worktree and staging remained clean on every disposable QA copy;
- the TS140 canonical working copy and Proxmox `CANDIDATE_2` were not touched;
- pre-existing untracked Proxmox evidence files were preserved with matching
  before/after hashes;
- the first TS140 invocation omitted Cargo from the non-interactive SSH `PATH`;
  rerunning with the configured Cargo path passed and was classified as a
  harness issue, not a product failure;
- no runtime, worker, P2P, model, download, or inference path was started.

Controlled merge identity:

```text
develop merge: 48cb6b28fd3401ffa05b520d8043ed6984e3f1e3
tree: c7f81846cfaa5a79d24c3542e397fb909cb1e744
source branch preserved: yes
```

Post-merge validation:

- `cargo test -p iamine-agents`: PASS, 46 tests;
- `cargo clippy -p iamine-agents --all-targets -- -D warnings`: PASS;
- `git diff --check` and `git diff --cached --check`: PASS;
- repository and architecture guards: PASS;
- workspace Clippy: PASS with historical warnings in unchanged crates;
- raw `scripts/quality-gate.sh`: FAIL, `required_failures=3`, because
  `iamine-models` had 55 passes and 4 real TinyLlama/Metal failures,
  `iamine-node` had 479 passes and 1 daemon-socket failure, and the workspace
  suite repeated the model failures;
- exact-base comparison at `435b391ccf9b3fd71c914426c09c4148f54252c7`
  reproduced `test_real_inference` and `test_daemon_start_stop` with the same
  failure signatures;
- the implementation does not touch `iamine-models`, `iamine-node`, model
  inference, daemon runtime, or Unix socket handling;
- `cargo audit`, `cargo deny`, and `gitleaks`: skipped, unavailable.

Post-merge QA classification:

```text
PASS WITH ACCEPTED BASELINE / ENVIRONMENT EXCEPTIONS
MERGED / VALIDATED / CLOSED
```

## Recommendation Boundary

QA may recommend:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA must not treat a scope `Allow` decision as permission grant, package-load
eligibility, execution authorization, or milestone closure.
