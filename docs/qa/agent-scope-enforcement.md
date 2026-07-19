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

Implementation checks completed so far:

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

Exact implementation commit identity, field QA, final Architecture review,
merge, and post-merge validation remain pending.

## Recommendation Boundary

QA may recommend:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA must not treat a scope `Allow` decision as permission grant, package-load
eligibility, execution authorization, or milestone closure.
