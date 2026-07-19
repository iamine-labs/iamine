# Agent Permission Enforcement QA

Feature:

```text
AGENT-PERMISSION-ENFORCEMENT-001
```

## Objective

Validate that the pure in-memory permission gate runs only after Scope,
approves only exact reviewed actions and categories, denies unknown or unsafe
permissions, prevents confirmation-based escalation, preserves privacy, and
cannot authorize package loading or runtime execution.

## Expected Scope

```text
iamine-agents/src/lib.rs
iamine-agents/src/permission_enforcement/
iamine-agents/tests/permission_enforcement.rs
iamine-agents/README.md
docs/architecture/agent-permission-enforcement.md
docs/qa/agent-permission-enforcement.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime integration change:

```text
none
```

## Required Behavior

- Scope must return `Allow` before permission evaluation can allow;
- default policy must be deny;
- only exact package, approved action, and approved category matches allow;
- blocked actions and forbidden categories refuse before confirmation;
- undeclared actions and categories refuse;
- malformed, empty, duplicate, and oversized requests fail closed;
- confirmation can complete only an already approved permission;
- package, prompt, or model output cannot self-assert trusted confirmation;
- broad, contradictory, duplicate, oversized, unsupported, or permissive
  policies fail construction;
- mandatory unsafe category and action denies cannot be omitted;
- decisions are deterministic and expose stable bounded codes;
- policy, request, and evaluation debug output does not echo values;
- package load remains blocked on permission parsing, review, integration,
  audit, and execution evidence;
- no filesystem, process, network, persistence, runtime, model, inference,
  worker, scheduler, hardware, service, or CLI side effect occurs.

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agents --test permission_enforcement
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets -- -D warnings
cargo test --workspace
cargo clippy --workspace --all-targets
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
wc -l iamine-agents/src/permission_enforcement/*.rs
wc -l iamine-node/src/main.rs
wc -l iamine-node/src/cluster_registry.rs
rg -n "std::fs|std::process|std::net|File::|read_to_string|Command::" \
  iamine-agents/src/permission_enforcement
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
cargo test -p iamine-agents --test permission_enforcement
cargo clippy -p iamine-agents --all-targets -- -D warnings
```

Field QA validates deterministic cross-platform behavior only. It must not
start package loading, agent runtime, workers, P2P, model loading, downloads,
inference, or service mutation.

## Evidence Status

Baseline before implementation:

```text
base: 5e61fedc21cc67ef209a770f767e89d7c56ad592
tree: 4c11938e613784af86e795e65eceb43ede488cfe
cargo test -p iamine-agents: PASS, 46 tests
```

Implementation checks completed so far:

```text
cargo check -p iamine-agents: PASS
cargo test -p iamine-agents --test permission_enforcement: PASS, 17 tests
cargo test -p iamine-agents: PASS, 63 tests
cargo clippy -p iamine-agents --all-targets -- -D warnings: PASS
cargo fmt --all -- --check: PASS
cargo test --workspace: PASS, 926 tests
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
- largest new production Rust file: `policy.rs`, 403 lines;
- focused `iamine-agents` Clippy passed with warnings denied;
- workspace Clippy warnings are historical and occur only in unchanged crates;
- `cargo audit`, `cargo deny`, and `gitleaks`: skipped, unavailable.

Exact implementation commit identity, Architecture review, field QA, merge,
and post-merge validation remain pending.

## Recommendation Boundary

QA may recommend:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA must not treat a permission `Allow` decision as an operating-system grant,
package-load eligibility, execution authorization, or milestone closure.
