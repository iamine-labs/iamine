# Agent Package Load Gate QA

Feature:

```text
AGENT-PACKAGE-LOAD-GATE-001
```

## Objective

Validate that a canonical root manifest can be assessed for package loading,
but cannot become load-eligible while downstream validators and enforcement
gates remain unavailable.

## Expected Scope

```text
iamine-agents/src/package_load.rs
iamine-agents/src/lib.rs
iamine-agents/tests/package_load_gate.rs
iamine-agents/README.md
docs/agents/agent-manifest-schema-source-of-truth.md
docs/agents/agent-package-manifest.md
docs/architecture/agent-manifest-parser-validator.md
docs/architecture/agent-package-load-gate.md
docs/architecture/agent-package-manifest.md
docs/architecture/node-doctor-agent-dependency-reconciliation.md
docs/qa/agent-package-load-gate.md
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
```

Expected runtime behavior change:

```text
none
```

## Required Behavior

- valid root YAML produces a `Blocked` report;
- no positive or caller-forgeable load path exists;
- all 19 current prerequisite blockers are present;
- blocker codes are unique, stable, deterministic, and bounded;
- unknown fields and invalid root metadata fail before assessment;
- `execution_authorized: true` fails before assessment;
- the existing 64 KiB root-input limit remains active;
- path-shaped input is not opened as a file;
- reports do not retain or echo package IDs or reference paths;
- the module performs no filesystem access;
- no package, child metadata, runtime, sandbox, registry, worker, scheduler,
  network, model, inference, hardware, or CLI behavior is started.

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
wc -l iamine-agents/src/package_load.rs
wc -l iamine-node/src/main.rs
wc -l iamine-node/src/cluster_registry.rs
rg -n "std::fs|std::path|File::|read_to_string" iamine-agents/src/package_load.rs
```

The final `rg` command must return no matches.

## Observed Local Validation

Validation date:

```text
2026-07-18
```

Exact base used for comparison:

```text
3c9728e1b4d08e806a996e0cffcf4be410d1aa11
tree 2e635b3363f84b66c067c5b1eb2f878728d39e83
```

Feature-owned checks:

- `cargo fmt --all -- --check`: pass;
- `cargo test -p iamine-agents`: pass, 31 tests;
- `cargo clippy -p iamine-agents --all-targets`: pass, no warnings;
- package-load negative, privacy, determinism, and limit tests: pass, 9 tests;
- filesystem API scan: pass, no matches;
- `git diff --check`: pass.

Broader checks:

- `cargo test -p iamine-network`: pass, 167 tests;
- `cargo build -p iamine-node`: pass with historical warnings;
- `cargo clippy --workspace --all-targets`: pass with historical warnings outside
  `iamine-agents`;
- quality-gate repository and architecture guards: pass;
- `main.rs`: 4,929 lines, delta 0;
- `cluster_registry.rs`: 862 lines, delta 0;
- `package_load.rs`: 115 lines.

Accepted base/environment exceptions:

- `cargo test --workspace` reached the real TinyLlama/Metal integration block
  with 55 passing and 4 failing tests: real inference, queue, concurrency, and
  streaming. The exact base reproduced the same 55/4 result and failures;
- `cargo test -p iamine-node` reached 479 passing and 1 failing test. The daemon
  socket test failed with `Operation not permitted` under `/private/tmp`; the
  exact base reproduced the same focused failure.

Neither failure family is reachable from, imports, or is modified by the
in-memory `iamine-agents` package-load assessment. The full quality-gate wrapper
was not allowed to repeat successful checks after these classified failures;
its guard-only mode passed and its remaining required commands were run
individually.

Optional tools:

- `cargo audit`: skipped, unavailable;
- `cargo deny`: skipped, unavailable;
- `gitleaks`: skipped, unavailable.

Local QA result:

```text
PASS WITH ACCEPTED BASELINE / ENVIRONMENT EXCEPTIONS
```

## Field QA Decision

TS140 and Proxmox QA are not required because the gate is an in-memory library
assessment with no filesystem or runtime integration. Field QA becomes required
when package-root I/O or runtime loading is introduced.

## Recommendation Boundary

Passing QA may recommend this feature for Architecture merge review. It must
not authorize package loading, agent execution, or functional Node Doctor
development.
