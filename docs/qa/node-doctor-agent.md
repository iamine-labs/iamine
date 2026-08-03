# Node Doctor Agent QA

Feature:

```text
NODE-DOCTOR-AGENT-001
```

Current state:

```text
LOCAL VALIDATION PASSED
FIELD QA REQUIRED
```

## Authorized Identity

```text
branch: feature/node-doctor-agent-001
base: 3374e27f7b6b132b39c3e979af7a1a03cd5daf9b
origin: https://github.com/iamine-labs/iamine
```

Source commit, source tree, bundle hash, Linux binary hash, and remote checkout
identity are recorded only after the exact QA checkpoint is committed.

## Scope

Created:

```text
agents/official/node-doctor/
iamine-node/src/node_doctor_agent/
docs/architecture/node-doctor-agent.md
docs/qa/node-doctor-agent.md
```

Updated surfaces are limited to CLI/control-plane wiring, the existing Node
Doctor evidence-provider visibility boundary, additive runtime output
classification and safe package-subject inspection, Cargo dependency wiring,
agent contracts, and roadmap state.

## Local Results

```text
focused Node Doctor tests: 7/7 PASS
iamine-agent-runtime: 149/149 PASS
iamine-agents: 109/109 PASS
iamine-node: 493/493 PASS
cargo build -p iamine-node: PASS
JSON CLI smoke: PASS
fresh HOME absent before and after smoke: PASS
git diff --check: PASS
new feature warnings: 0
```

Four warning groups in `task_cache.rs`, `wallet.rs`, and `worker_pool.rs` remain
historical baseline findings. A sandboxed Mac process-inspection command could
not access `sysmond`; process-count evidence must be collected during field QA
outside that sandbox.

## Focused Assertions

```text
official package and all seven references validate
all nine required boundary classes match runtime scope decisions
only the exact compiled package snapshot executes
one-byte package changes fail closed
typed manifest changes and missing packages fail closed
missing evidence returns blocked_action_report
private owner messages and details do not appear in output
runtime completion has audit and cleanup evidence
scheduler, transport, persistence, shell, children, and network remain off
```

## Field Matrix

Execute the exact committed source on:

```text
Mac
TS140
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

Each role must verify commit/tree or artifact hash, package hash, JSON schema,
six categories, privacy strings, fresh-HOME behavior, process counts, runtime
side-effect declarations, and modified-package rejection. Build and focused
tests may run once on TS140; the four Proxmox guests may consume that exact
Linux x86_64 artifact after hash verification.

## Architecture Maintenance

```text
main.rs: 4934 -> 4931, wiring only
cluster_registry.rs: 862 -> 862
largest new Rust file: node_doctor_agent/execution.rs, 436 lines
new non-main Rust files above 750 lines: 0
```

## Recommendation

```text
FIELD QA REQUIRED
```

Do not emit a merge recommendation until the exact-source six-role matrix and
the broad local gate have completed.
