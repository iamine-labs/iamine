# IAMINE - Architecture Cleanup Checkpoint 002

## 1. Objective

This document formally closes ARCHITECTURE-CLEANUP-LINE-002 after completion
of Block A and Block B.

The line reduced structural debt in runtime orchestration, network event
handling, configuration, observability, broadcast state, prompt analysis,
test organization, and quality-gate enforcement. It did not introduce new
features or change runtime behavior.

## 2. Scope

ARCHITECTURE-CLEANUP-LINE-002 was a controlled architecture cleanup line.
Each runtime-sensitive extraction preserved existing behavior and was
validated before merge. Later items were limited to network analyzer
organization, test-only restructuring, CI scripts, and documentation.

The completed line:

- reduced and stabilized `iamine-node/src/main.rs`
- removed the remaining large-file warnings targeted by Block B
- preserved Mac, TS140, and Proxmox/R5500 field baselines where applicable
- introduced a progressively stricter local and CI quality gate
- kept feature work paused until this final checkpoint

## 3. Completed Work

| Item | Merge Commit | Status | Runtime Behavior Changed |
| --- | --- | --- | --- |
| REFACTOR-LIBP2P-EVENT-LOOP-004 | `022a13f` | PASS COMPLETO | no |
| REFACTOR-CONFIG-ENV-006 | `c646541` | PASS COMPLETO | no |
| REFACTOR-OBSERVABILITY-HELPERS-005 | `87388e3` | PASS COMPLETO | no |
| ARCHITECTURE-CLEANUP-CHECKPOINT-A | `f958350` | DOCS CHECKPOINT | no |
| REFACTOR-BROADCAST-RUNTIME-002 | `dafc591` | PASS COMPLETO | no |
| REFACTOR-PROMPT-ANALYZER-001 | `eadb3c6` | PASS LOCAL | no |
| REFACTOR-TEST-SUITE-SPLIT-001 | `086c18a2ad64a5ebb954a3c99d95fde0b300fcb9` | TEST-ONLY | no |
| QUALITY-GATE-STRICTNESS-002 | `4fc8538c6252d260f5982100c2ae18eb1427fefb` | CI-SCRIPTS-DOCS ONLY | no |

Block A:

- extracted libp2p discovery and gossipsub event helpers
- centralized runtime environment and path configuration
- extracted common runtime observability helpers
- reduced `main.rs` from 6017 to 4830 lines

Block B:

- extracted broadcast state and broadcast observability helpers
- split prompt analyzer task detection, semantic signals, and routing hints
- split the monolithic model integration test file into focused test modules
- hardened local and GitHub Actions quality gates

## 4. main.rs Status

| Stage | main.rs Lines | Delta |
| --- | ---: | ---: |
| Start of Block A | 6017 | - |
| End of Block A | 4830 | -1187 |
| End of Block B | 4830 | 0 |
| Current | 4830 | -1187 from line baseline |

Current quality-gate policy:

- warn when `main.rs` exceeds 5000 lines
- fail when `main.rs` exceeds 5500 lines
- fail when a branch grows `main.rs` by more than 150 lines relative to
  merge-base

`main.rs` remains below the warning threshold. Block B did not grow it.

## 5. Large-File Cleanup

| File | Before | After | Result |
| --- | ---: | ---: | --- |
| `iamine-node/src/broadcast_runtime.rs` | 1178 | 598 | warning removed |
| `iamine-network/src/prompt_analyzer.rs` | 1389 | 504 | warning removed |
| `iamine-models/tests/integration.rs` | 1017 | 7 | split into 8 files |
| largest non-main Rust file | - | `iamine-node/src/infer_observability.rs`, 780 | below warning |
| non-main Rust files over 900 lines | - | 0 | clean |

Current quality-gate policy for tracked Rust files other than `main.rs`:

- warn above 900 lines
- fail above 1500 lines

## 6. Modules Created

Block A:

- `iamine-node/src/discovery_runtime.rs`
- `iamine-node/src/gossipsub_message_runtime.rs`
- `iamine-node/src/network_event_observability.rs`
- `iamine-node/src/backend_config.rs`
- `iamine-node/src/config_test_utils.rs`
- `iamine-node/src/env_config.rs`
- `iamine-node/src/network_config.rs`
- `iamine-node/src/path_config.rs`
- `iamine-node/src/runtime_config.rs`
- `iamine-node/src/runtime_observability.rs`

Block B:

- `iamine-node/src/broadcast_state.rs`
- `iamine-node/src/broadcast_observability.rs`
- `iamine-network/src/prompt_task_detection.rs`
- `iamine-network/src/prompt_semantic_signals.rs`
- `iamine-network/src/prompt_routing_hints.rs`
- `iamine-models/tests/integration/common.rs`
- `iamine-models/tests/integration/registry.rs`
- `iamine-models/tests/integration/storage.rs`
- `iamine-models/tests/integration/inference_engine.rs`
- `iamine-models/tests/integration/distributed_inference.rs`
- `iamine-models/tests/integration/capabilities.rs`
- `iamine-models/tests/integration/download_validation.rs`

## 7. QA Baseline

Mac/local:

- CLI and help commands: PASS
- tasks trace and stats: PASS
- cluster status human and JSON modes: PASS
- semantic-eval: PASS, 100.0% accuracy
- workspace tests: PASS

TS140:

- real worker: PASS
- backend CPU real: PASS
- `real_inference_available=true`: PASS
- Broadcast Mac -> TS140 where field QA was required: PASS
- TaskResult accepted: PASS
- `final_outcome=success`: PASS
- trace CLI: PASS
- no SIGILL evidence: PASS

Proxmox/R5500:

- mock/skip workers where field QA was required: PASS
- `backend=mock`: PASS
- `real_inference_available=false`: PASS
- no real LLM load: PASS
- Broadcast: PASS
- exactly one worker executes: PASS
- non-winners `will_execute=false`: PASS
- TaskResult accepted: PASS
- `final_outcome=success`: PASS
- trace CLI: PASS
- no SIGILL evidence: PASS

Field QA was executed for:

- REFACTOR-LIBP2P-EVENT-LOOP-004
- REFACTOR-CONFIG-ENV-006
- REFACTOR-OBSERVABILITY-HELPERS-005
- REFACTOR-BROADCAST-RUNTIME-002

New field QA was not required for:

- REFACTOR-PROMPT-ANALYZER-001: network analyzer organization only;
  semantic-eval PASS
- REFACTOR-TEST-SUITE-SPLIT-001: test-only
- QUALITY-GATE-STRICTNESS-002: CI, scripts, and docs only
- ARCHITECTURE-CLEANUP-CHECKPOINT-002: documentation only

## 8. Protected Regressions

The following regressions remain protected after ARCHITECTURE-CLEANUP-LINE-002:

- no runtime behavior change
- no TaskOffer, TaskBid, TaskAssign, or TaskResult wire change
- no result acceptance change
- no PubSub readiness regression
- no worker startup regression
- no mock/skip regression
- no real backend under mock/skip
- no SIGILL
- no duplicate execution or result
- `final_outcome=success`
- `broadcast_recovery_cancelled`
- scheduler metadata preserved
- task lifecycle preserved
- NDJSON remains parseable
- protected grep contract preserved
- prompt classification outputs preserved
- semantic-eval preserved

## 9. Quality Gate Final State

Required blocking checks:

```bash
cargo fmt --all -- --check
cargo test -p iamine-models
cargo test -p iamine-network
cargo test -p iamine-node
cargo build -p iamine-node
cargo test --workspace
git diff --check
git diff --cached --check
```

Optional checks:

- `cargo clippy --workspace --all-targets`
- `cargo audit`
- `cargo deny check`
- GitLeaks secret scan

Architecture and repository guards:

- `main.rs` warn above 5000 lines
- `main.rs` fail above 5500 lines
- branch `main.rs` delta fail above +150 lines relative to merge-base
- tracked Rust large-file warning above 900 lines
- tracked Rust large-file failure above 1500 lines
- generated artifact guard: tracked-only and blocking
- sensitive file guard: tracked-only and blocking
- new Rust debt markers: warning-only
- GitLeaks findings: blocking when GitLeaks runs

Latest validated result:

- quality gate: PASS WITH WARNINGS
- required failures: 0
- warnings: 0
- skipped optional tools: 3

The skipped local tools are `cargo audit`, `cargo deny`, and GitLeaks. Their
absence remains non-blocking locally. GitLeaks runs as a blocking GitHub
Actions job when configured by the workflow.

## 10. Remaining Non-Blockers

- existing Rust `dead_code` warnings
- existing Clippy warnings
- optional `cargo audit`, `cargo deny`, and GitLeaks unavailable locally
  unless installed
- metrics fallback for worker ports below 9000
- storage/model registry display can show tinyllama under mock/skip
- existing untracked local artifacts remain outside version control

These items are known debt or environment notes. They do not block closure of
ARCHITECTURE-CLEANUP-LINE-002.

## 11. Decision

ARCHITECTURE-CLEANUP-LINE-002: CLOSED.

Return to feature work: yes.

Next recommended branch:

- `feature/model-capability-consistency-001`

Reason:

Capabilities, readiness reporting, scheduler metadata, task traces, and the
quality gate are now cleaner and more explicit. The next feature should
resolve consistency across:

- `models_in_storage`
- `models_in_registry`
- `executable_models`
- `backend`
- `execution_mode`
- `real_inference_available`
- `supported_task_types`
- `ready_for_tasks`
- `readiness_reason`

This consistency work should be completed before
SCHEDULER-CAPABILITY-MATCHING-001.
