# IAMINE - Architecture Cleanup Checkpoint A

## 1. Objective

This document formally closes Block A of ARCHITECTURE-CLEANUP-LINE-002.

Block A isolated runtime networking handlers, centralized runtime
configuration, and extracted common observability helpers while preserving
validated behavior. This checkpoint documents architecture state only. It
does not introduce features, refactors, or runtime behavior changes.

## 2. Refactors Closed

- REFACTOR-LIBP2P-EVENT-LOOP-004
- REFACTOR-CONFIG-ENV-006
- REFACTOR-OBSERVABILITY-HELPERS-005

## 3. State By Refactor

| Refactor | Branch | Merge Commit | Runtime Behavior Changed | QA Status | Main Before | Main After | Delta |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| REFACTOR-LIBP2P-EVENT-LOOP-004 | `refactor/libp2p-event-loop-004` | `022a13f` | no | PASS COMPLETO | 6017 | 5515 | -502 |
| REFACTOR-CONFIG-ENV-006 | `refactor/config-env-006` | `c646541` | no | PASS COMPLETO | 5515 | 5259 | -256 |
| REFACTOR-OBSERVABILITY-HELPERS-005 | `refactor/observability-helpers-005` | `87388e3` | no | PASS COMPLETO | 5259 | 4830 | -429 |

Field QA summary:

- REFACTOR-LIBP2P-EVENT-LOOP-004: Mac PASS, TS140 PASS, Proxmox/R5500 PASS.
- REFACTOR-CONFIG-ENV-006: Mac PASS, TS140 PASS, Proxmox/R5500 PASS.
- REFACTOR-OBSERVABILITY-HELPERS-005: Mac PASS, TS140 PASS, Proxmox/R5500 PASS.

## 4. main.rs Reduction

| Stage | main.rs Lines | Delta |
| --- | ---: | ---: |
| Start of Block A | 6017 | - |
| After libp2p event loop | 5515 | -502 |
| After config/env | 5259 | -256 |
| After observability helpers | 4830 | -429 |
| Total Block A | 4830 | -1187 |

## 5. Modules Created

REFACTOR-LIBP2P-EVENT-LOOP-004:

- `iamine-node/src/discovery_runtime.rs`
- `iamine-node/src/gossipsub_message_runtime.rs`
- `iamine-node/src/network_event_observability.rs`

REFACTOR-CONFIG-ENV-006:

- `iamine-node/src/backend_config.rs`
- `iamine-node/src/config_test_utils.rs`
- `iamine-node/src/env_config.rs`
- `iamine-node/src/network_config.rs`
- `iamine-node/src/path_config.rs`
- `iamine-node/src/runtime_config.rs`

REFACTOR-OBSERVABILITY-HELPERS-005:

- `iamine-node/src/runtime_observability.rs`

## 6. QA Field Baseline

Mac:

- CLI and help commands: PASS
- tasks trace and stats: PASS
- cluster status human and JSON modes: PASS
- env smoke: PASS
- NDJSON smoke: PASS

TS140:

- real worker: PASS
- backend CPU real: PASS
- `real_inference_available=true`: PASS
- Broadcast Mac -> TS140: PASS
- TaskResult accepted: PASS
- `final_outcome=success`: PASS
- trace CLI: PASS
- no SIGILL evidence: PASS

Proxmox/R5500:

- 3 workers mock/skip: PASS
- `backend=mock`: PASS
- `real_inference_available=false`: PASS
- `worker_model_load_skipped`: PASS
- `worker_pubsub_ready`: PASS
- `worker_startup_ready`: PASS
- no real LLM load: PASS
- Broadcast: PASS
- exactly one worker executes: PASS
- non-winners `will_execute=false`: PASS
- TaskResult accepted: PASS
- `final_outcome=success`: PASS
- trace CLI: PASS
- no SIGILL evidence: PASS

## 7. Protected Regressions

The following regressions remain protected after Block A:

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

## 8. Residual Architecture

`main.rs` is smaller, but it still owns runtime coordination that should
remain stable until a focused reason exists to change it:

- top-level runtime loop
- heavy orchestration state
- remaining direct result routing
- some human runtime output coupled to flow

No new runtime-critical refactor should be opened automatically. Block B
targets the remaining structural debt in focused increments.

## 9. Remaining Technical Debt And Block B

Block B remains pending:

1. REFACTOR-BROADCAST-RUNTIME-002
2. REFACTOR-PROMPT-ANALYZER-001
3. REFACTOR-TEST-SUITE-SPLIT-001
4. QUALITY-GATE-STRICTNESS-002
5. ARCHITECTURE-CLEANUP-CHECKPOINT-002

Current large-file warnings:

- `iamine-network/src/prompt_analyzer.rs`
- `iamine-node/src/broadcast_runtime.rs`
- `iamine-models/tests/integration.rs`

Known non-blockers:

- existing Rust `dead_code` warnings
- existing clippy warnings
- `cargo audit`, `cargo deny`, and `gitleaks` unavailable locally
- metrics fallback for ports below 9000
- storage/model registry display can show tinyllama under mock/skip

## 10. Decision

Decision: continue with Block B.

Next recommended branch:

- `refactor/broadcast-runtime-002`

Reason:

`iamine-node/src/broadcast_runtime.rs` remains a large-file warning and is the
next structural focus before prompt analyzer cleanup, test suite splitting,
and quality gate strictness.
