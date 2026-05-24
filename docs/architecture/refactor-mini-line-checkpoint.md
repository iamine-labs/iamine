# IAMINE - Refactor Mini-Line Checkpoint

## 1. Objective

This document closes the controlled refactor mini-line completed after
ROUTER-SCHEDULER-BASELINE-001.

The goal of the mini-line was to reduce `iamine-node/src/main.rs`, isolate
critical runtime flows, and preserve validated behavior before returning to
feature work.

This checkpoint documents architecture state only. It does not introduce new
features and does not change runtime behavior.

## 2. Refactors Closed

- REFACTOR-INFER-RUNTIME-001
- REFACTOR-WORKER-RUNTIME-002
- REFACTOR-CONTROLLER-RUNTIME-003

## 3. State By Refactor

| Refactor | Branch | Merge Commit | Runtime Behavior Changed | QA Status | Main Before | Main After | Delta |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| REFACTOR-INFER-RUNTIME-001 | `refactor/infer-runtime-001` | `1de1d9c` | no | PASS COMPLETO | 9309 | 7401 | -1908 |
| REFACTOR-WORKER-RUNTIME-002 | `refactor/worker-runtime-002` | `891653c` | no | PASS COMPLETO | 7401 | 6532 | -869 |
| REFACTOR-CONTROLLER-RUNTIME-003 | `refactor/controller-runtime-003` | `a121af3` | no | PASS COMPLETO | 6532 | 6017 | -515 |

Field QA summary:

- REFACTOR-INFER-RUNTIME-001: Mac PASS, TS140 PASS, Proxmox/R5500 PASS.
- REFACTOR-WORKER-RUNTIME-002: Mac PASS, TS140 PASS, Proxmox/R5500 PASS.
- REFACTOR-CONTROLLER-RUNTIME-003: Mac PASS, TS140 PASS, Proxmox/R5500 PASS.

## 4. main.rs Reduction

| Stage | main.rs Lines | Delta |
| --- | ---: | ---: |
| Before infer runtime | 9309 | - |
| After infer runtime | 7401 | -1908 |
| After worker runtime | 6532 | -869 |
| After controller runtime | 6017 | -515 |
| Total | 6017 | -3292 |

## 5. Modules Created

Infer runtime:

- `iamine-node/src/infer_runtime.rs`
- `iamine-node/src/infer_retry.rs`
- `iamine-node/src/infer_watchdog.rs`
- `iamine-node/src/infer_observability.rs`

Worker runtime:

- `iamine-node/src/worker_runtime.rs`
- `iamine-node/src/worker_pubsub_runtime.rs`
- `iamine-node/src/worker_assignment_runtime.rs`
- `iamine-node/src/worker_result_runtime.rs`

Controller runtime:

- `iamine-node/src/controller_broadcast_runtime.rs`
- `iamine-node/src/controller_cluster_runtime.rs`
- `iamine-node/src/controller_result_runtime.rs`

## 6. QA Field Baseline

TS140:

- real worker: PASS
- backend CPU real: PASS
- `real_inference_available=true`: PASS
- Broadcast Mac -> TS140: PASS
- TaskResult accepted: PASS
- `final_outcome=success`: PASS
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
- no SIGILL evidence: PASS

## 7. Protected Regressions

These regressions remain protected after the mini-line:

- worker mock/skip startup
- no real backend under mock/skip
- no SIGILL
- Broadcast TaskOffer -> TaskBid -> TaskAssign -> TaskResult
- exactly one worker executes
- no duplicate result accepted
- wrong worker rejected
- `broadcast_recovery_cancelled`
- `final_outcome=success`
- task lifecycle trace
- scheduler metadata
- cluster status
- infer local/help
- infer force-network path

## 8. Residual Architecture

`main.rs` is materially smaller, but it still owns some global orchestration:

- top-level swarm loop
- global libp2p event routing
- remaining global runtime orchestration
- heartbeat/discovery wiring
- some status/runtime glue

This is acceptable for the current architecture checkpoint. A new refactor
should not be opened automatically. Future extraction should be triggered by
feature pressure, quality gate evidence, or bugs in these areas.

## 9. Pending Non-Blockers

- existing Rust `dead_code` warnings
- existing clippy warnings
- quality gate large-file warnings
- `cargo audit`, `cargo deny`, and `gitleaks` skipped locally when unavailable
- metrics fallback for ports below 9000
- storage/model registry display can show tinyllama under mock/skip
- TS140 can appear as a candidate during Proxmox smoke, but does not execute

## 10. Decision

Recommendation: return to feature work.

Next recommended feature:

- MODEL-CAPABILITY-CONSISTENCY-001

Reason:

Scheduler baseline exists, but it should not become more authoritative until
model and capability fields are consistent and unambiguous across:

- `models_in_storage`
- `models_in_registry`
- `executable_models`
- `backend`
- `execution_mode`
- `real_inference_available`
- `supported_task_types`
- `ready_for_tasks`
- `readiness_reason`

## 11. Future Refactors

Keep these in backlog, not as immediate work:

- REFACTOR-LIBP2P-EVENT-LOOP-004
- REFACTOR-OBSERVABILITY-HELPERS-005
- REFACTOR-CONFIG-ENV-006

Trigger future refactors only if:

- feature work starts growing `main.rs` again
- the quality gate makes it blocking
- a bugfix requires touching those areas
- `main.rs` becomes difficult to reason about again
