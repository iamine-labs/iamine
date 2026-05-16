# Pre-Cluster Refactor Closeout Summary

## Gate

CLOSEOUT-REFACTORING-GATE-001

## Purpose

This document closes the controlled refactor cycle that prepared IAMINE for
CLUSTER-LAN-AUTO-DISCOVERY-001. The goal of the cycle was to reduce
`iamine-node/src/main.rs`, isolate validated runtime paths, and avoid adding
Cluster LAN discovery on top of a monolithic node entrypoint.

## Baseline

```text
integration branch: develop
base commit: 69c292a
stable branch: main
main status: untouched during the refactor cycle
```

## Refactors Closed

| Refactor | Status | Main Before | Main After | Lines Removed | Field Status |
| --- | --- | ---: | ---: | ---: | --- |
| REFACTOR-BROADCAST-RUNTIME-001 | closed / merged | 14150 | 12285 | 1865 | PASS |
| REFACTOR-WORKER-STARTUP-POLICY-002 | closed / merged | 12285 | 11333 | 952 | PASS |
| REFACTOR-CLI-MODE-DISPATCH-006 | closed / merged | 11333 | 10554 | 779 | PASS |
| REFACTOR-PUBSUB-READINESS-003 | closed / merged | 10554 | 9877 | 677 | PASS |
| REFACTOR-MODEL-REGISTRY-DISPLAY-007 | closed / merged | 9877 | 9566 | 311 | PASS |
| REFACTOR-RESULT-PROTOCOL-004 | closed / merged | 9566 | 9176 | 390 | PASS |
| REFACTOR-METRICS-SERVER-008 | merged | 9176 | 8931 | 245 | PASS-TS140 / PROXMOX-PENDING |

Total reduction from `main.rs`:

```text
14150 - 8931 = 5219 lines removed
```

## Modules Extracted

Broadcast:

- `iamine-node/src/broadcast_protocol.rs`
- `iamine-node/src/broadcast_runtime.rs`
- `iamine-node/src/broadcast_worker.rs`

Worker startup and backend policy:

- `iamine-node/src/worker_startup_policy.rs`
- `iamine-node/src/backend_policy.rs`
- `iamine-node/src/cpu_feature_guard.rs`
- `iamine-node/src/worker_capability_advertisement.rs`

CLI and mode dispatch:

- `iamine-node/src/cli.rs`
- `iamine-node/src/node_modes.rs`
- `iamine-node/src/mode_dispatch.rs`
- `iamine-node/src/usage.rs`

PubSub readiness:

- `iamine-node/src/pubsub_topics.rs`
- `iamine-node/src/pubsub_readiness.rs`
- `iamine-node/src/pubsub_topic_tracker.rs`
- `iamine-node/src/pubsub_observability.rs`

Model and capability display:

- `iamine-node/src/model_display_policy.rs`
- `iamine-node/src/model_executability.rs`
- `iamine-node/src/capability_display.rs`

Result protocol:

- `iamine-node/src/result_protocol.rs`
- `iamine-node/src/result_acceptance.rs`
- `iamine-node/src/result_observability.rs`
- `iamine-node/src/final_outcome.rs`

Metrics:

- `iamine-node/src/metrics_policy.rs`
- `iamine-node/src/metrics_server.rs`

## Validated Baseline

The following runtime baseline is considered preserved by the refactor cycle:

- worker mock/skip startup: PASS
- backend=mock: PASS
- real_inference_available=false: PASS
- no real LLM load in Proxmox mock/skip mode: PASS
- no SIGILL in Proxmox mock/skip mode: PASS
- PubSub readiness: PASS
- subscriber tracking: PASS
- Broadcast TaskOffer: PASS
- Broadcast TaskBid: PASS
- Broadcast TaskAssign: PASS
- exactly one worker executes: PASS
- TaskResult published: PASS
- TaskResult received: PASS
- result accepted: PASS
- duplicate and wrong_worker protections: PASS
- recovery cancellation: PASS
- final_outcome=success: PASS
- no rebroadcast after success: PASS
- no duplicate execution/result: PASS
- model display separation between storage, registry, and executable models: PASS
- TS140 real worker baseline: PASS
- metrics fallback on TS140 port 7002: PASS

## Remaining Coupling

`main.rs` is significantly smaller but still owns the top-level async runtime
and libp2p event loop. This is acceptable for Cluster LAN if new code enters
through small modules and helpers instead of expanding the main event loop
directly.

Areas to watch:

- Swarm event wiring remains in `main.rs`.
- Some runtime orchestration still coordinates Broadcast, worker, and result
  helpers from `main.rs`.
- Cluster LAN should reuse the extracted PubSub readiness, startup, capability,
  metrics, and result helpers.

## Pending QA

Proxmox/R5500 metrics field smoke is pending for REFACTOR-METRICS-SERVER-008.

Required validation targets:

- `iamine-wrk-01` on port 4101
- `iamine-wrk-02` on port 4102
- `iamine-wrk-heavy-01` on port 4103

Expected:

- `worker_startup_invalid_math` does not block startup
- `continue_without_metrics_server` appears or is inferable
- workers remain alive
- worker ports are LISTEN
- `worker_startup_ready`
- `worker_pubsub_ready`
- backend=mock
- real_inference_available=false
- Broadcast end-to-end PASS
- final_outcome=success
- no SIGILL

If this fails, open a bugfix on `develop` before closing or shipping Cluster
LAN.

## Follow-Ups

- QA-CLI-UNKNOWN-MODE-EXIT-CODE-007: unknown mode does not start runtime, but a
  field run observed possible exit code 0. Verify and fix if still present.
- `worker_startup_invalid_math` remains expected when worker_port is below the
  metrics base. Cluster LAN should display metrics unavailable/fallback clearly.
- Existing Rust dead_code warnings are non-blocking.
- Cluster assignment log spam is non-blocking and can be cleaned later.

## Decision

GO WITH CONDITIONS for CLUSTER-LAN-AUTO-DISCOVERY-001.

Conditions:

- Run Proxmox/R5500 metrics field smoke when Dell is available.
- If Proxmox metrics fallback regresses, open a bugfix on `develop` before
  closing Cluster LAN.
- Keep Broadcast end-to-end smoke as a protected regression.
- Keep worker mock/skip startup as the Proxmox safety baseline.
- Do not implement legacy real CPU inference as part of Cluster LAN.
- Reuse extracted modules; do not grow `main.rs` with new Cluster LAN state.
