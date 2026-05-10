# LAN Proxmox Broadcast Flow Closeout

## Milestone

MILESTONE-CLOSEOUT-QUALITY-GATE-001

## Final Status

PASS COMPLETO in field for the LAN Proxmox Broadcast flow.

Final validated branch:

```text
fix/proxmox-broadcast-taskresult-return-004
```

Final validated commit:

```text
4987d02
```

## Environment

Controller:

```text
host: iamine-ctrl-01
ip: 192.168.2.220
```

Workers:

```text
host: iamine-wrk-01
ip: 192.168.2.221
port: 4101

host: iamine-wrk-02
ip: 192.168.2.222
port: 4102

host: iamine-wrk-heavy-01
ip: 192.168.2.223
port: 4103
```

Worker mode baseline:

```text
IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1
IAMINE_INFERENCE_BACKEND=mock
```

Validated worker properties:

- backend=mock
- worker_model_load_skipped=true
- worker_startup_ready=true
- worker_pubsub_ready=true
- no SIGILL
- no trap invalid opcode
- no real tinyllama/llama/ggml load

## Closed Bugs

QA-PROXMOX-WORKER-MODEL-LOAD-SIGILL-001:
Resolved by FIX-PROXMOX-WORKER-SAFE-STARTUP-MOCK-001. Workers start with skip + mock, avoid real LLM backend paths, keep ports open, and emit startup readiness events.

QA-PROXMOX-BROADCAST-INSUFFICIENT-PEERS-AFTER-CONNECT-001:
Resolved by FIX-PROXMOX-BROADCAST-PUBSUB-READINESS-002 and later verified through subscriber tracking. Root cause was PubSub readiness, not worker death.

QA-PROXMOX-BROADCAST-PUBSUB-READINESS-STILL-ZERO-SUBSCRIBERS-002:
Resolved by FIX-PROXMOX-BROADCAST-SUBSCRIBER-TRACKING-003. Controller now observes worker subscriptions and does not mark broadcast readiness from connected peers alone.

BROADCAST-001:
Resolved. Controller publishes TaskOffer, workers bid, controller assigns one worker, and exactly one worker executes.

QA-PROXMOX-BROADCAST-TASKRESULT-NOT-PUBLISHED-002:
Resolved by FIX-PROXMOX-BROADCAST-TASKRESULT-RETURN-004. Assigned worker publishes TaskResult and controller accepts it.

BROADCAST-002:
Resolved. TaskAssign execution now produces TaskResult, the broadcaster receives it, cancels recovery, and emits success.

## Validated End-To-End Flow

1. Workers start in mock/skip mode.
2. Workers avoid real model load and real CPU llama/ggml runtime.
3. Workers subscribe to PubSub topics.
4. Controller enters Broadcast mode.
5. Controller subscribes to required topics.
6. Controller observes real worker subscriptions.
7. Controller waits for subscriber or mesh readiness.
8. Controller publishes TaskOffer.
9. Workers receive TaskOffer.
10. Workers publish TaskBid.
11. Controller receives bids.
12. Controller selects a winning worker.
13. Controller publishes TaskAssign.
14. Only assigned worker executes.
15. Winning worker emits task_completed success=true.
16. Winning worker publishes TaskResult.
17. Controller receives TaskResult.
18. Controller validates task_id + assigned_worker.
19. Controller accepts result.
20. Controller cancels recovery.
21. Controller emits final_outcome=success and final_outcome_success.

## Baseline Worker Command

```bash
IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1 \
IAMINE_INFERENCE_BACKEND=mock \
IAMINE_LOG_FORMAT=ndjson \
IAMINE_LOG_PATH=~/iamine-logs/worker_<port>_baseline.ndjson \
./target/release/iamine-node --worker --port=<port>
```

## Baseline Broadcast Smoke

```bash
SMOKE_ID="cluster-baseline-smoke-$(date +%s)"

IAMINE_LOG_FORMAT=ndjson \
IAMINE_LOG_PATH=~/iamine-logs/controller_cluster_baseline.ndjson \
timeout 75s ./target/release/iamine-node --broadcast reverse_string "$SMOKE_ID"
```

Expected:

- broadcast_task_offer_published
- broadcast_bid_received
- broadcast_task_assign_published
- exactly one worker has will_execute=true
- task_completed success=true on assigned worker
- broadcast_result_published
- task_result_published
- task_result_received
- broadcast_result_received
- output equals reverse_string(SMOKE_ID)
- broadcast_recovery_cancelled
- final_outcome=success
- no rebroadcast after success

## Known Non-Blockers

worker_startup_invalid_math / metrics port:
Ports 4101, 4102, and 4103 are below the metrics base 9000. Current behavior continues without metrics server. Status: non-blocking.

QA-PROXMOX-MOCK-CAPABILITIES-DISPLAY-001:
Human logs may still show registry/storage models while backend=mock and real_inference_available=false. Future cleanup should distinguish models in storage, models in registry, executable models by active backend, and real backend availability. Status: non-blocking.

LEGACY-BACKEND-REAL-INFERENCE-001:
Real CPU llama/ggml inference on Proxmox/R5500 remains future work because of SIGILL risk. It is not part of Cluster LAN. Status: future feature.

## QA Pass Criteria

The milestone remains PASS as long as:

- Workers remain alive in mock/skip.
- Controller sees real PubSub readiness.
- Broadcast does not publish from connected_peers alone.
- TaskOffer reaches at least one worker.
- At least one TaskBid reaches controller.
- Exactly one TaskAssign executes.
- Assigned worker publishes TaskResult.
- Controller accepts only assigned worker result.
- Controller emits final_outcome=success.
- Recovery does not rebroadcast after accepted result.
