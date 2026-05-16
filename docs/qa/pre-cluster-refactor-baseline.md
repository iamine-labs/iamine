# Pre-Cluster Refactor QA Baseline

## Gate

CLOSEOUT-REFACTORING-GATE-001

## Baseline Branch

```text
branch: develop
base commit: 69c292a
next documentation branch: chore/refactoring-closeout-gate-001
```

## Local Validation

Required commands before starting CLUSTER-LAN-AUTO-DISCOVERY-001:

```bash
cargo fmt --all
cargo test -p iamine-node
cargo test -p iamine-network
cargo build -p iamine-node
git diff --check
git diff --cached --check
```

Expected:

- `cargo fmt --all`: PASS
- `cargo test -p iamine-node`: PASS, 194 tests or more
- `cargo test -p iamine-network`: PASS
- `cargo build -p iamine-node`: PASS
- `git diff --check`: PASS
- `git diff --cached --check`: PASS

Existing Rust dead_code warnings are accepted for this gate if they do not
increase materially.

## Field Baseline

Validated before this closeout:

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
- duplicate result rejected: PASS
- wrong_worker result rejected: PASS
- recovery cancellation: PASS
- final_outcome=success: PASS
- no rebroadcast after success: PASS
- no duplicate execution/result: PASS
- model display separation under mock/skip: PASS
- TS140 real worker baseline: PASS
- TS140 metrics fallback: PASS

## Worker Mock/Skip Command

```bash
IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1 \
IAMINE_INFERENCE_BACKEND=mock \
IAMINE_LOG_FORMAT=ndjson \
IAMINE_LOG_PATH=~/iamine-logs/worker_<port>_pre_cluster_baseline.ndjson \
./target/release/iamine-node --worker --port=<port>
```

Expected:

- process alive
- worker port LISTEN
- backend=mock
- real_inference_available=false
- `worker_model_load_skipped`
- `worker_startup_ready`
- `worker_pubsub_ready`
- no SIGILL
- no invalid opcode
- no real `llama`/`ggml` path

## Broadcast Baseline Command

```bash
SMOKE_ID="pre-cluster-baseline-$(date +%s)"

IAMINE_LOG_FORMAT=ndjson \
IAMINE_LOG_PATH=~/iamine-logs/controller_pre_cluster_baseline.ndjson \
timeout 75s ./target/release/iamine-node --broadcast reverse_string "$SMOKE_ID"
```

Expected:

- TaskOffer published
- TaskBid received
- TaskAssign published
- exactly one worker executes
- TaskResult published
- TaskResult received
- output equals reverse_string(SMOKE_ID)
- `broadcast_recovery_cancelled`
- `final_outcome=success`
- no rebroadcast after success
- no duplicate execution/result

## Proxmox/R5500 Metrics Pending

Status: PENDING.

Reason:

REFACTOR-METRICS-SERVER-008 passed local validation and TS140 real-worker smoke,
but Dell/Proxmox was unavailable for the final metrics field smoke.

Run when available:

- `iamine-wrk-01` port 4101
- `iamine-wrk-02` port 4102
- `iamine-wrk-heavy-01` port 4103

Expected:

- `worker_startup_invalid_math` does not block startup
- `continue_without_metrics_server` appears or is inferable
- workers remain alive
- worker ports LISTEN
- `worker_startup_ready`
- `worker_pubsub_ready`
- backend=mock
- real_inference_available=false
- Broadcast end-to-end PASS
- `final_outcome=success`
- no SIGILL

If this fails, open a bugfix on `develop` before closing Cluster LAN.

## TS140 Real Worker Baseline

Status: PASS.

Validated:

- TS140 worker on port 7002
- CPU backend real
- AVX2/FMA present
- mistral-7b loaded
- tinyllama-1b loaded
- `worker_startup_invalid_math` expected because 7002 is below metrics base 9000
- fallback_behavior=continue_without_metrics_server
- worker remains alive
- Broadcast end-to-end PASS
- TaskResult return PASS
- `final_outcome=success`

## Non-Blocking Items

- QA-CLI-UNKNOWN-MODE-EXIT-CODE-007: verify unknown mode exit code if still 0.
- `worker_startup_invalid_math` remains expected for worker ports below metrics
  base 9000.
- Rust dead_code warnings are non-blocking.
- Cluster assignment log spam is non-blocking.

## QA Decision

GO WITH CONDITIONS for CLUSTER-LAN-AUTO-DISCOVERY-001.

Primary condition:

Run the Proxmox/R5500 metrics smoke when Dell is available. If metrics fallback
regresses, fix it on `develop` before closing Cluster LAN.
