# IAMINE Cluster Stress Multi-Model Concurrency

## Objective

`CLUSTER-STRESS-MULTI-MODEL-CONCURRENCY-001` adds a bounded field-QA runner for
the existing Broadcast path:

```bash
./target/release/iamine-node cluster stress \
  --requests 20 \
  --concurrency 4 \
  --task reverse_string \
  --prefix qa-cluster-stress-ts140 \
  --json
```

The runner starts isolated Broadcast controller child processes, preserves the
existing scheduler and wire path, and aggregates lifecycle and NDJSON evidence
per request. It does not own swarm logic, mutate the cluster registry, bypass
capability matching, or load models.

## Safe Scope

The field runner accepts bounded simple tasks:

- `reverse_string`
- `echo`
- `test`

Limits:

- requests: `0..=200`
- concurrency: `1..=16`
- timeout per request: `1..=900` seconds
- concurrency cannot exceed non-zero request count

`--requests 0 --json` is a validation-only smoke that starts no Broadcast
children.

Explicit required-model transport is not added by this iteration. Model
compatibility remains enforced by the scheduler and covered by its tests.
Mixed required-model field batches remain deferred until an existing
backward-compatible protocol surface can carry that requirement.

## Output

Default artifacts are written under the operating system temporary directory:

```text
<system-temp>/iamine-<prefix>/
```

Each request has isolated artifacts:

- `request-NNNN.trace.json`
- `request-NNNN.ndjson`
- `request-NNNN.human.log`

The summary reports:

- total, observed, completed, failed, and timed-out requests
- retries and fallback usage
- duplicate accepted results and duplicate executions
- duplicate request IDs and duplicate task IDs across concurrent children
- incompatible assignments
- min, max, p50, p95, and p99 latency
- lifecycle validation failures

A passing request must preserve the complete lifecycle:

1. `task_lifecycle_created`
2. `task_lifecycle_assigned`
3. `task_lifecycle_scheduler_decision`
4. `task_lifecycle_result_received`
5. `task_lifecycle_completed`
6. `task_lifecycle_finalized`

## Local Smoke

```bash
cargo build -p iamine-node
./target/debug/iamine-node cluster stress --help
./target/debug/iamine-node cluster stress --requests 0 --json
```

Expected:

- help exits without runtime side effects
- zero-request smoke returns `passed=true`
- p95 and p99 fields are present and `null`

## TS140 Field QA

Start the TS140 real worker as usual, then run from the Mac controller:

```bash
STRESS_ID="qa-cluster-stress-ts140-$(date +%s)"

./target/release/iamine-node cluster stress \
  --requests 20 \
  --concurrency 4 \
  --task reverse_string \
  --prefix "$STRESS_ID" \
  --json
```

Required result:

- `completed=20`
- `failed=0`
- `timed_out=0`
- `duplicate_results=0`
- `duplicate_executions=0`
- `duplicate_request_ids=0`
- `duplicate_task_ids=0`
- `incompatible_assignments=0`
- p95 and p99 present
- TS140 remains `backend=cpu`, `execution_mode=real`
- no `SIGILL`

## Proxmox/R5500 Field QA

Start the three workers with:

```bash
IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1
IAMINE_INFERENCE_BACKEND=mock
```

Then run from `iamine-ctrl-01`:

```bash
STRESS_ID="qa-cluster-stress-proxmox-$(date +%s)"

./target/release/iamine-node cluster stress \
  --requests 30 \
  --concurrency 6 \
  --task reverse_string \
  --prefix "$STRESS_ID" \
  --json
```

Required result:

- `completed=30`
- `failed=0`
- `timed_out=0`
- `duplicate_results=0`
- `duplicate_executions=0`
- `duplicate_request_ids=0`
- `duplicate_task_ids=0`
- `incompatible_assignments=0`
- p95 and p99 present
- workers remain `backend=mock`, `execution_mode=mock`
- `executable_models=[]`
- no real LLM load
- no `SIGILL`

## Failure Handling

Use `--stop-on-first-failure` for a short diagnostic run. The summary preserves
`not_run` so an early stop cannot look like a complete PASS.

Inspect the request-specific trace, NDJSON, and human log in the output
directory. A non-zero command exit means at least one request failed or a
validation guard detected a regression.
