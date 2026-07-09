# IAMINE Testnet Load Resilience QA

Feature:

```text
TESTNET-LOAD-RESILIENCE-001
```

## Objective

Validate the explicit `testnet-load-resilience` cluster stress profile without
changing scheduler, worker, transport, model loading, or inference behavior.

## Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-node cluster_stress
cargo test -p iamine-node cli::tests::cli_detects_cluster_stress_testnet_load_profile
cargo build -p iamine-node
./target/debug/iamine-node cluster stress --help
./target/debug/iamine-node cluster stress --requests 0 --profile testnet-load-resilience --json
```

Expected:

- standard profile remains the default;
- zero-request testnet profile starts no Broadcast child processes;
- JSON includes `resilience`;
- `resilience.profile` is `testnet_load_resilience`;
- `resilience.passed=true`;
- `passed=true`;
- no model download or real backend load is triggered.

## Guardrail Smokes

These commands must fail before runtime execution:

```bash
./target/debug/iamine-node cluster stress \
  --requests 1 \
  --concurrency 1 \
  --profile testnet-load-resilience \
  --json

./target/debug/iamine-node cluster stress \
  --requests 2 \
  --concurrency 1 \
  --profile testnet-load-resilience \
  --json

./target/debug/iamine-node cluster stress \
  --requests 2 \
  --concurrency 2 \
  --profile testnet-load-resilience \
  --stop-on-first-failure \
  --json

./target/debug/iamine-node cluster stress \
  --requests 0 \
  --require-recovery-evidence \
  --json
```

Expected:

- command exits non-zero;
- no output directory is created for the rejected runtime shapes;
- error explains the invalid combination.

## TS140 Field QA

Start the TS140 worker with the normal controlled testnet configuration, then
run from the controller:

```bash
STRESS_ID="qa-testnet-load-ts140-$(date +%s)"

./target/release/iamine-node cluster stress \
  --requests 20 \
  --concurrency 4 \
  --task reverse_string \
  --profile testnet-load-resilience \
  --prefix "$STRESS_ID" \
  --json
```

Required result:

- `passed=true`;
- `resilience.passed=true`;
- `resilience.all_requests_accounted=true`;
- `resilience.concurrency_exercised=true`;
- `resilience.no_failed_requests=true`;
- `resilience.no_timed_out_requests=true`;
- `resilience.no_duplicate_results=true`;
- `resilience.no_duplicate_executions=true`;
- `resilience.no_duplicate_identities=true`;
- `resilience.no_incompatible_assignments=true`;
- `resilience.lifecycle_validated=true`;
- `resilience.blocking_failures=[]`;
- TS140 remains on the expected backend and execution mode;
- no `SIGILL`.

## Proxmox/R5500 Field QA

Run only after the Proxmox guests are available.

Start the workers with:

```bash
IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1
IAMINE_INFERENCE_BACKEND=mock
```

Then run from the controller:

```bash
STRESS_ID="qa-testnet-load-proxmox-$(date +%s)"

./target/release/iamine-node cluster stress \
  --requests 30 \
  --concurrency 6 \
  --task reverse_string \
  --profile testnet-load-resilience \
  --prefix "$STRESS_ID" \
  --json
```

Required result:

- `passed=true`;
- `resilience.passed=true`;
- `resilience.blocking_failures=[]`;
- workers remain `backend=mock`, `execution_mode=mock`;
- `executable_models=[]`;
- no real LLM load;
- no `SIGILL`.

## Recovery Evidence Pass

When Architecture wants an outage/recovery proof, enable:

```bash
--require-recovery-evidence
```

Required result:

- at least one request has retry or fallback evidence;
- `resilience.recovery_evidence_required=true`;
- `resilience.recovery_evidence_observed=true`;
- `resilience.blocking_failures` does not contain `recovery_evidence_missing`.

If no outage/retry/fallback was intentionally induced, this mode must fail.
