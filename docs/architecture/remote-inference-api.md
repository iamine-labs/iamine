# Remote Inference API

Feature:

```text
REMOTE-INFERENCE-API-001
```

## Purpose

Private testnet nodes need a bounded remote inference API before broader
multi-operator testnet operation. The API accepts remote inference requests only
after the transport, peer admission, request bounds, and model execution gates
all pass.

This feature does not introduce public access, billing, rewards, reputation,
model selection policy, scheduler policy, model download behavior, or new trust
roots.

## Runtime Contract

Remote inference requests are admitted only when all of the following are true:

```text
secure transport allowed
AND requester peer admitted
AND task id present and bounded
AND attempt id present and bounded
AND model id present and bounded
AND prompt present and bounded
AND max tokens within API bound
AND worker model execution gate permits execution
-> remote inference request accepted
```

The worker execution gate remains the owner for local model availability,
registry admission, hardware compatibility, backend availability, and model
network policy. The remote API may reject on those reasons, but it must not
reimplement them.

Current API bounds:

| Field | Bound |
| --- | --- |
| task id | 128 bytes |
| attempt id | 128 bytes |
| model id | 128 bytes |
| prompt | 32768 bytes |
| max tokens | 1-4096 |

Missing `max_tokens` keeps the existing default of 200. Numeric overflow is
converted into an explicit `max_tokens_out_of_bounds` rejection rather than
wrapping.

## Integration

The remote API gate lives in `iamine-node/src/remote_inference_api.rs`.

Worker runtime integration is intentionally narrow:

- `InferenceRequest` and `DirectInferenceRequest` parse bounded token values;
- `start_worker_inference_request` evaluates the remote API gate before model
  loading, inference, daemon calls, or result publication;
- rejected requests emit `remote_inference_api_rejected` with reason codes;
- `main.rs` only wires the current secure transport decision and active
  testnet admission policy into `WorkerInferenceRuntimeContext`.

## Rejection Codes

The gate can reject with:

```text
secure transport reason code
peer_not_admitted
missing_task_id
task_id_too_large
missing_attempt_id
attempt_id_too_large
missing_model_id
model_id_too_large
empty_prompt
prompt_too_large
max_tokens_out_of_bounds
model_not_installed
registry_admission_blocked
hardware_incompatible
backend_unavailable
network_policy_blocked
```

## Boundaries

This feature must not:

- bypass secure transport construction;
- weaken or replace testnet admission;
- select fallback models silently;
- start model downloads;
- start workers implicitly;
- change scheduler selection policy;
- change task/result wire formats outside the bounded worker API gate;
- collect or log hostnames, IP addresses, usernames, local paths, MAC
  addresses, serials, machine IDs, keys, tokens, or credentials.

## Validation

Required local validation:

```text
cargo fmt --all -- --check
cargo test -p iamine-node remote_inference_api
cargo test -p iamine-node worker_runtime
cargo test -p iamine-node
cargo build -p iamine-node
git diff --check
```

Because this touches worker runtime request admission, field QA is required
before merge review:

```text
Mac local validation
TS140 worker validation
Proxmox/R5500 worker validation
```

Field QA must prove that accepted requests still run on admitted peers and that
blocked requests fail before model load or inference execution.
