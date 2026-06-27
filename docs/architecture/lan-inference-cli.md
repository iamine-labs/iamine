# LAN Inference CLI

Feature:

```text
LAN-INFERENCE-CLI-001
```

## Purpose

Expose the supported LAN beta inference entrypoint without creating a second
runtime path. The command must make network inference explicit and must not
start hidden workers, downloads, model installs, or local inference fallback.

## CLI Contract

```bash
iamine-node lan infer <prompt> --model <model_id> [--max-tokens N]
```

`lan infer` is an explicit LAN wrapper over the existing distributed inference
mode. It requires `--model` so a requested LAN model is not silently replaced by
a local or inferred default selection. The parser sets:

```text
force_network = true
no_local = false
prefer_local = false
```

`--prefer-local` is rejected because the command is specifically for LAN
inference.

## Operator Flow

The supported beta flow remains explicit:

```text
iamine-node lan doctor
iamine-node models catalog
iamine-node models select
iamine-node models license accept <model_id> --yes
iamine-node models download <model_id>
iamine-node worker lifecycle readiness --port=N
iamine-node --worker --port=N
iamine-node lan infer <prompt> --model <model_id>
```

Each step is operator-invoked. The LAN inference CLI does not perform earlier
steps automatically.

## Runtime Boundary

This feature does not add scheduler policy, worker startup behavior, model
policy, model download behavior, PubSub behavior, or result acceptance behavior.
Runtime execution stays on the existing distributed inference path.

## Failure Semantics

Parser failures must be explicit:

- missing prompt reports the required command shape;
- missing model reports the required `--model <model_id>` flag;
- local-preference flags are rejected for `lan infer`;
- error text points to the diagnostic and model catalog commands.

Runtime failures after parsing remain owned by the existing inference,
scheduler, network, and worker modules.

## QA Notes

Required local evidence:

- parser tests prove `lan infer` maps to force-network inference;
- parser tests prove missing model and local preference are rejected;
- usage text includes the supported command;
- CLI smokes prove help and parser errors return before network startup;
- quality gate before merge review.

Field QA is not required unless this feature expands beyond parser and CLI
wiring into runtime, scheduler, worker, capabilities, cluster status, or
inference execution behavior.
