# Testnet Observability

Feature:

```text
TESTNET-OBSERVABILITY-001
```

## Purpose

Private testnet operation needs correlated, privacy-safe evidence for admission,
routing, execution, recovery, result delivery, and health across multiple
operators and physical networks.

This feature adds structured-log enrichment only. It does not add a runtime
mode, alter scheduler selection, change peer admission, change secure
transport, change remote inference request formats, start workers, load models,
download models, or change result acceptance.

## Contract

Known private-testnet events receive these additive fields:

```text
testnet_phase
testnet_observability_scope=private_testnet
testnet_observability_schema_version=1.0.0
```

Supported phase values:

```text
admission
routing
execution
recovery
result_delivery
health
```

Operators and QA can correlate a flow by filtering on the existing `trace_id`
and grouping emitted events by `testnet_phase`. Not every flow must emit every
phase. The contract is that mapped events expose a stable phase when the phase
is known.

## Integration

The mapping lives in `iamine-node/src/testnet_observability.rs` and is applied by
the common helper in `iamine-node/src/runtime_observability.rs`.

Call sites continue to emit existing event names, trace IDs, task IDs, model
IDs, error codes, and fields. Unknown events are not forced into a testnet
phase.

## Boundaries

This feature must not log usernames, home directories, full hostnames, MAC
addresses, IP addresses, serial numbers, disk UUIDs, machine IDs, process lists,
personal paths, keys, tokens, secrets, or credentials.

It must also not reimplement scheduler, admission, secure transport, model
eligibility, worker execution, or result acceptance decisions. Those owner
modules continue to emit the underlying events.

## QA Notes

Local validation must prove:

- mapped events include parseable private-testnet observability fields;
- existing LAN observability enrichment is preserved;
- result delivery and recovery events preserve task, model, and error context;
- unknown events remain unchanged;
- `main.rs` growth is wiring only and `cluster_registry.rs` does not change.

Because this changes runtime observability output, field QA is required before
merge review.

## Closure Evidence

State:

```text
CLOSED
```

Merge:

```text
d6068ceaf6bbaf58e87cdabce99e8d90c03cb64a
```

Tree:

```text
feda5490c897eec716e5719ac31fce76007f4e94
```

Validation:

- local `./scripts/quality-gate.sh`: PASS WITH WARNINGS;
- TS140 field QA: PASS;
- Proxmox field QA on `iamine-ctrl`, `iamine-wrk1`, `iamine-wrk2`, and
  `iamine-heavy`: PASS;
- post-merge `./scripts/quality-gate.sh`: PASS WITH WARNINGS.

Warnings were historical lint/dead-code warnings. Optional tools unavailable in
the local gate were `cargo audit`, `cargo deny`, and `gitleaks`.
