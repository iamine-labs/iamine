# V1 Observability LAN Phase

Feature:

```text
V1-OBSERVABILITY-001 (LAN phase)
```

## Purpose

Provide correlated, privacy-safe LAN beta evidence for setup, dispatch,
execution, recovery, and result delivery without changing scheduler policy,
network payloads, worker behavior, model eligibility, or result acceptance.

This is a structured-log contract. It does not add a new runtime mode and does
not start workers, PubSub, model downloads, model loads, or inference.

## Scope

In scope:

- add stable LAN phase markers to existing structured observability events;
- keep the enrichment in the observability owner helper;
- preserve existing event names, trace IDs, task IDs, model IDs, and error
  codes;
- keep event enrichment additive and parseable by existing log readers;
- document the phase mapping for QA and release evidence.

Out of scope:

- changing scheduler selection;
- changing PubSub readiness;
- changing retry, fallback, timeout, or result acceptance behavior;
- adding host identifiers, IP addresses, MAC addresses, usernames, home
  directories, or other private hardware/operator data;
- changing CLI output or public command shape.

## LAN Phase Markers

All mapped events receive these additional fields:

```text
lan_phase
lan_observability_scope=lan_beta
lan_observability_schema_version=1.0.0
```

The supported phase values are:

```text
setup
dispatch
execution
recovery
result_delivery
```

The mapping is intentionally derived from event names inside
`lan_observability.rs`, then applied by the common helper in
`runtime_observability.rs`. Call sites continue to emit their existing events;
the common helper enriches only known LAN beta events. Unknown or diagnostic
events remain unchanged.

## Contract

The LAN beta operator can correlate one task or startup flow by filtering on the
existing `trace_id` and then grouping mapped events by `lan_phase`:

```text
setup -> dispatch -> execution -> recovery -> result_delivery
```

Not every task must emit every phase. For example, a clean success may skip
recovery, and a pre-dispatch failure may never reach execution. The contract is
that emitted LAN beta events identify their phase consistently when the phase is
known.

## QA Notes

Local validation must prove:

- mapped events include parseable LAN phase fields;
- result delivery and recovery events preserve existing task/model/error
  context;
- unknown diagnostic events are not forced into a LAN phase;
- `main.rs` and `cluster_registry.rs` do not grow.

Because this changes runtime observability output, field QA is required before
merge review when the environment is available.
