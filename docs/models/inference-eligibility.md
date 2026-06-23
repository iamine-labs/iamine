# Model Inference Eligibility Gate

`MODEL-INFERENCE-ELIGIBILITY-GATE-001` defines one aggregate decision for whether
a model may be used for IAMINE network inference on a node.

The gate is additive. It does not replace the owner gates and it does not make
one gate silently assume another gate's responsibility.

## Inputs

The aggregate decision requires all of these signals:

- local installation state;
- model registry admission for existing execution;
- hardware compatibility decision;
- backend availability signal;
- network policy evaluated for `NetworkInference`.

## Decision

The model is eligible only when:

```text
installed
AND registry admission permits existing execution
AND hardware compatibility is compatible
AND backend is available
AND network policy permits distributed inference
```

Every failed input is preserved as an explicit blocking reason. The first
blocking reason is stable and ordered as:

```text
model_not_installed
registry_admission_blocked
hardware_incompatible
backend_unavailable
network_policy_blocked
```

## Reporting Contract

The decision exposes stable report codes for local observability and future
callers:

- `status_code()`: `eligible` or `ineligible`
- `first_blocking_reason_code()`: the first ordered blocking reason, if any
- `blocking_reason_codes()`: all blocking reasons in stable gate order
- `report()`: a read-only summary of the same status and reason codes

Reporting helpers must not start workers, networking, downloads, installs,
model loading, inference, hardware probes, or persistence.

## Ownership

The aggregate gate lives in `iamine-models` because it combines model-policy
decisions. Runtime-specific backend detection remains owned by the node runtime;
this gate accepts only the resulting backend availability signal.

## Non-Goals

This gate must not:

- start workers, P2P, downloads, installs, model loading, or inference;
- decide scheduler routing, reputation, rewards, or model compatibility classes;
- weaken license, checksum, source, format, hardware, backend, or network gates.
