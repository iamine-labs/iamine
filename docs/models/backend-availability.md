# IAMINE Model Backend Availability

MODEL-BACKEND-AVAILABILITY-GATE-001 defines the explicit local gate that decides
whether the active worker backend can execute real model inference.

This gate is intentionally separate from:

- download policy
- trusted registry integrity
- license policy
- license acceptance
- hardware compatibility
- network policy
- scheduler selection
- rewards or reputation

## Inputs

The gate evaluates already-known local runtime signals:

- selected backend is mock or real
- startup model loading was skipped
- CPU features are compatible with the real backend
- real inference is available after startup policy evaluation

It does not load models, start networking, download artifacts, mutate scheduler
state, or probe hardware directly.

## Status

The gate returns:

- `available`
- `unavailable`

## Reasons

The gate returns stable reasons:

- `available`
- `mock_backend`
- `model_load_skipped`
- `cpu_feature_incompatible`
- `real_inference_unavailable`

## Rule

Real model inference is allowed only when:

```text
backend is real
AND startup model loading is not skipped
AND CPU features are compatible
AND real inference is available
```

Mock/skip workers may remain available for simple tasks, but they must not
advertise real LLM models as executable by the active backend.
