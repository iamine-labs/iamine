# Worker Safe Startup Architecture

## Purpose

Worker safe startup allows LAN workers to participate in broadcast/simple tasks even when real LLM inference is unavailable or unsafe on the host CPU.

This was required for Proxmox/R5500, where real llama/ggml CPU initialization could trigger SIGILL.

## Supported Safety Flags

```text
IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1
IAMINE_INFERENCE_BACKEND=mock
```

## skip_model_load_on_startup

When skip is enabled:

- tinyllama is not loaded at startup
- real model load is avoided
- worker_model_load_skipped is emitted
- worker continues startup
- network port opens
- PubSub subscriptions are joined
- worker_startup_ready is emitted

## mock Backend

When backend=mock:

- inference_backend_selected backend=mock is emitted
- real CPU llama/ggml path is avoided
- model load is skipped
- simple tasks such as reverse_string can execute
- worker is limited/degraded, not dead

## CPU Feature Guard

Before real CPU backend selection, startup policy checks CPU compatibility. If incompatible:

- backend_cpu_feature_incompatible is emitted
- real inference is marked unavailable
- startup may continue in degraded/simple-task mode

This avoids entering unsafe backend code on unsupported CPUs.

## Legacy CPU Real Inference

`LEGACY-BACKEND-REAL-INFERENCE-001` introduces an explicit daemon-only mode for
legacy x86_64 CPU hosts:

```text
IAMINE_LEGACY_CPU_REAL_BACKEND=daemon_only
```

When this mode is active and AVX2 is absent:

- real inference may be considered available only through the daemon route;
- local `RealInferenceEngine` creation remains disabled for the worker;
- startup model advertisement must not validate models by loading the local
  backend;
- if the daemon is unavailable at task time, the worker returns a controlled
  error instead of falling back to the local backend;
- mock/skip behavior remains the default safe baseline.

This mode does not remove the CPU feature guard. It narrows legacy real
inference to an explicit daemon path that must be field-tested on the target
host before merge closure.

## Capabilities Rules

When real inference is unavailable:

- do not advertise real LLM models as executable by the active backend
- keep simple task capability available where supported
- represent the node as limited/degraded, not offline

For Cluster LAN status, capability output should distinguish:

- models in storage
- models in registry
- models executable by active backend
- real backend available/unavailable

## Startup Observability

Important events:

- worker_startup_started
- inference_backend_selected
- backend_cpu_feature_incompatible
- worker_model_load_attempt
- worker_model_load_skipped
- worker_model_load_failed
- worker_listening
- worker_topic_subscribed
- worker_pubsub_ready
- worker_startup_ready
- worker_startup_failed

## Non-Blocking Follow-Ups

worker_startup_invalid_math / metrics port:
Closed. Ports 4101, 4102, and 4103 remain below metrics base 9000, but this is
an explicit non-blocking fallback: workers continue with
continue_without_metrics_server and publish metrics_status=fallback.

QA-PROXMOX-MOCK-CAPABILITIES-DISPLAY-001:
Closed. Cluster status now keeps mock/skip model display explicit: registry and
storage models remain visible, executable models stay empty, real inference is
reported as unavailable, and unavailable model reasons include disabled_by_mock.

LEGACY-BACKEND-REAL-INFERENCE-001:
In progress as a daemon-only legacy CPU path. Proxmox/R5500 field QA is
required before closure.
