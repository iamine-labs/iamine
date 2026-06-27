# LAN Real Inference Resilience

Feature:

```text
LAN-REAL-INFERENCE-RESILIENCE-001
```

## Purpose

Recover safely from bounded LAN real-inference failures without widening the
runtime surface or adding hidden startup, model download, local inference
fallback, scheduler policy, or worker behavior.

This feature tightens retry state handling for directed retry paths. A retry
must not advance the distributed inference state unless there is an available
retry target and retry budget.

## Scope

In scope:

- prevent phantom retry state when validation or task failure has no compatible
  retry target;
- keep retry state ownership in `iamine-node/src/infer_retry.rs`;
- keep `iamine-node/src/main.rs` as wiring around existing runtime branches;
- preserve existing timeout fallback behavior and existing scheduler
  selection rules;
- document the runtime QA requirement.

Out of scope:

- changing scheduler compatibility policy;
- changing PubSub readiness or mesh timing;
- changing model eligibility, license, backend, or network gates;
- changing worker startup or capability advertisement;
- adding automatic model downloads or hidden worker startup;
- accepting wrong-worker, duplicate, or late results outside existing result
  acceptance rules.

## Behavior

Before this feature, validation-failure and task-failure paths could call the
generic retry scheduler before confirming that a retry target existed. If the
retry budget was available but no target was available, the final failure path
could see retry state that had already advanced.

The new behavior is:

```text
failure observed
-> preview retry target
-> if no retry target: final failure without advancing retry attempt state
-> if retry target and retry budget: schedule retry and reset attempt context
```

Timeout handling keeps its existing fallback-broadcast behavior because that
path can recover through PubSub without a selected direct target.

## Ownership

The state transition rule lives in `infer_retry.rs`:

```text
schedule_targeted_retry(...)
```

The runtime only asks for a targeted retry when the previewed scheduler target
exists. This keeps retry state mutation close to the retry owner while avoiding
another scheduler or runtime abstraction.

## QA Notes

This feature changes runtime retry behavior, so field QA is required before
merge review under the IAMINE workflow.

Minimum evidence:

- Mac local tests and quality gate;
- TS140 real or controlled LAN inference retry/failure evidence;
- Proxmox/R5500 retry/failure evidence when the server is available;
- confirmation that no wrong-worker, duplicate, or targetless retry is promoted
  to success;
- confirmation that `main.rs` growth remains wiring-only and bounded.

## Compatibility

The change is additive and preserves existing public CLI shape. It does not
change serialized task/result payloads, cluster status shape, worker capability
shape, or model registry semantics.
