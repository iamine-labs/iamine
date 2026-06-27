# LAN Worker Lifecycle

Feature:

```text
LAN-WORKER-LIFECYCLE-001
```

## Purpose

This feature gives operators a bounded worker lifecycle surface for the LAN
beta without introducing a background supervisor. It explains install, start,
stop, restart, readiness, recovery, and status steps while preserving the
existing explicit worker runtime command:

```bash
iamine-node --worker --port=N
```

The lifecycle command is diagnostic and planning-oriented. It must not start or
stop workers by itself.

## CLI Contract

```bash
iamine-node worker lifecycle install [--port=N] [--json]
iamine-node worker lifecycle start [--port=N] [--json]
iamine-node worker lifecycle stop [--port=N] [--json]
iamine-node worker lifecycle restart [--port=N] [--json]
iamine-node worker lifecycle readiness [--port=N] [--json]
iamine-node worker lifecycle recover [--port=N] [--json]
iamine-node worker lifecycle status [--port=N] [--json]
```

The default worker port is `9000`, matching existing worker behavior.

## Boundaries

The command reports:

- worker port;
- backend mode;
- skip-model-load state;
- legacy CPU daemon mode;
- real inference availability;
- derived metrics policy and fallback;
- static hardware profile visibility;
- explicit next-step commands;
- runtime side-effect flags.

The command does not:

- start workers;
- stop workers;
- kill processes;
- scan user process lists;
- start P2P or PubSub;
- download models;
- load models;
- run inference.

Process observation remains manual to avoid collecting user process lists.
Service manager integration is deferred to
`LAN-INFERENCE-BETA-PACKAGING-001`.

## Ownership

Lifecycle reporting lives in `iamine-node/src/worker_lifecycle.rs`. CLI parsing
and pre-network dispatch are wiring only. Hardware-to-model capability
projection is shared through `iamine-node/src/node_capability_snapshot.rs` so
the LAN doctor and lifecycle command use the same privacy-safe snapshot path.

`iamine-node/src/main.rs` must only register modules for this feature. Worker
runtime behavior remains owned by the existing worker runtime modules.

## Failure Semantics

Readiness is blocked when static hardware profiling fails or reports an
unsupported schema. Backend unavailability is reported as a warning because
existing worker policy can still allow degraded startup for mock/simple-task
flows. Metrics startup failures are reported as warnings when the existing
metrics policy can continue without a metrics server.

Stop and restart are manual until package/service manager ownership exists.
Recovery must point the operator to diagnostics and explicit restart/degraded
startup commands rather than executing them automatically.

## QA Notes

This feature touches worker lifecycle behavior and requires field QA before
merge review under the canonical workflow. Minimum expected validation:

- Mac local validation;
- TS140 lifecycle smoke;
- Proxmox/R5500 lifecycle smoke when the environment is available.

The lifecycle smokes should verify JSON parseability, static report fields,
absence of runtime side effects, and no hidden model/P2P/PubSub startup.
