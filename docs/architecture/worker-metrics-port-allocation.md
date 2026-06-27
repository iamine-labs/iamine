# Worker Metrics Port Allocation

Feature:

```text
WORKER-METRICS-PORT-ALLOCATION-001
```

## Purpose

IAMINE workers need deterministic HTTP metrics endpoints for LAN beta runs with
multiple worker processes. The allocation must cover the worker ports already
used in field QA, including TS140 and Proxmox/R5500 ports below `9000`, without
requiring process scans or hidden runtime probes.

## Allocation Contract

Metrics allocation is owned by `iamine-node/src/metrics_policy.rs`.

For worker ports below `9000`:

```text
metrics_port = worker_port + 10000
allocation_strategy = low_worker_port_offset
```

Examples:

```text
4101 -> 14101
4102 -> 14102
4103 -> 14103
7002 -> 17002
```

For worker ports at or above `9000`, IAMINE preserves the existing default
mapping:

```text
metrics_port = 9090 + (worker_port - 9000)
allocation_strategy = legacy_worker_base
```

Examples:

```text
9000 -> 9090
9001 -> 9091
```

If the derived metrics port would exceed the valid `u16` port range, worker
startup may continue without a metrics server through the existing non-blocking
fallback path.

## Reporting Contract

Diagnostic commands report the derived endpoint without binding it:

- `iamine-node worker lifecycle ... --json` includes `metrics_port`,
  `allocation_strategy`, `allocation_offset`, and `bind_probe: "not_run"`.
- `iamine-node lan doctor --json` reports the default worker-port allocation
  without starting workers, P2P, PubSub, model loading, downloads, or inference.

## Runtime Boundary

This feature does not add a process scanner, service manager, port reservation
database, or config schema migration. Runtime worker startup still owns the
actual metrics server bind attempt. Bind failure remains non-fatal and must not
block worker startup.

## Privacy

The allocation logic is pure arithmetic over the selected worker port. It must
not collect or emit usernames, home directories, hostnames, IP addresses, MAC
addresses, serial numbers, machine IDs, process lists, wallet keys, tokens, or
credentials.

## QA Notes

Required local evidence:

- unit tests for low-port, TS140, Proxmox, default, and overflow allocation;
- lifecycle JSON smoke for a low worker port;
- LAN doctor JSON smoke for the default worker port;
- quality gate before merge review.

Field QA is required before merge review because the feature changes worker
runtime metrics behavior. TS140 should validate `7002 -> 17002`. Proxmox/R5500
should validate `4101`, `4102`, and `4103` when the environment is available.
