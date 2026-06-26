# LAN Node Doctor

Feature:

```text
LAN-NODE-DOCTOR-001
```

## Purpose

`iamine-node lan doctor` gives an operator a local readiness snapshot before
starting LAN inference. It is a diagnostic command, not a runtime command.

The command reports:

- hardware profile visibility;
- approved model catalog gates;
- backend availability;
- worker startup policy;
- metrics policy derivation;
- node config schema readiness;
- LAN network readiness state.

## Runtime Boundary

The doctor must not start:

- workers;
- P2P;
- PubSub;
- model downloads;
- model loads;
- inference;
- dynamic hardware probes.

Network readiness is reported as `not_run` unless later work adds a bounded
probe that can prove readiness without starting P2P or PubSub as a hidden side
effect.

## Output Contract

Human output is intended for terminal diagnostics. JSON output is enabled with:

```bash
iamine-node lan doctor --json
```

Each check reports:

- `id`;
- `status`;
- `message`;
- `details`.

Status values are:

```text
pass
warn
fail
not_run
```

The overall status is `fail` when any check fails, `warn` when no check fails
but at least one check warns, and `pass` otherwise. `not_run` checks do not
lower the overall status.

## Known Limitations

`NODE-CONFIG-SCHEMA-MIGRATION-001` is not implemented yet, so the config schema
check reports `warn`.

The metrics check derives the default metrics endpoint but does not bind a
socket. Bind behavior remains owned by worker startup.

The model catalog path reuses existing model gate code. It does not download
or load models.
