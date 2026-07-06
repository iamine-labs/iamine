# LAN Beta First-Run Preflight

Feature: `LAN-BETA-FIRST-RUN-PREFLIGHT-001`

## Purpose

The LAN beta first-run preflight gives operators one package-local command to
validate a fresh IAMINE LAN beta install before starting a worker or loading a
service manager unit.

The preflight is intentionally operational packaging logic. It reuses existing
CLI diagnostics instead of adding runtime behavior.

## Scope

The packaged script is:

```text
scripts/first-run-preflight.sh
```

It validates:

- package manifest JSON when present;
- helper script executability;
- selected `iamine-node` binary executability;
- `iamine-node --help`;
- `node config status --json` and its no-runtime-side-effects contract;
- `lan doctor --json`;
- `worker lifecycle status --json`;
- `worker lifecycle readiness --json`;
- `models catalog`;
- `cluster status --json` as a bounded LAN smoke unless `--skip-lan-smoke` is
  passed.

The script emits stable summary lines:

```text
FIRST_RUN_PREFLIGHT_STATUS=pass|warn|fail
FIRST_RUN_PREFLIGHT_PASS_COUNT=<n>
FIRST_RUN_PREFLIGHT_WARN_COUNT=<n>
FIRST_RUN_PREFLIGHT_FAIL_COUNT=<n>
```

It exits nonzero only when one or more checks fail. Warnings preserve operator
visibility without blocking isolated-host validation.

## Runtime Boundaries

The preflight must not:

- start workers;
- start P2P;
- start PubSub;
- install, load, enable, start, stop, or restart services;
- download models;
- load models;
- run inference;
- mutate node config.

The `cluster status --json` LAN smoke is the only network-facing check. It is
bounded to the existing cluster-status diagnostic path and can be skipped with
`--skip-lan-smoke`.

## Integration

The package builder copies the script into generated LAN beta packages and
records it in `manifest.json` under both `artifacts` and `first_run_preflight`.

The installer copies the script into:

```text
<prefix>/share/iamine/scripts/first-run-preflight.sh
```

The installer remains manual and does not execute the preflight automatically.
Operators run it explicitly after install and before service activation.

## Validation

Minimum local validation for this feature:

```bash
bash -n packaging/lan-beta/scripts/first-run-preflight.sh
bash -n packaging/lan-beta/scripts/install.sh
bash -n packaging/lan-beta/scripts/uninstall.sh
scripts/lan-beta-package.sh --no-build --binary target/debug/iamine-node --output-dir <tmp-dir>
<package>/scripts/first-run-preflight.sh --binary <package>/bin/iamine-node --skip-lan-smoke
```

Run the full LAN smoke without `--skip-lan-smoke` when the host is available for
local cluster-status diagnostics.

## Risks

- Missing `python3` blocks JSON validation and fails the preflight.
- `lan doctor` or `worker lifecycle readiness` may warn on hosts that are not
  fully configured for real inference.
- The script must stay package-local. Runtime policy belongs in the existing
  CLI diagnostics and owner modules.
