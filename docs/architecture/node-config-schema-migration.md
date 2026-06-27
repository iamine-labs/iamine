# Node Config Schema Migration

Feature:

```text
NODE-CONFIG-SCHEMA-MIGRATION-001
```

## Purpose

IAMINE node configuration is versioned with an explicit local schema version so
operators and diagnostics can distinguish fresh, legacy, current, unsupported,
and invalid config states before runtime starts.

Current schema:

```text
1.0.0
```

Default config file:

```text
~/.iamine/config/node_config.json
```

The CLI redacts local paths in output and reports only file labels.

## Commands

```bash
iamine-node node config status [--path PATH] [--json]
iamine-node node config migrate [--path PATH] [--yes] [--json]
iamine-node node config rollback [--path PATH] [--yes] [--json]
```

`status` is read-only. `migrate` and `rollback` are dry-run unless `--yes` is
present.

## Migration Contract

Legacy flat JSON config without `schema_version` is migrated by adding:

```json
{
  "schema_version": "1.0.0"
}
```

Existing config fields are preserved. Before a write, a legacy backup is kept
beside the config as `node_config.legacy-backup.json` or an equivalent file
label based on the selected `--path`.

Unsupported schema versions and invalid JSON fail closed. They are not
rewritten automatically.

## Rollback Contract

Rollback restores the preserved legacy backup only when `--yes` is present. If
no backup exists, rollback reports a warning and does not write.

## Runtime Boundary

The config commands and `lan doctor` schema check must not start:

- workers;
- P2P;
- PubSub;
- model downloads;
- model loads;
- inference;
- dynamic hardware probes.

## Privacy

The feature must not log or persist usernames, home directories, hostnames, IP
addresses, MAC addresses, serial numbers, machine IDs, wallet keys, tokens, or
credentials. CLI and JSON output use redacted path labels.

## Known Limits

This feature versions the existing node setup config and provides bounded
migration and rollback. It does not define service packaging, metrics port
allocation, LAN inference CLI behavior, or release packaging.
