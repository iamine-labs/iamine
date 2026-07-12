# USER-DIAGNOSTICS-SUPPORT-001 QA

## Scope

Validate the privacy-safe support bundle command:

```text
iamine-node support bundle [--output PATH] [--json]
```

The feature is local diagnostic support only. It must not start workers, P2P,
PubSub, model downloads, model loading, inference, dynamic hardware probes,
installers, updaters, or rollback flows.

## Required Local Validation

```text
cargo fmt --all -- --check
cargo test -p iamine-node user_diagnostics_support
cargo test -p iamine-node cli_detects_support_bundle_json_output
cargo test -p iamine-node cli_valid_commands_do_not_show_unknown_mode
cargo test -p iamine-node cli_preserves_existing_help_text
cargo build -p iamine-node
git diff --check
git diff --cached --check
```

After build:

```text
./target/debug/iamine-node support bundle --json
./target/debug/iamine-node support bundle --output /tmp/iamine-support/support.json --json
```

Validate:

- JSON parses;
- `schema_version` is `1.0.0`;
- `feature` is `USER-DIAGNOSTICS-SUPPORT-001`;
- privacy flags for usernames, home directories, full hostnames, MAC addresses,
  IP addresses, serial numbers, disk UUIDs, machine IDs, user process lists,
  personal paths, raw logs, and secrets are all false;
- output metadata contains only the output file label;
- bundle file permissions are private on Unix;
- action items include next commands for non-pass diagnostics;
- runtime side-effect flags are all false.

## Field QA Decision

Field QA is not required for this implementation unless Architecture expands the
scope to runtime, P2P, worker behavior, scheduler behavior, inference behavior,
hardware profiling, installer behavior, update behavior, or remote support
upload behavior.

If field QA becomes required, execute the canonical matrix:

- Mac development machine;
- TS140;
- Proxmox/R5500 guests.

## QA Recommendation

QA may recommend:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA must not emit:

```text
MERGE APPROVED
MERGE AUTHORIZED
```
