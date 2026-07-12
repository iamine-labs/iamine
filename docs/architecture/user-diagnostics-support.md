# USER-DIAGNOSTICS-SUPPORT-001

## Goal

Provide a privacy-safe support bundle command for users and operators before
public beta infrastructure expands.

The feature is diagnostic only. It must not start workers, P2P, PubSub, model
downloads, model loading, inference, dynamic hardware probes, installers,
updaters, or rollback flows.

## Command

```text
iamine-node support bundle [--output PATH] [--json]
```

Behavior:

- without `--output`, print a support report to stdout;
- with `--output`, write a JSON bundle to the explicit path;
- redact the output path to a file-name label in command output and bundle
  metadata;
- on Unix, write bundle files with `0600` permissions;
- include actionable next commands for warnings, failures, and skipped network
  diagnostics.

## Ownership

Implementation lives in `iamine-node/src/user_diagnostics_support.rs`.

CLI wiring is limited to:

- `iamine-node/src/cli.rs`;
- `iamine-node/src/node_modes.rs`;
- `iamine-node/src/mode_dispatch.rs`;
- `iamine-node/src/usage.rs`;
- one module registration line in `iamine-node/src/main.rs`.

Existing `lan_node_doctor` remains the source of local readiness evidence. This
feature consumes its check summaries through narrow crate-local getters instead
of duplicating diagnostic logic.

## Privacy Boundary

The support bundle must not collect, persist, or print:

- usernames;
- home directories;
- full hostnames;
- MAC addresses;
- IP addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- user process lists;
- personal paths;
- raw logs;
- secrets or credentials.

Path handling is label-only. The bundle may indicate that an explicit output
file was written, but it must not include the parent directory.

## Runtime Boundary

The command is a control-plane diagnostic mode. It must be handled before network
startup and must report:

```text
workers_started=false
p2p_started=false
pubsub_started=false
model_download_started=false
model_load_started=false
inference_started=false
dynamic_hardware_probe_started=false
```

Network readiness remains optional and is not probed by the support bundle
because starting P2P or PubSub would violate the diagnostic-only boundary.

## Non-Goals

This feature does not:

- upload support bundles;
- include raw logs;
- include host identifiers or operator paths;
- inspect remote peers;
- start runtime services;
- mutate node config, identity, model store, scheduler state, or cluster state.
