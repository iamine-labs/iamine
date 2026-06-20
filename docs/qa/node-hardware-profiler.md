# IAMINE - Node Hardware Profiler

Feature:
NODE-HARDWARE-PROFILER-001

Purpose:
Provide a deterministic, versioned hardware profile for a node without loading models, changing scheduler behavior, changing PubSub, or collecting sensitive identifiers.

## Scope

The profiler lives in the `iamine-hardware` crate.

`iamine-node` only exposes CLI commands and renders the profile:

- `iamine-node hardware inspect`
- `iamine-node hardware inspect --json`
- `iamine-node hardware inspect --dynamic`
- `iamine-node hardware show`
- `iamine-node hardware refresh --yes`

The default persisted profile path is:

- `~/.iamine/hardware/profile.json`

For test and QA isolation, override it with:

- `IAMINE_HARDWARE_PROFILE_PATH=/tmp/iamine-hardware/profile.json`

## Profile Contract

Current schema version:

- `1.0.0`

The profile includes:

- CPU architecture, cores, recommended threads, and feature flags
- Memory total and availability when safely detectable
- Accelerator kind and memory when safely detectable
- Storage free-space summary
- Privacy-preserving network probe status
- Effective worker slots and effective accelerator
- Optional quick dynamic profile

The profile must not include:

- hostname
- username
- home directory
- MAC address
- IP address
- serial number
- hardware UUID
- wallet/key material

## Local Smoke

Run:

```bash
./target/debug/iamine-node hardware inspect
./target/debug/iamine-node hardware inspect --json
./target/debug/iamine-node hardware inspect --dynamic
```

Persist into an isolated QA path:

```bash
IAMINE_HARDWARE_PROFILE_PATH=/tmp/iamine-hardware/profile.json \
./target/debug/iamine-node hardware refresh --yes --json

IAMINE_HARDWARE_PROFILE_PATH=/tmp/iamine-hardware/profile.json \
./target/debug/iamine-node hardware show --json
```

Expected:

- schema_version is `1.0.0`
- JSON is parseable
- CPU fields are present
- memory total is present on supported platforms
- accelerator is explicit (`metal`, `cuda`, `rocm`, or `cpu`)
- dynamic quick profile completes quickly
- no runtime network starts
- no model load starts

## Validation

Required local validation:

```bash
cargo fmt --all -- --check
cargo test -p iamine-hardware
cargo test -p iamine-node
cargo test --workspace
cargo build -p iamine-node
cargo clippy --workspace --all-targets
git diff --check
git diff --cached --check
```

Field QA is required before merge because hardware detection varies by platform.

Field QA must confirm:

- Mac local profile PASS
- TS140 profile PASS
- Proxmox/R5500 mock/skip nodes profile PASS
- JSON parseable on every environment
- no sensitive identifiers in JSON
- no real LLM load under mock/skip
- no SIGILL

Do not mark this feature merged until field QA passes.
