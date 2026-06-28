# LAN Inference Beta Packaging

Feature:

```text
LAN-INFERENCE-BETA-PACKAGING-001
```

## Purpose

Provide explicit LAN beta packaging artifacts for clean install, upgrade,
service-manager integration, and rollback without starting workers, scanning
processes, loading models, downloading models, or running inference.

This feature packages the existing beta surfaces:

- `iamine-node lan doctor`
- `iamine-node node config ...`
- `iamine-node worker lifecycle ...`
- `iamine-node lan infer ...`

## Scope

In scope:

- a portable LAN beta package assembly script;
- static service-manager templates for Linux systemd and macOS launchd;
- a safe worker environment example;
- operator documentation for clean install, upgrade, rollback, and validation;
- lifecycle reporting that points to available packaging artifacts.

Out of scope:

- auto-update;
- signed releases;
- background supervisors controlled by IAMINE;
- process discovery or process killing;
- package-manager publishing;
- remote installation;
- downloading models or accepting licenses;
- changing scheduler, worker runtime, PubSub, model policy, inference, result
  acceptance, or network payload behavior.

## Artifact Contract

The package script creates an output directory and archive containing:

```text
bin/iamine-node
docs/README.md
env/iamine-worker.env.example
systemd/iamine-worker@.service
launchd/com.iamine.worker.plist.template
manifest.json
```

The generated manifest includes only package metadata and relative artifact
paths. It must not record usernames, home directories, hostnames, IP addresses,
MAC addresses, serial numbers, machine IDs, tokens, keys, or local absolute
paths.

The script fails if the target package directory or archive already exists. It
does not remove or overwrite previous artifacts.

## Runtime Boundary

Packaging artifacts must not:

- start or stop workers;
- start P2P or PubSub;
- probe LAN peers;
- run dynamic hardware probes;
- download or load models;
- run inference;
- mutate node configuration;
- install launchd or systemd units.

Operators install service templates explicitly through their OS tooling. The
templates are examples, not privileged installers.

## Upgrade and Rollback

Upgrade is a staged binary replacement:

1. create a new package;
2. run `iamine-node --help`;
3. run `iamine-node lan doctor --json`;
4. stop the operator-managed worker;
5. replace the binary with the new package binary;
6. restart the operator-managed worker;
7. run readiness and smoke checks.

Rollback is the inverse: keep the previous binary and service configuration,
stop the operator-managed worker, restore the previous binary, restart, and run
the same readiness checks. IAMINE packaging must preserve this manual rollback
path.

## QA Notes

Local validation must prove:

- package assembly succeeds into an isolated output path;
- manifest JSON is parseable;
- manifest paths are relative and privacy-safe;
- service templates contain placeholders or generic system paths only;
- lifecycle JSON reports packaging artifacts without runtime side effects;
- `main.rs` and `cluster_registry.rs` do not grow except for explicit wiring.

Field QA is required before merge review because the feature changes operator
packaging and lifecycle reporting. Proxmox/R5500 QA remains approval-gated.
