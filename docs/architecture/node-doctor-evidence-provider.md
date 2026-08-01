# Node Doctor Evidence Provider Architecture

Feature:

```text
NODE-DOCTOR-EVIDENCE-PROVIDER-001
```

## Purpose

Expose a bounded, typed, redacted, read-only node evidence contract for the
future functional Node Doctor agent. This feature is an internal data provider,
not an agent, CLI command, runtime program, repair path, or authorization gate.

## Ownership

The provider lives in:

```text
iamine-node/src/node_doctor_evidence_provider.rs
```

It consumes the owner-module summary that also feeds `iamine-node lan doctor`.
The future agent must call this provider rather than invoking or parsing the CLI.
Existing check identifiers are centralized in `lan_node_doctor.rs` so the two
surfaces do not duplicate string contracts.

`main.rs` contains wiring only. `cluster_registry.rs` is unchanged.

## Stable Contract

Schema:

```text
1.0.0
```

Entry points:

```text
collect_node_doctor_evidence
build_node_doctor_evidence
```

The collector obtains the existing static owner summary without requesting
network checks. The builder accepts an already-built owner report and emits a
privacy-reduced projection. It never copies owner messages or detail maps.
LAN Doctor uses `ModelStorage::for_read_only_inspection()` for this summary so a
diagnostic read cannot create the default models directory.

Evidence categories:

```text
node_status
hardware_profile
configuration_status
model_readiness
peer_network_status
remote_inference_readiness
```

Evidence states:

```text
ready
attention
blocked
unknown
not_observed
```

Missing required owner evidence becomes `unknown`. A failed owner check takes
precedence over warnings; warnings take precedence over `not_observed`.
Remote-inference readiness cannot become `ready` while passive peer/network
evidence is `not_observed`.

## Privacy Boundary

The provider emits only:

```text
schema version
feature identifier
static source label
read-only and redacted declarations
runtime side-effect declarations
category
status
static reason code
```

It does not emit owner messages, arbitrary JSON details, usernames, home
directories, hostnames, peer IDs, addresses, model IDs, file paths, logs,
prompts, outputs, process lists, credentials, hardware identifiers, or secrets.

## Runtime Boundary

Collection uses the existing static hardware mode. It does not request quick
dynamic profiling and does not start:

```text
workers
P2P
PubSub
model downloads
model loads
inference
network probes
```

Unavailable network state remains explicit instead of causing active discovery.
The provider does not bind ports, write files, mutate configuration, install a
package, persist evidence, or produce a user-facing response.

The owner storage crate keeps its existing mutating constructors for install and
runtime paths. The diagnostic-only constructor stores the target path without
creating it; missing model state is observed as absent.

## Consumer Boundary

The contract remains crate-private until the functional Node Doctor feature
creates its reviewed consumer. The module has a scoped `dead_code` allowance
for this single roadmap interval; the next feature must consume the provider or
remove that allowance as part of its Architecture review.

The provider does not authorize `NODE-DOCTOR-AGENT-001`. That feature still
requires its own manifest, scope, permissions, package review, runtime
authorization, tests, field QA, merge, and post-merge closure.

## Compatibility

The implementation preserves existing LAN Doctor JSON and human output. It does
not change node startup, scheduler, worker behavior, hardware profiling,
models, network policy, remote-inference API, agent runtime, or package loading.

## Validation Requirements

Required focused coverage:

```text
six categories exactly once
missing evidence fails closed
blocked and warning precedence
network-not-observed remote readiness
private owner data redaction
zero runtime and mutation side effects
missing model storage remains absent during read-only inspection
```

Because the collector reads hardware/configuration/model status and exposes
operational evidence, field QA is required on Mac, TS140, and the four
Proxmox/R5500 roles before merge review.
