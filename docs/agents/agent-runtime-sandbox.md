# IAMINE Agent Runtime Sandbox

Feature:

```text
AGENT-RUNTIME-SANDBOX-001
```

## Purpose

Define the sandbox requirements that must exist before IAMINE agent code can
run. This feature is a planning contract only; it does not implement sandbox
runtime behavior.

This document does not authorize executable agents, runtime execution, package
installation, dependency installation, filesystem mutation, network access,
shell execution, registry publication, marketplace publication, public beta,
wallet, reward, settlement, mainnet behavior, or distributed model MoE.

## Sandbox Contract

Sandbox policy answers:

```text
What isolation requirements must be satisfied before agent code can execute?
```

It does not answer whether any sandbox is currently available.

## Draft Schema

```text
iamine.agent.runtime_sandbox.draft-0.1
```

This feature does not implement parsers, sandboxes, process limits, filesystem
mounts, network filtering, container execution, WASM execution, interpreter
startup, or runtime loading.

## Required Sandbox Requirements

Future sandbox eligibility must define:

- default deny for filesystem writes;
- package-relative read boundaries;
- no private path access;
- no credentials or key access;
- no arbitrary shell;
- no unrestricted network;
- bounded CPU, memory, disk, and time limits;
- explicit cleanup paths;
- audit event hooks without raw prompt or private data capture;
- cancellation and timeout compatibility;
- permission review before any non-read-only capability;
- local registry and human review before execution.

## Allowed Modes

```text
metadata_only
local_readonly_review
future_wasm_wasi_sandbox
future_container_sandbox
```

Only `metadata_only` and `local_readonly_review` are valid in this phase, and
neither permits execution.

## Blocked Claims

Sandbox metadata must not claim runtime execution, package installation,
dependency installation, permission enforcement, scheduler priority, node
compatibility, model admission, worker startup, public registry availability,
marketplace publication, trust, reputation, rewards, wallet, settlement,
mainnet behavior, or distributed model MoE.

## Privacy Rules

Sandbox metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- sandbox availability remains false in this feature;
- write, shell, network, process, and private file access are blocked;
- future WASM/WASI and container modes remain deferred;
- sandbox policy cannot bypass scope, permission, audit, boundary eval, local
  registry, dependency, runtime matrix, lifecycle, timeout, or handoff gates;
- unsafe or missing sandbox metadata blocks install and execution by default.

## Next Roadmap Step

```text
AGENT-EXECUTION-LIFECYCLE-001
```
