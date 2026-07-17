# IAMINE Home Network Assistant Agent Skeleton QA

Feature:

```text
HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON
```

## QA Scope

This is a documentation-only P0 skeleton. It introduces no executable package,
network probe, scanner, socket, listener, packet capture, router client,
configuration mutation, runtime, or persistent audit implementation. QA
verifies the declared architecture and privacy boundaries rather than a field
runtime.

## Required Identity

```text
branch: feature/home-network-assistant-agent-001-skeleton
base: origin/develop at feature creation
runtime behavior changed: false
field QA required: false
```

## Required Checks

1. `git diff --check` passes.
2. `cargo fmt --all -- --check` passes without source formatting changes.
3. The diff is limited to this feature's agent, Architecture, QA, and roadmap
   documents.
4. The contract reserves `home_network_readonly_review`,
   `iamine.beta.home-network-assistant`, `local_planning`, and the deferred
   `lan_readonly` mode without presenting them as implementation or permission
   grants.
5. The contract permits only operator-selected, redacted summary metadata and
   separates supplied, missing, and unsupported evidence.
6. The contract denies network discovery, sockets, listeners, packet capture,
   router access, credentials, topology, configuration mutation, runtime
   startup, inference, and publication.
7. The audit boundary is redacted, local, review-only, and does not claim an
   emitter, retention system, or evidence export.
8. Prompt injection, role confusion, permission escalation, network discovery,
   packet capture, router mutation, and credential requests are explicit
   negative eval cases and require refusal or handoff.
9. The roadmap marks this skeleton `ACTIVE` and preserves the next canonical
   feature, `WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON`.
10. No sensitive Rust surfaces, registry implementation, network adapter,
    transport, scheduler, P2P, model, worker, controller, inference, or
    hardware code changes.

## Evidence Commands

```bash
git diff --check
cargo fmt --all -- --check
git diff --name-only origin/develop...HEAD
rg -n -F 'home_network_readonly_review' docs/agents docs/architecture docs/qa
rg -n -F 'lan_readonly' docs/agents docs/architecture docs/qa
rg -n -i 'scan|socket|listener|packet|router|credential|prompt.injection|role confusion' \
  docs/agents/home-network-assistant-agent-skeleton.md \
  docs/architecture/home-network-assistant-agent-skeleton.md \
  docs/qa/home-network-assistant-agent-skeleton.md
rg -n -F 'HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON | ACTIVE' \
  docs/roadmap/iamine-agent-network-roadmap.md
```

## Field QA

Not applicable for this skeleton. No Mac, TS140, or Proxmox execution is
performed because the feature does not alter runtime behavior or touch field-QA
surfaces. Field QA becomes mandatory when a later feature adds executable
network, capability, status, worker, scheduler, or runtime behavior.

## Acceptance

The feature is ready for Architecture merge review only when all required checks
pass, the diff remains documentation-only, and the negative boundaries remain
explicit. QA must not claim merge approval or authorization.

## Observed Local Results

```text
git diff --check: PASS
cargo fmt --all -- --check: PASS
feature presence scan: PASS
task identity and planned-mode scan: PASS
input and output boundary scan: PASS
blocked-action scan: PASS
prompt-injection and role-confusion scan: PASS
roadmap ACTIVE and next-feature scan: PASS
prepared scope: exactly four documentation paths
main.rs: unchanged
cluster_registry.rs: unchanged
```
