# IAMINE Windows Optimizer Assistant Agent Skeleton QA

Feature:

```text
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON
```

## QA Scope

This is a documentation-only P0 skeleton. It introduces no executable package,
system probe, shell adapter, registry reader, process or service inspector,
filesystem access, operating-system mutation, runtime, or persistent audit
implementation. QA verifies architecture and privacy boundaries, not a runtime.

## Required Identity

```text
branch: feature/windows-optimizer-assistant-agent-001-skeleton
base: origin/develop at feature creation
runtime behavior changed: false
field QA required: false
```

## Required Checks

1. `git diff --check` passes.
2. `cargo fmt --all -- --check` passes without source formatting changes.
3. The diff is limited to agent, Architecture, QA, and roadmap documents.
4. The contract reserves `windows_optimizer_readonly_review`,
   `iamine.beta.windows-optimizer-assistant`, `local_planning`, and deferred
   `windows_diagnostic_readonly` without granting implementation or permission.
5. The contract permits only operator-selected, redacted summary metadata and
   separates supplied, missing, and unsupported evidence.
6. The contract denies shell, PowerShell, registry, file, process, service,
   task, startup, driver, update, network, credential, and mutation actions.
7. The audit boundary is redacted, local, review-only, and has no claimed
   emitter, retention system, or evidence export.
8. Prompt injection, role confusion, permission escalation, shell execution,
   registry access, service mutation, and filesystem mutation are negative eval
   cases that require refusal or handoff.
9. The roadmap marks this skeleton `ACTIVE` and records the P0 baseline as
   complete rather than authorizing executable Windows behavior.
10. No sensitive Rust, registry implementation, OS adapter, transport,
    scheduler, P2P, model, worker, controller, inference, or hardware changes.

## Evidence Commands

```bash
git diff --check
cargo fmt --all -- --check
git diff --name-only origin/develop...HEAD
rg -n -F 'windows_optimizer_readonly_review' docs/agents docs/architecture docs/qa
rg -n -F 'windows_diagnostic_readonly' docs/agents docs/architecture docs/qa
rg -n -i 'PowerShell|registry|process|service|mutation|prompt.injection|role confusion' \
  docs/agents/windows-optimizer-assistant-agent-skeleton.md \
  docs/architecture/windows-optimizer-assistant-agent-skeleton.md \
  docs/qa/windows-optimizer-assistant-agent-skeleton.md
rg -n -F 'WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON | ACTIVE' \
  docs/roadmap/iamine-agent-network-roadmap.md
```

## Field QA

Not applicable for this skeleton. No Mac, TS140, or Proxmox execution is
performed because the feature does not alter runtime behavior or touch field-QA
surfaces. Field QA becomes mandatory when a later feature adds executable
Windows, capability, status, worker, scheduler, or runtime behavior.

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
roadmap ACTIVE and baseline-complete scan: PASS
prepared scope: exactly four documentation paths
main.rs: unchanged
cluster_registry.rs: unchanged
```
