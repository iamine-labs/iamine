# IAMINE Photo Library Organizer Agent Skeleton QA

Feature:

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON
```

## QA Scope

This is a documentation-only P0 skeleton. It introduces no executable package,
filesystem reader, media decoder, metadata parser, model activity, filesystem
mutation, runtime, or persistent audit implementation. QA verifies the declared
architecture and privacy boundaries rather than a field runtime.

## Required Identity

```text
branch: feature/photo-library-organizer-agent-001-skeleton
base: origin/develop at feature creation
runtime behavior changed: false
field QA required: false
```

## Required Checks

1. `git diff --check` passes.
2. `cargo fmt --all -- --check` passes without source formatting changes.
3. The diff is limited to this feature's agent, Architecture, QA, and roadmap
   documents.
4. The contract reserves `photo_library_organizer_review`,
   `iamine.beta.photo-library-organizer`, `local_planning`, and the deferred
   `local_photo_library_readonly` mode without presenting them as implementation
   or permission grants.
5. The contract permits only operator-selected, redacted summary metadata and
   separates supplied, missing, and unsupported evidence.
6. The contract denies library scanning, image access, EXIF, OCR, vision,
   face/location analysis, credentials, paths, filesystem mutation, cloud
   transfer, runtime startup, inference, and publication.
7. The audit boundary is redacted, local, review-only, and does not claim an
   emitter, retention system, or evidence export.
8. Prompt injection, role confusion, permission escalation, private-media,
   metadata-extraction, filesystem-mutation, and cloud-transfer requests are
   explicit negative eval cases and require refusal or handoff.
9. The roadmap marks this skeleton `ACTIVE` and preserves the next canonical
   feature, `HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON`.
10. No sensitive Rust surfaces, registry implementation, filesystem adapter,
    transport, scheduler, P2P, model, worker, controller, inference, or
    hardware code changes.

## Evidence Commands

```bash
git diff --check
cargo fmt --all -- --check
git diff --name-only origin/develop...HEAD
rg -n -F 'photo_library_organizer_review' docs/agents docs/architecture docs/qa
rg -n -F 'local_photo_library_readonly' docs/agents docs/architecture docs/qa
rg -n -i 'scan|EXIF|OCR|face|location|filesystem|cloud|prompt.injection|role confusion' \
  docs/agents/photo-library-organizer-agent-skeleton.md \
  docs/architecture/photo-library-organizer-agent-skeleton.md \
  docs/qa/photo-library-organizer-agent-skeleton.md
rg -n -F 'PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON | ACTIVE' \
  docs/roadmap/iamine-agent-network-roadmap.md
```

## Field QA

Not applicable for this skeleton. No Mac, TS140, or Proxmox execution is
performed because the feature does not alter runtime behavior or touch field-QA
surfaces. Field QA becomes mandatory when a later feature adds executable
filesystem, media, capability, status, worker, scheduler, or runtime behavior.

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
