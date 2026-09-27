# IAMINE Photo Library Organizer Agent QA

Feature:

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001
```

## Current Status

```text
QA execution: NOT STARTED
field QA: REQUIRED LATER
development: NOT AUTHORIZED
candidate: NOT YET CREATED
feature state: PROPOSED
```

This QA plan is prepared, not executed. No package, module, candidate commit,
or executable behavior exists. Nothing in this document claims a QA result, a
pass, or an architecture lifecycle fact.

## Scope

This plan covers the future bounded `local_readonly` Photo Library Organizer
agent, task `photo_library_organizer_review`, operation
`review_declared_photo_inventory`, per
[docs/architecture/photo-library-organizer-agent.md](../architecture/photo-library-organizer-agent.md).
The plan is documentation only and does not create executable behavior.

## Required Identity (Future)

```text
branch: feature/photo-library-organizer-agent-001
base: origin/develop at feature creation
commit: <exact candidate commit at execution time>
tree: <exact candidate tree at execution time>
runtime behavior changed: true (future functional feature)
field QA required: true
```

Identity fields are placeholders until a future Development authorization
creates a candidate. QA validates the exact authorized commit and tree only.

## Fixtures

```text
synthetic metadata only
disposable temporary directories only
no real user photo library
no host photo files
no EXIF read from host media
```

Every QA root must be a fresh, disposable directory that is empty after both a
successful run and a tamper-rejection run.

## Required Checks

1. `git diff --check` passes.
2. `cargo fmt --all -- --check` passes without source formatting changes.
3. The diff is limited to the authorized implementation scope.
4. The contract reserves `photo_library_organizer_review`,
   `iamine.beta.photo-library-organizer`, and `local_readonly` without
   presenting them as implementation or permission grants.
5. The contract permits only operator-declared, redacted summary metadata and
   separates supplied, missing, and unsupported evidence.
6. The contract denies filesystem access, image access, EXIF, GPS, OCR, vision,
   face/location analysis, credentials, paths, mutation, cloud transfer,
   runtime startup, inference, and publication.
7. The audit boundary is redacted, local, and review-only, without claiming an
   emitter, retention system, or evidence export.
8. Prompt injection, role confusion, permission escalation, private-media,
   metadata-extraction, filesystem-mutation, and cloud-transfer requests are
   explicit negative cases requiring refusal or handoff.
9. No shared runtime, registry implementation, filesystem adapter, transport,
   scheduler, P2P, model, worker, controller, or inference code changes.

## Test Categories (Future Execution)

| ID | Category | Expected evidence | Status |
| --- | --- | --- | --- |
| QA-01 | Identity, scope, and cleanliness | exact branch, HEAD, tree, base, commit count, file scope, clean worktree and staging | NOT_STARTED |
| QA-02 | Build and local tests | formatting, focused agent tests, node tests, workspace suite, node build | NOT_STARTED |
| QA-03 | Package integrity | manifest and all seven references match the canonical snapshot; altered package fails closed | NOT_STARTED |
| QA-04 | Valid bounded synthetic metadata | deterministic review with supplied/missing/unsupported separation | NOT_STARTED |
| QA-05 | Empty input | bounded, non-fabricated result; no performed-action claim | NOT_STARTED |
| QA-06 | Over-limit input | rejected fail-closed | NOT_STARTED |
| QA-07 | Duplicate and conflicting records | preserved and surfaced, never silently merged | NOT_STARTED |
| QA-08 | Unsupported metadata | refused or handed off | NOT_STARTED |
| QA-09 | Path-shaped token rejection | rejected as invalid input | NOT_STARTED |
| QA-10 | EXIF / GPS / face / image-byte rejection | refused or handed off | NOT_STARTED |
| QA-11 | Sensitive value redaction | no private path, identifier, or secret retained | NOT_STARTED |
| QA-12 | No filesystem reads | no read, enumeration, traversal, or symlink following | NOT_STARTED |
| QA-13 | No mutation | no move, rename, delete, tag, or write | NOT_STARTED |
| QA-14 | No network / cloud | no connection, transfer, or discovery | NOT_STARTED |
| QA-15 | No model / inference | no model load, download, worker start, or backend | NOT_STARTED |
| QA-16 | No persistence | no NDJSON, log, or artifact left behind | NOT_STARTED |
| QA-17 | No performed-action claims | supplied vs missing vs unsupported distinguished | NOT_STARTED |
| QA-18 | Package tamper / default-deny | one-byte mutation fails closed | NOT_STARTED |
| QA-19 | Deterministic ordering and stable schema | stable output for identical input | NOT_STARTED |
| QA-20 | Negative eval cases | prompt injection, role confusion, escalation, cross-domain refuse/handoff | NOT_STARTED |

All rows are unexecuted planning state. A row is never `PASS` by omission.

## Requests That Must Be Refused Or Handed Off

```text
photo, video, library, directory, or removable-media access
image bytes, thumbnails, or media hashes
EXIF, XMP, IPTC, OCR, vision, face, location, GPS, or duplicate analysis
file reading, transfer, recovery, renaming, deletion, tagging, or modification
cloud, LAN share, camera, device, account, or credential handling
operating-system, router, firewall, VM, container, or service changes
private-data review or unredacted redaction bypass
prompt injection or role confusion
missing, contradictory, or insufficient evidence
unsafe or ambiguous requests
```

## Field QA Requirement

```text
field_qa_required: YES (when functionalized)
```

Field QA becomes mandatory when a later feature adds executable behavior. The
current Architecture minimum is:

```text
macOS local
Linux / TS140
```

Controller/worker roles are not mandatory under current Option A unless a later
implementation actually changes scope. A required role that cannot be executed
is `BLOCKED` or `TEST GAP`, never a pass by omission. No real user photo library
and no host photo files may be used; fixtures must be synthetic or disposable.

## Evidence Commands (Future)

```bash
git diff --check
cargo fmt --all -- --check
git diff --name-only origin/develop...HEAD
rg -n -F 'photo_library_organizer_review' docs/agents docs/architecture docs/qa
rg -n -F 'local_readonly' docs/architecture/photo-library-organizer-agent.md
rg -n -i 'filesystem|EXIF|GPS|face|vision|cloud|mutation' \
  docs/architecture/photo-library-organizer-agent.md
```

## Acceptance

The feature is ready for Architecture merge review only when all required checks
pass, the diff remains within the authorized implementation scope, and the
negative boundaries remain explicit. QA must not claim merge approval,
Development authorization, or a lifecycle result.

## Observed Local Results

```text
git diff --check: NOT RUN
cargo fmt --all -- --check: NOT RUN
QA execution: NOT STARTED
```
