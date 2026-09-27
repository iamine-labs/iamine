# Photo Library Organizer Agent Architecture

Feature:

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001
```

## Document Status

```text
document: functional Architecture contract
data class: SOURCE
architecture analysis: COMPLETE
A7 Option A human decision: RECORDED
functional architecture contract: RECORDED
architecture lifecycle event: NOT YET RECORDED
development authorization: NOT AUTHORIZED
implementation: NOT STARTED
feature state: PROPOSED
```

This document records the functional contract only: problem, scope, contracts,
ownership, restrictions, non-regression rules, and the implementation
allow-list. Recording a contract is not a lifecycle decision, and this document
does not itself approve Architecture, authorize Development, or create any
Control Ledger event. No `architecture_approved` event exists for this subject.

The contract is owned by Architecture. Development may not widen it, and no
prompt, agent output, operator confirmation, roadmap text, or role instruction
may expand it.

## Roadmap And Milestone Position

Milestone `v0.12.0` - P0 Official Agents. The closed verticals
`NODE-DOCTOR-AGENT-001`, `REPORTER-AGENT-001`, and
`LAN-FILE-SHARE-ASSISTANT-AGENT-001` precede this row.

Skeleton contract `PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON` is `CLOSED`.
The functional row is `PROPOSED` and `NOT AUTHORIZED`. It is not executable and
not user available.

Preserved, unmodified skeleton documents:

```text
docs/agents/photo-library-organizer-agent-skeleton.md
docs/architecture/photo-library-organizer-agent-skeleton.md
docs/qa/photo-library-organizer-agent-skeleton.md
```

## A7 Option A Decision

The operating-mode decision for this subject is fixed. It is not reopened here.

```text
A7 OPTION A

agent.earliest_mode:
local_readonly

runtime operating modes:
[local_readonly]

shared runtime change:
NOT AUTHORIZED

iamine-agent-runtime change:
NOT AUTHORIZED

iamine-agents change:
NOT AUTHORIZED

official_agent_execution.rs change:
NOT AUTHORIZED

local_planning runtime support:
OUT OF SCOPE

local_photo_library_readonly:
DEFERRED
```

`local_readonly` is a runtime envelope, not filesystem authority. It does not
grant, imply, or anticipate filesystem discovery, library enumeration, file
access, photo decoding, metadata parsing, network reach, or privilege
expansion.

## Skeleton Supersession

```text
The closed skeleton reserved local_planning.

A7 Architecture subsequently selected local_readonly
for the functional feature because the current executable
runtime supports LocalReadonly and the bounded functional
behavior requires no shared-runtime expansion.

The skeleton reservation is historical and superseded for
PHOTO-LIBRARY-ORGANIZER-AGENT-001.

This does not alter the closed skeleton evidence.
```

Handling rule:

```text
skeleton handling: PRESERVE_AND_SUPERSEDE
```

## Product Objective

Help an operator who has already scoped a photo-library organization question
understand what their declared, already-redacted inventory summary does and does
not support, and obtain a non-destructive organization recommendation without
exposing the photo library to the agent.

The agent reasons only over operator-declared, redacted metadata. It never
discovers, enumerates, reads, decodes, or modifies any photo, video, folder,
library, or removable media.

## Operator Workflow

```text
1. The operator selects and redacts the inventory summary outside the agent.
2. The operator declares a bounded set of inventory records and, optionally, an
   organization question and declared organization intent.
3. The operator runs:
   iamine-node agents photo-library-organizer --package-root PATH [--item ...] [--json]
4. The agent validates the declared input, compares it against the declared
   organization boundary, and returns a bounded review, clarification request,
   handoff request, refusal, or blocked-action report.
5. Nothing is read from, written to, or discovered in the photo library.
```

## Functional Contract

```text
task: photo_library_organizer_review
operation: review_declared_photo_inventory
package_id: iamine.beta.photo-library-organizer
task_type: photo_library_organizer_review
scope_id: photo_library_organizer_review
input schema: iamine.agent.photo-library-organizer.input-0.1
output schema: iamine.agent.photo-library-organizer.output-0.1
earliest_mode: local_readonly
filesystem authority: NONE
network authority: NONE
mutation authority: NONE
model/inference: NONE
photo data authority: operator-declared redacted inventory/summary metadata only
```

The agent reuses the existing `local_readonly` official-agent execution chain
(`execute_official_local_readonly_agent`) without modifying the shared runtime,
the scope, permission, execution-authorization, sandbox, or audit machinery.

## Input Contract

The input is a single bounded JSON object carrying a schema version and a small
number of operator-declared inventory records. Accepted declared fields are
limited to:

- a redacted inventory item label;
- a coarse declared category;
- a declared status;
- a declared evidence claim;
- an optional declared organization question and declared organization intent.

Hard limits apply to the record count and to each field length. Unknown fields
are rejected. Path-shaped tokens are rejected. Coarse declared timestamps, if
present, must be bucketed and never precise.

The following are not accepted inputs and must be rejected or handed off:

- image bytes, thumbnails, or media hashes;
- EXIF, XMP, or IPTC records;
- GPS or geolocation values;
- face, person, or biometric data;
- camera, device, account, or credential identifiers;
- filesystem paths, directory listings, or filenames;
- cloud, LAN, device, or removable-media references.

## Output Contract

Allowed output classes:

```text
photo_library_organization_review
result_summary
clarification_request
handoff_request
refusal_report
blocked_action_report
```

Every review must distinguish supplied evidence, missing evidence, and
unsupported claims. The agent may recommend a manual, non-destructive next
step, but it must never claim that a photo was viewed, a duplicate was found,
metadata was read, a file was moved, renamed, deleted, tagged, or transcoded, or
that any organization action occurred.

## Classification Vocabulary

```text
PhotoLibraryReview
BlockedActionReport
HandoffRequest
```

## Next-Step Vocabulary

```text
NoActionRequired
ReviewAttentionInventoryMetadata
ReviewBlockedInventoryMetadata
ProvideRedactedInventoryMetadata
HandoffForPhotoOrFilesystemAction
```

## Scope

The agent may only summarize an operator-declared, redacted inventory, explain
the declared organization boundary, highlight missing or unsafe declared
metadata, suggest non-destructive organization options, request clarification,
and hand off photo or filesystem actions it is not authorized to perform.

## Permissions

Deny-by-default. The only permitted review categories are:

```text
local_readonly
user_provided_text
redacted_status_summary
```

Potential user confirmation cannot elevate a blocked action. Out-of-category
requests must be refused or returned to the orchestrator.

## Security Invariants

```text
local-first
deny-by-default
no cloud upload
no network discovery or network access
no public metadata emission
no biometric or face processing
no hidden-library traversal
no automatic deletion, move, rename, tag, or transcode
no filesystem discovery, enumeration, read, or traversal
no symlink following
no unrestricted recursive scan
no secret, credential, or key access
no home-directory discovery beyond explicit operator authority
no persistence outside approved evidence/output contracts
no privileged operation, shell, or service control
no performed-action claims
```

## Privacy And Data Boundaries

The design is deny-by-default and local-first. Future input and output handling
must preserve the project privacy policy: no personal paths, usernames, host or
network identifiers, image contents, GPS or EXIF values, face or location data,
credentials, keys, tokens, raw prompts, raw outputs, or unredacted evidence in
package metadata, audit records, docs, or committed artifacts.

Supplied metadata is treated as untrusted. Prompt injection, role confusion,
fabricated status, and instructions to bypass permissions, scope, or handoff
must be refused and recorded as boundary outcomes.

## Blocked Functionality

The agent must not:

- perform filesystem discovery, enumeration, reads, traversal, or symlink
  following;
- read image bytes or generate thumbnails;
- parse EXIF, XMP, IPTC, filenames, timestamps, GPS, or other media metadata;
- perform OCR, vision, embeddings, image classification, face or person
  recognition, or any local or remote model inference;
- perform content-based duplicate detection;
- perform automatic move, rename, delete, tag, rotate, or transcode;
- access cloud storage, a LAN share, a device, a camera, or any third-party
  service;
- collect, request, retain, or use credentials, keys, tokens, passwords,
  private paths, media identifiers, or account identities;
- execute shell commands or scripts;
- start workers, P2P, PubSub, downloads, model loads, inference, or dynamic
  hardware probes;
- change scheduler, model store, worker lifecycle, or shared runtime behavior;
- fabricate, overstate, or treat unverified evidence as library state, or make
  any performed-action claim;
- publish to a registry, marketplace, or third party.

## Runtime Dependency Reuse

```text
official-agent package/manifest:     reuse (new package data only)
execution authorization:             reuse without change
scope model:                         reuse without change
permission model:                    reuse without change
resource mode (local_readonly):      reuse without change
runtime executor:                    reuse without change
result/handoff schema:               reuse without change
audit/evidence enforcement:          reuse without change
CLI dispatch:                        bounded new mode entry
node wiring:                         bounded new agent module
```

No `iamine-agent-runtime`, `iamine-agents`, `iamine-core`, `iamine-models`,
`iamine-network`, `iamine-hardware`, dashboard, scheduler, P2P/PubSub, Local
Control API, worker-lifecycle, model-store, or inference-backend change is
authorized.

## No-Model Decision

```text
VISION_MODEL_REQUIRED: NO
TEXT_MODEL_REQUIRED: NO
INFERENCE_BACKEND_REQUIRED: NO
```

The bounded workflow is deterministic review of operator-declared redacted
metadata. It requires no model, no backend, no model store, no download, and no
worker.

## Implementation Surface (Future)

```text
agents/official/photo-library-organizer/**
iamine-node/src/photo_library_organizer_agent/**
iamine-node/src/cli.rs
iamine-node/src/mode_dispatch.rs
iamine-node/src/node_modes.rs      (FUTURE_IMPLEMENTATION / REQUIRED_MODIFY: add the NodeMode::AgentPhotoLibraryOrganizer variant and its mode_label arm required by the already-approved bounded CLI entry)
iamine-node/src/usage.rs
iamine-node/src/main.rs   (module declaration and wiring only)
```

This list is a prospective implementation allow-list for a future authorized
feature. It is not authorized by this document.

`iamine-node/src/node_modes.rs` is recorded here as an allow-list completion
only. `NodeMode` is defined in that file, so the already-approved bounded CLI
mode entry cannot be implemented without one `NodeMode` variant and one
`mode_label` arm there. Recording this path:

- corrects an omitted implementation path only;
- does not change the A7 Option A decision;
- does not widen functional scope;
- grants no new authority;
- grants no filesystem authority;
- grants no network authority;
- grants no mutation authority;
- grants no model/inference authority;
- introduces no shared-runtime change;
- does not authorize implementation;
- does not authorize Field QA;
- does not authorize merge of implementation.

No code file has been modified by this contract. Option A remains
`DECLARED_METADATA_ONLY` with `local_readonly`, task
`photo_library_organizer_review`, operation
`review_declared_photo_inventory`, and filesystem, network, mutation, and
model/inference authority `NONE`.

## Forbidden Scope

```text
iamine-agent-runtime/**
iamine-agents/**
iamine-node/src/official_agent_execution.rs
iamine-models/**  iamine-network/**  iamine-core/**  iamine-hardware/**
scheduler  P2P/PubSub  dashboard  Local Control API
worker lifecycle  model store  inference backend
```

## Testing Contract

Future package-relative evals must cover:

```text
in_scope_positive
out_of_scope_negative
ambiguous_task
dangerous_task
cross_domain_task
permission_escalation
prompt_injection
role_confusion
handoff_to_orchestrator
privacy_redaction
private_media_request
metadata_extraction_request
filesystem_mutation_request
cloud_transfer_request
local_only
```

Missing, broad, contradictory, unsafe, stale, unverifiable, or privacy-invasive
metadata must block package review, installation, registry advancement, and
execution by default.

## Field QA Requirements

Field QA becomes mandatory when a later feature adds executable behavior. The
current Architecture minimum is macOS local and Linux/TS140. Controller/worker
roles are not mandatory under Option A unless a later implementation actually
changes scope. Synthetic or disposable fixtures only; no real user photo
library.

## Architecture Authority Boundary

Recording this contract grants no lifecycle authority. Architecture approval,
Development authorization, implementation, local validation, architecture
checkpoint, Field QA, final review, human merge authorization, merge,
post-merge validation, and closure remain separate, individually authorized
steps recorded in the Control Ledger.

```text
architecture analysis: COMPLETE
functional architecture contract: RECORDED
architecture lifecycle event: NOT YET RECORDED
development authorization: NOT AUTHORIZED
implementation: NOT STARTED
```

## Out-Of-Scope Future Capabilities

The following remain deferred and require their own dedicated, separately
authorized Architecture feature:

- `local_photo_library_readonly` operator-selected read-only library access;
- EXIF/XMP/IPTC metadata extraction;
- GPS, face, person, or biometric analysis;
- vision, OCR, or embedding-based classification;
- content-based duplicate detection;
- any move, rename, delete, tag, or transcode operation;
- cloud, LAN, device, or removable-media integration.

## Development Authorization State

```text
DEVELOPMENT AUTHORIZATION: NOT AUTHORIZED
```

No branch, commit, package, module, test, or executable behavior is authorized
by this document.
