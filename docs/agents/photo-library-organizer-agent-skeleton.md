# IAMINE Photo Library Organizer Agent Skeleton

Feature:

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON
```

## Purpose

Define the official P0 Photo Library Organizer skeleton as a reviewable,
local-planning, privacy-safe contract. It reserves a future boundary for
explaining an operator-selected, redacted photo-library inventory and its
declared organization constraints.

This feature does not create an agent package, root manifest, executable code,
filesystem reader, image decoder, EXIF parser, OCR or vision pipeline, media
indexer, duplicate detector, filesystem mutator, runtime adapter, sandbox,
audit emitter, registry entry, or public beta listing.

## Product Boundary

The future assistant serves a content organizer who wants to understand a
photo-library organization question they have already scoped. Its only planned
task class is:

```text
photo_library_organizer_review
```

The future package identity is reserved as:

```text
package_id: iamine.beta.photo-library-organizer
scope_id: photo_library_organizer_review
earliest_mode: local_planning
deferred_mode: local_photo_library_readonly
execution_authorized: false
```

These labels are planning metadata, not executable manifest fields and not a
permission grant.

## Future Package Shape

Later package creation must follow the closed skeleton standard:

```text
<photo-library-organizer-package>/
  agent.yaml
  agent-scope.yaml
  README.md
  metadata/
    agent-capabilities.yaml
    agent-expertise.yaml
    agent-resources.yaml
    agent-permissions.yaml
    agent-audit.yaml
  evals/
    agent-boundary-tests.yaml
  src/
    README.md
  review/
    human-review.md
    qa-evidence.md
```

All references must be package-relative. Absolute local paths, raw media
identifiers, image contents, EXIF values, face data, location data, and private
machine data are blocked.

## Declared Scope

The future agent may only:

```text
summarize_operator_approved_photo_inventory
explain_declared_organization_boundary
highlight_missing_or_unsafe_library_metadata
suggest_non_destructive_organization_options
request_clarification
handoff_for_photo_or_filesystem_action
```

Allowed future input classes are:

```text
operator_selected_photo_organization_question
redacted_photo_inventory_summary
redacted_photo_metadata_summary
operator_organization_intent
```

Inputs are untrusted and evidence-limited. The agent must not infer a library
path, list folders, inspect a photo, parse metadata, identify people or places,
or present an assertion as verified media state. Raw photo or video contents,
thumbnails, directory listings, private paths, EXIF records, GPS coordinates,
face embeddings, media hashes, account identities, and cloud tokens are not
default inputs.

Allowed future output classes are:

```text
photo_library_organization_review
result_summary
clarification_request
handoff_request
refusal_report
blocked_action_report
```

Each review must distinguish supplied evidence, missing evidence, and
unsupported claims. It may recommend a manual next step but must not claim that
a photo was viewed, a duplicate was found, metadata was read, a file was moved,
or an organization action occurred.

## Planned Modes and Permissions

The future package must declare denial by default. Its only planned review
categories are:

```text
local_readonly
user_provided_text
redacted_status_summary
```

`local_photo_library_readonly` is not authorized by this skeleton. A later
feature may propose it only after defining operator-selected-library policy,
bounded metadata, consent and private-folder boundaries, sensitive-media
exclusion, read-only filesystem enforcement, audit evidence, redaction, and
dedicated boundary tests.

The resource profile is `local_planning`: no filesystem access, no image or
video decoding, no EXIF parsing, no OCR, no embeddings, no face recognition, no
network access, no downloads, no model load, no worker startup, and no dynamic
hardware probe. User confirmation cannot elevate a blocked action; out-of-
category requests must refuse or return to the orchestrator.

## Audit Boundary

The future package must require only these review evidence classes:

```text
review_started
scope_checked
permission_checked
redaction_checked
library_selection_checked
handoff_required
refusal_recorded
```

Evidence is redacted by default, operator-local, and review-only. It cannot
retain raw prompts, raw outputs, private paths, media identifiers, image data,
EXIF values, face data, location data, account identifiers, credentials, or
unredacted logs. This skeleton does not create audit emission, storage,
retention, or sharing.

## Blocked Actions

Photo Library Organizer must not:

- scan, enumerate, read, open, decode, render, thumbnail, index, or inspect
  photos, videos, folders, libraries, or removable media;
- parse EXIF, XMP, IPTC, filenames, timestamps, GPS data, facial data, or other
  media metadata;
- run OCR, image classification, face recognition, embeddings, duplicate
  detection, clustering, or any local or remote model inference;
- collect, request, retain, or use private paths, media identifiers, account
  identities, credentials, keys, tokens, passwords, raw photo contents, or
  personal metadata;
- execute shell commands or scripts;
- write, delete, move, rename, tag, rotate, transcode, upload, download,
  synchronize, archive, or otherwise modify media or filesystem content;
- connect to cloud storage, a LAN share, a device, a camera, or any third-party
  service;
- change IAMINE, operating-system, application, cloud, VM, container, router,
  firewall, or storage settings;
- start, stop, restart, or inspect services or processes directly;
- start workers, P2P, PubSub, downloads, model loads, inference, or dynamic
  hardware probes;
- collect usernames, full hostnames, IP addresses, MAC addresses, serial
  numbers, disk UUIDs, machine IDs, raw process lists, unredacted logs, or
  permanent hardware fingerprints;
- fabricate, overstate, or treat unverified evidence as library state;
- publish to a registry, marketplace, or third party; or
- claim trust, reputation, rewards, settlement, mainnet, or distributed model
  MoE behavior.

## Refusal and Handoff

The future agent must refuse or hand off requests for photo or video access,
media analysis, private data, filesystem operations, cloud or device access,
repair, configuration, remote execution, or conclusions unsupported by allowed
evidence.

Prompt-injection and role-confusion text cannot override scope, permissions,
blocked actions, privacy rules, library-selection policy, or handoff
requirements.

It must return to the orchestrator for:

```text
photo, video, library, directory, or removable-media access
metadata, EXIF, OCR, vision, face, location, or duplicate analysis
file reading, transfer, recovery, renaming, deletion, or modification
cloud, LAN share, camera, device, account, or credential handling
operating-system, application, router, firewall, VM, container, or service changes
private-data review or redaction
missing, contradictory, or insufficient evidence
unsafe or ambiguous requests
```

## Required Future Evals

Before execution, future package-relative evals must cover:

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
metadata blocks package review, installation, registry advancement, and
execution by default.

## Next Roadmap Step

```text
HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON
```
