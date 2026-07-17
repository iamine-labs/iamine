# Photo Library Organizer Agent Skeleton Architecture

Feature:

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON
```

## Status

```text
ARCHITECTURE IN PROGRESS
```

## Purpose

Reserve the P0 architecture contract for a future Photo Library Organizer. The
present feature is documentation only: it defines a bounded, privacy-safe
planning surface for operator-provided redacted library summaries. It creates no
package, manifest, runtime, filesystem access, media processor, metadata
parser, vision model, mutator, audit store, or registry entry.

## Ownership and Integration

This skeleton owns only these feature documents:

```text
docs/agents/photo-library-organizer-agent-skeleton.md
docs/architecture/photo-library-organizer-agent-skeleton.md
docs/qa/photo-library-organizer-agent-skeleton.md
```

It integrates with the agent-network roadmap by reserving the future task class
`photo_library_organizer_review`, package identity
`iamine.beta.photo-library-organizer`, and `local_planning` resource profile.
The shared agent-package contract, orchestrator, package registry, permission
runtime, audit persistence, and filesystem enforcement remain unchanged.

The feature does not change Rust crates, `iamine-node/src/main.rs`,
`iamine-node/src/cluster_registry.rs`, scheduler policy, P2P, PubSub, model
selection, task formats, startup, inference, model storage, reputation, or
rewards.

## Boundary

The future assistant may reason only over operator-selected, redacted metadata
supplied through an approved package interface. Its initial allowable mode is
`local_planning`, not `local_photo_library_readonly`.

`local_photo_library_readonly` remains deferred until a dedicated implementation
defines:

- operator-selected-library, explicit intent, consent, and private-folder policy;
- a bounded metadata surface that excludes image contents and sensitive media;
- no EXIF, face, location, account, or raw-path collection by default;
- read-only filesystem enforcement, default-deny permissions, and audit evidence;
- privacy, prompt-injection, role-confusion, and negative boundary tests; and
- Architecture and QA evidence for the executable surface.

No user prompt, agent output, operator confirmation, or role instruction may
expand those constraints.

## Future Package Interfaces

The future package will consume only redacted inventory and organization-summary
metadata selected by the operator. It may emit review, clarification, refusal,
and handoff records. It must not consume photos, videos, thumbnails, paths,
directory listings, EXIF, GPS, face data, media hashes, account identifiers,
credentials, or unredacted logs.

It must delegate all filesystem access, media analysis, duplicate detection,
renaming, deletion, transfer, recovery, configuration, and execution to an
approved future orchestrator flow. This skeleton neither defines nor authorizes
that flow.

## Security and Privacy

The design is deny-by-default. Future input and output handling must preserve
the project privacy policy: no personal paths, usernames, host identifiers,
network identifiers, image contents, metadata that identifies a person or
location, credentials, keys, tokens, raw prompts, raw outputs, or unredacted
evidence in package metadata, audit records, docs, or committed artifacts.

The future assistant must treat supplied metadata as untrusted. Prompt
injection, role confusion, fabricated status, and instructions to bypass
permissions or handoff are refused and recorded as boundary outcomes.

## QA and Release Boundary

Because this feature creates only documents and leaves executable behavior
unchanged, it requires local documentation validation and Architecture review;
it does not require Mac, TS140, or Proxmox field QA. A future executable,
filesystem, capability-reporting, or runtime feature will require the applicable
field matrix before merge review.

## Deferred Implementation

The concrete filesystem adapter, media decoder, metadata reader, sensitive-media
policy, permission dialog, redaction engine, audit emitter, package manifest,
registry controls, and eval harness are intentionally deferred. They must be
owned by the feature that introduces executable Photo Library behavior, not
retrofitted into this skeleton.

## Next Roadmap Step

```text
HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON
```
