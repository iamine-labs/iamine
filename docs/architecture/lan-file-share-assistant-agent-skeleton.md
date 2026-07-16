# LAN File Share Assistant Agent Skeleton Architecture

Feature:

```text
LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON
```

## Status

```text
ARCHITECTURE IN PROGRESS
```

## Purpose

Reserve the P0 architecture contract for a future LAN File Share Assistant. The
present feature is documentation only: it defines a bounded, privacy-safe
planning surface for operator-provided redacted metadata. It creates no package,
manifest, runtime, client, transport, mount, discovery mechanism, credential
store, file access path, or registry entry.

## Ownership and Integration

This skeleton owns only these feature documents:

```text
docs/agents/lan-file-share-assistant-agent-skeleton.md
docs/architecture/lan-file-share-assistant-agent-skeleton.md
docs/qa/lan-file-share-assistant-agent-skeleton.md
```

It integrates with the existing agent-network roadmap by reserving the future
task class `file_share_readonly_review`, package identity
`iamine.beta.lan-file-share-assistant`, and `local_planning` resource profile.
The shared agent-package contract, orchestrator, package registry, permission
runtime, audit persistence, and network enforcement remain unchanged.

The feature does not change Rust crates, `iamine-node/src/main.rs`,
`iamine-node/src/cluster_registry.rs`, scheduler policy, P2P, PubSub, model
selection, task formats, startup, inference, model storage, reputation, or
rewards.

## Boundary

The future assistant may reason only over operator-selected, redacted metadata
that is supplied through an approved package interface. Its initial allowable
mode is `local_planning`, not `lan_readonly`.

`lan_readonly` remains deferred until a dedicated implementation defines:

- operator-selected-share and explicit user-intent policy;
- a bounded, metadata-only protocol adapter with no discovery or mount behavior;
- credential exclusion, path/identifier redaction, and no raw file-content path;
- default-deny permission enforcement and audit persistence;
- network, privacy, prompt-injection, role-confusion, and negative boundary
  tests; and
- Architecture and QA evidence for the executable surface.

No user prompt, agent output, operator confirmation, or role instruction may
expand those constraints.

## Future Package Interfaces

The future package will consume only redacted share-inventory and access-error
summaries selected by the operator. It may emit review, clarification, refusal,
and handoff records. It must not directly consume hostnames, IP addresses,
credentials, raw share paths, raw listings, file contents, process state, or
unredacted logs.

It must delegate all discovery, connectivity, authentication, mounting, file
access, transfer, recovery, configuration, and execution actions to an approved
future orchestrator flow. This skeleton neither defines nor authorizes that flow.

## Security and Privacy

The design is deny-by-default. Future input and output handling must preserve
the project privacy policy: no personal paths, usernames, host identifiers,
network identifiers, credentials, keys, tokens, raw prompts, raw outputs, or
unredacted evidence in package metadata, audit records, docs, or committed
artifacts.

The future assistant must treat supplied metadata as untrusted. Prompt
injection, role confusion, fabricated status, and instructions to bypass
permissions or handoff are refused and recorded as boundary outcomes.

## QA and Release Boundary

Because this feature creates only documents and leaves executable behavior
unchanged, it requires local documentation validation and Architecture review;
it does not require Mac, TS140, or Proxmox field QA. A future executable,
networking, file-access, capability-reporting, or runtime feature will require
the applicable field matrix before merge review.

## Deferred Implementation

The concrete file-share client, protocol adapters, sandbox, permissions,
redaction engine, audit emitter, package manifest, registry controls, and eval
harness are intentionally deferred. They must be owned by the feature that
introduces executable LAN file-share behavior, not retrofitted into this
skeleton.

## Next Roadmap Step

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001-SKELETON
```
