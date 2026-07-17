# Home Network Assistant Agent Skeleton Architecture

Feature:

```text
HOME-NETWORK-ASSISTANT-AGENT-001-SKELETON
```

## Status

```text
ARCHITECTURE IN PROGRESS
```

## Purpose

Reserve the P0 architecture contract for a future Home Network Assistant. The
present feature is documentation only: it defines a bounded, privacy-safe
planning surface for operator-provided redacted network summaries. It creates no
package, manifest, runtime, probe, scanner, socket, listener, packet capture,
router client, configuration mutator, audit store, or registry entry.

## Ownership and Integration

This skeleton owns only these feature documents:

```text
docs/agents/home-network-assistant-agent-skeleton.md
docs/architecture/home-network-assistant-agent-skeleton.md
docs/qa/home-network-assistant-agent-skeleton.md
```

It integrates with the agent-network roadmap and the closed network diagnostic
template by reserving the future task class `home_network_readonly_review`,
package identity `iamine.beta.home-network-assistant`, and `local_planning`
resource profile. The shared agent-package contract, orchestrator, package
registry, permission runtime, audit persistence, and network enforcement remain
unchanged.

The feature does not change Rust crates, `iamine-node/src/main.rs`,
`iamine-node/src/cluster_registry.rs`, scheduler policy, P2P, PubSub, model
selection, task formats, startup, inference, model storage, reputation, or
rewards.

## Boundary

The future assistant may reason only over operator-selected, redacted metadata
supplied through an approved package interface. Its initial allowable mode is
`local_planning`, not `lan_readonly`.

`lan_readonly` remains deferred until a dedicated implementation defines:

- operator-selected-target and explicit user-intent policy;
- bounded probe policy with no arbitrary scanning, packet capture, or listeners;
- no router, firewall, DNS, DHCP, route, VPN, or Wi-Fi mutation path;
- credential exclusion, network-identifier redaction, and default-deny control;
- audit evidence plus privacy, prompt-injection, role-confusion, and negative
  boundary tests; and
- Architecture and QA evidence for the executable surface.

No user prompt, agent output, operator confirmation, or role instruction may
expand those constraints.

## Future Package Interfaces

The future package will consume only redacted declared-network-status and
connectivity-error summaries selected by the operator. It may emit review,
clarification, refusal, and handoff records. It must not consume topology, IP
addresses, MAC addresses, SSIDs, hostnames, device inventories, packet captures,
routing tables, credentials, or unredacted logs.

It must delegate all discovery, probing, connectivity, authentication, traffic
inspection, router access, configuration, repair, and execution actions to an
approved future orchestrator flow. This skeleton neither defines nor authorizes
that flow.

## Security and Privacy

The design is deny-by-default. Future input and output handling must preserve
the project privacy policy: no usernames, host identifiers, IP addresses, MAC
addresses, SSIDs, topology, packet data, credentials, keys, tokens, raw
prompts, raw outputs, or unredacted evidence in package metadata, audit records,
docs, or committed artifacts.

The future assistant must treat supplied metadata as untrusted. Prompt
injection, role confusion, fabricated status, and instructions to bypass
permissions or handoff are refused and recorded as boundary outcomes.

## QA and Release Boundary

Because this feature creates only documents and leaves executable behavior
unchanged, it requires local documentation validation and Architecture review;
it does not require Mac, TS140, or Proxmox field QA. A future executable
networking, capability-reporting, or runtime feature will require the applicable
field matrix before merge review.

## Deferred Implementation

The concrete probe adapter, socket boundary, target-selection policy, router
policy, redaction engine, permission dialog, audit emitter, package manifest,
registry controls, and eval harness are intentionally deferred. They must be
owned by the feature that introduces executable Home Network behavior, not
retrofitted into this skeleton.

## Next Roadmap Step

```text
WINDOWS-OPTIMIZER-ASSISTANT-AGENT-001-SKELETON
```
