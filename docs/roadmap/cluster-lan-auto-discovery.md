# Cluster LAN Auto Discovery Roadmap Gate

## Next Milestone

CLUSTER-LAN-AUTO-DISCOVERY-001

## Baseline

Start from:

```text
branch: fix/proxmox-broadcast-taskresult-return-004
commit: 4987d02
```

This baseline has field-validated LAN Proxmox Broadcast:

```text
TaskOffer -> TaskBid -> TaskAssign -> TaskResult -> final_outcome=success
```

## Goal

Add Cluster LAN auto discovery on top of the validated LAN Broadcast baseline.

Expected direction:

- discover LAN peers
- classify controller/worker roles
- expose cluster membership
- expose cluster readiness/status
- reuse safe worker startup
- reuse PubSub readiness semantics
- keep Broadcast baseline passing

## In Scope For Cluster LAN

- cluster peer discovery state
- cluster readiness model
- cluster status observability
- cluster status CLI or equivalent, if requested in the cluster milestone
- capability aggregation using existing worker safe startup policy
- conservative PubSub readiness checks

## Out Of Scope

- payments
- wallet/tokenomics changes
- inference splitting
- WAN/global network behavior
- legacy real CPU inference
- dashboard/installer/autoupdate
- major scheduler rewrite

## Dependencies

Cluster LAN should depend on:

- worker safe startup policy
- Broadcast PubSub topic readiness
- controller/worker topic subscription events
- worker capabilities with mock/degraded mode
- existing heartbeat/capability events
- final Broadcast smoke as regression baseline

## Code Quality Review Answers

1. Has main.rs grown too large again?
Yes. At closeout it is about 14k lines. This is manageable for a milestone baseline but high-risk for continued feature growth.

2. Should Broadcast be extracted before cluster work?
GO WITH CONDITIONS. A full extraction is not required before starting Cluster LAN, but Broadcast should be treated as a protected regression path. Any Cluster LAN work that touches Broadcast should first extract small pure helpers or add tests.

3. Is TaskOffer/TaskBid/TaskAssign/TaskResult protocol centralized enough?
Partially. Builders exist as helpers, but they still live in main.rs. This is acceptable short term, but a broadcast_protocol module is recommended.

4. Is PubSub topic tracking reusable for Cluster LAN?
Yes, conceptually. It needs extraction for clean reuse. Do not reimplement connected_peers-only readiness.

5. Is worker startup policy reusable for Cluster LAN?
Yes. The mock/skip/degraded model is directly reusable for cluster status.

6. Is capability advertisement correct enough for cluster status?
Mostly, with one caveat. Machine-readable state correctly distinguishes backend=mock and real_inference_available=false, but human display still needs cleanup.

7. Is there duplicated result handling between Broadcast and inference paths?
Yes. Broadcast result handling and distributed inference result handling share concepts but use different payloads and acceptance paths. This is P1 technical debt.

8. Is recovery/timer logic clean and isolated?
No. Recovery remains embedded in the Broadcast runtime path and should be isolated before more timeout policies are added. It is not a P0 blocker because current Broadcast behavior is validated.

9. Are NDJSON events consistent enough for QA and future dashboard?
Good enough for QA. For dashboards, event field names should be normalized later, especially worker_id vs worker_peer_id and output vs output_preview.

10. Is it safe to start CLUSTER-LAN-AUTO-DISCOVERY-001 from 4987d02?
GO WITH CONDITIONS. The field baseline is strong and no P0 issue blocks Cluster LAN, but main.rs modularity should be watched closely.

## Risks

P1: main.rs density and mode branching can make Cluster LAN changes risky if added directly into the same select loop.

P1: Broadcast recovery and result acceptance are coupled to runtime state and should become a small state machine.

P1: Result handling is duplicated between Broadcast and distributed inference paths.

P2: PubSub readiness helpers are reusable but not yet extracted into a module.

P2: Human capability display in mock/skip mode can confuse operators.

P2: NDJSON event schema is useful but not yet formally versioned.

## Recommended Refactors

P1: Extract broadcast_protocol.rs:
TaskOffer, TaskBid, TaskAssign, TaskResult payload builders and acceptance helpers.

P1: Extract broadcast_runtime.rs:
Controller-side BroadcastOfferState, assignment, result acceptance, and recovery state.

P1: Extract broadcast_worker.rs:
Worker-side TaskOffer, TaskAssign execution, and TaskResult publication.

P2: Extract pubsub_readiness.rs:
topic tracker sync, subscriber counts, mesh counts, readiness snapshots.

P2: Add an event schema reference for NDJSON fields used by QA and future dashboard work.

P2: Clean up mock capability display before a user-facing cluster status UI.

## Go/No-Go

GO WITH CONDITIONS.

Conditions:

- Keep Broadcast smoke as a regression test for Cluster LAN.
- Do not add Cluster LAN logic by expanding the Broadcast match arms significantly.
- Reuse existing PubSub readiness semantics.
- Keep worker mock/skip startup as the default safe Proxmox validation mode.
- Treat real CPU inference as out of scope.
- Prefer small module extraction if Cluster LAN needs to touch Broadcast internals.

## Cluster Acceptance Baseline

Before and after Cluster LAN changes, run:

```bash
SMOKE_ID="cluster-baseline-smoke-$(date +%s)"

IAMINE_LOG_FORMAT=ndjson \
IAMINE_LOG_PATH=~/iamine-logs/controller_cluster_baseline.ndjson \
timeout 75s ./target/release/iamine-node --broadcast reverse_string "$SMOKE_ID"
```

Expected:

- TaskOffer published
- TaskBid received
- TaskAssign published
- exactly one worker executes
- TaskResult published
- TaskResult received
- output == reverse_string(SMOKE_ID)
- broadcast_recovery_cancelled
- final_outcome=success
- no rebroadcast after success
