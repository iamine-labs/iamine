# Cluster LAN Auto Discovery Roadmap Gate

## Next Milestone

CLUSTER-LAN-AUTO-DISCOVERY-001

## Recommended Base

Start from `develop` after CLOSEOUT-REFACTORING-GATE-001 is merged.

Current technical base for the closeout gate:

```text
branch: develop
commit: 69c292a
status: pre-cluster refactors merged
decision: GO WITH CONDITIONS
```

## Why Cluster LAN Is Unblocked

The pre-cluster refactor cycle reduced `iamine-node/src/main.rs` from 14150
lines to 8931 lines and moved critical behavior into dedicated modules:

- Broadcast protocol/runtime/worker helpers
- worker startup and backend policy
- CLI parsing and mode dispatch
- PubSub readiness and topic tracking
- model display and executability classification
- result protocol and acceptance
- metrics policy and metrics server fallback

The validated Broadcast baseline remains:

```text
TaskOffer -> TaskBid -> TaskAssign -> TaskResult -> final_outcome=success
```

## Cluster LAN Goal

Add LAN cluster auto discovery and status on top of the validated Broadcast and
worker startup baseline.

Expected direction:

- discover LAN peers
- classify controller and worker roles
- expose cluster membership
- expose cluster readiness/status
- aggregate node capabilities
- show backend, real inference availability, and metrics availability clearly
- reuse PubSub readiness semantics
- keep Broadcast baseline passing

## In Scope For Cluster LAN

- cluster peer discovery state
- cluster readiness model
- cluster status observability
- cluster status CLI or equivalent if requested by the milestone
- capability aggregation using extracted worker startup and capability modules
- metrics availability using extracted metrics policy
- conservative PubSub readiness checks
- regression-safe integration with Broadcast

## Out Of Scope

- payments
- wallet/tokenomics changes
- inference splitting
- WAN/global network behavior
- legacy real CPU inference
- dashboard/installer/autoupdate
- major scheduler rewrite

## Completed Dependencies

- Broadcast runtime helpers extracted
- worker startup policy extracted
- backend policy extracted
- CPU guard extracted
- worker capability advertisement extracted
- CLI parsing and mode dispatch extracted
- PubSub topics/readiness/tracker/observability extracted
- model display and executability extracted
- result protocol and acceptance extracted
- final outcome helpers extracted
- metrics policy and server fallback extracted

## Reuse Requirements

Cluster LAN should reuse:

- `pubsub_topic_tracker.rs` for observed peer subscriptions
- `pubsub_readiness.rs` for real readiness decisions
- `worker_startup_policy.rs` for degraded/mock startup state
- `backend_policy.rs` and `cpu_feature_guard.rs` for backend availability
- `worker_capability_advertisement.rs` for advertised capabilities
- `model_display_policy.rs` and `model_executability.rs` for display semantics
- `metrics_policy.rs` for metrics availability and fallback state
- `result_acceptance.rs` if task/result status is surfaced

Do not reintroduce connected_peers-only readiness.

## Required Baseline Smoke

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
- output equals reverse_string(SMOKE_ID)
- `broadcast_recovery_cancelled`
- `final_outcome=success`
- no rebroadcast after success
- no duplicate execution/result

## Pending QA

Proxmox/R5500 metrics field smoke is still pending for
REFACTOR-METRICS-SERVER-008.

This is not blocking for starting Cluster LAN, but it is a condition for closing
Cluster LAN cleanly.

If the Proxmox metrics smoke fails, open a bugfix on `develop` before closing or
shipping Cluster LAN.

## Follow-Ups

- QA-CLI-UNKNOWN-MODE-EXIT-CODE-007: verify unknown mode exit code and fix if it
  still exits 0.
- Keep metrics unavailable/fallback visible in cluster status.
- Existing Rust dead_code warnings are not blocking.
- Cluster assignment log spam can be cleaned later.

## Go/No-Go

GO WITH CONDITIONS.

Conditions:

- Run Proxmox/R5500 metrics smoke when Dell is available.
- Keep Broadcast smoke as a protected regression.
- Do not add Cluster LAN state directly into `main.rs` when an extracted module
  can own it.
- Keep worker mock/skip startup as the Proxmox safety baseline.
- Treat real CPU inference on Proxmox/R5500 as out of scope.

Recommended next branch:

```text
feature/cluster-lan-auto-discovery-001
```
