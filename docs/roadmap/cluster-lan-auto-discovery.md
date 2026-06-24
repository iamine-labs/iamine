# Cluster LAN Auto Discovery Closeout

## Milestone

CLUSTER-LAN-AUTO-DISCOVERY-001

## Status

Implemented in `develop`.

Reconciled from current `develop` after the model gate line:

```text
branch: develop
commit: e390befa3c4e7c4eb86c825c69a2bdd60d9bd8cf
tree: 0b0c02f1df6c0bb23cb6c428c84931cea02e62c8
status: CLUSTER-LAN-AUTO-DISCOVERY-001 implemented
```

Primary implementation entered `develop` through:

```text
c6ffe3b Merge pull request: add cluster LAN auto discovery status
57fe8ca style: format cluster lan modules
```

## Why Cluster LAN Was Unblocked

The pre-cluster refactor cycle reduced `iamine-node/src/main.rs` from 14150
lines to 8931 lines and moved critical behavior into dedicated modules:

- Broadcast protocol/runtime/worker helpers
- worker startup and backend policy
- CLI parsing and mode dispatch
- PubSub readiness and topic tracking
- model display and executability classification
- result protocol and acceptance
- metrics policy and metrics server fallback

The validated Broadcast baseline remained:

```text
TaskOffer -> TaskBid -> TaskAssign -> TaskResult -> final_outcome=success
```

## Cluster LAN Goal And Current Contract

Cluster LAN adds LAN peer discovery and status on top of the validated Broadcast
and worker startup baseline.

Current contract:

- discover LAN peers;
- classify controller and worker roles;
- expose cluster membership;
- expose cluster readiness/status;
- aggregate node capabilities;
- show backend, real inference availability, and metrics availability clearly;
- reuse PubSub readiness semantics;
- keep Broadcast baseline passing.

## Implemented Scope

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

## Completed Dependencies And Follow-Ups

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
- QA-CLI-UNKNOWN-MODE-EXIT-CODE-007 closed
- Proxmox/R5500 metrics fallback closure completed
- QA-PROXMOX-MOCK-CAPABILITIES-DISPLAY-001 closed

## Reuse Requirements Preserved

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

## Required Baseline Smoke For Future Changes

Before and after future Cluster LAN changes, run:

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

## QA Status

Proxmox/R5500 metrics fallback is closed by the later follow-up branch and the
LAN Proxmox Broadcast closeout.

Current evidence is tracked in:

```text
docs/qa/lan-proxmox-broadcast-flow.md
```

Future changes that touch runtime, worker behavior, cluster status, capabilities,
broadcast, or inference must still run field QA.

## Remaining Follow-Ups

- Existing Rust dead_code warnings are not blocking.
- Cluster assignment log spam can be cleaned later.
- LEGACY-BACKEND-REAL-INFERENCE-001 remained out of scope for Cluster LAN and
  was closed separately with a dedicated legacy CPU daemon build.

## Closeout Decision

CLUSTER-LAN-AUTO-DISCOVERY-001 is implemented in `develop`. Treat this roadmap
as a closeout record, not as a request to reopen the historical feature branch.
