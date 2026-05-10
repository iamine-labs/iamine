# PubSub Readiness Architecture

## Lesson Learned

connected_peers is not enough for Gossipsub publish readiness.

In Proxmox field validation, the controller discovered and connected workers, but publishing TaskOffer failed with InsufficientPeers because the iamine-tasks topic had no observed subscribers or mesh peers from the controller perspective.

## Readiness Rule

Broadcast TaskOffer publish is ready only when at least one of these is true:

- subscribed_peers for iamine-tasks > 0
- mesh_peers for iamine-tasks > 0

Connected peers are diagnostic only. They must not make readiness true by themselves.

## Controller-Side Tracking

The controller tracks topic readiness from two sources:

- Gossipsub subscription events
- Gossipsub all_peers() topic state

When a remote subscription is observed, the controller emits:

- controller_observed_peer_subscription
- broadcast_topic_subscriber_seen for iamine-tasks

## Worker-Side Tracking

Workers emit:

- worker_topic_subscribed
- worker_pubsub_ready
- worker_observed_peer_subscription when they observe controller subscriptions

These events help identify asymmetry between what workers see and what the controller sees.

## Readiness State Event

broadcast_readiness_state includes:

- topic
- connected_peers
- subscribed_peers
- mesh_peers
- all_peers_per_topic
- gossipsub_mesh_peers
- elapsed_ms
- attempts
- readiness_reason
- ready
- last_publish_failure_reason when available

Important readiness reasons:

- task_topic_subscriber_seen
- task_topic_mesh_ready
- waiting_for_task_topic_subscription
- waiting_for_connected_peers

## InsufficientPeers Handling

If publish returns InsufficientPeers:

- emit broadcast_task_offer_publish_failed
- include connected/subscribed/mesh counts
- store last_publish_failure_reason
- continue bounded wait/backoff while within readiness timeout
- do not spin rapid retries when subscribed_peers=0 and mesh_peers=0

## Proxmox Lessons

The final fix required both:

- waiting for real topic readiness
- deriving subscribers from Gossipsub internal peer topic state, not only from explicit subscription events

This should be reused for Cluster LAN because cluster discovery will also need to distinguish:

- node discovered
- peer connected
- peer subscribed to capability topics
- peer ready for workload topics

## Reuse For Cluster LAN

PubSub readiness tracking is reusable for Cluster LAN if extracted behind a small API:

```text
tracker.sync_from_gossipsub(all_peers)
tracker.topic_peer_count(topic)
tracker.topic_mesh_count(topic)
tracker.readiness_snapshot(topic)
```

Until extraction, Cluster LAN should call the existing readiness helpers conservatively and avoid adding new connected_peers-only readiness checks.
