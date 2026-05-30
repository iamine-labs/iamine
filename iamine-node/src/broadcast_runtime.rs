#[path = "broadcast_observability.rs"]
mod observability;
#[path = "broadcast_state.rs"]
mod state;

pub(crate) use observability::*;
pub(crate) use state::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast_worker::emit_worker_pubsub_ready_event;
    use crate::broadcast_worker::emit_worker_topic_subscribed_event;
    use crate::result_observability::{
        emit_broadcast_final_outcome_success_event, emit_broadcast_recovery_cancelled_event,
    };
    use crate::{PubsubTopicTracker, BIDS_TOPIC, BROADCAST_PUBSUB_TOPICS, TASK_TOPIC};
    use libp2p::{gossipsub, PeerId};
    use std::collections::HashSet;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn test_id(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    #[test]
    fn broadcast_waits_for_task_topic_subscriber_before_publish() {
        assert!(!broadcast_offer_ready(0, 0, 0, 0));
        assert!(!broadcast_offer_ready(0, 0, 1, 60_000));
    }

    #[test]
    fn broadcast_publishes_after_task_topic_subscriber_seen() {
        assert!(broadcast_offer_ready(1, 0, 0, 0));
    }

    #[test]
    fn broadcast_marks_ready_when_mesh_peer_present() {
        assert!(broadcast_offer_ready(0, 1, 0, 0));
    }

    #[test]
    fn controller_records_worker_subscription_to_iamine_tasks() {
        let mut tracker = PubsubTopicTracker::default();
        let worker_peer = PeerId::random();
        let inserted = tracker.register_peer_subscription(
            &worker_peer,
            &gossipsub::IdentTopic::new(TASK_TOPIC).hash(),
        );

        assert!(inserted);
        assert_eq!(tracker.topic_peer_count(TASK_TOPIC), 1);
    }

    #[test]
    fn broadcast_does_not_mark_ready_from_connected_peers_only() {
        let snapshot = BroadcastReadinessSnapshot::new(3, 0, 0, 60_000, 0);

        assert!(!snapshot.ready);
        assert_eq!(
            snapshot.readiness_reason,
            "waiting_for_task_topic_subscription"
        );
    }

    #[test]
    fn broadcast_marks_ready_when_task_subscriber_present() {
        let snapshot = BroadcastReadinessSnapshot::new(3, 1, 0, 500, 0);

        assert!(snapshot.ready);
        assert_eq!(snapshot.readiness_reason, "task_topic_subscriber_seen");
    }

    #[test]
    fn broadcast_publishes_initial_task_offer_once() {
        let mut state = BroadcastOfferState::new("broadcast-task-once".to_string());

        assert!(state.can_attempt_publish());
        state.record_publish_result(true, None);

        assert!(state.published);
        assert_eq!(state.attempts, 1);
        assert!(!state.can_attempt_publish());
    }

    #[test]
    fn broadcast_retries_offer_publish_if_publish_fails() {
        let mut state = BroadcastOfferState::new("broadcast-task-retry".to_string());

        state.record_publish_result(false, Some("InsufficientPeers"));
        assert!(!state.published);
        assert!(!state.failed);
        assert_eq!(
            state.last_publish_failure_reason.as_deref(),
            Some("InsufficientPeers")
        );
        state.last_attempt_at = Some(
            tokio::time::Instant::now() - Duration::from_millis(state.current_retry_delay_ms()),
        );
        assert!(state.can_attempt_publish());
    }

    #[test]
    fn broadcast_insufficient_peers_triggers_backoff_not_immediate_terminal_failure() {
        let mut state = BroadcastOfferState::new("broadcast-task-insufficient".to_string());

        state.record_publish_result(false, Some("InsufficientPeers"));
        assert!(!state.failed);
        assert!(!state.can_attempt_publish());
        assert_eq!(state.attempts, 1);
        assert_eq!(
            state.last_publish_failure_reason.as_deref(),
            Some("InsufficientPeers")
        );
    }

    #[test]
    fn insufficient_peers_does_not_retry_publish_without_subscriber_or_mesh() {
        let mut state = BroadcastOfferState::new("broadcast-task-no-subscribers".to_string());
        state.record_publish_result(false, Some("InsufficientPeers"));
        state.last_attempt_at = Some(
            tokio::time::Instant::now() - Duration::from_millis(state.current_retry_delay_ms() + 1),
        );
        let snapshot = BroadcastReadinessSnapshot::new(3, 0, 0, 60_000, state.attempts);

        assert!(state.can_attempt_publish());
        assert!(!snapshot.ready);
        assert!(!broadcast_offer_ready(
            snapshot.subscribed_peers,
            snapshot.mesh_peers,
            snapshot.connected_peers,
            snapshot.elapsed_ms
        ));
    }

    #[test]
    fn broadcast_topic_subscriber_seen_emitted_on_worker_subscription() {
        let peer_id = test_id("worker-subscription");

        emit_broadcast_topic_subscriber_seen_event(
            TASK_TOPIC,
            &peer_id,
            3,
            1,
            0,
            "gossipsub_subscription",
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_topic_subscriber_seen")
            .expect("subscriber seen event should be present");

        assert_eq!(
            entry.fields.get("topic").and_then(|value| value.as_str()),
            Some(TASK_TOPIC)
        );
        assert_eq!(
            entry.fields.get("peer_id").and_then(|value| value.as_str()),
            Some(peer_id.as_str())
        );
        assert_eq!(
            entry
                .fields
                .get("source_event")
                .and_then(|value| value.as_str()),
            Some("gossipsub_subscription")
        );
    }

    #[test]
    fn broadcast_readiness_state_includes_peer_counts() {
        let task_id = test_id("broadcast-readiness-state");
        let snapshot = BroadcastReadinessSnapshot::new(3, 2, 1, 2_500, 4).with_gossipsub_details(
            serde_json::json!({
                TASK_TOPIC: ["worker-peer"],
                BIDS_TOPIC: [],
            }),
            vec!["worker-peer".to_string()],
            Some("InsufficientPeers"),
        );

        emit_broadcast_readiness_state_event(&task_id, &snapshot);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_readiness_state" && entry.trace_id == task_id)
            .expect("readiness event should be present");

        assert_eq!(
            entry.fields.get("topic").and_then(|value| value.as_str()),
            Some(TASK_TOPIC)
        );
        assert_eq!(
            entry
                .fields
                .get("connected_peers")
                .and_then(|value| value.as_u64()),
            Some(3)
        );
        assert_eq!(
            entry
                .fields
                .get("subscribed_peers")
                .and_then(|value| value.as_u64()),
            Some(2)
        );
        assert_eq!(
            entry
                .fields
                .get("mesh_peers")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
        assert_eq!(
            entry
                .fields
                .get("attempts")
                .and_then(|value| value.as_u64()),
            Some(4)
        );
        assert_eq!(
            entry
                .fields
                .get("last_publish_failure_reason")
                .and_then(|value| value.as_str()),
            Some("InsufficientPeers")
        );
        assert!(entry.fields.get("all_peers_per_topic").is_some());
        assert!(entry.fields.get("gossipsub_mesh_peers").is_some());
    }

    #[test]
    fn broadcast_readiness_state_reports_zero_subscribers_explicitly() {
        let task_id = test_id("broadcast-zero-subscribers");
        let snapshot = BroadcastReadinessSnapshot::new(3, 0, 0, 30_000, 2);

        emit_broadcast_readiness_state_event(&task_id, &snapshot);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_readiness_state" && entry.trace_id == task_id)
            .expect("readiness event should be present");

        assert_eq!(
            entry
                .fields
                .get("connected_peers")
                .and_then(|value| value.as_u64()),
            Some(3)
        );
        assert_eq!(
            entry
                .fields
                .get("subscribed_peers")
                .and_then(|value| value.as_u64()),
            Some(0)
        );
        assert_eq!(
            entry
                .fields
                .get("mesh_peers")
                .and_then(|value| value.as_u64()),
            Some(0)
        );
        assert_eq!(
            entry.fields.get("ready").and_then(|value| value.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn controller_emits_topic_subscribed_for_required_topics() {
        let peer_id = test_id("controller-topic-test");
        for topic in BROADCAST_PUBSUB_TOPICS {
            emit_controller_topic_subscribed_event(topic, &peer_id);
        }
        emit_broadcast_pubsub_ready_event(&peer_id, &BROADCAST_PUBSUB_TOPICS);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let topics: HashSet<String> = entries
            .iter()
            .filter(|entry| entry.event == "controller_topic_subscribed")
            .filter(|entry| {
                entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
            })
            .filter_map(|entry| {
                entry
                    .fields
                    .get("topic")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .collect();

        for topic in BROADCAST_PUBSUB_TOPICS {
            assert!(topics.contains(topic), "missing controller topic {topic}");
        }
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "broadcast_pubsub_ready"
                && entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
        }));
    }

    #[test]
    fn worker_and_controller_observed_peer_subscription_events_are_emitted() {
        let observer_peer_id = test_id("observer");
        let observed_peer_id = test_id("observed");

        emit_observed_peer_subscription_event(
            "controller_observed_peer_subscription",
            &observer_peer_id,
            &observed_peer_id,
            TASK_TOPIC,
            "broadcast",
            "gossipsub_subscription",
        );
        emit_observed_peer_subscription_event(
            "worker_observed_peer_subscription",
            &observed_peer_id,
            &observer_peer_id,
            TASK_TOPIC,
            "worker",
            "gossipsub_subscription",
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "controller_observed_peer_subscription"
                && entry
                    .fields
                    .get("observer_peer_id")
                    .and_then(|value| value.as_str())
                    == Some(observer_peer_id.as_str())
                && entry.fields.get("topic").and_then(|value| value.as_str()) == Some(TASK_TOPIC)
        }));
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "worker_observed_peer_subscription"
                && entry
                    .fields
                    .get("observer_peer_id")
                    .and_then(|value| value.as_str())
                    == Some(observed_peer_id.as_str())
                && entry.fields.get("topic").and_then(|value| value.as_str()) == Some(TASK_TOPIC)
        }));
    }

    #[test]
    fn broadcast_does_not_spin_all_retries_before_subscription_propagates() {
        let mut state = BroadcastOfferState::new("broadcast-task-backoff".to_string());
        state.record_publish_result(false, Some("InsufficientPeers"));

        assert_eq!(state.attempts, 1);
        assert!(!state.can_attempt_publish());

        state.last_attempt_at = Some(
            tokio::time::Instant::now() - Duration::from_millis(state.current_retry_delay_ms() - 1),
        );
        assert!(!state.can_attempt_publish());

        state.mark_subscription_seen();
        assert!(broadcast_offer_ready(1, 0, 1, state.elapsed_ms()));
    }

    #[test]
    fn broadcast_task_offer_published_after_readiness() {
        let task_id = test_id("broadcast-published");
        let snapshot = BroadcastReadinessSnapshot::new(3, 1, 1, 1_200, 1);

        emit_broadcast_task_offer_published_event(
            &task_id,
            "reverse_string",
            "qa-payload",
            "message-id-test",
            1,
            &snapshot,
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_task_offer_published")
            .expect("published event should be present");

        assert_eq!(
            entry
                .fields
                .get("task_type")
                .and_then(|value| value.as_str()),
            Some("reverse_string")
        );
        assert_eq!(
            entry.fields.get("topic").and_then(|value| value.as_str()),
            Some(TASK_TOPIC)
        );
        assert_eq!(
            entry
                .fields
                .get("attempt_number")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
    }

    #[test]
    fn controller_accepts_task_result_from_assigned_worker() {
        let mut state = BroadcastOfferState::new("broadcast-result-accept".to_string());
        state.mark_assigned("assigned-worker");

        assert!(evaluate_broadcast_result_acceptance(
            Some(&state),
            "broadcast-result-accept",
            "assigned-worker",
            true,
        )
        .is_ok());
    }

    #[test]
    fn controller_rejects_task_result_from_non_assigned_worker() {
        let mut state = BroadcastOfferState::new("broadcast-result-wrong".to_string());
        state.mark_assigned("assigned-worker");

        assert_eq!(
            evaluate_broadcast_result_acceptance(
                Some(&state),
                "broadcast-result-wrong",
                "other-worker",
                true,
            ),
            Err("wrong_worker")
        );
    }

    #[test]
    fn controller_rejects_duplicate_broadcast_result() {
        let mut state = BroadcastOfferState::new("broadcast-result-duplicate".to_string());
        state.mark_assigned("assigned-worker");
        assert!(evaluate_broadcast_result_acceptance(
            Some(&state),
            "broadcast-result-duplicate",
            "assigned-worker",
            true,
        )
        .is_ok());

        state.mark_result_accepted();
        assert_eq!(
            evaluate_broadcast_result_acceptance(
                Some(&state),
                "broadcast-result-duplicate",
                "assigned-worker",
                true,
            ),
            Err("duplicate_result")
        );
    }

    #[test]
    fn controller_cancels_recovery_after_result_accepted() {
        let task_id = test_id("broadcast-recovery-cancel");

        emit_broadcast_recovery_cancelled_event(&task_id, "assigned-worker");

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| {
                entry.trace_id == task_id && entry.event == "broadcast_recovery_cancelled"
            })
            .expect("broadcast_recovery_cancelled should be present");

        assert_eq!(
            entry.fields.get("reason").and_then(|value| value.as_str()),
            Some("result_accepted")
        );
    }

    #[test]
    fn broadcast_final_outcome_success_after_result_received() {
        let task_id = test_id("broadcast-final-success");

        emit_broadcast_final_outcome_success_event(
            &task_id,
            "assigned-worker",
            "tset-tluser-tsacdaorb-aq",
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == task_id && entry.event == "final_outcome")
            .expect("final_outcome should be present");

        assert_eq!(
            entry.fields.get("outcome").and_then(|value| value.as_str()),
            Some("success")
        );
        assert_eq!(
            entry.fields.get("failed").and_then(|value| value.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn broadcast_no_timeout_after_worker_success_result_accepted() {
        let mut state = BroadcastOfferState::new("broadcast-no-timeout".to_string());
        state.mark_assigned("assigned-worker");

        assert!(evaluate_broadcast_result_acceptance(
            Some(&state),
            "broadcast-no-timeout",
            "assigned-worker",
            true,
        )
        .is_ok());
        state.mark_result_accepted();

        assert!(state.result_accepted);
        assert_eq!(
            evaluate_broadcast_result_acceptance(
                Some(&state),
                "broadcast-no-timeout",
                "assigned-worker",
                true,
            ),
            Err("duplicate_result")
        );
    }

    #[test]
    fn worker_emits_topic_subscribed_for_required_topics() {
        let peer_id = test_id("worker-topic-test");
        for topic in BROADCAST_PUBSUB_TOPICS {
            emit_worker_topic_subscribed_event(topic, &peer_id, "mock");
        }
        emit_worker_pubsub_ready_event(&peer_id, "mock", &BROADCAST_PUBSUB_TOPICS);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let topics: HashSet<String> = entries
            .iter()
            .filter(|entry| entry.event == "worker_topic_subscribed")
            .filter(|entry| {
                entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
            })
            .filter_map(|entry| {
                entry
                    .fields
                    .get("topic")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .collect();

        for topic in BROADCAST_PUBSUB_TOPICS {
            assert!(topics.contains(topic), "missing worker topic {topic}");
        }
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "worker_pubsub_ready"
                && entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
        }));
    }
}
