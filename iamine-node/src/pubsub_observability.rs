use crate::{
    log_observability_event, DispatchReadinessError, DispatchReadinessSnapshot, PubsubTopicTracker,
    RESULTS_TOPIC, TASK_TOPIC,
};
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use serde_json::Map;

pub(crate) fn emit_remote_delivery_topic_ready_events(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    tracker: &PubsubTopicTracker,
) {
    let result_peer_count = tracker.topic_peer_count(RESULTS_TOPIC);
    let progress_peer_count = tracker
        .topic_peer_count(TASK_TOPIC)
        .max(tracker.topic_peer_count(RESULTS_TOPIC));
    for (event_name, topic, peer_count, field_name) in [
        (
            "result_topic_ready",
            RESULTS_TOPIC,
            result_peer_count,
            "result_subscription_peer_count",
        ),
        (
            "progress_topic_ready",
            TASK_TOPIC,
            progress_peer_count,
            "progress_subscription_peer_count",
        ),
    ] {
        log_observability_event(
            LogLevel::Info,
            event_name,
            trace_task_id,
            Some(trace_task_id),
            Some(model_id),
            None,
            {
                let mut fields = Map::new();
                fields.insert("attempt_id".to_string(), attempt_id.into());
                fields.insert("topic".to_string(), topic.into());
                fields.insert("joined".to_string(), tracker.joined(topic).into());
                fields.insert(field_name.to_string(), (peer_count as u64).into());
                fields.insert(
                    "result_subscription_peer_count".to_string(),
                    (result_peer_count as u64).into(),
                );
                fields.insert(
                    "progress_subscription_peer_count".to_string(),
                    (progress_peer_count as u64).into(),
                );
                fields
            },
        );
    }
}

pub(crate) fn emit_dispatch_context_event(
    trace_task_id: &str,
    attempt_id: &str,
    selected_model: &str,
    candidates: &[String],
    connected_peer_count: usize,
    topic_peer_count: usize,
    selected_topic: &str,
    target_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "task_dispatch_context",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert(
                "selected_model".to_string(),
                selected_model.to_string().into(),
            );
            fields.insert("candidates".to_string(), serde_json::json!(candidates));
            fields.insert(
                "connected_peer_count".to_string(),
                (connected_peer_count as u64).into(),
            );
            fields.insert(
                "topic_peer_count".to_string(),
                (topic_peer_count as u64).into(),
            );
            fields.insert(
                "selected_topic".to_string(),
                selected_topic.to_string().into(),
            );
            if let Some(target_peer_id) = target_peer_id {
                fields.insert("target_peer_id".to_string(), target_peer_id.into());
                fields.insert("selected_peer_id".to_string(), target_peer_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_dispatch_readiness_failure_event(
    trace_task_id: &str,
    attempt_id: &str,
    selected_model: &str,
    candidates: &[String],
    target_peer_id: Option<&str>,
    snapshot: &DispatchReadinessSnapshot,
    error: &DispatchReadinessError,
) {
    log_observability_event(
        LogLevel::Error,
        "task_dispatch_readiness_failed",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        Some(error.code),
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("reason".to_string(), error.reason.into());
            fields.insert("error_kind".to_string(), "dispatch_readiness".into());
            fields.insert("recoverable".to_string(), true.into());
            fields.insert("candidates".to_string(), serde_json::json!(candidates));
            fields.insert(
                "connected_peer_count".to_string(),
                (snapshot.connected_peer_count as u64).into(),
            );
            fields.insert(
                "mesh_peer_count".to_string(),
                (snapshot.mesh_peer_count as u64).into(),
            );
            fields.insert(
                "topic_peer_count".to_string(),
                (snapshot.topic_peer_count as u64).into(),
            );
            fields.insert(
                "joined_task_topic".to_string(),
                snapshot.joined_task_topic.into(),
            );
            fields.insert(
                "joined_direct_topic".to_string(),
                snapshot.joined_direct_topic.into(),
            );
            fields.insert(
                "joined_results_topic".to_string(),
                snapshot.joined_results_topic.into(),
            );
            fields.insert("selected_topic".to_string(), snapshot.selected_topic.into());
            if let Some(target_peer_id) = target_peer_id {
                fields.insert("target_peer_id".to_string(), target_peer_id.into());
                fields.insert("selected_peer_id".to_string(), target_peer_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_task_publish_attempt_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    publish_peer_count: usize,
    payload_size: usize,
    selected_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "task_publish_attempt",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert(
                "publish_peer_count".to_string(),
                (publish_peer_count as u64).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_task_published_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    publish_peer_count: usize,
    payload_size: usize,
    message_id: &str,
    selected_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "task_published",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert(
                "publish_peer_count".to_string(),
                (publish_peer_count as u64).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            fields.insert("message_id".to_string(), message_id.into());
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_task_publish_failed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    publish_peer_count: usize,
    payload_size: usize,
    error: &str,
    selected_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Error,
        "task_publish_failed",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert(
                "publish_peer_count".to_string(),
                (publish_peer_count as u64).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            fields.insert("error".to_string(), error.into());
            fields.insert("error_kind".to_string(), "publish_error".into());
            fields.insert("recoverable".to_string(), true.into());
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PubsubTopicTracker, DIRECT_INF_TOPIC, RESULTS_TOPIC, TASK_TOPIC};
    use libp2p::{gossipsub, PeerId};

    fn trace_id(prefix: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    #[test]
    fn pubsub_readiness_events_preserve_names() {
        let trace_id = trace_id("pubsub-event-names");
        let mut tracker = PubsubTopicTracker::default();
        tracker.register_local_subscription(TASK_TOPIC);
        tracker.register_local_subscription(DIRECT_INF_TOPIC);
        tracker.register_local_subscription(RESULTS_TOPIC);
        let peer = PeerId::random();
        tracker.register_peer_subscription(&peer, &gossipsub::IdentTopic::new(TASK_TOPIC).hash());
        tracker
            .register_peer_subscription(&peer, &gossipsub::IdentTopic::new(RESULTS_TOPIC).hash());

        emit_remote_delivery_topic_ready_events(&trace_id, "attempt-2", "mistral-7b", &tracker);

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();

        assert!(entries
            .iter()
            .any(|entry| entry.trace_id == trace_id && entry.event == "result_topic_ready"));
        assert!(entries
            .iter()
            .any(|entry| entry.trace_id == trace_id && entry.event == "progress_topic_ready"));
    }

    #[test]
    fn pubsub_remote_delivery_topic_ready_counts_are_emitted() {
        let trace_id = trace_id("topic-ready");
        let mut tracker = PubsubTopicTracker::default();
        tracker.register_local_subscription(TASK_TOPIC);
        tracker.register_local_subscription(DIRECT_INF_TOPIC);
        tracker.register_local_subscription(RESULTS_TOPIC);
        let peer = PeerId::random();
        tracker.register_peer_subscription(&peer, &gossipsub::IdentTopic::new(TASK_TOPIC).hash());
        tracker
            .register_peer_subscription(&peer, &gossipsub::IdentTopic::new(RESULTS_TOPIC).hash());

        emit_remote_delivery_topic_ready_events(&trace_id, "attempt-2", "mistral-7b", &tracker);

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let result_ready = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "result_topic_ready")
            .expect("result_topic_ready entry not found");
        let progress_ready = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "progress_topic_ready")
            .expect("progress_topic_ready entry not found");

        assert_eq!(
            result_ready
                .fields
                .get("result_subscription_peer_count")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
        assert_eq!(
            progress_ready
                .fields
                .get("progress_subscription_peer_count")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
    }

    #[test]
    fn pubsub_dispatch_observability_events_are_emitted() {
        let trace_id = trace_id("dispatch-observability");
        let attempt_id = "attempt-observability-1";
        let model_id = "tinyllama-1b";
        let candidates = vec![model_id.to_string(), "llama3-3b".to_string()];

        emit_dispatch_context_event(
            &trace_id,
            attempt_id,
            model_id,
            &candidates,
            3,
            2,
            DIRECT_INF_TOPIC,
            Some("peer-target-1"),
        );
        emit_task_publish_attempt_event(
            &trace_id,
            attempt_id,
            model_id,
            DIRECT_INF_TOPIC,
            2,
            128,
            Some("peer-target-1"),
        );
        emit_task_published_event(
            &trace_id,
            attempt_id,
            model_id,
            DIRECT_INF_TOPIC,
            2,
            128,
            "message-1",
            Some("peer-target-1"),
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();

        assert!(entries
            .iter()
            .any(|entry| { entry.trace_id == trace_id && entry.event == "task_dispatch_context" }));
        let dispatch_entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "task_dispatch_context")
            .expect("task_dispatch_context entry not found");
        assert_eq!(dispatch_entry.task_id.as_deref(), Some(trace_id.as_str()));
        assert_eq!(dispatch_entry.model_id.as_deref(), Some(model_id));
        assert_eq!(
            dispatch_entry
                .fields
                .get("attempt_id")
                .and_then(|value| value.as_str()),
            Some(attempt_id)
        );
        assert_eq!(
            dispatch_entry
                .fields
                .get("selected_peer_id")
                .and_then(|value| value.as_str()),
            Some("peer-target-1")
        );
        assert!(entries
            .iter()
            .any(|entry| { entry.trace_id == trace_id && entry.event == "task_publish_attempt" }));
        assert!(entries.iter().any(|entry| {
            entry.trace_id == trace_id
                && entry.event == "task_published"
                && entry
                    .fields
                    .get("message_id")
                    .and_then(|value| value.as_str())
                    == Some("message-1")
        }));
    }

    #[test]
    fn pubsub_error_events_include_required_error_fields() {
        let trace_id = trace_id("dispatch-error");
        emit_task_publish_failed_event(
            &trace_id,
            "attempt-error-1",
            "tinyllama-1b",
            DIRECT_INF_TOPIC,
            0,
            256,
            "insufficient peers",
            Some("peer-target-err"),
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "task_publish_failed")
            .expect("task_publish_failed entry not found");

        assert_eq!(
            entry.error_code.as_deref(),
            Some(TASK_DISPATCH_UNCONFIRMED_001)
        );
        assert_eq!(
            entry
                .fields
                .get("error_kind")
                .and_then(|value| value.as_str()),
            Some("publish_error")
        );
        assert_eq!(
            entry
                .fields
                .get("recoverable")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
    }
}
