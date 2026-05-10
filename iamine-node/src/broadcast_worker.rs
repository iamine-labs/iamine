use crate::broadcast_protocol::{broadcast_payload_preview, BroadcastResultToPublish};
use crate::{log_observability_event, BIDS_TOPIC, RESULTS_TOPIC};
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use serde_json::{Map, Value};

pub(crate) fn should_execute_task_assignment(
    assigned_worker: &str,
    local_peer_id: &str,
    already_executed: bool,
) -> bool {
    assigned_worker == local_peer_id && !already_executed
}

pub(crate) fn emit_broadcast_result_prepare_event(result: &BroadcastResultToPublish) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_result_prepare",
        &result.task_id,
        Some(&result.task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), result.task_type.clone().into());
            fields.insert(
                "worker_peer_id".to_string(),
                result.worker_peer_id.clone().into(),
            );
            fields.insert("origin_peer".to_string(), result.origin_peer.clone().into());
            fields.insert(
                "controller_peer_id".to_string(),
                result.origin_peer.clone().into(),
            );
            fields.insert("success".to_string(), result.success.into());
            fields.insert(
                "output_preview".to_string(),
                broadcast_payload_preview(&result.output).into(),
            );
            fields.insert("elapsed_ms".to_string(), result.elapsed_ms.into());
            fields.insert("transport".to_string(), "pubsub".into());
            fields.insert("topic".to_string(), RESULTS_TOPIC.into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_result_publish_attempt_event(
    result: &BroadcastResultToPublish,
    payload_size: usize,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_result_publish_attempt",
        &result.task_id,
        Some(&result.task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), result.task_type.clone().into());
            fields.insert(
                "worker_peer_id".to_string(),
                result.worker_peer_id.clone().into(),
            );
            fields.insert("origin_peer".to_string(), result.origin_peer.clone().into());
            fields.insert("success".to_string(), result.success.into());
            fields.insert("elapsed_ms".to_string(), result.elapsed_ms.into());
            fields.insert(
                "output_preview".to_string(),
                broadcast_payload_preview(&result.output).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            fields.insert("transport".to_string(), "pubsub".into());
            fields.insert("topic".to_string(), RESULTS_TOPIC.into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_result_published_event(
    result: &BroadcastResultToPublish,
    message_id: &str,
    payload_size: usize,
) {
    for event_name in ["broadcast_result_published", "task_result_published"] {
        log_observability_event(
            LogLevel::Info,
            event_name,
            &result.task_id,
            Some(&result.task_id),
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("task_type".to_string(), result.task_type.clone().into());
                fields.insert(
                    "worker_peer_id".to_string(),
                    result.worker_peer_id.clone().into(),
                );
                fields.insert("origin_peer".to_string(), result.origin_peer.clone().into());
                fields.insert(
                    "controller_peer_id".to_string(),
                    result.origin_peer.clone().into(),
                );
                fields.insert("success".to_string(), result.success.into());
                fields.insert(
                    "output_preview".to_string(),
                    broadcast_payload_preview(&result.output).into(),
                );
                fields.insert("elapsed_ms".to_string(), result.elapsed_ms.into());
                fields.insert("transport".to_string(), "pubsub".into());
                fields.insert("topic".to_string(), RESULTS_TOPIC.into());
                fields.insert("message_id".to_string(), message_id.into());
                fields.insert("payload_size".to_string(), (payload_size as u64).into());
                fields
            },
        );
    }
}

pub(crate) fn emit_broadcast_result_publish_failed_event(
    result: &BroadcastResultToPublish,
    reason: &str,
    recoverable: bool,
) {
    log_observability_event(
        LogLevel::Error,
        "result_publish_failed",
        &result.task_id,
        Some(&result.task_id),
        None,
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert(
                "worker_peer_id".to_string(),
                result.worker_peer_id.clone().into(),
            );
            fields.insert("reason".to_string(), reason.into());
            fields.insert("transport".to_string(), "pubsub".into());
            fields.insert("topic".to_string(), RESULTS_TOPIC.into());
            fields.insert("recoverable".to_string(), recoverable.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_task_offer_received_event(
    task_id: &str,
    task_type: &str,
    data: &str,
    origin_peer: &str,
    from_peer: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "task_offer_received",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), task_type.into());
            fields.insert("data".to_string(), data.into());
            fields.insert("origin_peer".to_string(), origin_peer.into());
            fields.insert("from_peer".to_string(), from_peer.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_task_bid_published_event(
    task_id: &str,
    worker_id: &str,
    message_id: &str,
    available_slots: usize,
) {
    log_observability_event(
        LogLevel::Info,
        "task_bid_published",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("worker_id".to_string(), worker_id.into());
            fields.insert("topic".to_string(), BIDS_TOPIC.into());
            fields.insert("message_id".to_string(), message_id.into());
            fields.insert(
                "available_slots".to_string(),
                (available_slots as u64).into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_worker_task_assign_received_event(
    task_id: &str,
    assigned_worker: &str,
    local_peer_id: &str,
    will_execute: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "task_assign_received",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("assigned_worker".to_string(), assigned_worker.into());
            fields.insert("local_peer_id".to_string(), local_peer_id.into());
            fields.insert("will_execute".to_string(), will_execute.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_topic_subscribed_event(topic: &str, peer_id: &str, backend: &str) {
    log_observability_event(
        LogLevel::Info,
        "worker_topic_subscribed",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("topic".to_string(), topic.into());
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("mode".to_string(), "worker".into());
            fields.insert("backend".to_string(), backend.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_pubsub_ready_event(peer_id: &str, backend: &str, topics: &[&str]) {
    log_observability_event(
        LogLevel::Info,
        "worker_pubsub_ready",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("mode".to_string(), "worker".into());
            fields.insert("backend".to_string(), backend.into());
            fields.insert(
                "topics".to_string(),
                Value::Array(topics.iter().map(|topic| (*topic).into()).collect()),
            );
            fields
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast_protocol::{build_broadcast_task_result_payload, test_broadcast_result};
    use crate::executor::TaskExecutor;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_id(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    #[test]
    fn assigned_worker_publishes_task_result_after_broadcast_execution() {
        let task_id = test_id("broadcast-result-publish");
        let result = test_broadcast_result(&task_id, "assigned-worker");
        let payload = build_broadcast_task_result_payload(&result);

        emit_broadcast_result_prepare_event(&result);
        emit_broadcast_result_publish_attempt_event(&result, payload.to_string().len());
        emit_broadcast_result_published_event(
            &result,
            "message-id-result",
            payload.to_string().len(),
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();

        assert!(entries.iter().rev().any(|entry| {
            entry.trace_id == task_id && entry.event == "broadcast_result_prepare"
        }));
        assert!(entries.iter().rev().any(|entry| {
            entry.trace_id == task_id && entry.event == "broadcast_result_publish_attempt"
        }));
        assert!(entries.iter().rev().any(|entry| {
            entry.trace_id == task_id && entry.event == "broadcast_result_published"
        }));
        assert!(entries
            .iter()
            .rev()
            .any(|entry| { entry.trace_id == task_id && entry.event == "task_result_published" }));
    }

    #[test]
    fn broadcast_result_output_matches_reverse_string() {
        let response = TaskExecutor::execute_task(
            "broadcast-result-output".to_string(),
            "reverse_string".to_string(),
            "qa-broadcast-result-test".to_string(),
        );

        assert!(response.success);
        assert_eq!(response.result, "tset-tluser-tsacdaorb-aq");
    }

    #[test]
    fn only_assigned_worker_executes_task() {
        assert!(should_execute_task_assignment(
            "worker-peer",
            "worker-peer",
            false
        ));
        assert!(!should_execute_task_assignment(
            "worker-peer",
            "worker-peer",
            true
        ));
    }

    #[test]
    fn non_assigned_worker_ignores_task_assign() {
        assert!(!should_execute_task_assignment(
            "winner-peer",
            "other-peer",
            false
        ));
    }
}
