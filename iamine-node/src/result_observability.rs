use crate::broadcast_protocol::broadcast_payload_preview;
use crate::{log_observability_event, RESULTS_TOPIC};
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use serde_json::Map;

pub(crate) fn should_print_result_output(output: &str) -> bool {
    !output.trim().is_empty()
}

pub(crate) fn emit_worker_task_completed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    result_size: usize,
    success: bool,
    worker_peer_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "task_completed",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("result_size".to_string(), (result_size as u64).into());
            fields.insert("success".to_string(), success.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_result_published_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    result_size: usize,
    message_id: Option<&str>,
    worker_peer_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "result_published",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("result_size".to_string(), (result_size as u64).into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            if let Some(message_id) = message_id {
                fields.insert("message_id".to_string(), message_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_result_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    from_peer: &str,
    latency_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "result_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("from_peer".to_string(), from_peer.into());
            fields.insert("worker_peer_id".to_string(), from_peer.into());
            fields.insert("latency_ms".to_string(), latency_ms.into());
            fields
        },
    );
}

pub(crate) fn emit_late_result_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    model_id: Option<&str>,
    elapsed_ms: u64,
    accepted: bool,
    reason: &str,
    prior_attempt_state: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "late_result_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields.insert("accepted".to_string(), accepted.into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert(
                "prior_attempt_state".to_string(),
                prior_attempt_state.into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_retry_result_accepted_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    accepted_after_current_moved: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "retry_result_accepted",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert(
                "accepted_after_current_moved".to_string(),
                accepted_after_current_moved.into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_final_outcome_success_event(
    trace_task_id: &str,
    model_id: Option<&str>,
    attempts: &[String],
) {
    log_observability_event(
        LogLevel::Info,
        "final_outcome_success",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("failed".to_string(), false.into());
            fields.insert("attempts".to_string(), serde_json::json!(attempts));
            fields
        },
    );
}

pub(crate) struct BroadcastResultReceived<'a> {
    pub(crate) task_id: &'a str,
    pub(crate) task_type: &'a str,
    pub(crate) worker_peer_id: &'a str,
    pub(crate) assigned_worker_peer_id: Option<&'a str>,
    pub(crate) origin_peer: Option<&'a str>,
    pub(crate) success: bool,
    pub(crate) output: &'a str,
    pub(crate) elapsed_ms: u64,
    pub(crate) transport: &'a str,
    pub(crate) accepted: bool,
    pub(crate) rejection_reason: Option<&'a str>,
}

pub(crate) fn emit_broadcast_result_received_events(result: &BroadcastResultReceived<'_>) {
    for event_name in [
        "task_result_received",
        "broadcast_result_received",
        "result_received",
    ] {
        log_observability_event(
            LogLevel::Info,
            event_name,
            result.task_id,
            Some(result.task_id),
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("task_type".to_string(), result.task_type.into());
                fields.insert("worker_peer_id".to_string(), result.worker_peer_id.into());
                if let Some(assigned_worker_peer_id) = result.assigned_worker_peer_id {
                    fields.insert(
                        "assigned_worker_peer_id".to_string(),
                        assigned_worker_peer_id.into(),
                    );
                }
                if let Some(origin_peer) = result.origin_peer {
                    fields.insert("origin_peer".to_string(), origin_peer.into());
                    fields.insert("controller_peer_id".to_string(), origin_peer.into());
                }
                fields.insert("success".to_string(), result.success.into());
                fields.insert("output".to_string(), result.output.into());
                fields.insert(
                    "output_preview".to_string(),
                    broadcast_payload_preview(result.output).into(),
                );
                fields.insert("elapsed_ms".to_string(), result.elapsed_ms.into());
                fields.insert("transport".to_string(), result.transport.into());
                fields.insert("topic".to_string(), RESULTS_TOPIC.into());
                fields.insert("accepted".to_string(), result.accepted.into());
                if let Some(rejection_reason) = result.rejection_reason {
                    fields.insert("rejection_reason".to_string(), rejection_reason.into());
                }
                fields
            },
        );
    }
}

pub(crate) fn emit_broadcast_result_rejected_event(
    result: &BroadcastResultReceived<'_>,
    reason: &str,
) {
    log_observability_event(
        LogLevel::Warn,
        "broadcast_result_rejected",
        result.task_id,
        Some(result.task_id),
        None,
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), result.task_type.into());
            fields.insert("worker_peer_id".to_string(), result.worker_peer_id.into());
            if let Some(assigned_worker_peer_id) = result.assigned_worker_peer_id {
                fields.insert(
                    "assigned_worker_peer_id".to_string(),
                    assigned_worker_peer_id.into(),
                );
            }
            if let Some(origin_peer) = result.origin_peer {
                fields.insert("origin_peer".to_string(), origin_peer.into());
                fields.insert("controller_peer_id".to_string(), origin_peer.into());
            }
            fields.insert("success".to_string(), result.success.into());
            fields.insert("transport".to_string(), result.transport.into());
            fields.insert("topic".to_string(), RESULTS_TOPIC.into());
            fields.insert("accepted".to_string(), false.into());
            fields.insert("rejection_reason".to_string(), reason.into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_recovery_cancelled_event(task_id: &str, worker_peer_id: &str) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_recovery_cancelled",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("reason".to_string(), "result_accepted".into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_final_outcome_success_event(
    task_id: &str,
    worker_peer_id: &str,
    output: &str,
) {
    for event_name in ["final_outcome", "final_outcome_success"] {
        log_observability_event(
            LogLevel::Info,
            event_name,
            task_id,
            Some(task_id),
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("outcome".to_string(), "success".into());
                fields.insert("failed".to_string(), false.into());
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                fields.insert("output".to_string(), output.into());
                fields.insert(
                    "output_preview".to_string(),
                    broadcast_payload_preview(output).into(),
                );
                fields
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_observability_preserves_event_names() {
        let result = BroadcastResultReceived {
            task_id: "task-result-events",
            task_type: "reverse_string",
            worker_peer_id: "worker-a",
            assigned_worker_peer_id: Some("worker-a"),
            origin_peer: Some("controller"),
            success: true,
            output: "ok",
            elapsed_ms: 42,
            transport: "pubsub",
            accepted: true,
            rejection_reason: None,
        };

        emit_broadcast_result_received_events(&result);
        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();

        for event_name in [
            "task_result_received",
            "broadcast_result_received",
            "result_received",
        ] {
            assert!(entries
                .iter()
                .rev()
                .any(|entry| entry.trace_id == "task-result-events" && entry.event == event_name));
        }
    }

    #[test]
    fn final_outcome_success_preserves_fields() {
        emit_broadcast_final_outcome_success_event("task-final-success", "worker-a", "output");
        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| {
                entry.trace_id == "task-final-success" && entry.event == "final_outcome_success"
            })
            .expect("final_outcome_success should be present");

        assert_eq!(
            entry.fields.get("outcome").and_then(|value| value.as_str()),
            Some("success")
        );
        assert_eq!(
            entry.fields.get("failed").and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            entry
                .fields
                .get("worker_peer_id")
                .and_then(|value| value.as_str()),
            Some("worker-a")
        );
    }

    #[test]
    fn broadcast_result_rejected_preserves_rejection_reason() {
        let result = BroadcastResultReceived {
            task_id: "task-result-rejected",
            task_type: "reverse_string",
            worker_peer_id: "wrong-worker",
            assigned_worker_peer_id: Some("worker-a"),
            origin_peer: Some("controller"),
            success: true,
            output: "wrong",
            elapsed_ms: 5,
            transport: "pubsub",
            accepted: false,
            rejection_reason: Some("wrong_worker"),
        };

        emit_broadcast_result_rejected_event(&result, "wrong_worker");
        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| {
                entry.trace_id == "task-result-rejected"
                    && entry.event == "broadcast_result_rejected"
            })
            .expect("broadcast_result_rejected should be present");

        assert_eq!(
            entry
                .fields
                .get("rejection_reason")
                .and_then(|value| value.as_str()),
            Some("wrong_worker")
        );
    }

    #[test]
    fn result_acceptance_preserves_success_output() {
        let result = BroadcastResultReceived {
            task_id: "task-output",
            task_type: "reverse_string",
            worker_peer_id: "worker-a",
            assigned_worker_peer_id: Some("worker-a"),
            origin_peer: Some("controller"),
            success: true,
            output: "abc",
            elapsed_ms: 1,
            transport: "pubsub",
            accepted: true,
            rejection_reason: None,
        };

        assert!(result.success);
        assert_eq!(result.output, "abc");
        assert!(should_print_result_output(result.output));
    }
}
