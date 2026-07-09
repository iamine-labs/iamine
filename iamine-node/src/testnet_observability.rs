use serde_json::{Map, Value};

const TESTNET_OBSERVABILITY_SCHEMA_VERSION: &str = "1.0.0";
const TESTNET_OBSERVABILITY_SCOPE: &str = "private_testnet";

pub(crate) fn enrich_testnet_observability_fields(event: &str, fields: &mut Map<String, Value>) {
    let Some(phase) = testnet_observability_phase_for_event(event) else {
        return;
    };
    insert_if_missing(fields, "testnet_phase", phase);
    insert_if_missing(
        fields,
        "testnet_observability_scope",
        TESTNET_OBSERVABILITY_SCOPE,
    );
    insert_if_missing(
        fields,
        "testnet_observability_schema_version",
        TESTNET_OBSERVABILITY_SCHEMA_VERSION,
    );
}

fn insert_if_missing(fields: &mut Map<String, Value>, key: &str, value: &str) {
    if !fields.contains_key(key) {
        fields.insert(key.to_string(), value.into());
    }
}

fn testnet_observability_phase_for_event(event: &str) -> Option<&'static str> {
    if is_testnet_admission_event(event) {
        Some("admission")
    } else if is_testnet_routing_event(event) {
        Some("routing")
    } else if is_testnet_execution_event(event) {
        Some("execution")
    } else if is_testnet_recovery_event(event) {
        Some("recovery")
    } else if is_testnet_result_delivery_event(event) {
        Some("result_delivery")
    } else if is_testnet_health_event(event) {
        Some("health")
    } else {
        None
    }
}

fn is_testnet_admission_event(event: &str) -> bool {
    matches!(
        event,
        "p2p_protocol_identify_failed"
            | "p2p_protocol_version_checked"
            | "remote_inference_api_rejected"
            | "testnet_admission_peer_rejected"
    )
}

fn is_testnet_routing_event(event: &str) -> bool {
    event.starts_with("broadcast_readiness_")
        || event.starts_with("broadcast_task_offer_")
        || matches!(
            event,
            "broadcast_bid_received"
                | "broadcast_task_assign_published"
                | "broadcast_topic_subscriber_seen"
                | "dispatch_deduplicated_inflight"
                | "node_rejected"
                | "observed_peer_subscription"
                | "progress_topic_ready"
                | "result_topic_ready"
                | "scheduler_candidate_rejected"
                | "scheduler_candidates_built"
                | "scheduler_decision_recorded"
                | "scheduler_no_compatible_worker"
                | "scheduler_node_selected"
                | "scheduler_worker_selected"
                | "task_dispatch_context"
                | "task_dispatch_readiness_failed"
                | "task_publish_attempt"
                | "task_publish_failed"
                | "task_published"
        )
}

fn is_testnet_execution_event(event: &str) -> bool {
    event.starts_with("attempt_progress")
        || event.starts_with("remote_progress_")
        || matches!(
            event,
            "direct_inference_request_received"
                | "task_message_received"
                | "task_received"
                | "watchdog_reset_on_progress"
        )
}

fn is_testnet_recovery_event(event: &str) -> bool {
    event.starts_with("attempt_timeout_")
        || event.starts_with("fallback_")
        || matches!(
            event,
            "attempt_stalled"
                | "attempt_state_changed"
                | "broadcast_recovery_cancelled"
                | "retry_counted"
                | "retry_scheduled"
                | "task_failed"
                | "task_timeout"
        )
}

fn is_testnet_result_delivery_event(event: &str) -> bool {
    event.starts_with("broadcast_result_")
        || event.starts_with("task_result_")
        || matches!(
            event,
            "final_outcome"
                | "final_outcome_success"
                | "final_trace_summary_constructed"
                | "late_result_received"
                | "remote_result_client_received"
                | "result_received"
                | "retry_result_accepted"
                | "task_completed"
                | "worker_result_published"
        )
}

fn is_testnet_health_event(event: &str) -> bool {
    event.starts_with("cluster_")
        || matches!(
            event,
            "health_update" | "node_blacklisted" | "node_degraded" | "node_recovered"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testnet_observability_phase_markers_cover_private_testnet_flow() {
        let cases = [
            ("testnet_admission_peer_rejected", "admission"),
            ("scheduler_worker_selected", "routing"),
            ("remote_progress_published", "execution"),
            ("task_failed", "recovery"),
            ("task_result_received", "result_delivery"),
            ("cluster_node_health_changed", "health"),
        ];

        for (event, expected) in cases {
            assert_eq!(testnet_observability_phase_for_event(event), Some(expected));
        }
        assert_eq!(
            testnet_observability_phase_for_event("diagnostic_only_event"),
            None
        );
    }

    #[test]
    fn testnet_observability_enrichment_preserves_existing_fields() {
        let mut fields = Map::new();
        fields.insert("testnet_phase".to_string(), "custom_phase".into());
        fields.insert("attempt_id".to_string(), "attempt-1".into());

        enrich_testnet_observability_fields("task_completed", &mut fields);

        assert_eq!(
            fields.get("testnet_phase").and_then(Value::as_str),
            Some("custom_phase")
        );
        assert_eq!(
            fields.get("attempt_id").and_then(Value::as_str),
            Some("attempt-1")
        );
        assert_eq!(
            fields
                .get("testnet_observability_scope")
                .and_then(Value::as_str),
            Some("private_testnet")
        );
        assert_eq!(
            fields
                .get("testnet_observability_schema_version")
                .and_then(Value::as_str),
            Some("1.0.0")
        );
    }
}
