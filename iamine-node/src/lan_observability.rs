use serde_json::{Map, Value};

const LAN_OBSERVABILITY_SCHEMA_VERSION: &str = "1.0.0";
const LAN_OBSERVABILITY_SCOPE: &str = "lan_beta";

pub(crate) fn enrich_lan_observability_fields(event: &str, fields: &mut Map<String, Value>) {
    let Some(phase) = lan_observability_phase_for_event(event) else {
        return;
    };
    insert_if_missing(fields, "lan_phase", phase);
    insert_if_missing(fields, "lan_observability_scope", LAN_OBSERVABILITY_SCOPE);
    insert_if_missing(
        fields,
        "lan_observability_schema_version",
        LAN_OBSERVABILITY_SCHEMA_VERSION,
    );
}

fn insert_if_missing(fields: &mut Map<String, Value>, key: &str, value: &str) {
    if !fields.contains_key(key) {
        fields.insert(key.to_string(), value.into());
    }
}

fn lan_observability_phase_for_event(event: &str) -> Option<&'static str> {
    if is_lan_setup_event(event) {
        Some("setup")
    } else if is_lan_dispatch_event(event) {
        Some("dispatch")
    } else if is_lan_execution_event(event) {
        Some("execution")
    } else if is_lan_recovery_event(event) {
        Some("recovery")
    } else if is_lan_result_delivery_event(event) {
        Some("result_delivery")
    } else {
        None
    }
}

fn is_lan_setup_event(event: &str) -> bool {
    event.starts_with("worker_startup_")
        || event.starts_with("worker_model_load_")
        || matches!(
            event,
            "backend_cpu_feature_incompatible"
                | "broadcast_pubsub_ready"
                | "controller_topic_subscribed"
                | "daemon_started"
                | "health_policy_configured"
                | "inference_backend_selected"
                | "worker_listening"
                | "worker_pubsub_ready"
                | "worker_started"
                | "worker_topic_subscribed"
        )
}

fn is_lan_dispatch_event(event: &str) -> bool {
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
                | "scheduler_node_selected"
                | "task_dispatch_context"
                | "task_dispatch_readiness_failed"
                | "task_publish_attempt"
                | "task_publish_failed"
                | "task_published"
        )
}

fn is_lan_execution_event(event: &str) -> bool {
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

fn is_lan_recovery_event(event: &str) -> bool {
    event.starts_with("attempt_timeout_")
        || event.starts_with("fallback_")
        || matches!(
            event,
            "attempt_stalled"
                | "attempt_state_changed"
                | "broadcast_recovery_cancelled"
                | "health_update"
                | "node_blacklisted"
                | "node_degraded"
                | "node_recovered"
                | "retry_counted"
                | "retry_scheduled"
                | "task_failed"
                | "task_timeout"
        )
}

fn is_lan_result_delivery_event(event: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lan_observability_phase_markers_cover_beta_contract_flow() {
        let cases = [
            ("worker_startup_ready", "setup"),
            ("task_dispatch_context", "dispatch"),
            ("direct_inference_request_received", "execution"),
            ("retry_scheduled", "recovery"),
            ("task_result_received", "result_delivery"),
        ];
        for (event, expected) in cases {
            assert_eq!(lan_observability_phase_for_event(event), Some(expected));
        }
        assert_eq!(
            lan_observability_phase_for_event("cluster_status_requested"),
            None
        );
    }

    #[test]
    fn lan_observability_enrichment_preserves_existing_fields() {
        let mut fields = Map::new();
        fields.insert("lan_phase".to_string(), "custom_phase".into());
        fields.insert("attempt_id".to_string(), "attempt-1".into());

        enrich_lan_observability_fields("task_completed", &mut fields);

        assert_eq!(
            fields.get("lan_phase").and_then(Value::as_str),
            Some("custom_phase")
        );
        assert_eq!(
            fields.get("attempt_id").and_then(Value::as_str),
            Some("attempt-1")
        );
        assert_eq!(
            fields
                .get("lan_observability_scope")
                .and_then(Value::as_str),
            Some("lan_beta")
        );
    }
}
