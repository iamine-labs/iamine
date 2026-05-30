use crate::infer_watchdog::{AttemptDispatchType, AttemptTimeoutPolicy};
use iamine_network::{
    log_structured, LogLevel, NodeHealth, StructuredLogEntry, NODE_BLACKLISTED_001,
};
use serde_json::{Map, Value};
use std::collections::HashMap;

pub(crate) fn log_observability_event(
    level: LogLevel,
    event: &str,
    trace_id: &str,
    task_id: Option<&str>,
    model_id: Option<&str>,
    error_code: Option<&str>,
    fields: Map<String, Value>,
) {
    let mut entry = StructuredLogEntry::new(level, event, trace_id, "-");
    if let Some(task_id) = task_id {
        entry = entry.with_task_id(task_id.to_string());
    }
    if let Some(model_id) = model_id {
        entry = entry.with_model_id(model_id.to_string());
    }
    if let Some(error_code) = error_code {
        entry = entry.with_error_code(error_code.to_string());
    }
    entry.fields = fields;
    let _ = log_structured(entry);
}

fn health_fields(peer_id: &str, health: &NodeHealth) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("peer_id".to_string(), peer_id.into());
    fields.insert(
        "success_rate".to_string(),
        (health.success_rate as f64).into(),
    );
    fields.insert(
        "avg_latency_ms".to_string(),
        (health.avg_latency_ms as f64).into(),
    );
    fields.insert("failure_count".to_string(), health.failure_count.into());
    fields.insert("timeout_count".to_string(), health.timeout_count.into());
    fields.insert("blacklisted".to_string(), health.is_blacklisted().into());
    if let Some(last_success) = health.last_success_timestamp {
        fields.insert("last_success_timestamp".to_string(), last_success.into());
    }
    fields
}

pub(crate) fn log_health_update(
    trace_id: &str,
    peer_id: &str,
    model_id: Option<&str>,
    health: &NodeHealth,
    error_code: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "health_update",
        trace_id,
        None,
        model_id,
        error_code,
        health_fields(peer_id, health),
    );

    if health.is_blacklisted() {
        log_observability_event(
            LogLevel::Warn,
            "node_blacklisted",
            trace_id,
            None,
            model_id,
            Some(NODE_BLACKLISTED_001),
            health_fields(peer_id, health),
        );
    } else if health.last_success_timestamp.is_some() {
        log_observability_event(
            LogLevel::Info,
            "node_recovered",
            trace_id,
            None,
            model_id,
            None,
            health_fields(peer_id, health),
        );
    }
}

fn emit_health_policy_decision_event(
    trace_id: &str,
    peer_id: &str,
    model_id: Option<&str>,
    trigger: &str,
    previous_state: &str,
    health: &NodeHealth,
) {
    let state = health.policy_state();
    let event = match state {
        "blacklisted" => "node_blacklisted",
        "degraded" => "node_degraded",
        "healthy" => "node_recovered",
        _ => "node_health_policy_decision",
    };
    log_observability_event(LogLevel::Info, event, trace_id, None, model_id, None, {
        let mut fields = Map::new();
        fields.insert("peer_id".to_string(), peer_id.into());
        fields.insert("trigger".to_string(), trigger.into());
        fields.insert("previous_state".to_string(), previous_state.into());
        fields.insert("state".to_string(), state.into());
        fields.insert(
            "success_rate".to_string(),
            (health.success_rate as f64).into(),
        );
        fields.insert("timeout_count".to_string(), health.timeout_count.into());
        fields.insert("failure_count".to_string(), health.failure_count.into());
        fields.insert("total_runs".to_string(), health.total_runs.into());
        if let Some(until) = health.blacklisted_until {
            fields.insert("blacklisted_until".to_string(), until.into());
        }
        fields
    });
}

pub(crate) fn record_health_policy_state_transition(
    health_state_tracker: &mut HashMap<String, String>,
    trace_id: &str,
    peer_id: &str,
    model_id: Option<&str>,
    trigger: &str,
    health: &NodeHealth,
) {
    let new_state = health.policy_state().to_string();
    let previous_state = health_state_tracker
        .get(peer_id)
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    if previous_state != new_state {
        emit_health_policy_decision_event(
            trace_id,
            peer_id,
            model_id,
            trigger,
            &previous_state,
            health,
        );
    }
    health_state_tracker.insert(peer_id.to_string(), new_state);
}

pub(crate) fn emit_daemon_started_event(peer_id: &str, socket_path: &str) {
    log_observability_event(
        LogLevel::Info,
        "daemon_started",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("socket_path".to_string(), socket_path.into());
            fields.insert("mode".to_string(), "daemon".into());
            fields
        },
    );
}

pub(crate) fn emit_worker_started_event(
    peer_id: &str,
    worker_port: u16,
    worker_slots: usize,
    resource_policy: Value,
) {
    log_observability_event(
        LogLevel::Info,
        "worker_started",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("port".to_string(), (worker_port as u64).into());
            fields.insert("worker_slots".to_string(), (worker_slots as u64).into());
            fields.insert("resource_policy".to_string(), resource_policy);
            fields
        },
    );
}

pub(crate) fn emit_health_policy_configured_event(
    timeout_blacklist_threshold: u32,
    failure_blacklist_threshold: u32,
) {
    log_observability_event(
        LogLevel::Info,
        "health_policy_configured",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert(
                "timeout_blacklist_threshold".to_string(),
                timeout_blacklist_threshold.into(),
            );
            fields.insert(
                "failure_blacklist_threshold".to_string(),
                failure_blacklist_threshold.into(),
            );
            fields.insert(
                "policy".to_string(),
                "degraded_first_blacklist_after_threshold".into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_dispatch_deduplicated_inflight_event(
    trace_task_id: &str,
    attempt_id: &str,
    selected_peer_id: &str,
    selected_model: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "dispatch_deduplicated_inflight",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            fields.insert("selected_model".to_string(), selected_model.into());
            fields.insert(
                "reason".to_string(),
                "inflight_attempt_same_worker_model".into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_scheduler_node_selected_event(
    trace_task_id: &str,
    model_id: &str,
    selected_peer_id: &str,
    cluster_id: Option<&str>,
    score: f64,
    candidate_models: &[String],
) {
    log_observability_event(
        LogLevel::Info,
        "scheduler_node_selected",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            fields.insert("cluster_id".to_string(), cluster_id.unwrap_or("-").into());
            fields.insert("score".to_string(), score.into());
            fields.insert("reason".to_string(), "high_success_low_latency".into());
            fields.insert(
                "candidate_models".to_string(),
                serde_json::json!(candidate_models),
            );
            fields
        },
    );
}

pub(crate) fn emit_node_rejected_no_compatible_model_event(
    trace_task_id: &str,
    known_nodes: usize,
    candidate_models: &[String],
    error_code: &str,
) {
    log_observability_event(
        LogLevel::Error,
        "node_rejected",
        trace_task_id,
        Some(trace_task_id),
        None,
        Some(error_code),
        {
            let mut fields = Map::new();
            fields.insert("reason".to_string(), "no_compatible_model".into());
            fields.insert("known_nodes".to_string(), (known_nodes as u64).into());
            fields.insert(
                "candidate_models".to_string(),
                serde_json::json!(candidate_models),
            );
            fields
        },
    );
}

pub(crate) fn emit_attempt_timeout_policy_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: Option<&str>,
    dispatch_type: Option<AttemptDispatchType>,
    claimable: Option<bool>,
    policy: &AttemptTimeoutPolicy,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_timeout_policy",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            if let Some(worker_peer_id) = worker_peer_id {
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            } else if dispatch_type.is_some() {
                fields.insert("worker_peer_id".to_string(), Value::Null);
            }
            if let Some(dispatch_type) = dispatch_type {
                fields.insert("attempt_type".to_string(), dispatch_type.as_str().into());
            }
            if let Some(claimable) = claimable {
                fields.insert("claimable".to_string(), claimable.into());
            }
            fields.insert("timeout_ms".to_string(), policy.timeout_ms.into());
            fields.insert(
                "claim_timeout_ms".to_string(),
                policy.claim_timeout_ms.into(),
            );
            fields.insert(
                "first_progress_timeout_ms".to_string(),
                policy.first_progress_timeout_ms.into(),
            );
            fields.insert(
                "model_load_timeout_ms".to_string(),
                policy.model_load_timeout_ms.into(),
            );
            fields.insert(
                "first_token_timeout_ms".to_string(),
                policy.first_token_timeout_ms.into(),
            );
            fields.insert(
                "stall_timeout_ms".to_string(),
                policy.stall_timeout_ms.into(),
            );
            fields.insert("max_wait_ms".to_string(), policy.max_wait_ms.into());
            fields.insert(
                "max_execution_timeout_ms".to_string(),
                policy.max_execution_timeout_ms.into(),
            );
            fields.insert("latency_class".to_string(), policy.latency_class.into());
            fields
        },
    );
}

pub(crate) fn emit_distributed_task_failed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    reason: &str,
    error_kind: &str,
    error_code: &str,
) {
    log_observability_event(
        LogLevel::Error,
        "task_failed",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        Some(error_code),
        {
            let mut fields = Map::new();
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert("retry".to_string(), true.into());
            fields.insert("error_kind".to_string(), error_kind.into());
            fields.insert("recoverable".to_string(), true.into());
            fields
        },
    );
}

pub(crate) struct TaskTimeoutEvent<'a> {
    pub(crate) trace_task_id: &'a str,
    pub(crate) attempt_id: &'a str,
    pub(crate) model_id: &'a str,
    pub(crate) worker_peer_id: Option<&'a str>,
    pub(crate) latency_ms: u64,
    pub(crate) elapsed_since_last_progress_ms: u64,
    pub(crate) adaptive_timeout_ms: u64,
    pub(crate) max_wait_ms: u64,
}

pub(crate) fn emit_task_timeout_event(timeout: &TaskTimeoutEvent<'_>, error_code: &str) {
    log_observability_event(
        LogLevel::Error,
        "task_timeout",
        timeout.trace_task_id,
        Some(timeout.trace_task_id),
        Some(timeout.model_id),
        Some(error_code),
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), timeout.attempt_id.into());
            fields.insert("latency_ms".to_string(), timeout.latency_ms.into());
            fields.insert(
                "elapsed_since_last_progress_ms".to_string(),
                timeout.elapsed_since_last_progress_ms.into(),
            );
            fields.insert(
                "adaptive_timeout_ms".to_string(),
                timeout.adaptive_timeout_ms.into(),
            );
            fields.insert("max_wait_ms".to_string(), timeout.max_wait_ms.into());
            fields.insert("retry".to_string(), true.into());
            fields.insert("error_kind".to_string(), "timeout".into());
            fields.insert("recoverable".to_string(), true.into());
            fields.insert("watchdog_reason".to_string(), "no_progress".into());
            if let Some(worker_peer_id) = timeout.worker_peer_id {
                fields.insert("peer_id".to_string(), worker_peer_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_distributed_task_completed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    latency_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "task_completed",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("latency_ms".to_string(), latency_ms.into());
            fields.insert("success".to_string(), true.into());
            fields
        },
    );
}

pub(crate) fn emit_direct_task_completed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    tokens_generated: u64,
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
            fields.insert("success".to_string(), true.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("tokens_generated".to_string(), tokens_generated.into());
            fields.insert("transport".to_string(), "request_response".into());
            fields
        },
    );
}

pub(crate) fn emit_worker_task_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    peer_id: &str,
    cluster_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "task_received",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("cluster_id".to_string(), cluster_id.unwrap_or("-").into());
            fields
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_timeout_policy() -> AttemptTimeoutPolicy {
        AttemptTimeoutPolicy {
            timeout_ms: 10,
            claim_timeout_ms: 11,
            first_progress_timeout_ms: 12,
            model_load_timeout_ms: 13,
            first_token_timeout_ms: 14,
            stall_timeout_ms: 15,
            extension_step_ms: 16,
            max_wait_ms: 17,
            max_execution_timeout_ms: 18,
            latency_class: "test",
        }
    }

    #[test]
    fn runtime_observability_ndjson_entry_is_parseable() {
        let trace_id = format!("runtime-observability-{}", crate::uuid_simple());
        emit_distributed_task_completed_event(
            &trace_id,
            "attempt-1",
            Some("tinyllama-1b"),
            "worker-a",
            42,
        );
        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "task_completed")
            .expect("task_completed entry not found");

        let encoded = serde_json::to_string(entry).unwrap();
        assert!(serde_json::from_str::<serde_json::Value>(&encoded).is_ok());
        assert_eq!(
            entry.fields.get("worker_peer_id").and_then(Value::as_str),
            Some("worker-a")
        );
    }

    #[test]
    fn timeout_policy_event_preserves_fallback_fields() {
        let trace_id = format!("timeout-policy-{}", crate::uuid_simple());
        emit_attempt_timeout_policy_event(
            &trace_id,
            "attempt-2",
            "mistral-7b",
            None,
            Some(AttemptDispatchType::FallbackBroadcast),
            Some(true),
            &test_timeout_policy(),
        );
        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "attempt_timeout_policy")
            .expect("attempt_timeout_policy entry not found");

        assert_eq!(entry.fields.get("worker_peer_id"), Some(&Value::Null));
        assert_eq!(
            entry.fields.get("attempt_type").and_then(Value::as_str),
            Some("fallback_broadcast")
        );
        assert_eq!(
            entry.fields.get("claimable").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            entry
                .fields
                .get("max_execution_timeout_ms")
                .and_then(Value::as_u64),
            Some(18)
        );
    }

    #[test]
    fn distributed_task_failure_event_preserves_contract_fields() {
        let trace_id = format!("task-failed-{}", crate::uuid_simple());
        emit_distributed_task_failed_event(
            &trace_id,
            "attempt-3",
            Some("tinyllama-1b"),
            "worker-b",
            "invalid output",
            "validation_failure",
            "TASK_FAILED_002",
        );
        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "task_failed")
            .expect("task_failed entry not found");

        assert_eq!(
            entry.fields.get("attempt_id").and_then(Value::as_str),
            Some("attempt-3")
        );
        assert_eq!(
            entry.fields.get("error_kind").and_then(Value::as_str),
            Some("validation_failure")
        );
        assert_eq!(
            entry.fields.get("recoverable").and_then(Value::as_bool),
            Some(true)
        );
    }
}
