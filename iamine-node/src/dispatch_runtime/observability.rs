use super::*;

pub(crate) struct DispatchContextEvent<'a> {
    pub(crate) trace_task_id: &'a str,
    pub(crate) attempt_id: &'a str,
    pub(crate) selected_model: &'a str,
    pub(crate) candidates: &'a [String],
    pub(crate) connected_peer_count: usize,
    pub(crate) topic_peer_count: usize,
    pub(crate) selected_topic: &'a str,
    pub(crate) target_peer_id: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub(crate) struct PublishEventContext<'a> {
    pub(crate) trace_task_id: &'a str,
    pub(crate) attempt_id: &'a str,
    pub(crate) model_id: &'a str,
    pub(crate) topic: &'a str,
    pub(crate) publish_peer_count: usize,
    pub(crate) payload_size: usize,
    pub(crate) selected_peer_id: Option<&'a str>,
}

pub(crate) struct LateResultReceivedEvent<'a> {
    pub(crate) trace_task_id: &'a str,
    pub(crate) attempt_id: &'a str,
    pub(crate) worker_peer_id: &'a str,
    pub(crate) model_id: Option<&'a str>,
    pub(crate) elapsed_ms: u64,
    pub(crate) accepted: bool,
    pub(crate) reason: &'a str,
    pub(crate) prior_attempt_state: &'a str,
}

fn insert_attempt_id(fields: &mut Map<String, Value>, attempt_id: &str) {
    fields.insert("attempt_id".to_string(), attempt_id.into());
}

#[cfg(test)]
pub(crate) fn apply_retry_fallback_metrics(
    metrics: &mut DistributedTaskMetrics,
    retry_taken: bool,
    fallback_broadcast: bool,
) {
    if retry_taken {
        metrics.retry_recorded();
    }
    if fallback_broadcast {
        metrics.fallback_recorded();
    }
}

pub(crate) fn emit_dispatch_context_event(event: DispatchContextEvent<'_>) {
    let DispatchContextEvent {
        trace_task_id,
        attempt_id,
        selected_model,
        candidates,
        connected_peer_count,
        topic_peer_count,
        selected_topic,
        target_peer_id,
    } = event;
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
            insert_attempt_id(&mut fields, attempt_id);
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

pub(crate) fn emit_task_published_event(context: PublishEventContext<'_>, message_id: &str) {
    let PublishEventContext {
        trace_task_id,
        attempt_id,
        model_id,
        topic,
        publish_peer_count,
        payload_size,
        selected_peer_id,
    } = context;
    log_observability_event(
        LogLevel::Info,
        "task_published",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            insert_attempt_id(&mut fields, attempt_id);
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

pub(crate) fn emit_fallback_attempt_registered_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    candidates: &[String],
) {
    log_observability_event(
        LogLevel::Info,
        "fallback_attempt_registered",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("attempt_type".to_string(), "fallback".into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("selected_peer_id".to_string(), "-".into());
            fields.insert("broadcast_target".to_string(), "topic_mesh".into());
            fields.insert("candidates".to_string(), serde_json::json!(candidates));
            fields
        },
    );
}

pub(crate) fn emit_task_publish_failed_event(context: PublishEventContext<'_>, error: &str) {
    let PublishEventContext {
        trace_task_id,
        attempt_id,
        model_id,
        topic,
        publish_peer_count,
        payload_size,
        selected_peer_id,
    } = context;
    log_observability_event(
        LogLevel::Error,
        "task_publish_failed",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            insert_attempt_id(&mut fields, attempt_id);
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

pub(crate) fn emit_worker_task_message_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    topic: &str,
    from_peer: &str,
    payload_parse_result: &str,
    selected_model: &str,
    local_model_available: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "task_message_received",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("from_peer".to_string(), from_peer.into());
            fields.insert(
                "payload_parse_result".to_string(),
                payload_parse_result.into(),
            );
            fields.insert(
                "local_model_available".to_string(),
                local_model_available.into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_direct_inference_request_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    selected_model: &str,
    from_peer: &str,
    worker_peer_id: &str,
    local_model_available: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "direct_inference_request_received",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("from_peer".to_string(), from_peer.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert(
                "local_model_available".to_string(),
                local_model_available.into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_retry_scheduled_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    failed_peer_id: Option<&str>,
    selected_peer_id: Option<&str>,
    error_kind: &str,
) {
    log_observability_event(
        LogLevel::Warn,
        "retry_scheduled",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("error_kind".to_string(), error_kind.into());
            fields.insert("recoverable".to_string(), true.into());
            if let Some(failed_peer_id) = failed_peer_id {
                fields.insert("failed_peer_id".to_string(), failed_peer_id.into());
            }
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
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

pub(crate) fn emit_attempt_state_changed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: Option<&str>,
    previous_state: &str,
    new_state: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_state_changed",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("previous_state".to_string(), previous_state.into());
            fields.insert("state".to_string(), new_state.into());
            if let Some(worker_peer_id) = worker_peer_id {
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_attempt_timeout_extended_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: Option<&str>,
    timeout_ms: u64,
    reason: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_timeout_extended",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("timeout_ms".to_string(), timeout_ms.into());
            fields.insert("reason".to_string(), reason.into());
            if let Some(worker_peer_id) = worker_peer_id {
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_attempt_stalled_event(
    trace_task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    model_id: Option<&str>,
    last_progress_stage: &str,
    elapsed_since_last_progress_ms: u64,
) {
    log_observability_event(
        LogLevel::Warn,
        "attempt_stalled",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        Some(TASK_TIMEOUT_001),
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert(
                "last_progress_stage".to_string(),
                last_progress_stage.into(),
            );
            fields.insert(
                "elapsed_since_last_progress_ms".to_string(),
                elapsed_since_last_progress_ms.into(),
            );
            fields.insert("watchdog_reason".to_string(), "no_progress".into());
            fields.insert("error_kind".to_string(), "stall".into());
            fields.insert("recoverable".to_string(), true.into());
            fields
        },
    );
}

pub(crate) fn emit_late_result_received_event(event: LateResultReceivedEvent<'_>) {
    let LateResultReceivedEvent {
        trace_task_id,
        attempt_id,
        worker_peer_id,
        model_id,
        elapsed_ms,
        accepted,
        reason,
        prior_attempt_state,
    } = event;
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

pub(crate) fn emit_attempt_progress_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    tokens_generated_count: Option<u64>,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_progress",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            if let Some(tokens_generated_count) = tokens_generated_count {
                fields.insert(
                    "tokens_generated_count".to_string(),
                    tokens_generated_count.into(),
                );
            }
            fields
        },
    );
}
