use super::*;

pub(super) struct DispatchReadinessError {
    pub(super) code: &'static str,
    pub(super) reason: &'static str,
}

impl DispatchReadinessError {
    pub(super) fn new(code: &'static str, reason: &'static str) -> Self {
        Self { code, reason }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DispatchReadinessSnapshot {
    pub(super) connected_peer_count: usize,
    pub(super) mesh_peer_count: usize,
    pub(super) topic_peer_count: usize,
    pub(super) joined_task_topic: bool,
    pub(super) joined_direct_topic: bool,
    pub(super) joined_results_topic: bool,
    pub(super) selected_topic: &'static str,
}

pub(super) fn topic_hash_string(topic_name: &str) -> String {
    gossipsub::IdentTopic::new(topic_name).hash().to_string()
}

pub(super) fn evaluate_dispatch_readiness(
    snapshot: &DispatchReadinessSnapshot,
) -> Result<(), DispatchReadinessError> {
    if snapshot.connected_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            NETWORK_NO_PUBSUB_PEERS_001,
            "no_connected_peers",
        ));
    }

    if !snapshot.joined_task_topic
        || !snapshot.joined_direct_topic
        || !snapshot.joined_results_topic
    {
        return Err(DispatchReadinessError::new(
            PUBSUB_TOPIC_NOT_READY_001,
            "required_topics_not_joined",
        ));
    }

    if snapshot.mesh_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            NETWORK_NO_PUBSUB_PEERS_001,
            "no_pubsub_mesh_peers",
        ));
    }

    if snapshot.topic_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            PUBSUB_TOPIC_NOT_READY_001,
            "destination_topic_has_zero_peers",
        ));
    }

    Ok(())
}

pub(super) fn should_print_result_output(output: &str) -> bool {
    !output.trim().is_empty()
}

#[cfg(test)]
pub(super) fn apply_retry_fallback_metrics(
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

pub(super) fn emit_dispatch_context_event(
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

pub(super) fn emit_dispatch_readiness_failure_event(
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

pub(super) fn emit_task_publish_attempt_event(
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

pub(super) fn emit_task_published_event(
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

pub(super) fn emit_fallback_attempt_registered_event(
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

pub(super) fn emit_task_publish_failed_event(
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

pub(super) fn emit_worker_task_message_received_event(
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

pub(super) fn emit_direct_inference_request_received_event(
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

pub(super) fn emit_retry_scheduled_event(
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

pub(super) fn emit_worker_task_completed_event(
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

pub(super) fn emit_worker_result_published_event(
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

pub(super) fn emit_result_received_event(
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

pub(super) fn emit_attempt_state_changed_event(
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

pub(super) fn emit_attempt_timeout_extended_event(
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

pub(super) fn emit_attempt_stalled_event(
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

pub(super) fn emit_late_result_received_event(
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

pub(super) fn emit_attempt_progress_event(
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

pub(super) fn publish_worker_progress_message(
    swarm: &mut Swarm<IamineBehaviour>,
    task_id: &str,
    attempt_id: &str,
    request_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    tokens_generated_count: Option<u64>,
) {
    let payload = serde_json::json!({
        "type": "InferenceProgress",
        "task_id": task_id,
        "attempt_id": attempt_id,
        "request_id": request_id,
        "model_id": model_id,
        "worker_peer": worker_peer_id,
        "stage": stage,
        "tokens_generated_count": tokens_generated_count,
    });
    let _ = swarm.behaviour_mut().gossipsub.publish(
        gossipsub::IdentTopic::new(RESULTS_TOPIC),
        serde_json::to_vec(&payload).unwrap_or_default(),
    );
    emit_attempt_progress_event(
        task_id,
        attempt_id,
        model_id,
        worker_peer_id,
        stage,
        tokens_generated_count,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkerInferenceRequestKind {
    InferenceRequest,
    DirectInferenceRequest,
}

impl WorkerInferenceRequestKind {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::InferenceRequest => "InferenceRequest",
            Self::DirectInferenceRequest => "DirectInferenceRequest",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct WorkerInferenceRequestContext {
    pub(super) request_kind: WorkerInferenceRequestKind,
    pub(super) message_topic: String,
    pub(super) from_peer: String,
    pub(super) request_id: String,
    pub(super) task_id: String,
    pub(super) attempt_id: String,
    pub(super) model_id: String,
    pub(super) prompt: String,
    pub(super) max_tokens: u32,
    pub(super) temperature: f32,
}

pub(super) fn build_inference_request_context(
    msg: &serde_json::Value,
    message_topic: &str,
    from_peer: &str,
) -> WorkerInferenceRequestContext {
    let request_id = msg["request_id"].as_str().unwrap_or("").to_string();
    let task_id = msg["task_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(request_id.as_str())
        .to_string();
    let attempt_id = msg["attempt_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(request_id.as_str())
        .to_string();

    WorkerInferenceRequestContext {
        request_kind: WorkerInferenceRequestKind::InferenceRequest,
        message_topic: message_topic.to_string(),
        from_peer: from_peer.to_string(),
        request_id,
        task_id,
        attempt_id,
        model_id: msg["model_id"].as_str().unwrap_or("").to_string(),
        prompt: msg["prompt"].as_str().unwrap_or("").to_string(),
        max_tokens: msg["max_tokens"].as_u64().unwrap_or(200) as u32,
        temperature: msg["temperature"].as_f64().unwrap_or(0.7) as f32,
    }
}

pub(super) fn build_direct_inference_request_context(
    msg: &serde_json::Value,
    message_topic: &str,
    from_peer: &str,
) -> WorkerInferenceRequestContext {
    let request_id = msg["request_id"].as_str().unwrap_or("").to_string();
    let task_id = msg["task_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(request_id.as_str())
        .to_string();
    let attempt_id = msg["attempt_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(request_id.as_str())
        .to_string();

    WorkerInferenceRequestContext {
        request_kind: WorkerInferenceRequestKind::DirectInferenceRequest,
        message_topic: message_topic.to_string(),
        from_peer: from_peer.to_string(),
        request_id,
        task_id,
        attempt_id,
        model_id: msg["model"].as_str().unwrap_or("tinyllama-1b").to_string(),
        prompt: msg["prompt"].as_str().unwrap_or("").to_string(),
        max_tokens: msg["max_tokens"].as_u64().unwrap_or(200) as u32,
        temperature: 0.7,
    }
}

pub(super) async fn run_worker_inference_pipeline(
    swarm: &mut Swarm<IamineBehaviour>,
    request: WorkerInferenceRequestContext,
    peer_id: &PeerId,
    model_storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
    inference_engine: &Arc<RealInferenceEngine>,
    metrics: &Arc<RwLock<NodeMetrics>>,
) {
    let local_model_available = model_storage.has_model(&request.model_id);
    let peer_id_string = peer_id.to_string();
    emit_worker_task_message_received_event(
        &request.task_id,
        &request.attempt_id,
        &request.message_topic,
        &request.from_peer,
        "ok",
        &request.model_id,
        local_model_available,
    );
    emit_direct_inference_request_received_event(
        &request.task_id,
        &request.attempt_id,
        &request.model_id,
        &request.from_peer,
        &peer_id_string,
        local_model_available,
    );
    publish_worker_progress_message(
        swarm,
        &request.task_id,
        &request.attempt_id,
        &request.request_id,
        &request.model_id,
        &peer_id_string,
        "attempt_started",
        None,
    );

    println!(
        "🧠 [Worker] {}: model={} prompt='{}'",
        request.request_kind.label(),
        request.model_id,
        &request.prompt[..request.prompt.len().min(40)]
    );

    if !local_model_available {
        println!("   ⚠️ Modelo {} no instalado — ignorando", request.model_id);
        return;
    }

    if let Some(req) = ModelRequirements::for_model(&request.model_id) {
        if !can_node_run_model(node_caps, &req) {
            println!(
                "   ⚠️ Hardware insuficiente para {} — ignorando",
                request.model_id
            );
            return;
        }
    }

    publish_worker_progress_message(
        swarm,
        &request.task_id,
        &request.attempt_id,
        &request.request_id,
        &request.model_id,
        &peer_id_string,
        "model_load_started",
        None,
    );
    publish_worker_progress_message(
        swarm,
        &request.task_id,
        &request.attempt_id,
        &request.request_id,
        &request.model_id,
        &peer_id_string,
        "model_load_completed",
        None,
    );
    publish_worker_progress_message(
        swarm,
        &request.task_id,
        &request.attempt_id,
        &request.request_id,
        &request.model_id,
        &peer_id_string,
        "inference_started",
        None,
    );

    let engine_ref = Arc::clone(inference_engine);
    let metrics_ref = Arc::clone(metrics);
    let registry_clone = ModelRegistry::new();
    let peer_id_for_inference = peer_id_string.clone();
    let request_id_for_inference = request.request_id.clone();
    let model_id_for_inference = request.model_id.clone();
    let prompt_for_inference = request.prompt.clone();
    let max_tokens = request.max_tokens;
    let temperature = request.temperature;
    let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);

    let inference_handle = tokio::spawn(async move {
        let req = RealInferenceRequest {
            task_id: request_id_for_inference.clone(),
            model_id: model_id_for_inference.clone(),
            prompt: prompt_for_inference,
            max_tokens,
            temperature,
        };
        let daemon_socket = daemon_socket_path();
        let result = if daemon_is_available(&daemon_socket).await {
            match infer_via_daemon(&daemon_socket, req, |token| {
                let _ = token_tx.try_send(token);
            })
            .await
            {
                Ok(response) => response.result,
                Err(e) => {
                    return InferenceTaskResult::failure(
                        request_id_for_inference.clone(),
                        model_id_for_inference.clone(),
                        peer_id_for_inference.clone(),
                        e,
                    );
                }
            }
        } else {
            let eng = Arc::clone(&engine_ref);
            let hash = registry_clone
                .get(&model_id_for_inference)
                .map(|m| m.hash.clone())
                .unwrap_or_default();

            if let Err(e) = eng.load_model(&model_id_for_inference, &hash) {
                return InferenceTaskResult::failure(
                    request_id_for_inference.clone(),
                    model_id_for_inference.clone(),
                    peer_id_for_inference.clone(),
                    e,
                );
            }

            eng.run_inference(req, Some(token_tx)).await
        };

        InferenceTaskResult::success(
            request_id_for_inference,
            model_id_for_inference,
            result.output,
            result.tokens_generated,
            result.truncated,
            result.continuation_steps,
            result.execution_ms,
            peer_id_for_inference,
            result.accelerator_used,
        )
    });

    let mut token_idx = 0u32;
    let mut produced_tokens = 0u64;
    let mut first_token_emitted = false;
    while let Some(token) = token_rx.recv().await {
        let st = StreamedToken {
            request_id: request.request_id.clone(),
            token,
            index: token_idx,
            is_final: false,
        };
        if let Ok(payload) = serde_json::to_vec(&st.to_gossip_json()) {
            let _ = swarm
                .behaviour_mut()
                .gossipsub
                .publish(gossipsub::IdentTopic::new(RESULTS_TOPIC), payload);
        }
        produced_tokens = produced_tokens.saturating_add(1);
        if !first_token_emitted {
            first_token_emitted = true;
            publish_worker_progress_message(
                swarm,
                &request.task_id,
                &request.attempt_id,
                &request.request_id,
                &request.model_id,
                &peer_id_string,
                "first_token_generated",
                Some(produced_tokens),
            );
        } else if produced_tokens % 16 == 0 {
            publish_worker_progress_message(
                swarm,
                &request.task_id,
                &request.attempt_id,
                &request.request_id,
                &request.model_id,
                &peer_id_string,
                "tokens_generated_count",
                Some(produced_tokens),
            );
        }
        token_idx += 1;
    }

    match inference_handle.await {
        Ok(result) => {
            {
                let mut m = metrics_ref.write().await;
                if result.success {
                    m.inference_success(result.execution_ms, result.tokens_generated as u64);
                } else {
                    m.inference_failed();
                }
            }
            publish_worker_progress_message(
                swarm,
                &request.task_id,
                &request.attempt_id,
                &request.request_id,
                &request.model_id,
                &peer_id_string,
                "inference_finished",
                Some(result.tokens_generated as u64),
            );
            let result_size = result.output.len();
            emit_worker_task_completed_event(
                &request.task_id,
                &request.attempt_id,
                &request.model_id,
                result_size,
                result.success,
                &peer_id_string,
            );
            let mut result_payload = result.to_gossip_json();
            if let Some(map) = result_payload.as_object_mut() {
                map.insert("task_id".to_string(), request.task_id.clone().into());
                map.insert("attempt_id".to_string(), request.attempt_id.clone().into());
            }
            publish_worker_progress_message(
                swarm,
                &request.task_id,
                &request.attempt_id,
                &request.request_id,
                &request.model_id,
                &peer_id_string,
                "result_serialized",
                Some(result.tokens_generated as u64),
            );
            let payload_size = result_payload.to_string().len();
            match serde_json::to_vec(&result_payload) {
                Ok(serialized_payload) => {
                    let publish_result = swarm.behaviour_mut().gossipsub.publish(
                        gossipsub::IdentTopic::new(RESULTS_TOPIC),
                        serialized_payload,
                    );
                    match publish_result {
                        Ok(message_id) => {
                            publish_worker_progress_message(
                                swarm,
                                &request.task_id,
                                &request.attempt_id,
                                &request.request_id,
                                &request.model_id,
                                &peer_id_string,
                                "result_published",
                                Some(result.tokens_generated as u64),
                            );
                            emit_worker_result_published_event(
                                &request.task_id,
                                &request.attempt_id,
                                &request.model_id,
                                RESULTS_TOPIC,
                                payload_size,
                                Some(&message_id.to_string()),
                                &peer_id_string,
                            )
                        }
                        Err(error) => log_observability_event(
                            LogLevel::Error,
                            "result_publish_failed",
                            &request.task_id,
                            Some(&request.task_id),
                            Some(&request.model_id),
                            Some(TASK_DISPATCH_UNCONFIRMED_001),
                            {
                                let mut fields = Map::new();
                                fields.insert(
                                    "attempt_id".to_string(),
                                    request.attempt_id.clone().into(),
                                );
                                fields.insert("topic".to_string(), RESULTS_TOPIC.into());
                                fields.insert("error".to_string(), error.to_string().into());
                                fields
                            },
                        ),
                    }
                }
                Err(error) => log_observability_event(
                    LogLevel::Error,
                    "result_publish_failed",
                    &request.task_id,
                    Some(&request.task_id),
                    Some(&request.model_id),
                    Some(TASK_DISPATCH_UNCONFIRMED_001),
                    {
                        let mut fields = Map::new();
                        fields.insert("attempt_id".to_string(), request.attempt_id.clone().into());
                        fields.insert("topic".to_string(), RESULTS_TOPIC.into());
                        fields.insert("error".to_string(), error.to_string().into());
                        fields
                    },
                ),
            }

            println!(
                "✅ [Worker] {} completada: {} tokens en {}ms",
                request.request_kind.label(),
                result.tokens_generated,
                result.execution_ms
            );
        }
        Err(error) => log_observability_event(
            LogLevel::Error,
            "worker_inference_join_failed",
            &request.task_id,
            Some(&request.task_id),
            Some(&request.model_id),
            Some(TASK_FAILED_002),
            {
                let mut fields = Map::new();
                fields.insert("attempt_id".to_string(), request.attempt_id.into());
                fields.insert(
                    "request_kind".to_string(),
                    request.request_kind.label().into(),
                );
                fields.insert("error".to_string(), error.to_string().into());
                fields.insert("recoverable".to_string(), false.into());
                fields
            },
        ),
    }
}
