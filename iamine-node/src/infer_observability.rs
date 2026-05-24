use crate::infer_watchdog::{is_unclaimed_worker_peer_id, AttemptClaimSource, AttemptDispatchType};
use crate::pubsub_topics::{RESULTS_TOPIC, TASK_TOPIC};
use crate::{log_observability_event, IamineBehaviour};
use iamine_network::{
    default_semantic_log_path, LogLevel, SemanticFeedbackEngine,
    ValidationResult as SemanticValidationResult, TASK_DISPATCH_UNCONFIRMED_001, TASK_TIMEOUT_001,
};
use libp2p::{gossipsub, swarm::Swarm};
use serde_json::{Map, Value};

pub(crate) fn record_semantic_feedback(prompt: &str, validation: &SemanticValidationResult) {
    let engine = SemanticFeedbackEngine::default();
    if let Err(error) = engine.append_from_validation(prompt, validation) {
        eprintln!("[Feedback] Logging failed: {}", error);
    } else {
        println!(
            "[Feedback] Logged: {}",
            default_semantic_log_path().display()
        );
    }
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

pub(crate) fn emit_retry_counted_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    reason: &str,
    task_retry_count: u32,
) {
    log_observability_event(
        LogLevel::Info,
        "retry_counted",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert("task_retry_count".to_string(), task_retry_count.into());
            fields
        },
    );
}

pub(crate) fn emit_fallback_counted_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    reason: &str,
    task_fallback_count: u32,
) {
    log_observability_event(
        LogLevel::Info,
        "fallback_counted",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert(
                "task_fallback_count".to_string(),
                task_fallback_count.into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_fallback_attempt_created_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    candidates: &[String],
) {
    log_observability_event(
        LogLevel::Info,
        "fallback_attempt_created",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert(
                "attempt_type".to_string(),
                AttemptDispatchType::FallbackBroadcast.as_str().into(),
            );
            fields.insert("worker_peer_id".to_string(), Value::Null);
            fields.insert("claimable".to_string(), true.into());
            fields.insert("broadcast_target".to_string(), "topic_mesh".into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("candidates".to_string(), serde_json::json!(candidates));
            fields
        },
    );
}

pub(crate) fn emit_fallback_attempt_claimed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    previous_worker_peer_id: &str,
    claim_source: AttemptClaimSource,
) {
    log_observability_event(
        LogLevel::Info,
        "fallback_attempt_claimed",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            if is_unclaimed_worker_peer_id(previous_worker_peer_id) {
                fields.insert("previous_worker_peer_id".to_string(), Value::Null);
            } else {
                fields.insert(
                    "previous_worker_peer_id".to_string(),
                    previous_worker_peer_id.into(),
                );
            }
            fields.insert("claim_source".to_string(), claim_source.as_str().into());
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

pub(crate) fn emit_attempt_progress_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
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
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
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

fn remote_progress_stage_event(stage: &str) -> Option<&'static str> {
    match stage {
        "model_loading" | "model_load_started" => Some("remote_model_loading_progress"),
        "inference_warmup" => Some("remote_inference_warmup_progress"),
        "still_running" => Some("remote_still_running"),
        _ => None,
    }
}

pub(crate) fn emit_remote_progress_published_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
    published_topics: &[&str],
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_published",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields.insert("topics".to_string(), serde_json::json!(published_topics));
            if let Some(tokens_generated_count) = tokens_generated_count {
                fields.insert(
                    "tokens_generated_count".to_string(),
                    tokens_generated_count.into(),
                );
            }
            fields
        },
    );

    if let Some(event_name) = remote_progress_stage_event(stage) {
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
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                fields.insert("stage".to_string(), stage.into());
                fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
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
}

pub(crate) fn emit_remote_progress_publish_attempt_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    topic: &str,
    elapsed_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_publish_attempt",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields
        },
    );
}

pub(crate) fn emit_remote_progress_publish_success_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    topic: &str,
    elapsed_ms: u64,
    message_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_publish_success",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields.insert("message_id".to_string(), message_id.into());
            fields
        },
    );
}

pub(crate) fn emit_remote_progress_client_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    stage: &str,
    topic: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_client_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
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

pub(crate) fn emit_remote_result_client_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    topic_or_protocol: &str,
    elapsed_ms: u64,
    success: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_result_client_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("transport".to_string(), topic_or_protocol.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields.insert("success".to_string(), success.into());
            fields
        },
    );
}

fn remote_progress_status(stage: &str, tokens_generated_count: Option<u64>) -> &'static str {
    if matches!(
        stage,
        "inference_finished" | "result_serialized" | "result_published"
    ) {
        "completed"
    } else if matches!(stage, "model_loading" | "model_load_started") {
        "preparing_model"
    } else if matches!(
        stage,
        "model_loaded" | "model_load_completed" | "inference_warmup"
    ) {
        "warming_up"
    } else if tokens_generated_count.unwrap_or_default() > 0
        || matches!(stage, "first_token_generated" | "tokens_generated_count")
    {
        "generating"
    } else {
        "thinking"
    }
}

pub(crate) fn format_remote_progress_line(
    worker_peer_id: &str,
    attempt_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) -> String {
    let elapsed_secs = elapsed_ms / 1_000;
    match remote_progress_status(stage, tokens_generated_count) {
        "completed" => format!(
            "[Remote] worker={} attempt={} status=completed tokens={}",
            worker_peer_id,
            attempt_id,
            tokens_generated_count.unwrap_or_default()
        ),
        "generating" => format!(
            "[Remote] worker={} attempt={} status=generating elapsed={}s tokens={}",
            worker_peer_id,
            attempt_id,
            elapsed_secs,
            tokens_generated_count.unwrap_or_default()
        ),
        "preparing_model" => format!(
            "[Remote] worker={} attempt={} status=preparing_model elapsed={}s stage={}",
            worker_peer_id, attempt_id, elapsed_secs, stage
        ),
        "warming_up" => format!(
            "[Remote] worker={} attempt={} status=warming_up elapsed={}s stage={}",
            worker_peer_id, attempt_id, elapsed_secs, stage
        ),
        _ => format!(
            "[Remote] worker={} attempt={} status=thinking elapsed={}s stage={}",
            worker_peer_id, attempt_id, elapsed_secs, stage
        ),
    }
}

pub(crate) fn elapsed_ms_since(started_at: tokio::time::Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
}

pub(crate) fn emit_attempt_progress_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_progress_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
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

pub(crate) fn emit_attempt_progress_recovered_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_progress_recovered",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields
        },
    );
}

pub(crate) fn emit_watchdog_reset_on_progress_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    stage: &str,
    deadline_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "watchdog_reset_on_progress",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("deadline_ms".to_string(), deadline_ms.into());
            fields
        },
    );
}

pub(crate) fn publish_worker_progress_message(
    swarm: &mut Swarm<IamineBehaviour>,
    task_id: &str,
    attempt_id: &str,
    request_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) {
    let payload = serde_json::json!({
        "type": "InferenceProgress",
        "task_id": task_id,
        "attempt_id": attempt_id,
        "request_id": request_id,
        "model_id": model_id,
        "worker_peer": worker_peer_id,
        "worker_peer_id": worker_peer_id,
        "stage": stage,
        "elapsed_ms": elapsed_ms,
        "tokens_generated_count": tokens_generated_count,
    });
    let mut published_topics = Vec::new();
    for topic in [RESULTS_TOPIC, TASK_TOPIC] {
        emit_remote_progress_publish_attempt_event(
            task_id,
            attempt_id,
            model_id,
            worker_peer_id,
            stage,
            topic,
            elapsed_ms,
        );
        match swarm.behaviour_mut().gossipsub.publish(
            gossipsub::IdentTopic::new(topic),
            serde_json::to_vec(&payload).unwrap_or_default(),
        ) {
            Ok(message_id) => {
                let message_id = message_id.to_string();
                published_topics.push(topic);
                emit_remote_progress_publish_success_event(
                    task_id,
                    attempt_id,
                    model_id,
                    worker_peer_id,
                    stage,
                    topic,
                    elapsed_ms,
                    &message_id,
                );
            }
            Err(error) => log_observability_event(
                LogLevel::Warn,
                "remote_progress_publish_failed",
                task_id,
                Some(task_id),
                Some(model_id),
                Some(TASK_DISPATCH_UNCONFIRMED_001),
                {
                    let mut fields = Map::new();
                    fields.insert("attempt_id".to_string(), attempt_id.into());
                    fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                    fields.insert("topic".to_string(), topic.into());
                    fields.insert("stage".to_string(), stage.into());
                    fields.insert("error".to_string(), error.to_string().into());
                    fields
                },
            ),
        }
    }
    emit_remote_progress_published_event(
        task_id,
        attempt_id,
        model_id,
        worker_peer_id,
        stage,
        elapsed_ms,
        tokens_generated_count,
        &published_topics,
    );
    emit_attempt_progress_event(
        task_id,
        attempt_id,
        model_id,
        worker_peer_id,
        stage,
        elapsed_ms,
        tokens_generated_count,
    );
}
