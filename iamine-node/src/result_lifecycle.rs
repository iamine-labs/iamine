use super::*;

pub(super) fn clear_stream_state(
    token_buffer: &mut HashMap<u32, String>,
    next_token_idx: &mut u32,
    rendered_output: &mut String,
) {
    token_buffer.clear();
    *next_token_idx = 0;
    rendered_output.clear();
}

pub(super) async fn apply_late_result_lifecycle(
    task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    model_id: Option<&str>,
    success: bool,
    output: &str,
    execution_ms: u64,
    attempt_watchdogs: &mut HashMap<String, AttemptWatchdog>,
    registry: &SharedNodeRegistry,
) {
    let prior_state = attempt_watchdogs
        .get(attempt_id)
        .map(|watchdog| watchdog.state.as_str().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let elapsed_ms = attempt_watchdogs
        .get(attempt_id)
        .map(|watchdog| watchdog.elapsed_ms())
        .unwrap_or(execution_ms);
    emit_late_result_received_event(
        task_id,
        attempt_id,
        worker_peer_id,
        model_id,
        elapsed_ms,
        false,
        "arrived_after_timeout_policy_ignore",
        &prior_state,
    );
    let _ = record_distributed_task_late_result();
    if let Some(watchdog) = attempt_watchdogs.get_mut(attempt_id) {
        let previous_state = watchdog.state;
        if watchdog.transition_state(AttemptLifecycleState::LateCompleted) {
            emit_attempt_state_changed_event(
                task_id,
                attempt_id,
                Some(&watchdog.model_id),
                Some(worker_peer_id),
                previous_state.as_str(),
                watchdog.state.as_str(),
            );
        }
    }
    if success && should_print_result_output(output) {
        if let Some(health) = registry
            .write()
            .await
            .record_success(worker_peer_id, execution_ms)
        {
            log_health_update(task_id, worker_peer_id, model_id, &health, None);
        }
    }
}

pub(super) async fn apply_result_success_lifecycle(
    task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    model_id: Option<&str>,
    latency_ms: u64,
    registry: &SharedNodeRegistry,
    attempt_watchdogs: &mut HashMap<String, AttemptWatchdog>,
) {
    if let Some(health) = registry
        .write()
        .await
        .record_success(worker_peer_id, latency_ms)
    {
        log_health_update(task_id, worker_peer_id, model_id, &health, None);
    }
    if let Some(watchdog) = attempt_watchdogs.get_mut(attempt_id) {
        let previous_state = watchdog.state;
        if watchdog.transition_state(AttemptLifecycleState::Completed) {
            emit_attempt_state_changed_event(
                task_id,
                attempt_id,
                Some(&watchdog.model_id),
                Some(worker_peer_id),
                previous_state.as_str(),
                watchdog.state.as_str(),
            );
        }
    }
    log_observability_event(
        LogLevel::Info,
        "task_completed",
        task_id,
        Some(task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert(
                "worker_peer_id".to_string(),
                worker_peer_id.to_string().into(),
            );
            fields.insert("attempt_id".to_string(), attempt_id.to_string().into());
            fields.insert("latency_ms".to_string(), latency_ms.into());
            fields.insert("success".to_string(), true.into());
            fields
        },
    );
}

pub(super) async fn apply_result_failure_lifecycle(
    task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    reason: &str,
    output: &str,
    failure_kind: FailureKind,
    failure_error_kind: &str,
    model_id: Option<String>,
    registry: &SharedNodeRegistry,
    distributed_infer_state: &mut Option<DistributedInferState>,
    attempt_watchdogs: &mut HashMap<String, AttemptWatchdog>,
    task_manager: &Arc<TaskManager>,
    pending_inference: &mut HashMap<String, tokio::time::Instant>,
    token_buffer: &mut HashMap<u32, String>,
    next_token_idx: &mut u32,
    rendered_output: &mut String,
    infer_request_id: &mut Option<String>,
    infer_broadcast_sent: &mut bool,
    waiting_for_response: &mut bool,
    infer_started_at: Option<tokio::time::Instant>,
) -> bool {
    if let Some(watchdog) = attempt_watchdogs.get_mut(attempt_id) {
        let previous_state = watchdog.state;
        if watchdog.transition_state(AttemptLifecycleState::Failed) {
            emit_attempt_state_changed_event(
                task_id,
                attempt_id,
                Some(&watchdog.model_id),
                Some(worker_peer_id),
                previous_state.as_str(),
                watchdog.state.as_str(),
            );
        }
    }

    if let Some(health) = registry.write().await.record_failure(worker_peer_id) {
        log_health_update(
            task_id,
            worker_peer_id,
            model_id.as_deref(),
            &health,
            Some(NODE_UNHEALTHY_002),
        );
    }

    let error_code = if output.trim().is_empty() {
        TASK_EMPTY_RESULT_003
    } else {
        TASK_FAILED_002
    };
    log_observability_event(
        LogLevel::Error,
        "task_failed",
        task_id,
        Some(task_id),
        model_id.as_deref(),
        Some(error_code),
        {
            let mut fields = Map::new();
            fields.insert(
                "worker_peer_id".to_string(),
                worker_peer_id.to_string().into(),
            );
            fields.insert("attempt_id".to_string(), attempt_id.to_string().into());
            fields.insert("reason".to_string(), reason.to_string().into());
            fields.insert("retry".to_string(), true.into());
            fields.insert("error_kind".to_string(), failure_error_kind.into());
            fields.insert("recoverable".to_string(), true.into());
            fields
        },
    );

    let retry_target = if let Some(infer_state) = distributed_infer_state.as_ref() {
        let mut preview_retry = infer_state.retry_state.clone();
        preview_retry.record_failure(Some(worker_peer_id), infer_state.current_model.as_deref());
        let reg = registry.read().await;
        select_retry_target(
            &reg,
            &IntelligentScheduler::new(),
            &infer_state.candidate_models,
            infer_state.local_cluster_id.as_deref(),
            &preview_retry,
        )
    } else {
        None
    };

    task_manager.fail(task_id, reason).await;
    pending_inference.remove(attempt_id);
    clear_stream_state(token_buffer, next_token_idx, rendered_output);

    if let Some(infer_state) = distributed_infer_state.as_mut() {
        let trace_task_id = infer_state.trace_task_id.clone();
        let failed_model = infer_state.current_model.clone();
        if infer_state.schedule_retry(Some(worker_peer_id), failed_model.as_deref())
            && retry_target.is_some()
        {
            emit_retry_scheduled_event(
                &trace_task_id,
                &infer_state.current_request_id,
                failed_model.as_deref(),
                Some(worker_peer_id),
                retry_target.as_ref().map(|target| target.peer_id.as_str()),
                failure_error_kind,
            );
            if let Some(target) = retry_target {
                println!(
                    "[Retry] task_id={} attempt_id={} kind={:?} peer_id={} model_id={}",
                    trace_task_id,
                    infer_state.current_request_id,
                    failure_kind,
                    target.peer_id,
                    target.model_id
                );
            } else {
                println!(
                    "[Retry] task_id={} attempt_id={} kind={:?}",
                    trace_task_id, infer_state.current_request_id, failure_kind
                );
            }
            *infer_request_id = Some(infer_state.current_request_id.clone());
            *infer_broadcast_sent = false;
            *waiting_for_response = false;
            return true;
        }

        let (trace, aggregate_metrics) =
            finalize_distributed_task_observability(&trace_task_id, infer_started_at, true);
        if let Some(trace) = trace {
            println!(
                "[Metrics] latency={} retries={} fallbacks={}",
                trace.total_latency_ms, trace.retries, trace.fallbacks
            );
        }
        println!(
            "[Metrics] totals: tasks={} failed={} avg_latency_ms={:.1}",
            aggregate_metrics.total_tasks,
            aggregate_metrics.failed_tasks,
            aggregate_metrics.avg_latency_ms
        );
    }

    false
}
