use super::*;

async fn record_worker_success(
    registry: &SharedNodeRegistry,
    task_id: &str,
    worker_peer_id: &str,
    model_id: Option<&str>,
    latency_ms: u64,
) {
    if let Some(health) = registry
        .write()
        .await
        .record_success(worker_peer_id, latency_ms)
    {
        log_health_update(task_id, worker_peer_id, model_id, &health, None);
    }
}

async fn record_worker_failure(
    registry: &SharedNodeRegistry,
    task_id: &str,
    worker_peer_id: &str,
    model_id: Option<&str>,
    error_code: &'static str,
) {
    if let Some(health) = registry.write().await.record_failure(worker_peer_id) {
        log_health_update(task_id, worker_peer_id, model_id, &health, Some(error_code));
    }
}

pub(super) fn clear_stream_state(
    token_buffer: &mut HashMap<u32, String>,
    next_token_idx: &mut u32,
    rendered_output: &mut String,
) {
    token_buffer.clear();
    *next_token_idx = 0;
    rendered_output.clear();
}

pub(super) struct LateResultLifecycleInput<'a> {
    pub(super) task_id: &'a str,
    pub(super) attempt_id: &'a str,
    pub(super) worker_peer_id: &'a str,
    pub(super) model_id: Option<&'a str>,
    pub(super) success: bool,
    pub(super) output: &'a str,
    pub(super) execution_ms: u64,
}

pub(super) async fn apply_late_result_lifecycle(
    input: LateResultLifecycleInput<'_>,
    attempt_watchdogs: &mut HashMap<String, AttemptWatchdog>,
    registry: &SharedNodeRegistry,
) {
    let LateResultLifecycleInput {
        task_id,
        attempt_id,
        worker_peer_id,
        model_id,
        success,
        output,
        execution_ms,
    } = input;

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
        let _ = watchdog.transition_state_with_event(
            AttemptLifecycleState::LateCompleted,
            Some(worker_peer_id),
        );
    }

    if success && should_print_result_output(output) {
        record_worker_success(registry, task_id, worker_peer_id, model_id, execution_ms).await;
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
    record_worker_success(registry, task_id, worker_peer_id, model_id, latency_ms).await;
    if let Some(watchdog) = attempt_watchdogs.get_mut(attempt_id) {
        let _ = watchdog
            .transition_state_with_event(AttemptLifecycleState::Completed, Some(worker_peer_id));
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

pub(super) struct ResultFailureInput<'a> {
    pub(super) task_id: &'a str,
    pub(super) attempt_id: &'a str,
    pub(super) worker_peer_id: &'a str,
    pub(super) reason: &'a str,
    pub(super) output: &'a str,
    pub(super) failure_kind: FailureKind,
    pub(super) failure_error_kind: &'a str,
    pub(super) model_id: Option<String>,
}

pub(super) struct ResultFailureLifecycleContext<'a> {
    pub(super) distributed_infer_state: &'a mut Option<DistributedInferState>,
    pub(super) attempt_watchdogs: &'a mut HashMap<String, AttemptWatchdog>,
    pub(super) task_manager: &'a Arc<TaskManager>,
    pub(super) pending_inference: &'a mut HashMap<String, tokio::time::Instant>,
    pub(super) token_buffer: &'a mut HashMap<u32, String>,
    pub(super) next_token_idx: &'a mut u32,
    pub(super) rendered_output: &'a mut String,
    pub(super) infer_request_id: &'a mut Option<String>,
    pub(super) infer_broadcast_sent: &'a mut bool,
    pub(super) waiting_for_response: &'a mut bool,
    pub(super) infer_started_at: Option<tokio::time::Instant>,
}

pub(super) async fn apply_result_failure_lifecycle(
    input: ResultFailureInput<'_>,
    registry: &SharedNodeRegistry,
    ctx: &mut ResultFailureLifecycleContext<'_>,
) -> bool {
    let ResultFailureInput {
        task_id,
        attempt_id,
        worker_peer_id,
        reason,
        output,
        failure_kind,
        failure_error_kind,
        model_id,
    } = input;

    if let Some(watchdog) = ctx.attempt_watchdogs.get_mut(attempt_id) {
        let _ = watchdog
            .transition_state_with_event(AttemptLifecycleState::Failed, Some(worker_peer_id));
    }

    record_worker_failure(
        registry,
        task_id,
        worker_peer_id,
        model_id.as_deref(),
        NODE_UNHEALTHY_002,
    )
    .await;

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

    let retry_target = if let Some(infer_state) = ctx.distributed_infer_state.as_ref() {
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

    ctx.task_manager.fail(task_id, reason).await;
    ctx.pending_inference.remove(attempt_id);
    clear_stream_state(ctx.token_buffer, ctx.next_token_idx, ctx.rendered_output);

    if let Some(infer_state) = ctx.distributed_infer_state.as_mut() {
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
            *ctx.infer_request_id = Some(infer_state.current_request_id.clone());
            *ctx.infer_broadcast_sent = false;
            *ctx.waiting_for_response = false;
            return true;
        }

        let (trace, aggregate_metrics) =
            finalize_distributed_task_observability(&trace_task_id, ctx.infer_started_at, true);
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
