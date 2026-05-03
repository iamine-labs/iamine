use super::*;
use event_loop_control::EventLoopDirective;

pub(crate) struct TimeoutPhaseContext<'a> {
    pub(crate) mode: &'a NodeMode,
    pub(crate) infer_runtime: &'a mut InferRuntimeState,
    pub(crate) registry: &'a SharedNodeRegistry,
    pub(crate) task_manager: &'a Arc<TaskManager>,
    pub(crate) client_state: &'a mut ClientRuntimeState,
}

struct TimeoutContext {
    trace_task_id: String,
    attempt_id: String,
    worker_peer_id: String,
    model_id: String,
    elapsed_ms: u64,
    elapsed_since_progress_ms: u64,
    adaptive_timeout_ms: u64,
    max_wait_ms: u64,
}

pub(crate) async fn handle_timeout_phase(
    ctx: TimeoutPhaseContext<'_>,
) -> Result<EventLoopDirective, Box<dyn Error>> {
    let TimeoutPhaseContext {
        mode,
        infer_runtime,
        registry,
        task_manager,
        client_state,
    } = ctx;

    // Timeout total de inferencia distribuida
    if matches!(mode, NodeMode::Infer { .. }) {
        if let Some(rid) = infer_runtime.infer_request_id.clone() {
            if infer_runtime.pending_inference.contains_key(&rid) {
                let mut timeout_context: Option<TimeoutContext> = None;
                let mut should_retry = false;
                if let Some(watchdog) = infer_runtime.attempt_watchdogs.get_mut(&rid) {
                    match watchdog.check() {
                        WatchdogCheck::Healthy => {}
                        WatchdogCheck::Extended => {
                            emit_attempt_timeout_extended_event(
                                &watchdog.task_id,
                                &watchdog.attempt_id,
                                Some(&watchdog.model_id),
                                Some(&watchdog.worker_peer_id),
                                watchdog
                                    .deadline_at
                                    .duration_since(watchdog.started_at)
                                    .as_millis() as u64,
                                "real_progress",
                            );
                        }
                        WatchdogCheck::Stalled | WatchdogCheck::TimedOut => {
                            let worker_peer_for_event = watchdog.worker_peer_id.clone();
                            let _ = watchdog.transition_state_with_event(
                                AttemptLifecycleState::Stalled,
                                Some(&worker_peer_for_event),
                            );
                            emit_attempt_stalled_event(
                                &watchdog.task_id,
                                &watchdog.attempt_id,
                                &watchdog.worker_peer_id,
                                Some(&watchdog.model_id),
                                &watchdog.last_progress_stage,
                                watchdog.elapsed_since_last_progress_ms(),
                            );
                            let _ = watchdog.transition_state_with_event(
                                AttemptLifecycleState::TimedOut,
                                Some(&worker_peer_for_event),
                            );
                            timeout_context = Some(TimeoutContext {
                                trace_task_id: watchdog.task_id.clone(),
                                attempt_id: watchdog.attempt_id.clone(),
                                worker_peer_id: watchdog.worker_peer_id.clone(),
                                model_id: watchdog.model_id.clone(),
                                elapsed_ms: watchdog.elapsed_ms(),
                                elapsed_since_progress_ms: watchdog
                                    .elapsed_since_last_progress_ms(),
                                adaptive_timeout_ms: watchdog.policy.timeout_ms,
                                max_wait_ms: watchdog.policy.max_wait_ms,
                            });
                            should_retry = true;
                        }
                    }
                } else if let Some(t0) = infer_runtime.pending_inference.get(&rid) {
                    if t0.elapsed().as_millis() as u64 >= INFER_TIMEOUT_MS {
                        timeout_context = Some(TimeoutContext {
                            trace_task_id: infer_runtime
                                .distributed_infer_state
                                .as_ref()
                                .map(|state| state.trace_task_id.clone())
                                .unwrap_or_else(|| "-".to_string()),
                            attempt_id: rid.clone(),
                            worker_peer_id: infer_runtime
                                .distributed_infer_state
                                .as_ref()
                                .and_then(|state| state.current_peer.clone())
                                .unwrap_or_else(|| "-".to_string()),
                            model_id: infer_runtime
                                .distributed_infer_state
                                .as_ref()
                                .and_then(|state| state.current_model.clone())
                                .unwrap_or_else(|| "-".to_string()),
                            elapsed_ms: INFER_TIMEOUT_MS,
                            elapsed_since_progress_ms: INFER_TIMEOUT_MS,
                            adaptive_timeout_ms: INFER_TIMEOUT_MS,
                            max_wait_ms: INFER_TIMEOUT_MS,
                        });
                        should_retry = true;
                    }
                }

                if should_retry {
                    let failure_kind = FailureKind::Timeout;
                    if let Some(timeout_context) = timeout_context {
                        let TimeoutContext {
                            trace_task_id,
                            attempt_id,
                            worker_peer_id,
                            model_id,
                            elapsed_ms,
                            elapsed_since_progress_ms,
                            adaptive_timeout_ms,
                            max_wait_ms,
                        } = timeout_context;
                        eprintln!(
                            "\n[Fault] task_id={} attempt_id={} kind={:?} timeout_ms={}",
                            trace_task_id, attempt_id, failure_kind, adaptive_timeout_ms
                        );
                        if let Some(infer_state) = infer_runtime.distributed_infer_state.as_mut() {
                            if worker_peer_id != "-" {
                                if let Some(health) =
                                    registry.write().await.record_timeout(&worker_peer_id)
                                {
                                    log_health_update(
                                        &trace_task_id,
                                        &worker_peer_id,
                                        infer_state.current_model.as_deref(),
                                        &health,
                                        Some(NODE_UNHEALTHY_002),
                                    );
                                }
                            }
                            log_observability_event(
                                LogLevel::Error,
                                "task_timeout",
                                &trace_task_id,
                                Some(&trace_task_id),
                                Some(&model_id),
                                Some(TASK_TIMEOUT_001),
                                {
                                    let mut fields = Map::new();
                                    fields.insert(
                                        "attempt_id".to_string(),
                                        attempt_id.clone().into(),
                                    );
                                    fields.insert("latency_ms".to_string(), elapsed_ms.into());
                                    fields.insert(
                                        "elapsed_since_last_progress_ms".to_string(),
                                        elapsed_since_progress_ms.into(),
                                    );
                                    fields.insert(
                                        "adaptive_timeout_ms".to_string(),
                                        adaptive_timeout_ms.into(),
                                    );
                                    fields.insert("max_wait_ms".to_string(), max_wait_ms.into());
                                    fields.insert("retry".to_string(), true.into());
                                    fields.insert("error_kind".to_string(), "timeout".into());
                                    fields.insert("recoverable".to_string(), true.into());
                                    fields.insert(
                                        "watchdog_reason".to_string(),
                                        "no_progress".into(),
                                    );
                                    if worker_peer_id != "-" {
                                        fields.insert(
                                            "peer_id".to_string(),
                                            worker_peer_id.clone().into(),
                                        );
                                    }
                                    fields
                                },
                            );
                            let failed_peer_opt = if worker_peer_id == "-" {
                                infer_state.current_peer.as_deref()
                            } else {
                                Some(worker_peer_id.as_str())
                            };
                            let failed_model_opt = if model_id == "-" {
                                infer_state.current_model.as_deref()
                            } else {
                                Some(model_id.as_str())
                            };
                            let mut preview_retry = infer_state.retry_state.clone();
                            preview_retry.record_failure(failed_peer_opt, failed_model_opt);
                            let reg = registry.read().await;
                            let retry_target = select_retry_target(
                                &reg,
                                &IntelligentScheduler::new(),
                                &infer_state.candidate_models,
                                infer_state.local_cluster_id.as_deref(),
                                &preview_retry,
                            );
                            drop(reg);
                            task_manager
                                .fail(&trace_task_id, "distributed timeout")
                                .await;
                            let failed_peer = infer_state.current_peer.clone();
                            let failed_model = infer_state.current_model.clone();
                            let retried = infer_state
                                .schedule_retry(failed_peer.as_deref(), failed_model.as_deref());
                            infer_runtime.pending_inference.remove(&rid);
                            clear_stream_state(
                                &mut infer_runtime.token_buffer,
                                &mut infer_runtime.next_token_idx,
                                &mut infer_runtime.rendered_output,
                            );
                            if retried {
                                emit_retry_scheduled_event(
                                    &trace_task_id,
                                    &infer_state.current_request_id,
                                    failed_model.as_deref(),
                                    failed_peer.as_deref(),
                                    retry_target.as_ref().map(|target| target.peer_id.as_str()),
                                    "timeout",
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
                                        "[Retry] task_id={} attempt_id={} retrying on new node",
                                        trace_task_id, infer_state.current_request_id
                                    );
                                }
                                infer_runtime.infer_request_id =
                                    Some(infer_state.current_request_id.clone());
                                infer_runtime.infer_broadcast_sent = false;
                                client_state.waiting_for_response = false;
                                return Ok(EventLoopDirective::Continue);
                            }

                            let (trace, aggregate_metrics) =
                                finalize_distributed_task_observability(
                                    &trace_task_id,
                                    infer_runtime.infer_started_at,
                                    true,
                                );
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
                        return Ok(EventLoopDirective::Break);
                    }
                }
            }
        }
    }

    Ok(EventLoopDirective::None)
}
