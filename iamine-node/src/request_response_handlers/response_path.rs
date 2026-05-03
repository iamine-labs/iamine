use super::*;
use event_loop_control::EventLoopDirective;

pub(crate) struct ResponseMessageContext<'a> {
    pub(crate) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(crate) peer: PeerId,
    pub(crate) response: TaskResponse,
    pub(crate) infer_runtime: &'a mut InferRuntimeState,
    pub(crate) client_state: &'a mut ClientRuntimeState,
    pub(crate) task_manager: &'a Arc<TaskManager>,
    pub(crate) registry: &'a SharedNodeRegistry,
}

pub(crate) async fn handle_response_message(ctx: ResponseMessageContext<'_>) -> EventLoopDirective {
    let ResponseMessageContext {
        swarm,
        peer,
        response,
        infer_runtime,
        client_state,
        task_manager,
        registry,
    } = ctx;

    if let Some(distributed_result) = response.distributed_result {
        let expected_task_id = infer_runtime
            .distributed_infer_state
            .as_ref()
            .map(|infer_state| infer_state.trace_task_id.as_str());
        if infer_runtime.infer_request_id.as_deref() != Some(distributed_result.attempt_id.as_str())
            || expected_task_id != Some(distributed_result.task_id.as_str())
        {
            let attempt_id = distributed_result.attempt_id.clone();
            let task_id = distributed_result.task_id.clone();
            let peer_id = peer.to_string();
            let elapsed_ms = infer_runtime
                .attempt_watchdogs
                .get(&attempt_id)
                .map(|watchdog| watchdog.elapsed_ms())
                .unwrap_or_default();
            apply_late_result_lifecycle(
                LateResultLifecycleInput {
                    task_id: &task_id,
                    attempt_id: &attempt_id,
                    worker_peer_id: &peer_id,
                    model_id: infer_runtime
                        .distributed_infer_state
                        .as_ref()
                        .and_then(|state| state.current_model.as_deref()),
                    success: distributed_result.success,
                    output: &distributed_result.output,
                    execution_ms: elapsed_ms,
                },
                &mut infer_runtime.attempt_watchdogs,
                registry,
            )
            .await;
            println!(
                "[Task] Ignoring stale response task_id={} attempt_id={} from peer_id={}",
                distributed_result.task_id, distributed_result.attempt_id, peer
            );
            return EventLoopDirective::Continue;
        }

        infer_runtime
            .pending_inference
            .remove(&distributed_result.attempt_id);
        let peer_id = peer.to_string();
        let latency_ms = infer_runtime
            .infer_started_at
            .as_ref()
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or_default();
        emit_result_received_event(
            &distributed_result.task_id,
            &distributed_result.attempt_id,
            infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.as_deref()),
            &peer_id,
            latency_ms,
        );
        let validation_status = if distributed_result.success {
            if !should_print_result_output(&distributed_result.output) {
                ResultStatus::Retryable("empty distributed response output".to_string())
            } else if let Some(infer_state) = infer_runtime.distributed_infer_state.as_ref() {
                validate_result(
                    infer_state.current_semantic_prompt.as_deref().unwrap_or(""),
                    infer_state
                        .current_task_type
                        .unwrap_or(PromptTaskType::General),
                    &distributed_result.output,
                )
            } else {
                ResultStatus::Valid
            }
        } else {
            ResultStatus::Retryable(distributed_result.output.clone())
        };

        let retry_reason = match validation_status {
            ResultStatus::Valid => None,
            ResultStatus::Invalid(reason) | ResultStatus::Retryable(reason) => Some(reason),
        };

        if let Some(reason) = retry_reason {
            let failure_kind = if distributed_result.success {
                FailureKind::InvalidOutput
            } else {
                FailureKind::TaskFailure
            };
            let current_model = infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.clone());
            eprintln!(
                "[Fault] task_id={} attempt_id={} peer_id={} model_id={} reason={}",
                distributed_result.task_id,
                distributed_result.attempt_id,
                peer,
                current_model.as_deref().unwrap_or("-"),
                reason
            );
            let mut failure_context = ResultFailureLifecycleContext {
                distributed_infer_state: &mut infer_runtime.distributed_infer_state,
                attempt_watchdogs: &mut infer_runtime.attempt_watchdogs,
                task_manager,
                pending_inference: &mut infer_runtime.pending_inference,
                token_buffer: &mut infer_runtime.token_buffer,
                next_token_idx: &mut infer_runtime.next_token_idx,
                rendered_output: &mut infer_runtime.rendered_output,
                infer_request_id: &mut infer_runtime.infer_request_id,
                infer_broadcast_sent: &mut infer_runtime.infer_broadcast_sent,
                waiting_for_response: &mut client_state.waiting_for_response,
                infer_started_at: infer_runtime.infer_started_at,
            };
            if apply_result_failure_lifecycle(
                ResultFailureInput {
                    task_id: &distributed_result.task_id,
                    attempt_id: &distributed_result.attempt_id,
                    worker_peer_id: &peer_id,
                    reason: &reason,
                    output: &distributed_result.output,
                    failure_kind,
                    failure_error_kind: "task_failure",
                    model_id: current_model,
                },
                registry,
                &mut failure_context,
            )
            .await
            {
                return EventLoopDirective::Continue;
            }
            return EventLoopDirective::Break;
        }

        apply_result_success_lifecycle(
            &distributed_result.task_id,
            &distributed_result.attempt_id,
            &peer_id,
            infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.as_deref()),
            latency_ms,
            registry,
            &mut infer_runtime.attempt_watchdogs,
        )
        .await;
        task_manager.complete(distributed_result.clone()).await;
        if should_print_result_output(&distributed_result.output) {
            println!("{}", distributed_result.output);
        }
        println!(
            "\n\n✅ Inference completada: task_id={} attempt_id={} peer_id={} model_id={}",
            distributed_result.task_id,
            distributed_result.attempt_id,
            peer,
            infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.as_deref())
                .unwrap_or("-")
        );
        if let Some(infer_state) = infer_runtime.distributed_infer_state.as_ref() {
            let (trace, aggregate_metrics) = finalize_distributed_task_observability(
                &infer_state.trace_task_id,
                infer_runtime.infer_started_at,
                false,
            );
            if let Some(trace) = trace {
                println!(
                    "[Trace] nodes={} models={}",
                    trace.node_history.join(" -> "),
                    trace.model_history.join(" -> ")
                );
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
        return EventLoopDirective::Break;
    }

    client_state.waiting_for_response = false;
    client_state.completed += 1;
    if !client_state.pending_tasks.is_empty() {
        client_state.pending_tasks.remove(0);
    }
    println!(
        "📩 [{}/{}] {} de {}: '{}'",
        client_state.completed,
        client_state.total_tasks,
        if response.success { "✅" } else { "❌" },
        peer,
        response.result
    );
    if let Some(next_task) = client_state.pending_tasks.first().cloned() {
        if let Some(pid) = client_state.connected_peer {
            swarm
                .behaviour_mut()
                .request_response
                .send_request(&pid, next_task);
            client_state.waiting_for_response = true;
        }
    } else {
        println!(
            "\n🎉 Completadas: {}/{}",
            client_state.completed, client_state.total_tasks
        );
        return EventLoopDirective::Break;
    }

    EventLoopDirective::None
}
