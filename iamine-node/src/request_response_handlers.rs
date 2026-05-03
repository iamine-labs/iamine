use super::*;
use event_loop_control::EventLoopDirective;

pub(super) struct RequestResponseHandlerContext<'a> {
    pub(super) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(super) event: RREvent<TaskRequest, TaskResponse>,
    pub(super) peer_id: PeerId,
    pub(super) debug_flags: DebugFlags,
    pub(super) is_client: bool,
    pub(super) topology: &'a SharedNetworkTopology,
    pub(super) queue: &'a Arc<TaskQueue>,
    pub(super) task_manager: &'a Arc<TaskManager>,
    pub(super) task_response_tx: &'a tokio::sync::mpsc::Sender<PendingTaskResponse>,
    pub(super) registry: &'a SharedNodeRegistry,
    pub(super) model_storage: &'a ModelStorage,
    pub(super) infer_runtime: &'a mut InferRuntimeState,
    pub(super) client_state: &'a mut ClientRuntimeState,
}

pub(super) async fn handle_request_response_event(
    ctx: RequestResponseHandlerContext<'_>,
) -> EventLoopDirective {
    let RequestResponseHandlerContext {
        swarm,
        event,
        peer_id,
        debug_flags,
        is_client,
        topology,
        queue,
        task_manager,
        task_response_tx,
        registry,
        model_storage,
        infer_runtime,
        client_state,
    } = ctx;

    match event {
        RREvent::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
        } => {
            if let Some(task) = request.distributed_task.clone() {
                let local_cluster_id = {
                    let topo = topology.read().await;
                    topo.cluster_for_peer(&peer_id.to_string())
                        .map(|s| s.to_string())
                };
                println!(
                    "[Task] Received task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                    task.id,
                    task.attempt_id,
                    peer,
                    local_cluster_id.as_deref().unwrap_or("-"),
                    task.model
                );
                emit_worker_task_message_received_event(
                    &task.id,
                    &task.attempt_id,
                    "request_response",
                    &peer.to_string(),
                    "ok",
                    &task.model,
                    model_storage.has_model(&task.model),
                );
                log_observability_event(
                    LogLevel::Info,
                    "task_received",
                    &task.id,
                    Some(&task.id),
                    Some(&task.model),
                    None,
                    {
                        let mut fields = Map::new();
                        fields.insert("peer_id".to_string(), peer.to_string().into());
                        fields.insert("attempt_id".to_string(), task.attempt_id.clone().into());
                        fields.insert(
                            "cluster_id".to_string(),
                            local_cluster_id
                                .clone()
                                .unwrap_or_else(|| "-".to_string())
                                .into(),
                        );
                        fields
                    },
                );
                debug_network_log(
                    debug_flags,
                    format!(
                        "received distributed request task_id={} attempt_id={} from_peer={} cluster_id={} model_id={}",
                        task.id,
                        task.attempt_id,
                        peer,
                        local_cluster_id.as_deref().unwrap_or("-"),
                        task.model
                    ),
                );

                let task_manager_ref = Arc::clone(task_manager);
                let task_response_tx_ref = task_response_tx.clone();
                let prompt = task.prompt.clone();
                let model = task.model.clone();
                let task_id = task.id.clone();
                let attempt_id = task.attempt_id.clone();
                let peer_string = peer.to_string();
                let local_cluster_for_task = local_cluster_id.clone();

                tokio::spawn(async move {
                    match task_manager_ref.claim_task(task.clone()).await {
                        TaskClaim::Started => {
                            println!(
                                "[Task] Started task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                                task_id,
                                attempt_id,
                                peer_string,
                                local_cluster_for_task.as_deref().unwrap_or("-"),
                                model
                            );
                        }
                        TaskClaim::InProgress => {
                            let duplicate_message = format!(
                                "duplicate task already running on node for task_id={}",
                                task_id
                            );
                            let _ = task_response_tx_ref
                                .send(PendingTaskResponse {
                                    channel,
                                    response: TaskResponse::distributed(
                                        DistributedTaskResult::failure_for_attempt(
                                            task_id.clone(),
                                            attempt_id.clone(),
                                            duplicate_message,
                                        ),
                                    ),
                                })
                                .await;
                            return;
                        }
                        TaskClaim::Finished(existing_result) => {
                            let cached = if existing_result.success {
                                DistributedTaskResult::success_for_attempt(
                                    existing_result.task_id,
                                    attempt_id.clone(),
                                    existing_result.output,
                                )
                            } else {
                                DistributedTaskResult::failure_for_attempt(
                                    existing_result.task_id,
                                    attempt_id.clone(),
                                    existing_result.output,
                                )
                            };
                            let _ = task_response_tx_ref
                                .send(PendingTaskResponse {
                                    channel,
                                    response: TaskResponse::distributed(cached),
                                })
                                .await;
                            return;
                        }
                    }

                    let resolution = resolve_policy_for_prompt(&prompt, Some(&model), &[]);
                    let profile = resolution.profile.clone();
                    let semantic_prompt = resolution.semantic_prompt.clone();
                    let output_policy = resolve_output_policy(&profile, &semantic_prompt, None);

                    let result = if let Some(runtime) = choose_inference_runtime().await {
                        run_local_inference_with_timeout(
                            runtime,
                            task_id.clone(),
                            model.clone(),
                            semantic_prompt,
                            profile.task_type,
                            output_policy.max_tokens as u32,
                            0.7,
                        )
                        .await
                    } else {
                        let registry = ModelRegistry::new();
                        let model_desc = match registry.get(&model) {
                            Some(desc) => desc,
                            None => {
                                let error = format!("Modelo {} no encontrado en registry", model);
                                task_manager_ref.fail(&task_id, &error).await;
                                let _ = task_response_tx_ref
                                    .send(PendingTaskResponse {
                                        channel,
                                        response: TaskResponse::distributed(
                                            DistributedTaskResult::failure_for_attempt(
                                                task_id.clone(),
                                                attempt_id.clone(),
                                                error,
                                            ),
                                        ),
                                    })
                                    .await;
                                return;
                            }
                        };
                        let engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));
                        if let Err(error) = engine.load_model(&model, &model_desc.hash) {
                            task_manager_ref.fail(&task_id, &error).await;
                            let _ = task_response_tx_ref
                                .send(PendingTaskResponse {
                                    channel,
                                    response: TaskResponse::distributed(
                                        DistributedTaskResult::failure_for_attempt(
                                            task_id.clone(),
                                            attempt_id.clone(),
                                            error,
                                        ),
                                    ),
                                })
                                .await;
                            return;
                        }

                        run_local_inference_with_timeout(
                            InferenceRuntime::Engine(engine),
                            task_id.clone(),
                            model.clone(),
                            semantic_prompt,
                            profile.task_type,
                            output_policy.max_tokens as u32,
                            0.7,
                        )
                        .await
                    };

                    match result {
                        Ok(result) => {
                            record_semantic_feedback(&prompt, &resolution.validation);
                            match validate_result(
                                &resolution.semantic_prompt,
                                profile.task_type,
                                &result.output,
                            ) {
                                ResultStatus::Valid => {}
                                ResultStatus::Invalid(reason) | ResultStatus::Retryable(reason) => {
                                    println!("[Fault] {}", reason);
                                    task_manager_ref.fail(&task_id, &reason).await;
                                    let _ = task_response_tx_ref
                                        .send(PendingTaskResponse {
                                            channel,
                                            response: TaskResponse::distributed(
                                                DistributedTaskResult::failure_for_attempt(
                                                    task_id.clone(),
                                                    attempt_id.clone(),
                                                    reason,
                                                ),
                                            ),
                                        })
                                        .await;
                                    return;
                                }
                            }
                            let distributed_result =
                                DistributedTaskResult::success(task_id.clone(), result.output);
                            let distributed_result = DistributedTaskResult::success_for_attempt(
                                distributed_result.task_id,
                                attempt_id.clone(),
                                distributed_result.output,
                            );
                            task_manager_ref.complete(distributed_result.clone()).await;
                            let _ = task_response_tx_ref
                                .send(PendingTaskResponse {
                                    channel,
                                    response: TaskResponse::distributed(distributed_result),
                                })
                                .await;
                        }
                        Err(error) => {
                            task_manager_ref.fail(&task_id, &error).await;
                            let _ = task_response_tx_ref
                                .send(PendingTaskResponse {
                                    channel,
                                    response: TaskResponse::distributed(
                                        DistributedTaskResult::failure_for_attempt(
                                            task_id.clone(),
                                            attempt_id.clone(),
                                            error,
                                        ),
                                    ),
                                })
                                .await;
                        }
                    }
                });
            } else {
                println!(
                    "📨 Tarea P2P de {}: {} → '{}'",
                    peer, request.task_type, request.data
                );
                let queue_ref = Arc::clone(queue);
                let t_id = request.task_id.clone();
                let t_type = request.task_type.clone();
                let t_data = request.data.clone();
                tokio::spawn(async move {
                    if let Err(e) = queue_ref.push(t_id, t_type, t_data).await {
                        eprintln!("❌ Error encolando: {}", e);
                    }
                    if let Some(outcome) = queue_ref.outcome_rx.lock().await.recv().await {
                        println!("🏁 {} → {:?}", outcome.task_id, outcome.status);
                    }
                });
                let response =
                    TaskExecutor::execute_task(request.task_id, request.task_type, request.data);
                let _ = swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, response);
            }
        }
        RREvent::Message {
            peer,
            message: Message::Response { response, .. },
        } => {
            if let Some(distributed_result) = response.distributed_result {
                let expected_task_id = infer_runtime
                    .distributed_infer_state
                    .as_ref()
                    .map(|infer_state| infer_state.trace_task_id.as_str());
                if infer_runtime.infer_request_id.as_deref()
                    != Some(distributed_result.attempt_id.as_str())
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
                    } else if let Some(infer_state) = infer_runtime.distributed_infer_state.as_ref()
                    {
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
        }
        RREvent::OutboundFailure { peer, error, .. } => {
            eprintln!("❌ Outbound {}: {:?}", peer, error);
            if is_client && !client_state.waiting_for_response {
                return EventLoopDirective::Break;
            }
        }
        _ => {}
    }

    EventLoopDirective::None
}
