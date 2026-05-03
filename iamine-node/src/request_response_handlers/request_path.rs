use super::*;
use event_loop_control::EventLoopDirective;

pub(crate) struct RequestMessageContext<'a> {
    pub(crate) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(crate) peer: PeerId,
    pub(crate) request: TaskRequest,
    pub(crate) channel: request_response::ResponseChannel<TaskResponse>,
    pub(crate) peer_id: PeerId,
    pub(crate) debug_flags: DebugFlags,
    pub(crate) topology: &'a SharedNetworkTopology,
    pub(crate) queue: &'a Arc<TaskQueue>,
    pub(crate) task_manager: &'a Arc<TaskManager>,
    pub(crate) task_response_tx: &'a tokio::sync::mpsc::Sender<PendingTaskResponse>,
    pub(crate) model_storage: &'a ModelStorage,
}

pub(crate) async fn handle_request_message(ctx: RequestMessageContext<'_>) -> EventLoopDirective {
    let RequestMessageContext {
        swarm,
        peer,
        request,
        channel,
        peer_id,
        debug_flags,
        topology,
        queue,
        task_manager,
        task_response_tx,
        model_storage,
    } = ctx;

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
        let response = TaskExecutor::execute_task(request.task_id, request.task_type, request.data);
        let _ = swarm
            .behaviour_mut()
            .request_response
            .send_response(channel, response);
    }

    EventLoopDirective::None
}
