use super::*;

#[derive(Clone, Copy)]
struct WorkerProgressMessage<'a> {
    task_id: &'a str,
    attempt_id: &'a str,
    request_id: &'a str,
    model_id: &'a str,
    worker_peer_id: &'a str,
    stage: &'a str,
    tokens_generated_count: Option<u64>,
}

fn publish_worker_progress_message(
    swarm: &mut Swarm<IamineBehaviour>,
    message: WorkerProgressMessage<'_>,
) {
    let WorkerProgressMessage {
        task_id,
        attempt_id,
        request_id,
        model_id,
        worker_peer_id,
        stage,
        tokens_generated_count,
    } = message;
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
pub(crate) enum WorkerInferenceRequestKind {
    InferenceRequest,
    DirectInferenceRequest,
}

impl WorkerInferenceRequestKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::InferenceRequest => "InferenceRequest",
            Self::DirectInferenceRequest => "DirectInferenceRequest",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerInferenceRequestContext {
    pub(crate) request_kind: WorkerInferenceRequestKind,
    pub(crate) message_topic: String,
    pub(crate) from_peer: String,
    pub(crate) request_id: String,
    pub(crate) task_id: String,
    pub(crate) attempt_id: String,
    pub(crate) model_id: String,
    pub(crate) prompt: String,
    pub(crate) max_tokens: u32,
    pub(crate) temperature: f32,
}

pub(crate) fn build_inference_request_context(
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

pub(crate) fn build_direct_inference_request_context(
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

pub(crate) async fn run_worker_inference_pipeline(
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
        WorkerProgressMessage {
            task_id: &request.task_id,
            attempt_id: &request.attempt_id,
            request_id: &request.request_id,
            model_id: &request.model_id,
            worker_peer_id: &peer_id_string,
            stage: "attempt_started",
            tokens_generated_count: None,
        },
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
        WorkerProgressMessage {
            task_id: &request.task_id,
            attempt_id: &request.attempt_id,
            request_id: &request.request_id,
            model_id: &request.model_id,
            worker_peer_id: &peer_id_string,
            stage: "model_load_started",
            tokens_generated_count: None,
        },
    );
    publish_worker_progress_message(
        swarm,
        WorkerProgressMessage {
            task_id: &request.task_id,
            attempt_id: &request.attempt_id,
            request_id: &request.request_id,
            model_id: &request.model_id,
            worker_peer_id: &peer_id_string,
            stage: "model_load_completed",
            tokens_generated_count: None,
        },
    );
    publish_worker_progress_message(
        swarm,
        WorkerProgressMessage {
            task_id: &request.task_id,
            attempt_id: &request.attempt_id,
            request_id: &request.request_id,
            model_id: &request.model_id,
            worker_peer_id: &peer_id_string,
            stage: "inference_started",
            tokens_generated_count: None,
        },
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
                WorkerProgressMessage {
                    task_id: &request.task_id,
                    attempt_id: &request.attempt_id,
                    request_id: &request.request_id,
                    model_id: &request.model_id,
                    worker_peer_id: &peer_id_string,
                    stage: "first_token_generated",
                    tokens_generated_count: Some(produced_tokens),
                },
            );
        } else if produced_tokens.is_multiple_of(16) {
            publish_worker_progress_message(
                swarm,
                WorkerProgressMessage {
                    task_id: &request.task_id,
                    attempt_id: &request.attempt_id,
                    request_id: &request.request_id,
                    model_id: &request.model_id,
                    worker_peer_id: &peer_id_string,
                    stage: "tokens_generated_count",
                    tokens_generated_count: Some(produced_tokens),
                },
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
                WorkerProgressMessage {
                    task_id: &request.task_id,
                    attempt_id: &request.attempt_id,
                    request_id: &request.request_id,
                    model_id: &request.model_id,
                    worker_peer_id: &peer_id_string,
                    stage: "inference_finished",
                    tokens_generated_count: Some(result.tokens_generated as u64),
                },
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
                WorkerProgressMessage {
                    task_id: &request.task_id,
                    attempt_id: &request.attempt_id,
                    request_id: &request.request_id,
                    model_id: &request.model_id,
                    worker_peer_id: &peer_id_string,
                    stage: "result_serialized",
                    tokens_generated_count: Some(result.tokens_generated as u64),
                },
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
                                WorkerProgressMessage {
                                    task_id: &request.task_id,
                                    attempt_id: &request.attempt_id,
                                    request_id: &request.request_id,
                                    model_id: &request.model_id,
                                    worker_peer_id: &peer_id_string,
                                    stage: "result_published",
                                    tokens_generated_count: Some(result.tokens_generated as u64),
                                },
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
