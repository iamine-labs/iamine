use super::*;
use event_loop_control::EventLoopDirective;

pub(crate) struct DispatchPhaseContext<'a> {
    pub(crate) mode: &'a NodeMode,
    pub(crate) infer_runtime: &'a mut InferRuntimeState,
    pub(crate) model_storage: &'a ModelStorage,
    pub(crate) topology: &'a SharedNetworkTopology,
    pub(crate) peer_id: PeerId,
    pub(crate) registry: &'a SharedNodeRegistry,
    pub(crate) pubsub_topics: &'a PubsubTopicTracker,
    pub(crate) client_state: &'a mut ClientRuntimeState,
    pub(crate) debug_flags: DebugFlags,
    pub(crate) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(crate) metrics: &'a Arc<RwLock<NodeMetrics>>,
}

pub(crate) async fn handle_dispatch_phase(
    ctx: DispatchPhaseContext<'_>,
) -> Result<EventLoopDirective, Box<dyn Error>> {
    let DispatchPhaseContext {
        mode,
        infer_runtime,
        model_storage,
        topology,
        peer_id,
        registry,
        pubsub_topics,
        client_state,
        debug_flags,
        swarm,
        metrics,
    } = ctx;

    // Smart routing: intento envío directo cuando ya hay registry
    if matches!(mode, NodeMode::Infer { .. }) && !infer_runtime.infer_broadcast_sent {
        if let Some(infer_state) = infer_runtime.distributed_infer_state.as_mut() {
            let local_models = model_storage.list_local_models();
            let start = std::time::Instant::now();
            let local_cluster = {
                let topo = topology.read().await;
                topo.cluster_for_peer(&peer_id.to_string())
                    .map(|s| s.to_string())
            };
            let reg = registry.read().await;
            let total_nodes = reg.all_nodes().len();
            let network_models = reg.available_models();
            let mut available_models = local_models.clone();
            for model in network_models {
                if !available_models.contains(&model) {
                    available_models.push(model);
                }
            }

            let resolution = resolve_policy_for_prompt(
                &infer_state.prompt,
                infer_state.model_override.as_deref(),
                &available_models,
            );
            let profile = resolution.profile;
            let mut candidates = resolution.candidate_models;
            let selected_model = resolution.selected_model;
            let semantic_prompt = resolution.semantic_prompt;
            let output_policy =
                resolve_output_policy(&profile, &semantic_prompt, infer_state.max_tokens_override);
            if !candidates.contains(&selected_model) {
                candidates.insert(0, selected_model.clone());
            }

            let scheduler = IntelligentScheduler::new();
            let selected = scheduler
                .select_best_node_for_models_excluding(
                    &reg,
                    &candidates,
                    local_cluster.as_deref(),
                    &infer_state.retry_state.failed_peers,
                    &infer_state.retry_state.failed_models,
                )
                .map(|decision| {
                    let selected_capability = reg
                        .all_nodes()
                        .into_iter()
                        .find(|(peer_id, _)| peer_id == &decision.peer_id)
                        .map(|(_, capability)| capability);
                    let selected_cluster_id = selected_capability
                        .as_ref()
                        .and_then(|capability| capability.cluster_id.clone());
                    (
                        decision.peer_id,
                        decision.model_id,
                        decision.score,
                        selected_cluster_id,
                        selected_capability,
                    )
                });
            drop(reg);

            if let Some((
                best_peer,
                routed_model,
                node_score,
                selected_cluster_id,
                selected_capability,
            )) = selected
            {
                let rid = infer_state.current_request_id.clone();
                let trace_task_id = infer_state.trace_task_id.clone();
                let duplicate_inflight = infer_runtime.attempt_watchdogs.values().any(|watchdog| {
                    watchdog.task_id == trace_task_id
                        && watchdog.worker_peer_id == best_peer
                        && watchdog.model_id == routed_model
                        && watchdog.attempt_id != rid
                        && is_meaningfully_in_flight(watchdog.state)
                });
                if duplicate_inflight {
                    log_observability_event(
                        LogLevel::Info,
                        "dispatch_deduplicated_inflight",
                        &trace_task_id,
                        Some(&trace_task_id),
                        Some(&routed_model),
                        None,
                        {
                            let mut fields = Map::new();
                            fields.insert("attempt_id".to_string(), rid.clone().into());
                            fields.insert("selected_peer_id".to_string(), best_peer.clone().into());
                            fields
                                .insert("selected_model".to_string(), routed_model.clone().into());
                            fields.insert(
                                "reason".to_string(),
                                "inflight_attempt_same_worker_model".into(),
                            );
                            fields
                        },
                    );
                    return Ok(EventLoopDirective::Continue);
                }
                let is_retry_attempt = infer_state.retry_state.retry_count > 0;
                let is_model_switch_attempt = infer_state
                    .current_model
                    .as_ref()
                    .map(|previous_model| previous_model != &routed_model)
                    .unwrap_or(false);

                if is_retry_attempt {
                    println!(
                        "[Retry] Dispatching retry attempt {}/{}",
                        infer_state.retry_state.retry_count, infer_state.retry_policy.max_retries
                    );
                    if is_model_switch_attempt {
                        if let Some(previous_model) = infer_state.current_model.as_ref() {
                            println!(
                                "[Fallback] Switching model {} -> {}",
                                previous_model, routed_model
                            );
                        }
                    }
                }

                println!(
                    "[Trace] task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                    trace_task_id,
                    rid,
                    best_peer,
                    selected_cluster_id.as_deref().unwrap_or("-"),
                    routed_model
                );
                log_observability_event(
                    LogLevel::Info,
                    "scheduler_node_selected",
                    &trace_task_id,
                    Some(&trace_task_id),
                    Some(&routed_model),
                    None,
                    {
                        let mut fields = Map::new();
                        fields.insert("selected_peer_id".to_string(), best_peer.clone().into());
                        fields.insert(
                            "cluster_id".to_string(),
                            selected_cluster_id
                                .clone()
                                .unwrap_or_else(|| "-".to_string())
                                .into(),
                        );
                        fields.insert("score".to_string(), node_score.into());
                        fields.insert("reason".to_string(), "high_success_low_latency".into());
                        fields.insert(
                            "candidate_models".to_string(),
                            serde_json::json!(candidates),
                        );
                        fields
                    },
                );
                debug_scheduler_log(
                debug_flags,
                format!(
                    "task_id={} attempt_id={} peer_id={} cluster_id={} model_id={} score={:.3} retries={} total_nodes={}",
                    trace_task_id,
                    rid,
                    best_peer,
                    selected_cluster_id.as_deref().unwrap_or("-"),
                    routed_model,
                    node_score,
                    infer_state.retry_state.retry_count,
                    total_nodes
                ),
            );
                debug_task_log(
                    debug_flags,
                    &best_peer,
                    selected_cluster_id.as_deref(),
                    &routed_model,
                    &trace_task_id,
                    &rid,
                    "dispatching distributed task",
                );
                let topic_peer_count = pubsub_topics.topic_peer_count(DIRECT_INF_TOPIC);
                let readiness_snapshot = DispatchReadinessSnapshot {
                    connected_peer_count: client_state.known_workers.len(),
                    mesh_peer_count: pubsub_topics.mesh_peer_count(),
                    topic_peer_count,
                    joined_task_topic: pubsub_topics.joined(TASK_TOPIC),
                    joined_direct_topic: pubsub_topics.joined(DIRECT_INF_TOPIC),
                    joined_results_topic: pubsub_topics.joined(RESULTS_TOPIC),
                    selected_topic: DIRECT_INF_TOPIC,
                };
                emit_dispatch_context_event(DispatchContextEvent {
                    trace_task_id: &trace_task_id,
                    attempt_id: &rid,
                    selected_model: &routed_model,
                    candidates: &candidates,
                    connected_peer_count: readiness_snapshot.connected_peer_count,
                    topic_peer_count: readiness_snapshot.topic_peer_count,
                    selected_topic: DIRECT_INF_TOPIC,
                    target_peer_id: Some(&best_peer),
                });
                if let Err(readiness_error) = evaluate_dispatch_readiness(&readiness_snapshot) {
                    emit_dispatch_readiness_failure_event(
                        &trace_task_id,
                        &rid,
                        &routed_model,
                        &candidates,
                        Some(&best_peer),
                        &readiness_snapshot,
                        &readiness_error,
                    );
                    let elapsed_ms = infer_runtime
                        .infer_started_at
                        .map(|started| started.elapsed().as_millis() as u64)
                        .unwrap_or_default();
                    if elapsed_ms < INFER_TIMEOUT_MS {
                        debug_network_log(
                        debug_flags,
                        format!(
                            "dispatch waiting for readiness task_id={} attempt_id={} reason={} elapsed_ms={}",
                            trace_task_id, rid, readiness_error.reason, elapsed_ms
                        ),
                    );
                        return Ok(EventLoopDirective::Continue);
                    }
                    return Err(format!(
                        "Dispatch readiness timeout: task_id={} attempt_id={} reason={} [{}]",
                        trace_task_id, rid, readiness_error.reason, readiness_error.code
                    )
                    .into());
                }

                let direct = DirectInferenceRequest {
                    request_id: rid.clone(),
                    target_peer: best_peer.to_string(),
                    model: routed_model.clone(),
                    prompt: semantic_prompt.clone(),
                    max_tokens: output_policy.max_tokens as u32,
                };
                let mut direct_payload = direct.to_gossip_json();
                if let Some(map) = direct_payload.as_object_mut() {
                    map.insert("task_id".to_string(), trace_task_id.clone().into());
                    map.insert("attempt_id".to_string(), rid.clone().into());
                }
                let payload = serde_json::to_vec(&direct_payload)
                    .map_err(|error| format!("Direct dispatch payload error: {}", error))?;
                let payload_size = payload.len();
                emit_task_publish_attempt_event(
                    &trace_task_id,
                    &rid,
                    &routed_model,
                    DIRECT_INF_TOPIC,
                    topic_peer_count,
                    payload_size,
                    Some(&best_peer),
                );
                let publish_result = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub::IdentTopic::new(DIRECT_INF_TOPIC), payload);
                match publish_result {
                    Ok(message_id) => {
                        let message_id = message_id.to_string();
                        infer_state.record_attempt(
                            best_peer.clone(),
                            routed_model.clone(),
                            profile.task_type,
                            semantic_prompt.clone(),
                            local_cluster.clone(),
                            candidates.clone(),
                        );
                        let _ = record_task_attempt(
                            &trace_task_id,
                            &best_peer,
                            &routed_model,
                            is_retry_attempt,
                            is_model_switch_attempt,
                        );
                        if is_retry_attempt {
                            let _ = record_distributed_task_retry();
                        }
                        emit_task_published_event(
                            PublishEventContext {
                                trace_task_id: &trace_task_id,
                                attempt_id: &rid,
                                model_id: &routed_model,
                                topic: DIRECT_INF_TOPIC,
                                publish_peer_count: topic_peer_count,
                                payload_size,
                                selected_peer_id: Some(&best_peer),
                            },
                            &message_id,
                        );
                        let timeout_policy = AttemptTimeoutPolicy::from_model_and_node(
                            &routed_model,
                            selected_capability.as_ref(),
                        );
                        log_observability_event(
                            LogLevel::Info,
                            "attempt_timeout_policy",
                            &trace_task_id,
                            Some(&trace_task_id),
                            Some(&routed_model),
                            None,
                            {
                                let mut fields = Map::new();
                                fields.insert("attempt_id".to_string(), rid.clone().into());
                                fields
                                    .insert("worker_peer_id".to_string(), best_peer.clone().into());
                                fields.insert(
                                    "timeout_ms".to_string(),
                                    timeout_policy.timeout_ms.into(),
                                );
                                fields.insert(
                                    "stall_timeout_ms".to_string(),
                                    timeout_policy.stall_timeout_ms.into(),
                                );
                                fields.insert(
                                    "max_wait_ms".to_string(),
                                    timeout_policy.max_wait_ms.into(),
                                );
                                fields.insert(
                                    "latency_class".to_string(),
                                    timeout_policy.latency_class.into(),
                                );
                                fields
                            },
                        );
                        let mut watchdog = AttemptWatchdog::new(
                            trace_task_id.clone(),
                            rid.clone(),
                            best_peer.clone(),
                            routed_model.clone(),
                            timeout_policy,
                        );
                        let _ = watchdog.transition_state_with_event(
                            AttemptLifecycleState::Starting,
                            Some(&best_peer),
                        );
                        infer_runtime
                            .attempt_watchdogs
                            .insert(rid.clone(), watchdog);
                        metrics
                            .write()
                            .await
                            .routing_decision(start.elapsed().as_millis() as u64);
                        infer_runtime.infer_broadcast_sent = true;
                        client_state.waiting_for_response = true;
                        infer_runtime
                            .pending_inference
                            .insert(rid.clone(), tokio::time::Instant::now());
                        infer_runtime.infer_request_id = Some(rid.clone());
                        println!(
                        "[Routing] task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                        trace_task_id,
                        rid,
                        best_peer,
                        selected_cluster_id.as_deref().unwrap_or("-"),
                        routed_model
                    );
                        println!(
                            "[Scheduler] Selected node {} with score {:.3}",
                            best_peer, node_score
                        );
                        println!(
                            "🧠 DirectInferenceRequest enviado [{}] → {}",
                            &rid[..8.min(rid.len())],
                            best_peer
                        );
                    }
                    Err(error) => {
                        let error_text = error.to_string();
                        emit_task_publish_failed_event(
                            PublishEventContext {
                                trace_task_id: &trace_task_id,
                                attempt_id: &rid,
                                model_id: &routed_model,
                                topic: DIRECT_INF_TOPIC,
                                publish_peer_count: topic_peer_count,
                                payload_size,
                                selected_peer_id: Some(&best_peer),
                            },
                            &error_text,
                        );
                        return Err(format!(
                            "Dispatch publish failed: task_id={} attempt_id={} error={} [{}]",
                            trace_task_id, rid, error_text, TASK_DISPATCH_UNCONFIRMED_001
                        )
                        .into());
                    }
                }
            } else if total_nodes > 0 && candidates.iter().all(|m| !available_models.contains(m)) {
                log_observability_event(
                    LogLevel::Error,
                    "node_rejected",
                    &infer_state.trace_task_id,
                    Some(&infer_state.trace_task_id),
                    None,
                    Some(SCH_NO_NODE_001),
                    {
                        let mut fields = Map::new();
                        fields.insert("reason".to_string(), "no_compatible_model".into());
                        fields.insert("known_nodes".to_string(), (total_nodes as u64).into());
                        fields.insert(
                            "candidate_models".to_string(),
                            serde_json::json!(candidates),
                        );
                        fields
                    },
                );
                eprintln!("❌ Ningún nodo en la red tiene un modelo compatible instalado.");
                eprintln!("   Nodos conocidos: {}", total_nodes);
                eprintln!("   Candidates: {}", candidates.join(", "));
                return Err("No compatible node available for distributed inference".into());
            } else if infer_runtime
                .infer_started_at
                .map(|t| t.elapsed().as_millis() as u64)
                .unwrap_or(0)
                >= INFER_FALLBACK_AFTER_MS
            {
                // Fallback automático a broadcast legacy
                let rid = infer_state.current_request_id.clone();
                let mid = selected_model.clone();
                let trace_task_id = infer_state.trace_task_id.clone();
                let task = InferenceTask::new(
                    rid.clone(),
                    mid,
                    semantic_prompt.clone(),
                    output_policy.max_tokens as u32,
                    peer_id.to_string(),
                );
                let topic_peer_count = pubsub_topics.topic_peer_count(TASK_TOPIC);
                let readiness_snapshot = DispatchReadinessSnapshot {
                    connected_peer_count: client_state.known_workers.len(),
                    mesh_peer_count: pubsub_topics.mesh_peer_count(),
                    topic_peer_count,
                    joined_task_topic: pubsub_topics.joined(TASK_TOPIC),
                    joined_direct_topic: pubsub_topics.joined(DIRECT_INF_TOPIC),
                    joined_results_topic: pubsub_topics.joined(RESULTS_TOPIC),
                    selected_topic: TASK_TOPIC,
                };
                emit_dispatch_context_event(DispatchContextEvent {
                    trace_task_id: &trace_task_id,
                    attempt_id: &rid,
                    selected_model: &selected_model,
                    candidates: &candidates,
                    connected_peer_count: readiness_snapshot.connected_peer_count,
                    topic_peer_count: readiness_snapshot.topic_peer_count,
                    selected_topic: TASK_TOPIC,
                    target_peer_id: None,
                });
                if let Err(readiness_error) = evaluate_dispatch_readiness(&readiness_snapshot) {
                    emit_dispatch_readiness_failure_event(
                        &trace_task_id,
                        &rid,
                        &selected_model,
                        &candidates,
                        None,
                        &readiness_snapshot,
                        &readiness_error,
                    );
                    let elapsed_ms = infer_runtime
                        .infer_started_at
                        .map(|started| started.elapsed().as_millis() as u64)
                        .unwrap_or_default();
                    if elapsed_ms < INFER_TIMEOUT_MS {
                        debug_network_log(
                        debug_flags,
                        format!(
                            "fallback waiting for readiness task_id={} attempt_id={} reason={} elapsed_ms={}",
                            trace_task_id, rid, readiness_error.reason, elapsed_ms
                        ),
                    );
                        return Ok(EventLoopDirective::Continue);
                    }
                    return Err(format!(
                    "Fallback dispatch readiness timeout: task_id={} attempt_id={} reason={} [{}]",
                    trace_task_id, rid, readiness_error.reason, readiness_error.code
                )
                    .into());
                }

                let mut fallback_payload = task.to_gossip_json();
                if let Some(map) = fallback_payload.as_object_mut() {
                    map.insert("task_id".to_string(), trace_task_id.clone().into());
                    map.insert("attempt_id".to_string(), rid.clone().into());
                }
                let payload = serde_json::to_vec(&fallback_payload)
                    .map_err(|error| format!("Fallback dispatch payload error: {}", error))?;
                let payload_size = payload.len();
                emit_task_publish_attempt_event(
                    &trace_task_id,
                    &rid,
                    &selected_model,
                    TASK_TOPIC,
                    topic_peer_count,
                    payload_size,
                    None,
                );
                let publish_result = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub::IdentTopic::new(TASK_TOPIC), payload);
                match publish_result {
                    Ok(message_id) => {
                        let message_id = message_id.to_string();
                        emit_task_published_event(
                            PublishEventContext {
                                trace_task_id: &trace_task_id,
                                attempt_id: &rid,
                                model_id: &selected_model,
                                topic: TASK_TOPIC,
                                publish_peer_count: topic_peer_count,
                                payload_size,
                                selected_peer_id: None,
                            },
                            &message_id,
                        );
                        let timeout_policy =
                            AttemptTimeoutPolicy::from_model_and_node(&selected_model, None);
                        log_observability_event(
                            LogLevel::Info,
                            "attempt_timeout_policy",
                            &trace_task_id,
                            Some(&trace_task_id),
                            Some(&selected_model),
                            None,
                            {
                                let mut fields = Map::new();
                                fields.insert("attempt_id".to_string(), rid.clone().into());
                                fields.insert("worker_peer_id".to_string(), "-".into());
                                fields.insert(
                                    "timeout_ms".to_string(),
                                    timeout_policy.timeout_ms.into(),
                                );
                                fields.insert(
                                    "stall_timeout_ms".to_string(),
                                    timeout_policy.stall_timeout_ms.into(),
                                );
                                fields.insert(
                                    "max_wait_ms".to_string(),
                                    timeout_policy.max_wait_ms.into(),
                                );
                                fields.insert(
                                    "latency_class".to_string(),
                                    timeout_policy.latency_class.into(),
                                );
                                fields
                            },
                        );
                        let mut watchdog = AttemptWatchdog::new(
                            trace_task_id.clone(),
                            rid.clone(),
                            "-".to_string(),
                            selected_model.clone(),
                            timeout_policy,
                        );
                        let _ = watchdog.transition_state_with_event(
                            AttemptLifecycleState::Starting,
                            Some("-"),
                        );
                        infer_runtime
                            .attempt_watchdogs
                            .insert(rid.clone(), watchdog);
                        let is_retry_attempt = infer_state.retry_state.retry_count > 0;
                        infer_state.record_attempt(
                            "-".to_string(),
                            selected_model.clone(),
                            profile.task_type,
                            semantic_prompt.clone(),
                            local_cluster.clone(),
                            candidates.clone(),
                        );
                        let _ = record_task_attempt(
                            &trace_task_id,
                            "broadcast",
                            &selected_model,
                            is_retry_attempt,
                            true,
                        );
                        emit_fallback_attempt_registered_event(
                            &trace_task_id,
                            &rid,
                            &selected_model,
                            TASK_TOPIC,
                            &candidates,
                        );
                        let _ = record_distributed_task_fallback();
                        infer_runtime.infer_broadcast_sent = true;
                        client_state.waiting_for_response = true;
                        infer_runtime
                            .pending_inference
                            .insert(rid.clone(), tokio::time::Instant::now());
                        infer_runtime.infer_request_id = Some(rid.clone());
                        println!(
                        "↪️  Fallback broadcast enviado: task_id={} attempt_id={} message_id={}",
                        trace_task_id, rid, message_id
                    );
                    }
                    Err(error) => {
                        let error_text = error.to_string();
                        emit_task_publish_failed_event(
                            PublishEventContext {
                                trace_task_id: &trace_task_id,
                                attempt_id: &rid,
                                model_id: &selected_model,
                                topic: TASK_TOPIC,
                                publish_peer_count: topic_peer_count,
                                payload_size,
                                selected_peer_id: None,
                            },
                            &error_text,
                        );
                        return Err(format!(
                        "Fallback dispatch publish failed: task_id={} attempt_id={} error={} [{}]",
                        trace_task_id, rid, error_text, TASK_DISPATCH_UNCONFIRMED_001
                    )
                        .into());
                    }
                }
            }
        }
    }

    Ok(EventLoopDirective::None)
}
