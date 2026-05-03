use super::*;
use event_loop_control::EventLoopDirective;

pub(super) struct GossipsubHandlerContext<'a> {
    pub(super) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(super) mode: &'a NodeMode,
    pub(super) propagation_source: PeerId,
    pub(super) message: gossipsub::Message,
    pub(super) registry: &'a SharedNodeRegistry,
    pub(super) task_cache: &'a mut TaskCache,
    pub(super) capabilities: &'a WorkerCapabilities,
    pub(super) pool: &'a Arc<WorkerPool>,
    pub(super) queue: &'a Arc<TaskQueue>,
    pub(super) peer_id: PeerId,
    pub(super) scheduler: &'a Arc<TaskScheduler>,
    pub(super) client_state: &'a mut ClientRuntimeState,
    pub(super) task_topic: &'a gossipsub::IdentTopic,
    pub(super) peer_tracker: &'a mut PeerTracker,
    pub(super) metrics: &'a Arc<RwLock<NodeMetrics>>,
    pub(super) infer_runtime: &'a mut InferRuntimeState,
    pub(super) model_storage: &'a ModelStorage,
    pub(super) node_caps: &'a ModelNodeCapabilities,
    pub(super) inference_engine: &'a Arc<RealInferenceEngine>,
    pub(super) inference_backend_state: &'a InferenceBackendState,
    pub(super) task_manager: &'a Arc<TaskManager>,
}

pub(super) async fn handle_gossipsub_message(
    ctx: GossipsubHandlerContext<'_>,
) -> EventLoopDirective {
    let GossipsubHandlerContext {
        swarm,
        mode,
        propagation_source,
        message,
        registry,
        task_cache,
        capabilities,
        pool,
        queue,
        peer_id,
        scheduler,
        client_state,
        task_topic,
        peer_tracker,
        metrics,
        infer_runtime,
        model_storage,
        node_caps,
        inference_engine,
        inference_backend_state,
        task_manager,
    } = ctx;

    let from_peer = propagation_source.to_string();
    let message_topic = if message.topic == gossipsub::IdentTopic::new(TASK_TOPIC).hash() {
        TASK_TOPIC
    } else if message.topic == gossipsub::IdentTopic::new(DIRECT_INF_TOPIC).hash() {
        DIRECT_INF_TOPIC
    } else if message.topic == gossipsub::IdentTopic::new(RESULTS_TOPIC).hash() {
        RESULTS_TOPIC
    } else if message.topic == gossipsub::IdentTopic::new(CAP_TOPIC).hash() {
        CAP_TOPIC
    } else {
        "unknown"
    };

    if message.topic == gossipsub::IdentTopic::new(CAP_TOPIC).hash() {
        if let Ok(hb) = serde_json::from_slice::<NodeCapabilityHeartbeat>(&message.data) {
            let _ = registry.write().await.update_from_heartbeat(hb);
        }
        return EventLoopDirective::Continue;
    }

    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&message.data) {
        let msg_type = msg["type"].as_str().unwrap_or("");

        match msg_type {
            "TaskOffer" if matches!(mode, NodeMode::Worker) => {
                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                let task_type = msg["task_type"].as_str().unwrap_or("").to_string();
                let origin_peer = msg["origin_peer"].as_str().unwrap_or("").to_string();

                if task_cache.is_duplicate(&task_id) {
                    println!(
                        "⚡ [Cache] Tarea {} ya vista",
                        &task_id[..8.min(task_id.len())]
                    );
                } else if !capabilities.supports(&task_type) {
                    println!("🚫 [Worker] No soporta '{}'", task_type);
                } else {
                    let available = pool.available_slots();
                    if available > 0 {
                        println!(
                            "📋 [Worker] Bid para tarea {} ({} slots)",
                            task_id, available
                        );
                        let bid = serde_json::json!({
                            "type": "TaskBid",
                            "task_id": task_id,
                            "worker_id": peer_id.to_string(),
                            "origin_peer": origin_peer,
                            "reputation_score": queue.reputation().await.reputation_score,
                            "available_slots": available,
                            "estimated_ms": 10,
                        });
                        let _ = swarm.behaviour_mut().gossipsub.publish(
                            gossipsub::IdentTopic::new("iamine-bids"),
                            serde_json::to_vec(&bid).unwrap_or_default(),
                        );
                    }
                }
            }

            "TaskBid" if matches!(mode, NodeMode::Broadcast { .. }) => {
                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                let worker_id = msg["worker_id"].as_str().unwrap_or("").to_string();
                let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
                let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
                let est_ms = msg["estimated_ms"].as_u64().unwrap_or(1000);
                let latency = peer_tracker.get_latency(&worker_id);

                println!(
                    "📨 [Scheduler] Bid: worker={}... rep={} slots={} latency={:.1}ms",
                    &worker_id[..8.min(worker_id.len())],
                    rep,
                    slots,
                    latency
                );

                if let Some(winner) = scheduler
                    .receive_bid(&task_id, worker_id.clone(), rep, slots, est_ms)
                    .await
                {
                    println!(
                        "🏆 Asignando {} a {}...",
                        &task_id[..8],
                        &winner[..8.min(winner.len())]
                    );

                    let deadline_ms = 30_000u64;

                    if let Some(winner_peer) = client_state
                        .known_workers
                        .iter()
                        .find(|p| p.to_string().starts_with(&winner[..8.min(winner.len())]))
                        .copied()
                    {
                        client_state
                            .origin_peer_map
                            .insert(task_id.clone(), winner_peer);
                    }

                    let assign = serde_json::json!({
                        "type": "TaskAssign",
                        "task_id": task_id,
                        "assigned_worker": winner,
                        "origin_peer": peer_id.to_string(),
                        "deadline_ms": deadline_ms,
                        "task_type": if let NodeMode::Broadcast { task_type, .. } = mode { task_type } else { "" },
                        "data": if let NodeMode::Broadcast { data, .. } = mode { data } else { "" },
                    });
                    let _ = swarm.behaviour_mut().gossipsub.publish(
                        gossipsub::IdentTopic::new("iamine-assign"),
                        serde_json::to_vec(&assign).unwrap_or_default(),
                    );

                    let rebroadcast_task_id = task_id.clone();
                    let scheduler_ref = Arc::clone(scheduler);
                    let task_type_clone = if let NodeMode::Broadcast { task_type, .. } = mode {
                        task_type.clone()
                    } else {
                        String::new()
                    };
                    let data_clone = if let NodeMode::Broadcast { data, .. } = mode {
                        data.clone()
                    } else {
                        String::new()
                    };
                    let origin_str = peer_id.to_string();
                    let (rebroadcast_tx, mut rebroadcast_rx) =
                        tokio::sync::mpsc::channel::<serde_json::Value>(1);

                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(deadline_ms)).await;
                        let stats = scheduler_ref.stats().await;
                        if stats.1 > 0 {
                            println!(
                                "⏱️ [Recovery] Tarea {} expiró — rebroadcasting...",
                                rebroadcast_task_id
                            );
                            let offer = serde_json::json!({
                                "type": "TaskOffer",
                                "task_id": format!("{}r", rebroadcast_task_id),
                                "task_type": task_type_clone,
                                "data": data_clone,
                                "requester_id": origin_str,
                                "origin_peer": origin_str,
                                "is_retry": true,
                            });
                            let _ = rebroadcast_tx.send(offer).await;
                        }
                    });

                    if let Ok(offer) = rebroadcast_rx.try_recv() {
                        let _ = swarm.behaviour_mut().gossipsub.publish(
                            task_topic.clone(),
                            serde_json::to_vec(&offer).unwrap_or_default(),
                        );
                    }
                }
            }

            "TaskAssign" if matches!(mode, NodeMode::Worker) => {
                let assigned = msg["assigned_worker"].as_str().unwrap_or("");
                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                let task_type = msg["task_type"].as_str().unwrap_or("").to_string();
                let data = msg["data"].as_str().unwrap_or("").to_string();
                let origin_peer_str = msg["origin_peer"].as_str().unwrap_or("").to_string();
                let deadline_ms = msg["deadline_ms"].as_u64().unwrap_or(30_000);

                if assigned == peer_id.to_string() {
                    println!(
                        "🎯 [Worker] ¡Asignado! {} (deadline: {}ms)",
                        task_id, deadline_ms
                    );

                    let _origin_pid = client_state
                        .known_workers
                        .iter()
                        .find(|p| p.to_string() == origin_peer_str)
                        .copied();

                    let queue_ref = Arc::clone(queue);
                    let metrics_ref = Arc::clone(metrics);

                    tokio::spawn(async move {
                        let exec = async {
                            let _ = queue_ref.push(task_id.clone(), task_type, data).await;
                            queue_ref.outcome_rx.lock().await.recv().await
                        };

                        match tokio::time::timeout(Duration::from_millis(deadline_ms), exec).await {
                            Ok(Some(outcome)) => {
                                let success =
                                    matches!(outcome.status, task_queue::OutcomeStatus::Success);
                                {
                                    let mut m = metrics_ref.write().await;
                                    if success {
                                        m.task_success(0);
                                    } else {
                                        m.task_failed();
                                    }
                                }
                                println!(
                                    "✅ [Worker] Tarea {} completada (success={})",
                                    outcome.task_id, success
                                );
                                let rep = queue_ref.reputation().await;
                                println!("⭐ Reputación: {}/100", rep.reputation_score);
                                println!(
                                    "📤 [Worker] Resultado listo → origin {}",
                                    &origin_peer_str[..8.min(origin_peer_str.len())]
                                );
                            }
                            Ok(None) => eprintln!("❌ [Worker] Canal cerrado"),
                            Err(_) => {
                                eprintln!("⏱️ [Worker] Deadline expirado {}ms", deadline_ms);
                                metrics_ref.write().await.task_timed_out();
                            }
                        }
                    });
                } else {
                    println!("⏭️  [Worker] Tarea {} → otro worker ganó", task_id);
                }
            }

            "TaskCancel" if matches!(mode, NodeMode::Worker) => {
                let task_id = msg["task_id"].as_str().unwrap_or("");
                let reason = msg["reason"].as_str().unwrap_or("sin razón");
                println!("🚫 [Worker] Tarea {} cancelada: {}", task_id, reason);
            }

            "Heartbeat" => {
                let worker_id = msg["peer_id"].as_str().unwrap_or("");
                let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
                let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
                let uptime = msg["uptime_secs"].as_u64().unwrap_or(0);
                peer_tracker.update_heartbeat(worker_id, slots, rep);
                println!(
                    "💓 Heartbeat {}... slots={} rep={} uptime={}s peers={}",
                    &worker_id[..8.min(worker_id.len())],
                    slots,
                    rep,
                    uptime,
                    peer_tracker.peer_count()
                );
                let mut m = metrics.write().await;
                m.network_peers = peer_tracker.peer_count();
                m.mesh_peers = peer_tracker.peer_count();
            }

            "InferenceRequest" if matches!(mode, NodeMode::Worker) => {
                let request_context =
                    build_inference_request_context(&msg, message_topic, &from_peer);
                run_worker_inference_pipeline(
                    swarm,
                    request_context,
                    &peer_id,
                    model_storage,
                    node_caps,
                    inference_engine,
                    inference_backend_state,
                    metrics,
                )
                .await;
            }

            "InferenceProgress" if matches!(mode, NodeMode::Infer { .. }) => {
                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                let attempt_id = msg["attempt_id"].as_str().unwrap_or("").to_string();
                let stage = msg["stage"].as_str().unwrap_or("unknown");
                let worker_peer = msg["worker_peer"].as_str().unwrap_or("unknown");
                let model_id = msg["model_id"].as_str().unwrap_or("").to_string();
                let tokens_generated_count = msg["tokens_generated_count"].as_u64();
                let expected_task_id = infer_runtime
                    .distributed_infer_state
                    .as_ref()
                    .map(|state| state.trace_task_id.as_str());
                if expected_task_id != Some(task_id.as_str()) {
                    return EventLoopDirective::Continue;
                }

                if let Some(watchdog) = infer_runtime.attempt_watchdogs.get_mut(&attempt_id) {
                    let previous_state = watchdog.state;
                    let deadline_before = watchdog
                        .deadline_at
                        .duration_since(watchdog.started_at)
                        .as_millis() as u64;
                    let meaningful = watchdog.record_progress(stage, tokens_generated_count);
                    if meaningful {
                        if watchdog.state != previous_state {
                            emit_attempt_state_changed_event(
                                &task_id,
                                &attempt_id,
                                if model_id.is_empty() {
                                    None
                                } else {
                                    Some(model_id.as_str())
                                },
                                Some(worker_peer),
                                previous_state.as_str(),
                                watchdog.state.as_str(),
                            );
                        }
                        let deadline_after = watchdog
                            .deadline_at
                            .duration_since(watchdog.started_at)
                            .as_millis() as u64;
                        if deadline_after > deadline_before {
                            emit_attempt_timeout_extended_event(
                                &task_id,
                                &attempt_id,
                                if model_id.is_empty() {
                                    None
                                } else {
                                    Some(model_id.as_str())
                                },
                                Some(worker_peer),
                                deadline_after,
                                "real_progress",
                            );
                        }
                        log_observability_event(
                            LogLevel::Info,
                            "attempt_progress_received",
                            &task_id,
                            Some(&task_id),
                            if model_id.is_empty() {
                                None
                            } else {
                                Some(model_id.as_str())
                            },
                            None,
                            {
                                let mut fields = Map::new();
                                fields.insert("attempt_id".to_string(), attempt_id.into());
                                fields.insert("worker_peer_id".to_string(), worker_peer.into());
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
                }
            }

            "InferenceToken" if matches!(mode, NodeMode::Infer { .. }) => {
                let rid = msg["request_id"].as_str().unwrap_or("");
                if infer_runtime.infer_request_id.as_deref() == Some(rid) {
                    let idx = msg["index"].as_u64().unwrap_or(0) as u32;
                    let token = msg["token"].as_str().unwrap_or("").to_string();
                    infer_runtime.token_buffer.insert(idx, token);
                    while let Some(next) = infer_runtime
                        .token_buffer
                        .remove(&infer_runtime.next_token_idx)
                    {
                        print!("{}", next);
                        infer_runtime.rendered_output.push_str(&next);
                        let _ = std::io::Write::flush(&mut std::io::stdout());
                        infer_runtime.next_token_idx += 1;
                    }
                }
            }

            "InferenceResult" if matches!(mode, NodeMode::Infer { .. }) => {
                return handle_inference_result_message(
                    swarm,
                    &msg,
                    infer_runtime,
                    client_state,
                    registry,
                    task_manager,
                )
                .await;
            }

            "NodeCapabilities" => {
                if let Ok(hb) = serde_json::from_value::<NodeCapabilityHeartbeat>(msg.clone()) {
                    let _ = registry.write().await.update_from_heartbeat(hb);
                }
            }

            "DirectInferenceRequest" if matches!(mode, NodeMode::Worker) => {
                let target = msg["target_peer"].as_str().unwrap_or("");
                if target != peer_id.to_string() {
                    return EventLoopDirective::Continue;
                }
                let request_context =
                    build_direct_inference_request_context(&msg, message_topic, &from_peer);
                run_worker_inference_pipeline(
                    swarm,
                    request_context,
                    &peer_id,
                    model_storage,
                    node_caps,
                    inference_engine,
                    inference_backend_state,
                    metrics,
                )
                .await;
            }

            _ => {}
        }
    } else if matches!(mode, NodeMode::Worker)
        && (message_topic == TASK_TOPIC || message_topic == DIRECT_INF_TOPIC)
    {
        log_observability_event(
            LogLevel::Warn,
            "task_message_received",
            "dispatch",
            None,
            None,
            Some(TASK_DISPATCH_UNCONFIRMED_001),
            {
                let mut fields = Map::new();
                fields.insert("topic".to_string(), message_topic.into());
                fields.insert("from_peer".to_string(), from_peer.into());
                fields.insert("payload_parse_result".to_string(), "invalid_json".into());
                fields.insert(
                    "payload_size".to_string(),
                    (message.data.len() as u64).into(),
                );
                fields
            },
        );
    }

    EventLoopDirective::None
}

async fn handle_inference_result_message(
    _swarm: &mut Swarm<IamineBehaviour>,
    msg: &serde_json::Value,
    infer_runtime: &mut InferRuntimeState,
    client_state: &mut ClientRuntimeState,
    registry: &SharedNodeRegistry,
    task_manager: &Arc<TaskManager>,
) -> EventLoopDirective {
    let rid = msg["request_id"].as_str().unwrap_or("");
    if infer_runtime.infer_request_id.as_deref() == Some(rid) {
        let task_id = msg["task_id"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                infer_runtime
                    .distributed_infer_state
                    .as_ref()
                    .map(|state| state.trace_task_id.as_str())
            })
            .unwrap_or(rid)
            .to_string();
        let attempt_id = msg["attempt_id"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(rid)
            .to_string();
        let success = msg["success"].as_bool().unwrap_or(false);
        let tokens = msg["tokens_generated"].as_u64().unwrap_or(0);
        let truncated = msg["truncated"].as_bool().unwrap_or(false);
        let continuation_steps = msg["continuation_steps"].as_u64().unwrap_or(0);
        let ms = msg["execution_ms"].as_u64().unwrap_or(0);
        let worker = msg["worker_peer"].as_str().unwrap_or("unknown");
        let accel = msg["accelerator"].as_str().unwrap_or("unknown");
        let full_output = msg["output"].as_str().unwrap_or("").to_string();
        let latency_ms = infer_runtime
            .infer_started_at
            .as_ref()
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or(ms);
        emit_result_received_event(
            &task_id,
            &attempt_id,
            infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.as_deref()),
            worker,
            latency_ms,
        );
        let validation_status = if success {
            if !should_print_result_output(&full_output) {
                ResultStatus::Retryable("empty distributed inference output".to_string())
            } else if let Some(infer_state) = infer_runtime.distributed_infer_state.as_ref() {
                validate_result(
                    infer_state.current_semantic_prompt.as_deref().unwrap_or(""),
                    infer_state
                        .current_task_type
                        .unwrap_or(PromptTaskType::General),
                    &full_output,
                )
            } else {
                ResultStatus::Valid
            }
        } else {
            ResultStatus::Retryable(
                msg["error"]
                    .as_str()
                    .unwrap_or("unknown inference failure")
                    .to_string(),
            )
        };

        if success && should_print_result_output(&full_output) {
            if infer_runtime.rendered_output.is_empty() {
                print!("{}", full_output);
            } else if full_output.starts_with(&infer_runtime.rendered_output) {
                let suffix = &full_output[infer_runtime.rendered_output.len()..];
                if !suffix.is_empty() {
                    print!("{}", suffix);
                }
            }
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }

        let retry_reason = match validation_status {
            ResultStatus::Valid => None,
            ResultStatus::Invalid(reason) | ResultStatus::Retryable(reason) => Some(reason),
        };

        if let Some(reason) = retry_reason {
            let failure_kind = if success {
                FailureKind::InvalidOutput
            } else {
                FailureKind::TaskFailure
            };
            let current_model = infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.clone());
            println!(
                "\n[Fault] task_id={} attempt_id={} peer_id={} model_id={} reason={}",
                task_id,
                attempt_id,
                worker,
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
                    task_id: &task_id,
                    attempt_id: &attempt_id,
                    worker_peer_id: worker,
                    reason: &reason,
                    output: &full_output,
                    failure_kind,
                    failure_error_kind: "validation_failure",
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
            &task_id,
            &attempt_id,
            worker,
            infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.as_deref()),
            latency_ms,
            registry,
            &mut infer_runtime.attempt_watchdogs,
        )
        .await;
        task_manager
            .complete(DistributedTaskResult::success_for_attempt(
                task_id.clone(),
                attempt_id.clone(),
                full_output.clone(),
            ))
            .await;
        println!("\n\n═══════════════════════════════════");
        if success && should_print_result_output(&full_output) {
            println!(
                "✅ Distributed inference completada: task_id={} attempt_id={} peer_id={} model_id={}",
                task_id,
                attempt_id,
                worker,
                infer_runtime
                    .distributed_infer_state
                    .as_ref()
                    .and_then(|state| state.current_model.as_deref())
                    .unwrap_or("-")
            );
            println!("   Worker:  {}...", &worker[..12.min(worker.len())]);
            println!("   Tokens:  {}", tokens);
            println!("   Truncated: {}", truncated);
            println!("   Continuations: {}", continuation_steps);
            println!("   Tiempo:  {}ms", ms);
            println!("   Accel:   {}", accel);
            if truncated {
                println!("[Warning] Output truncated at token budget");
            }
        } else {
            let err = msg["error"].as_str().unwrap_or("unknown");
            println!(
                "❌ Inference falló: task_id={} attempt_id={} peer_id={} error={}",
                task_id, attempt_id, worker, err
            );
        }
        println!("═══════════════════════════════════");
        clear_stream_state(
            &mut infer_runtime.token_buffer,
            &mut infer_runtime.next_token_idx,
            &mut infer_runtime.rendered_output,
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

    let task_id = msg["task_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            infer_runtime
                .distributed_infer_state
                .as_ref()
                .map(|state| state.trace_task_id.as_str())
        })
        .unwrap_or(rid)
        .to_string();
    let attempt_id = msg["attempt_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(rid)
        .to_string();
    let worker = msg["worker_peer"].as_str().unwrap_or("unknown");
    let success = msg["success"].as_bool().unwrap_or(false);
    let output = msg["output"].as_str().unwrap_or("").to_string();
    let execution_ms = msg["execution_ms"].as_u64().unwrap_or(0);
    apply_late_result_lifecycle(
        LateResultLifecycleInput {
            task_id: &task_id,
            attempt_id: &attempt_id,
            worker_peer_id: worker,
            model_id: infer_runtime
                .distributed_infer_state
                .as_ref()
                .and_then(|state| state.current_model.as_deref()),
            success,
            output: &output,
            execution_ms,
        },
        &mut infer_runtime.attempt_watchdogs,
        registry,
    )
    .await;
    println!(
        "[LateResult] task_id={} attempt_id={} worker={} policy=ignored",
        task_id, attempt_id, worker
    );
    EventLoopDirective::None
}
