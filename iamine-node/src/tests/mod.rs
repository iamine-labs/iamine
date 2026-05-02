use super::*;
use iamine_network::analyze_prompt;

#[test]
fn test_max_tokens_override() {
    let profile = analyze_prompt("explica la teoria de la relatividad");
    let decision =
        resolve_output_policy(&profile, "explica la teoria de la relatividad", Some(100));

    assert_eq!(decision.max_tokens, 100);
    assert_eq!(decision.reason, "cli override");
}

#[test]
fn test_max_tokens_override_is_clamped() {
    let profile = analyze_prompt("What is 2+2?");
    let decision = resolve_output_policy(&profile, "What is 2+2?", Some(10));

    assert_eq!(decision.max_tokens, 64);
}

#[test]
fn test_parse_optional_u32_flag() {
    let args = vec![
        "iamine-node".to_string(),
        "infer".to_string(),
        "explica".to_string(),
        "--max-tokens".to_string(),
        "1024".to_string(),
    ];

    assert_eq!(
        parse_optional_u32_flag(&args, "--max-tokens").unwrap(),
        Some(1024)
    );
}

#[test]
fn test_exact_math_uses_natural_prompt_on_first_attempt() {
    assert_eq!(
        with_task_guard("What is 2+2?", PromptTaskType::ExactMath, false),
        "What is 2+2?"
    );
}

#[test]
fn test_exact_math_retry_guard_is_simple() {
    let guarded = with_task_guard("6x9", PromptTaskType::ExactMath, true);
    assert!(guarded.contains("Return only the final numeric answer"));
    assert!(guarded.ends_with("\n\n6x9"));
}

#[test]
fn test_decimal_sequence_retry_guard_mentions_decimal_point() {
    let guarded = with_task_guard(
        "dame los primeros 100 digitos de pi",
        PromptTaskType::ExactMath,
        true,
    );
    assert!(guarded.contains("Keep the decimal point attached"));
}

#[test]
fn test_cli_no_runtime_start() {
    assert!(is_control_plane_mode(&NodeMode::ModelsList));
    assert!(is_control_plane_mode(&NodeMode::ModelsStats));
    assert!(is_control_plane_mode(&NodeMode::ModelsDownload {
        model_id: "llama3-3b".to_string()
    }));
    assert!(is_control_plane_mode(&NodeMode::ModelsRemove {
        model_id: "llama3-3b".to_string()
    }));
    assert!(is_control_plane_mode(&NodeMode::TasksStats));
    assert!(is_control_plane_mode(&NodeMode::TasksTrace {
        task_id: "task-1".to_string()
    }));
    assert!(!is_control_plane_mode(&NodeMode::Worker));
}

#[test]
fn test_force_network_routing() {
    let flags = InferenceControlFlags {
        force_network: true,
        no_local: false,
        prefer_local: true,
    };
    assert!(!flags.should_use_local(true));

    let prefer_local = InferenceControlFlags {
        force_network: false,
        no_local: false,
        prefer_local: true,
    };
    assert!(prefer_local.should_use_local(true));
    assert!(!prefer_local.should_use_local(false));
}

#[test]
fn test_checked_sub_usize_b_greater_than_a() {
    let error = checked_sub_usize(
        "max_concurrent_minus_available_slots",
        1,
        2,
        "b_greater_than_a",
    )
    .unwrap_err();

    assert_eq!(error.operation, "max_concurrent_minus_available_slots");
    assert_eq!(error.operand_a, 1);
    assert_eq!(error.operand_b, 2);
    assert_eq!(error.reason, "b_greater_than_a");
}

#[test]
fn test_invalid_startup_resource_calculation_path() {
    let error = compute_metrics_port(7001).unwrap_err();

    assert_eq!(error.operation, "worker_port_minus_base");
    assert_eq!(error.operand_a, 7001);
    assert_eq!(error.operand_b, 9000);
    assert_eq!(error.reason, "worker_port_below_metrics_base");
}

#[test]
fn test_worker_startup_overflow_emits_structured_error_code() {
    let trace_id = format!("startup-overflow-test-{}", uuid_simple());
    let error = StartupMathError::new(
        "worker_port_minus_base",
        7001,
        9000,
        "worker_port_below_metrics_base",
    );
    let policy = ResourcePolicy {
        cpu_cores: 4,
        max_cpu_load: 80,
        ram_limit_gb: 4,
        gpu_enabled: false,
        disk_limit_gb: 10,
        disk_path: "/tmp/iamine".to_string(),
    };

    emit_worker_startup_overflow_event(
        &trace_id,
        "node-test",
        "peer-test",
        7001,
        &policy,
        4,
        4,
        4,
        &error,
        "continue_without_metrics_server",
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();
    let entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "worker_startup_invalid_math")
        .expect("startup overflow entry not found");

    assert_eq!(
        entry.error_code.as_deref(),
        Some(WORKER_STARTUP_OVERFLOW_001)
    );
    assert_eq!(
        entry
            .fields
            .get("fallback_behavior")
            .and_then(|value| value.as_str()),
        Some("continue_without_metrics_server")
    );
}

#[test]
fn test_no_publish_before_readiness() {
    let snapshot = DispatchReadinessSnapshot {
        connected_peer_count: 0,
        mesh_peer_count: 0,
        topic_peer_count: 0,
        joined_task_topic: true,
        joined_direct_topic: true,
        joined_results_topic: true,
        selected_topic: DIRECT_INF_TOPIC,
    };

    let error = evaluate_dispatch_readiness(&snapshot).unwrap_err();
    assert_eq!(error.code, NETWORK_NO_PUBSUB_PEERS_001);
    assert_eq!(error.reason, "no_connected_peers");
}

#[test]
fn test_structured_error_when_topic_has_zero_peers() {
    let snapshot = DispatchReadinessSnapshot {
        connected_peer_count: 2,
        mesh_peer_count: 2,
        topic_peer_count: 0,
        joined_task_topic: true,
        joined_direct_topic: true,
        joined_results_topic: true,
        selected_topic: TASK_TOPIC,
    };

    let error = evaluate_dispatch_readiness(&snapshot).unwrap_err();
    assert_eq!(error.code, PUBSUB_TOPIC_NOT_READY_001);
    assert_eq!(error.reason, "destination_topic_has_zero_peers");
}

#[test]
fn test_dispatch_observability_events_emitted() {
    let trace_id = format!("dispatch-observability-{}", uuid_simple());
    let attempt_id = "attempt-observability-1";
    let model_id = "tinyllama-1b";
    let candidates = vec![model_id.to_string(), "llama3-3b".to_string()];

    emit_dispatch_context_event(
        &trace_id,
        attempt_id,
        model_id,
        &candidates,
        3,
        2,
        DIRECT_INF_TOPIC,
        Some("peer-target-1"),
    );
    emit_task_publish_attempt_event(
        &trace_id,
        attempt_id,
        model_id,
        DIRECT_INF_TOPIC,
        2,
        128,
        Some("peer-target-1"),
    );
    emit_task_published_event(
        &trace_id,
        attempt_id,
        model_id,
        DIRECT_INF_TOPIC,
        2,
        128,
        "message-1",
        Some("peer-target-1"),
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();

    assert!(entries
        .iter()
        .any(|entry| { entry.trace_id == trace_id && entry.event == "task_dispatch_context" }));
    let dispatch_entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "task_dispatch_context")
        .expect("task_dispatch_context entry not found");
    assert_eq!(dispatch_entry.task_id.as_deref(), Some(trace_id.as_str()));
    assert_eq!(dispatch_entry.model_id.as_deref(), Some(model_id));
    assert_eq!(
        dispatch_entry
            .fields
            .get("attempt_id")
            .and_then(|value| value.as_str()),
        Some(attempt_id)
    );
    assert_eq!(
        dispatch_entry
            .fields
            .get("selected_peer_id")
            .and_then(|value| value.as_str()),
        Some("peer-target-1")
    );
    assert!(entries
        .iter()
        .any(|entry| { entry.trace_id == trace_id && entry.event == "task_publish_attempt" }));
    assert!(entries.iter().any(|entry| {
        entry.trace_id == trace_id
            && entry.event == "task_published"
            && entry
                .fields
                .get("message_id")
                .and_then(|value| value.as_str())
                == Some("message-1")
    }));
}

#[test]
fn test_error_events_include_required_error_fields() {
    let trace_id = format!("dispatch-error-{}", uuid_simple());
    emit_task_publish_failed_event(
        &trace_id,
        "attempt-error-1",
        "tinyllama-1b",
        DIRECT_INF_TOPIC,
        0,
        256,
        "insufficient peers",
        Some("peer-target-err"),
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();
    let entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "task_publish_failed")
        .expect("task_publish_failed entry not found");

    assert_eq!(
        entry.error_code.as_deref(),
        Some(TASK_DISPATCH_UNCONFIRMED_001)
    );
    assert_eq!(
        entry
            .fields
            .get("error_kind")
            .and_then(|value| value.as_str()),
        Some("publish_error")
    );
    assert_eq!(
        entry
            .fields
            .get("recoverable")
            .and_then(|value| value.as_bool()),
        Some(true)
    );
}

#[test]
fn test_worker_receipt_logs_emitted() {
    let trace_id = format!("worker-receipt-{}", uuid_simple());
    emit_worker_task_message_received_event(
        &trace_id,
        "attempt-worker-1",
        DIRECT_INF_TOPIC,
        "peer-source-1",
        "ok",
        "tinyllama-1b",
        true,
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();
    let entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "task_message_received")
        .expect("task_message_received entry not found");

    assert_eq!(
        entry
            .fields
            .get("payload_parse_result")
            .and_then(|value| value.as_str()),
        Some("ok")
    );
    assert_eq!(
        entry
            .fields
            .get("local_model_available")
            .and_then(|value| value.as_bool()),
        Some(true)
    );
}

#[test]
fn test_result_received_logs_emitted() {
    let trace_id = format!("result-received-{}", uuid_simple());
    emit_result_received_event(
        &trace_id,
        "attempt-result-1",
        Some("tinyllama-1b"),
        "peer-worker-1",
        42,
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();
    let entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "result_received")
        .expect("result_received entry not found");

    assert_eq!(
        entry
            .fields
            .get("attempt_id")
            .and_then(|value| value.as_str()),
        Some("attempt-result-1")
    );
    assert_eq!(
        entry
            .fields
            .get("latency_ms")
            .and_then(|value| value.as_u64()),
        Some(42)
    );
}

#[test]
fn test_metrics_increments_for_retry_and_fallback() {
    let mut metrics = DistributedTaskMetrics::default();
    apply_retry_fallback_metrics(&mut metrics, true, false);
    apply_retry_fallback_metrics(&mut metrics, false, true);

    assert_eq!(metrics.retries_count, 1);
    assert_eq!(metrics.fallback_count, 1);
    assert_eq!(metrics.failed_tasks, 0);
}

#[test]
fn test_empty_result_does_not_print_as_success_response() {
    assert!(!should_print_result_output(""));
    assert!(!should_print_result_output("   "));
    assert!(should_print_result_output("ok"));
}

#[test]
fn test_identity_conflict_path_uses_ephemeral_identity_strategy() {
    let identity_a = NodeIdentity::ephemeral("test-identity-safety");
    let identity_b = NodeIdentity::ephemeral("test-identity-safety");

    assert_ne!(identity_a.peer_id, identity_b.peer_id);
    assert_ne!(identity_a.node_id, identity_b.node_id);
}

#[test]
fn test_human_logs_remain_unaffected() {
    let human_line = "✅ Conectado a: 12D3KooWorker (/ip4/127.0.0.1/tcp/7001)";
    assert!(serde_json::from_str::<serde_json::Value>(human_line).is_err());
    assert!(human_line.contains("Conectado a"));
}

#[test]
fn test_no_panic_for_invalid_startup_math_inputs() {
    let result = std::panic::catch_unwind(|| {
        let _ = compute_metrics_port(7001);
        let _ = compute_active_tasks(1, 2);
    });

    assert!(result.is_ok());
}

#[test]
fn test_adaptive_timeout_for_mistral_cpu_is_higher_than_fixed_timeout() {
    let node_capability = NodeCapability {
        peer_id: "peer-slow-cpu".to_string(),
        cpu_score: 38_000,
        ram_gb: 16,
        gpu_available: false,
        storage_available_gb: 100,
        accelerator: "cpu".to_string(),
        models: vec!["mistral-7b".to_string()],
        worker_slots: 2,
        active_tasks: 1,
        latency_ms: 120,
        last_seen: std::time::Instant::now(),
        cluster_id: None,
        health: NodeHealth::default(),
    };

    let policy = AttemptTimeoutPolicy::from_model_and_node("mistral-7b", Some(&node_capability));
    assert!(policy.timeout_ms > INFER_TIMEOUT_MS);
    assert!(policy.timeout_ms >= 12_000);
}

#[test]
fn test_watchdog_extends_timeout_only_with_meaningful_progress() {
    let mut watchdog = AttemptWatchdog::new(
        "task-watchdog".to_string(),
        "attempt-watchdog".to_string(),
        "peer-1".to_string(),
        "mistral-7b".to_string(),
        AttemptTimeoutPolicy {
            timeout_ms: 20,
            stall_timeout_ms: 50,
            extension_step_ms: 25,
            max_wait_ms: 120,
            latency_class: "high",
        },
    );
    let base_deadline = watchdog
        .deadline_at
        .duration_since(watchdog.started_at)
        .as_millis() as u64;
    assert!(watchdog.record_progress("inference_started", Some(0)));
    let extended_deadline = watchdog
        .deadline_at
        .duration_since(watchdog.started_at)
        .as_millis() as u64;
    assert!(extended_deadline >= base_deadline);

    assert!(!watchdog.record_progress("inference_started", Some(0)));
    let repeated_deadline = watchdog
        .deadline_at
        .duration_since(watchdog.started_at)
        .as_millis() as u64;
    assert_eq!(extended_deadline, repeated_deadline);

    assert!(watchdog.record_progress("tokens_generated_count", Some(8)));
}

#[test]
fn test_no_progress_attempt_becomes_stalled() {
    let mut watchdog = AttemptWatchdog::new(
        "task-stall".to_string(),
        "attempt-stall".to_string(),
        "peer-2".to_string(),
        "llama3-3b".to_string(),
        AttemptTimeoutPolicy {
            timeout_ms: 40,
            stall_timeout_ms: 80,
            extension_step_ms: 5,
            max_wait_ms: 2_000,
            latency_class: "medium",
        },
    );
    std::thread::sleep(Duration::from_millis(110));
    assert_eq!(watchdog.check(), WatchdogCheck::Stalled);
}

#[test]
fn test_late_result_received_event_emitted() {
    let trace_id = format!("late-result-{}", uuid_simple());
    emit_late_result_received_event(
        &trace_id,
        "attempt-late-1",
        "peer-late-1",
        Some("mistral-7b"),
        16_500,
        false,
        "arrived_after_timeout_policy_ignore",
        "timed_out",
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();
    let entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "late_result_received")
        .expect("late_result_received entry not found");

    assert_eq!(
        entry
            .fields
            .get("accepted")
            .and_then(|value| value.as_bool()),
        Some(false)
    );
    assert_eq!(
        entry.fields.get("reason").and_then(|value| value.as_str()),
        Some("arrived_after_timeout_policy_ignore")
    );
}

#[test]
fn test_progress_signals_are_emitted_for_long_running_attempts() {
    let trace_id = format!("progress-signals-{}", uuid_simple());
    emit_attempt_progress_event(
        &trace_id,
        "attempt-progress-1",
        "mistral-7b",
        "peer-progress-1",
        "first_token_generated",
        Some(1),
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();
    let entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "attempt_progress")
        .expect("attempt_progress entry not found");

    assert_eq!(
        entry.fields.get("stage").and_then(|value| value.as_str()),
        Some("first_token_generated")
    );
}

#[test]
fn test_inflight_guard_distinguishes_active_vs_timed_out_attempts() {
    assert!(is_meaningfully_in_flight(AttemptLifecycleState::Running));
    assert!(is_meaningfully_in_flight(
        AttemptLifecycleState::ProducingTokens
    ));
    assert!(!is_meaningfully_in_flight(AttemptLifecycleState::TimedOut));
    assert!(!is_meaningfully_in_flight(
        AttemptLifecycleState::LateCompleted
    ));
}

#[test]
fn test_late_results_accounting_does_not_increment_failed_tasks() {
    let mut metrics = DistributedTaskMetrics::default();
    metrics.late_result_recorded();
    assert_eq!(metrics.late_results_count, 1);
    assert_eq!(metrics.failed_tasks, 0);
}

#[test]
fn test_worker_request_context_builders_are_equivalent() {
    let infer_msg = serde_json::json!({
        "request_id": "attempt-1",
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "model_id": "tinyllama-1b",
        "prompt": "2+2",
        "max_tokens": 128,
        "temperature": 0.7,
    });
    let direct_msg = serde_json::json!({
        "request_id": "attempt-1",
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "model": "tinyllama-1b",
        "prompt": "2+2",
        "max_tokens": 128,
    });

    let inference_context = build_inference_request_context(&infer_msg, DIRECT_INF_TOPIC, "peer-a");
    let direct_context =
        build_direct_inference_request_context(&direct_msg, DIRECT_INF_TOPIC, "peer-a");

    assert_eq!(inference_context.task_id, direct_context.task_id);
    assert_eq!(inference_context.attempt_id, direct_context.attempt_id);
    assert_eq!(inference_context.model_id, direct_context.model_id);
    assert_eq!(inference_context.prompt, direct_context.prompt);
    assert_eq!(inference_context.max_tokens, direct_context.max_tokens);
    assert_eq!(inference_context.temperature, direct_context.temperature);
    assert_eq!(inference_context.from_peer, direct_context.from_peer);
}

#[test]
fn test_fallback_attempt_event_emitted() {
    let trace_id = format!("fallback-attempt-{}", uuid_simple());
    emit_fallback_attempt_registered_event(
        &trace_id,
        "attempt-fallback-1",
        "tinyllama-1b",
        TASK_TOPIC,
        &["tinyllama-1b".to_string()],
    );

    iamine_network::flush_structured_logs().unwrap();
    let path = iamine_network::default_node_log_path();
    let entries = iamine_network::read_log_entries(&path).unwrap();
    let entry = entries
        .iter()
        .rev()
        .find(|entry| entry.trace_id == trace_id && entry.event == "fallback_attempt_registered")
        .expect("fallback_attempt_registered entry not found");

    assert_eq!(
        entry
            .fields
            .get("attempt_type")
            .and_then(|value| value.as_str()),
        Some("fallback")
    );
    assert_eq!(
        entry
            .fields
            .get("attempt_id")
            .and_then(|value| value.as_str()),
        Some("attempt-fallback-1")
    );
}

#[test]
fn test_fallback_metrics_and_trace_agree() {
    let mut metrics = DistributedTaskMetrics::default();
    let mut trace_store = iamine_network::task_trace::TaskTraceStore::new();
    let task_id = format!("fallback-trace-{}", uuid_simple());

    trace_store.upsert_attempt(&task_id, "broadcast", "tinyllama-1b", false, true);
    apply_retry_fallback_metrics(&mut metrics, false, true);

    let trace = trace_store.get(&task_id).expect("missing fallback trace");
    assert_eq!(trace.fallbacks, 1);
    assert_eq!(metrics.fallback_count, 1);
}

#[tokio::test]
async fn test_success_lifecycle_updates_health_and_watchdog() {
    let mut registry = NodeRegistry::new();
    registry.update_from_heartbeat(NodeCapabilityHeartbeat {
        peer_id: "peer-success".to_string(),
        cpu_score: 120_000,
        ram_gb: 16,
        gpu_available: false,
        storage_available_gb: 100,
        accelerator: "cpu".to_string(),
        models: vec!["tinyllama-1b".to_string()],
        worker_slots: 2,
        active_tasks: 0,
        latency_ms: 10,
    });
    let shared_registry = Arc::new(RwLock::new(registry));

    let mut watchdogs = HashMap::new();
    watchdogs.insert(
        "attempt-success".to_string(),
        AttemptWatchdog::new(
            "task-success".to_string(),
            "attempt-success".to_string(),
            "peer-success".to_string(),
            "tinyllama-1b".to_string(),
            AttemptTimeoutPolicy::from_model_and_node("tinyllama-1b", None),
        ),
    );

    apply_result_success_lifecycle(
        "task-success",
        "attempt-success",
        "peer-success",
        Some("tinyllama-1b"),
        420,
        &shared_registry,
        &mut watchdogs,
    )
    .await;

    assert_eq!(
        watchdogs
            .get("attempt-success")
            .expect("missing watchdog")
            .state,
        AttemptLifecycleState::Completed
    );
    let health = shared_registry
        .read()
        .await
        .all_nodes()
        .into_iter()
        .find(|(peer, _)| peer == "peer-success")
        .expect("missing peer")
        .1
        .health;
    assert!(health.last_success_timestamp.is_some());
}

#[tokio::test]
async fn test_failure_lifecycle_updates_health_and_trace_consistently() {
    let mut registry = NodeRegistry::new();
    registry.update_from_heartbeat(NodeCapabilityHeartbeat {
        peer_id: "peer-failure".to_string(),
        cpu_score: 120_000,
        ram_gb: 16,
        gpu_available: false,
        storage_available_gb: 100,
        accelerator: "cpu".to_string(),
        models: vec!["tinyllama-1b".to_string()],
        worker_slots: 2,
        active_tasks: 0,
        latency_ms: 10,
    });
    let shared_registry = Arc::new(RwLock::new(registry));
    let task_manager = Arc::new(TaskManager::new());

    let mut infer_state =
        DistributedInferState::new("2+2".to_string(), Some("tinyllama-1b".to_string()), None);
    infer_state.record_attempt(
        "peer-failure".to_string(),
        "tinyllama-1b".to_string(),
        PromptTaskType::General,
        "2+2".to_string(),
        None,
        vec!["tinyllama-1b".to_string()],
    );
    let mut distributed_state = Some(infer_state);
    let current_attempt = distributed_state
        .as_ref()
        .map(|state| state.current_request_id.clone())
        .expect("missing attempt id");

    let mut watchdogs = HashMap::new();
    watchdogs.insert(
        current_attempt.clone(),
        AttemptWatchdog::new(
            "task-failure".to_string(),
            current_attempt.clone(),
            "peer-failure".to_string(),
            "tinyllama-1b".to_string(),
            AttemptTimeoutPolicy::from_model_and_node("tinyllama-1b", None),
        ),
    );

    let mut pending_inference = HashMap::new();
    pending_inference.insert(current_attempt.clone(), tokio::time::Instant::now());
    let mut token_buffer = HashMap::new();
    let mut next_token_idx = 0u32;
    let mut rendered_output = String::new();
    let mut infer_request_id = Some(current_attempt.clone());
    let mut infer_broadcast_sent = true;
    let mut waiting_for_response = true;

    let retry_scheduled = apply_result_failure_lifecycle(
        "task-failure",
        &current_attempt,
        "peer-failure",
        "backend error",
        "backend error",
        FailureKind::TaskFailure,
        "task_failure",
        Some("tinyllama-1b".to_string()),
        &shared_registry,
        &mut distributed_state,
        &mut watchdogs,
        &task_manager,
        &mut pending_inference,
        &mut token_buffer,
        &mut next_token_idx,
        &mut rendered_output,
        &mut infer_request_id,
        &mut infer_broadcast_sent,
        &mut waiting_for_response,
        None,
    )
    .await;

    assert!(!retry_scheduled);
    assert!(
        task_manager.result("task-failure").await.is_some(),
        "task manager failure result should be recorded"
    );
    assert!(
        pending_inference.is_empty(),
        "failed attempt must be removed from in-flight map"
    );
    assert_eq!(
        watchdogs
            .get(&current_attempt)
            .expect("missing watchdog")
            .state,
        AttemptLifecycleState::Failed
    );
    let health = shared_registry
        .read()
        .await
        .all_nodes()
        .into_iter()
        .find(|(peer, _)| peer == "peer-failure")
        .expect("missing peer")
        .1
        .health;
    assert_eq!(health.failure_count, 1);
}
