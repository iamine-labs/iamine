use super::*;

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
