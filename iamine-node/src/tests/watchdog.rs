use super::*;

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
        inference_backend: "real".to_string(),
        real_inference_available: true,
        mock_inference_enabled: false,
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
    emit_late_result_received_event(LateResultReceivedEvent {
        trace_task_id: &trace_id,
        attempt_id: "attempt-late-1",
        worker_peer_id: "peer-late-1",
        model_id: Some("mistral-7b"),
        elapsed_ms: 16_500,
        accepted: false,
        reason: "arrived_after_timeout_policy_ignore",
        prior_attempt_state: "timed_out",
    });

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
