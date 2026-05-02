use super::*;

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
