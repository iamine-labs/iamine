use super::*;

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
fn test_late_results_accounting_does_not_increment_failed_tasks() {
    let mut metrics = DistributedTaskMetrics::default();
    metrics.late_result_recorded();
    assert_eq!(metrics.late_results_count, 1);
    assert_eq!(metrics.failed_tasks, 0);
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
