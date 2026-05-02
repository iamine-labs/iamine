use super::*;

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
fn test_no_panic_for_invalid_startup_math_inputs() {
    let result = std::panic::catch_unwind(|| {
        let _ = compute_metrics_port(7001);
        let _ = compute_active_tasks(1, 2);
    });

    assert!(result.is_ok());
}
