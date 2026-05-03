use super::*;

pub(super) struct StartupMathError {
    pub(super) operation: &'static str,
    pub(super) operand_a: u64,
    pub(super) operand_b: u64,
    pub(super) reason: &'static str,
}

impl StartupMathError {
    pub(super) fn new(
        operation: &'static str,
        operand_a: u64,
        operand_b: u64,
        reason: &'static str,
    ) -> Self {
        Self {
            operation,
            operand_a,
            operand_b,
            reason,
        }
    }

    pub(super) fn describe(&self) -> String {
        format!(
            "{} failed (a={}, b={}, reason={})",
            self.operation, self.operand_a, self.operand_b, self.reason
        )
    }
}

pub(super) fn checked_sub_u16(
    operation: &'static str,
    a: u16,
    b: u16,
    reason: &'static str,
) -> Result<u16, StartupMathError> {
    a.checked_sub(b)
        .ok_or_else(|| StartupMathError::new(operation, a as u64, b as u64, reason))
}

pub(super) fn checked_sub_usize(
    operation: &'static str,
    a: usize,
    b: usize,
    reason: &'static str,
) -> Result<usize, StartupMathError> {
    a.checked_sub(b)
        .ok_or_else(|| StartupMathError::new(operation, a as u64, b as u64, reason))
}

pub(super) fn compute_metrics_port(worker_port: u16) -> Result<u16, StartupMathError> {
    let offset = checked_sub_u16(
        "worker_port_minus_base",
        worker_port,
        9000,
        "worker_port_below_metrics_base",
    )?;

    9090u16.checked_add(offset).ok_or_else(|| {
        StartupMathError::new(
            "metrics_port_plus_offset",
            9090,
            offset as u64,
            "metrics_port_out_of_range",
        )
    })
}

pub(super) fn compute_active_tasks(
    max_concurrent: usize,
    available_slots: usize,
) -> Result<usize, StartupMathError> {
    checked_sub_usize(
        "max_concurrent_minus_available_slots",
        max_concurrent,
        available_slots,
        "available_slots_exceeds_max_concurrent",
    )
}

pub(super) fn resource_policy_value(resource_policy: &ResourcePolicy) -> Value {
    serde_json::json!({
        "cpu_cores": resource_policy.cpu_cores,
        "max_cpu_load": resource_policy.max_cpu_load,
        "ram_limit_gb": resource_policy.ram_limit_gb,
        "gpu_enabled": resource_policy.gpu_enabled,
        "disk_limit_gb": resource_policy.disk_limit_gb,
        "disk_path": resource_policy.disk_path,
    })
}

pub(super) struct WorkerStartupOverflowContext<'a> {
    pub(super) trace_id: &'a str,
    pub(super) node_id: &'a str,
    pub(super) peer_id: &'a str,
    pub(super) port: u16,
    pub(super) resource_policy: &'a ResourcePolicy,
    pub(super) worker_slots: usize,
    pub(super) max_concurrent: usize,
    pub(super) available_slots: usize,
    pub(super) error: &'a StartupMathError,
    pub(super) fallback_behavior: &'a str,
}

pub(super) fn emit_worker_startup_overflow_event(context: WorkerStartupOverflowContext<'_>) {
    let WorkerStartupOverflowContext {
        trace_id,
        node_id,
        peer_id,
        port,
        resource_policy,
        worker_slots,
        max_concurrent,
        available_slots,
        error,
        fallback_behavior,
    } = context;
    let mut fields = Map::new();
    fields.insert("node_id".to_string(), node_id.into());
    fields.insert("peer_id".to_string(), peer_id.into());
    fields.insert("port".to_string(), (port as u64).into());
    fields.insert(
        "resource_policy".to_string(),
        resource_policy_value(resource_policy),
    );
    fields.insert(
        "slots".to_string(),
        serde_json::json!({
            "worker_slots": worker_slots,
            "max_concurrent": max_concurrent,
            "available_slots": available_slots,
        }),
    );
    fields.insert("operation".to_string(), error.operation.into());
    fields.insert("operand_a".to_string(), error.operand_a.into());
    fields.insert("operand_b".to_string(), error.operand_b.into());
    fields.insert("reason".to_string(), error.reason.into());
    fields.insert("fallback_behavior".to_string(), fallback_behavior.into());

    log_observability_event(
        LogLevel::Error,
        "worker_startup_invalid_math",
        trace_id,
        None,
        None,
        Some(WORKER_STARTUP_OVERFLOW_001),
        fields,
    );
}
