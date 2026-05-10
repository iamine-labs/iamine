use crate::backend_policy::WorkerInferenceBackend;
use crate::cpu_feature_guard::cpu_features_are_compatible_for_real_backend;
use crate::log_observability_event;
use crate::resource_policy::ResourcePolicy;
use iamine_models::ModelNodeCapabilities;
use iamine_network::{LogLevel, MODEL_LOAD_FAILED_001, MODEL_UNSUPPORTED_HW_002, TASK_FAILED_002};
use libp2p::Multiaddr;
use serde_json::{Map, Value};

use iamine_network::WORKER_STARTUP_OVERFLOW_001;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerStartupPolicy {
    pub(crate) backend: WorkerInferenceBackend,
    pub(crate) skip_model_load_on_startup: bool,
    pub(crate) cpu_feature_compatible: bool,
    pub(crate) real_inference_available: bool,
    pub(crate) model_load_skip_reason: Option<&'static str>,
}

impl WorkerStartupPolicy {
    pub(crate) fn from_env(node_caps: &ModelNodeCapabilities) -> Self {
        let backend_env = std::env::var("IAMINE_INFERENCE_BACKEND").ok();
        let skip_env = std::env::var("IAMINE_SKIP_MODEL_LOAD_ON_STARTUP").ok();
        Self::from_values(
            backend_env.as_deref(),
            skip_env.as_deref(),
            &node_caps.cpu_features,
            &node_caps.accelerator,
            std::env::consts::ARCH,
        )
    }

    pub(crate) fn from_values(
        backend_env: Option<&str>,
        skip_env: Option<&str>,
        cpu_features: &[String],
        accelerator: &str,
        target_arch: &str,
    ) -> Self {
        let backend = WorkerInferenceBackend::from_env_value(backend_env);
        let skip_model_load_on_startup = env_truthy_option(skip_env);
        let cpu_feature_compatible =
            cpu_features_are_compatible_for_real_backend(cpu_features, accelerator, target_arch);
        let model_load_skip_reason = if backend.is_mock() {
            Some("mock_backend")
        } else if skip_model_load_on_startup {
            Some("skip_model_load_on_startup")
        } else if !cpu_feature_compatible {
            Some("cpu_feature_incompatible")
        } else {
            None
        };
        let real_inference_available =
            backend.is_real() && !skip_model_load_on_startup && cpu_feature_compatible;

        Self {
            backend,
            skip_model_load_on_startup,
            cpu_feature_compatible,
            real_inference_available,
            model_load_skip_reason,
        }
    }

    pub(crate) fn mock_backend(&self) -> bool {
        self.backend.is_mock()
    }
}

pub(crate) fn env_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn env_truthy_option(value: Option<&str>) -> bool {
    value.map(env_truthy).unwrap_or(false)
}

pub(crate) fn emit_worker_startup_started_event(peer_id: &str, port: u16) {
    log_observability_event(
        LogLevel::Info,
        "worker_startup_started",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("port".to_string(), (port as u64).into());
            fields
        },
    );
}

pub(crate) fn emit_inference_backend_selected_event(policy: &WorkerStartupPolicy) {
    log_observability_event(
        LogLevel::Info,
        "inference_backend_selected",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("backend".to_string(), policy.backend.as_str().into());
            fields.insert(
                "skip_model_load_on_startup".to_string(),
                policy.skip_model_load_on_startup.into(),
            );
            fields.insert(
                "real_inference_available".to_string(),
                policy.real_inference_available.into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_worker_model_load_attempt_event(model_id: &str, reason: &str) {
    log_observability_event(
        LogLevel::Info,
        "worker_model_load_attempt",
        "startup",
        None,
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("reason".to_string(), reason.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_model_load_skipped_event(reason: &str) {
    log_observability_event(
        LogLevel::Info,
        "worker_model_load_skipped",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("reason".to_string(), reason.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_model_load_failed_event(model_id: &str, error: &str, action: &str) {
    log_observability_event(
        LogLevel::Warn,
        "worker_model_load_failed",
        "startup",
        None,
        Some(model_id),
        Some(MODEL_LOAD_FAILED_001),
        {
            let mut fields = Map::new();
            fields.insert("error".to_string(), error.into());
            fields.insert("action".to_string(), action.into());
            fields
        },
    );
}

pub(crate) fn emit_backend_cpu_feature_incompatible_event(
    cpu_features: &[String],
    accelerator: &str,
    action: &str,
) {
    log_observability_event(
        LogLevel::Warn,
        "backend_cpu_feature_incompatible",
        "startup",
        None,
        None,
        Some(MODEL_UNSUPPORTED_HW_002),
        {
            let mut fields = Map::new();
            fields.insert("accelerator".to_string(), accelerator.into());
            fields.insert("cpu_features".to_string(), serde_json::json!(cpu_features));
            fields.insert("required_feature".to_string(), "AVX2".into());
            fields.insert("action".to_string(), action.into());
            fields
        },
    );
}

pub(crate) fn emit_worker_listening_event(peer_id: &str, port: u16, address: &Multiaddr) {
    log_observability_event(
        LogLevel::Info,
        "worker_listening",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("port".to_string(), (port as u64).into());
            fields.insert("address".to_string(), address.to_string().into());
            fields
        },
    );
}

pub(crate) fn emit_worker_startup_ready_event(
    peer_id: &str,
    port: u16,
    policy: &WorkerStartupPolicy,
) {
    log_observability_event(
        LogLevel::Info,
        "worker_startup_ready",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("port".to_string(), (port as u64).into());
            fields.insert("backend".to_string(), policy.backend.as_str().into());
            fields.insert(
                "real_inference_available".to_string(),
                policy.real_inference_available.into(),
            );
            fields
        },
    );
}

pub(crate) fn emit_worker_startup_failed_event(
    peer_id: &str,
    port: u16,
    reason: &str,
    action_hint: &str,
) {
    log_observability_event(
        LogLevel::Error,
        "worker_startup_failed",
        "startup",
        None,
        None,
        Some(TASK_FAILED_002),
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("port".to_string(), (port as u64).into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert("action_hint".to_string(), action_hint.into());
            fields
        },
    );
}

pub(crate) fn startup_listen_error_hint(error: &str, port: u16) -> String {
    let lower = error.to_ascii_lowercase();
    if lower.contains("address already in use") || lower.contains("addrinuse") {
        format!(
            "port {} already in use; try: lsof -nP -iTCP:{} -sTCP:LISTEN",
            port, port
        )
    } else {
        "check listen address and worker port".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartupMathError {
    pub(crate) operation: &'static str,
    pub(crate) operand_a: u64,
    pub(crate) operand_b: u64,
    pub(crate) reason: &'static str,
}

impl StartupMathError {
    pub(crate) fn new(
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

    pub(crate) fn describe(&self) -> String {
        format!(
            "{} failed (a={}, b={}, reason={})",
            self.operation, self.operand_a, self.operand_b, self.reason
        )
    }
}

pub(crate) fn checked_sub_u16(
    operation: &'static str,
    a: u16,
    b: u16,
    reason: &'static str,
) -> Result<u16, StartupMathError> {
    a.checked_sub(b)
        .ok_or_else(|| StartupMathError::new(operation, a as u64, b as u64, reason))
}

pub(crate) fn checked_sub_usize(
    operation: &'static str,
    a: usize,
    b: usize,
    reason: &'static str,
) -> Result<usize, StartupMathError> {
    a.checked_sub(b)
        .ok_or_else(|| StartupMathError::new(operation, a as u64, b as u64, reason))
}

pub(crate) fn compute_metrics_port(worker_port: u16) -> Result<u16, StartupMathError> {
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

pub(crate) fn compute_active_tasks(
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

pub(crate) fn resource_policy_value(resource_policy: &ResourcePolicy) -> Value {
    serde_json::json!({
        "cpu_cores": resource_policy.cpu_cores,
        "max_cpu_load": resource_policy.max_cpu_load,
        "ram_limit_gb": resource_policy.ram_limit_gb,
        "gpu_enabled": resource_policy.gpu_enabled,
        "disk_limit_gb": resource_policy.disk_limit_gb,
        "disk_path": resource_policy.disk_path,
    })
}

pub(crate) fn emit_worker_startup_overflow_event(
    trace_id: &str,
    node_id: &str,
    peer_id: &str,
    port: u16,
    resource_policy: &ResourcePolicy,
    worker_slots: usize,
    max_concurrent: usize,
    available_slots: usize,
    error: &StartupMathError,
    fallback_behavior: &str,
) {
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

#[cfg(test)]
pub(crate) fn test_node_caps_for_startup() -> ModelNodeCapabilities {
    ModelNodeCapabilities {
        node_id: "node-startup-test".to_string(),
        cpu_cores: 8,
        ram_gb: 16,
        gpu_type: None,
        npu_type: None,
        storage_available_gb: 100,
        worker_slots: 4,
        supported_models: vec!["tinyllama-1b".to_string()],
        cpu_features: vec!["AVX2".to_string(), "FMA".to_string()],
        accelerator: "CPU".to_string(),
    }
}

#[cfg(test)]
fn test_id(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend_policy::WorkerInferenceBackend;

    #[test]
    fn startup_policy_respects_skip_model_load() {
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            None,
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );

        assert!(policy.skip_model_load_on_startup);
        assert!(!policy.real_inference_available);
        assert_eq!(
            policy.model_load_skip_reason,
            Some("skip_model_load_on_startup")
        );
    }

    #[test]
    fn backend_policy_does_not_touch_real_backend_when_mock() {
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            None,
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );

        assert_eq!(policy.backend, WorkerInferenceBackend::Mock);
        assert!(policy.mock_backend());
        assert!(!policy.real_inference_available);
        assert_eq!(policy.model_load_skip_reason, Some("mock_backend"));
    }

    #[test]
    fn startup_policy_allows_degraded_worker_when_real_backend_unavailable() {
        let caps = ModelNodeCapabilities {
            cpu_features: vec![],
            ..test_node_caps_for_startup()
        };
        let policy = WorkerStartupPolicy::from_values(
            None,
            None,
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );

        assert!(!policy.cpu_feature_compatible);
        assert!(!policy.real_inference_available);
        assert_eq!(
            policy.model_load_skip_reason,
            Some("cpu_feature_incompatible")
        );
    }

    #[test]
    fn worker_mock_skip_startup_reaches_ready_state() {
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let peer_id = test_id("peer-ready");

        emit_worker_startup_ready_event(&peer_id, 4101, &policy);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "worker_startup_ready"
                && entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
                && entry.fields.get("backend").and_then(|value| value.as_str()) == Some("mock")
        }));
    }

    #[test]
    fn worker_startup_emits_ndjson_before_model_load() {
        let peer_id = test_id("peer-order");
        let model_id = test_id("tinyllama-order");

        emit_worker_startup_started_event(&peer_id, 4101);
        emit_worker_model_load_attempt_event(&model_id, "test_order");

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let started_index = entries
            .iter()
            .position(|entry| {
                entry.event == "worker_startup_started"
                    && entry.fields.get("peer_id").and_then(|value| value.as_str())
                        == Some(peer_id.as_str())
            })
            .expect("worker_startup_started entry not found");
        let attempt_index = entries
            .iter()
            .position(|entry| {
                entry.event == "worker_model_load_attempt"
                    && entry.model_id.as_deref() == Some(model_id.as_str())
            })
            .expect("worker_model_load_attempt entry not found");

        assert!(started_index < attempt_index);
    }

    #[test]
    fn cpu_feature_incompatible_marks_real_backend_unavailable() {
        let caps = ModelNodeCapabilities {
            cpu_features: vec![],
            ..test_node_caps_for_startup()
        };
        let policy = WorkerStartupPolicy::from_values(
            None,
            None,
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );

        emit_backend_cpu_feature_incompatible_event(
            &caps.cpu_features,
            &caps.accelerator,
            "test_degraded",
        );

        assert!(!policy.cpu_feature_compatible);
        assert!(!policy.real_inference_available);
        assert_eq!(
            policy.model_load_skip_reason,
            Some("cpu_feature_incompatible")
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        assert!(entries
            .iter()
            .rev()
            .any(|entry| entry.event == "backend_cpu_feature_incompatible"));
    }

    #[test]
    fn worker_startup_events_are_preserved() {
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let peer_id = test_id("peer-events");

        emit_worker_startup_started_event(&peer_id, 4101);
        emit_inference_backend_selected_event(&policy);
        emit_worker_model_load_skipped_event("mock_backend");
        emit_worker_startup_ready_event(&peer_id, 4101, &policy);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        for event_name in [
            "worker_startup_started",
            "inference_backend_selected",
            "worker_model_load_skipped",
            "worker_startup_ready",
        ] {
            assert!(
                entries.iter().rev().any(|entry| entry.event == event_name),
                "missing startup event {event_name}"
            );
        }
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
        let trace_id = test_id("startup-overflow-test");
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
            .find(|entry| {
                entry.trace_id == trace_id && entry.event == "worker_startup_invalid_math"
            })
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
}
