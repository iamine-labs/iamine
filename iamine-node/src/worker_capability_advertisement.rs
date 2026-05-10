use crate::log_observability_event;
use crate::worker_capabilities::WorkerCapabilities;
use crate::worker_startup_policy::{
    emit_worker_model_load_attempt_event, emit_worker_model_load_failed_event, WorkerStartupPolicy,
};
use iamine_models::{
    can_node_run_model, ModelNodeCapabilities, ModelRegistry, ModelRequirements, ModelStorage,
    RealInferenceEngine,
};
use iamine_network::{LogLevel, MODEL_LOAD_FAILED_001, MODEL_UNSUPPORTED_HW_002};
use serde_json::Map;

pub(crate) fn validate_models_for_advertising(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
    startup_policy: &WorkerStartupPolicy,
) -> Vec<String> {
    validate_model_advertisement_candidates(
        storage.list_local_models(),
        registry,
        node_caps,
        startup_policy,
        |model_id, expected_hash| {
            let engine = RealInferenceEngine::new(ModelStorage::new());
            engine.load_model(model_id, expected_hash)
        },
    )
}

pub(crate) fn validate_model_advertisement_candidates<F>(
    local_models: Vec<String>,
    registry: &ModelRegistry,
    node_caps: &ModelNodeCapabilities,
    startup_policy: &WorkerStartupPolicy,
    mut load_model: F,
) -> Vec<String>
where
    F: FnMut(&str, &str) -> Result<(), String>,
{
    let mut validated = Vec::new();

    if !startup_policy.real_inference_available {
        return validated;
    }

    for model_id in local_models {
        if let Some(requirements) = ModelRequirements::for_model(&model_id) {
            if !can_node_run_model(node_caps, &requirements) {
                println!(
                    "[Health] Skipping advertisement for {}: hardware requirements not satisfied",
                    model_id
                );
                log_observability_event(
                    LogLevel::Warn,
                    "model_advertisement_rejected",
                    "startup",
                    None,
                    Some(&model_id),
                    Some(MODEL_UNSUPPORTED_HW_002),
                    {
                        let mut fields = Map::new();
                        fields.insert("reason".to_string(), "hardware_requirements".into());
                        fields
                    },
                );
                continue;
            }
        }

        let Some(model_desc) = registry.get(&model_id) else {
            println!(
                "[Health] Skipping advertisement for {}: missing registry descriptor",
                model_id
            );
            continue;
        };

        emit_worker_model_load_attempt_event(&model_id, "advertisement_validation");
        match load_model(&model_id, &model_desc.hash) {
            Ok(()) => {
                log_observability_event(
                    LogLevel::Info,
                    "model_validated",
                    "startup",
                    None,
                    Some(&model_id),
                    None,
                    Map::new(),
                );
                validated.push(model_id)
            }
            Err(error) => {
                println!(
                    "[Health] Skipping advertisement for {}: backend validation failed ({})",
                    model_id, error
                );
                emit_worker_model_load_failed_event(
                    &model_id,
                    &error,
                    "continue_degraded_without_model_advertisement",
                );
                log_observability_event(
                    LogLevel::Error,
                    "model_advertisement_rejected",
                    "startup",
                    None,
                    Some(&model_id),
                    Some(MODEL_LOAD_FAILED_001),
                    {
                        let mut fields = Map::new();
                        fields.insert("reason".to_string(), error.into());
                        fields
                    },
                );
            }
        }
    }

    validated
}

pub(crate) fn apply_worker_startup_policy_to_capabilities(
    capabilities: &mut WorkerCapabilities,
    startup_policy: &WorkerStartupPolicy,
) {
    if !startup_policy.real_inference_available {
        capabilities
            .supported_tasks
            .retain(|task_type| task_type != "inference");
    }
    for task_type in [
        "reverse_string",
        "compute_hash",
        "validate_challenge",
        "test",
        "echo",
    ] {
        if !capabilities.supports(task_type) {
            capabilities.supported_tasks.push(task_type.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::TaskExecutor;
    use crate::worker_startup_policy::test_node_caps_for_startup;

    #[test]
    fn worker_skip_model_load_on_startup_does_not_load_model() {
        let registry = ModelRegistry::new();
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            None,
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let mut load_attempts = 0usize;

        let advertised = validate_model_advertisement_candidates(
            vec!["tinyllama-1b".to_string()],
            &registry,
            &caps,
            &policy,
            |_model_id, _hash| {
                load_attempts += 1;
                Ok(())
            },
        );

        assert!(advertised.is_empty());
        assert_eq!(load_attempts, 0);
    }

    #[test]
    fn worker_mock_backend_does_not_initialize_real_cpu_backend() {
        let registry = ModelRegistry::new();
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            None,
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let mut load_attempts = 0usize;

        let advertised = validate_model_advertisement_candidates(
            vec!["tinyllama-1b".to_string()],
            &registry,
            &caps,
            &policy,
            |_model_id, _hash| {
                load_attempts += 1;
                Ok(())
            },
        );

        assert!(!policy.real_inference_available);
        assert!(advertised.is_empty());
        assert_eq!(load_attempts, 0);
    }

    #[test]
    fn capability_advertisement_mock_excludes_real_llm_models() {
        let registry = ModelRegistry::new();
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let mut load_attempts = 0usize;

        let advertised = validate_model_advertisement_candidates(
            vec!["tinyllama-1b".to_string()],
            &registry,
            &caps,
            &policy,
            |_model_id, _hash| {
                load_attempts += 1;
                Ok(())
            },
        );

        assert!(advertised.is_empty());
        assert_eq!(load_attempts, 0);
    }

    #[test]
    fn capability_advertisement_keeps_simple_tasks() {
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let mut worker_caps = WorkerCapabilities {
            cpu_cores: 4,
            ram_gb: 8,
            gpu_available: false,
            disk_available_gb: 10,
            supported_tasks: vec!["inference".to_string()],
            avg_latency_ms: 0.0,
        };

        apply_worker_startup_policy_to_capabilities(&mut worker_caps, &policy);

        assert!(worker_caps.supports("reverse_string"));
        assert!(worker_caps.supports("test"));
        assert!(worker_caps.supports("echo"));
        assert!(!worker_caps.supports("inference"));
    }

    #[test]
    fn simple_broadcast_task_does_not_require_llm_model_loaded() {
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let mut worker_caps = WorkerCapabilities {
            cpu_cores: 4,
            ram_gb: 8,
            gpu_available: false,
            disk_available_gb: 10,
            supported_tasks: vec!["inference".to_string()],
            avg_latency_ms: 0.0,
        };

        apply_worker_startup_policy_to_capabilities(&mut worker_caps, &policy);
        let response = TaskExecutor::execute_task(
            "simple-task".to_string(),
            "reverse_string".to_string(),
            "broadcast".to_string(),
        );
        let echo = TaskExecutor::execute_task(
            "echo-task".to_string(),
            "echo".to_string(),
            "smoke".to_string(),
        );

        assert!(worker_caps.supports("reverse_string"));
        assert!(worker_caps.supports("test"));
        assert!(!worker_caps.supports("inference"));
        assert!(response.success);
        assert_eq!(response.result, "tsacdaorb");
        assert!(echo.success);
        assert_eq!(echo.result, "smoke");
    }

    #[test]
    fn capabilities_do_not_advertise_real_models_when_backend_unavailable() {
        let registry = ModelRegistry::new();
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            Some("mock"),
            Some("1"),
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let mut load_attempts = 0usize;

        let advertised = validate_model_advertisement_candidates(
            vec!["tinyllama-1b".to_string()],
            &registry,
            &caps,
            &policy,
            |_model_id, _hash| {
                load_attempts += 1;
                Ok(())
            },
        );

        assert!(advertised.is_empty());
        assert_eq!(load_attempts, 0);
    }

    #[test]
    fn worker_model_load_failure_can_continue_degraded_when_policy_allows() {
        let registry = ModelRegistry::new();
        let caps = test_node_caps_for_startup();
        let policy = WorkerStartupPolicy::from_values(
            None,
            None,
            &caps.cpu_features,
            &caps.accelerator,
            "x86_64",
        );
        let mut load_attempts = 0usize;

        let advertised = validate_model_advertisement_candidates(
            vec!["tinyllama-1b".to_string()],
            &registry,
            &caps,
            &policy,
            |_model_id, _hash| {
                load_attempts += 1;
                Err("simulated load failure".to_string())
            },
        );

        assert!(policy.real_inference_available);
        assert!(advertised.is_empty());
        assert_eq!(load_attempts, 1);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "worker_model_load_failed"
                && entry.fields.get("error").and_then(|value| value.as_str())
                    == Some("simulated load failure")
        }));
    }
}
