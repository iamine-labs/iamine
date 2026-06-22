use crate::model_backend_availability::ModelBackendAvailabilityDecision;
use crate::worker_startup_policy::WorkerStartupPolicy;
use iamine_models::{evaluate_node_model_compatibility, ModelNodeCapabilities, ModelStorage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModelExecutability {
    Executable,
    StorageOnly,
    RegistryOnly,
    StorageAndRegistryUnavailable,
    Unknown,
}

impl ModelExecutability {
    pub(crate) fn is_executable(self) -> bool {
        matches!(self, Self::Executable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelExecutabilityInput {
    pub(crate) in_storage: bool,
    pub(crate) in_registry: bool,
    pub(crate) backend_availability: ModelBackendAvailabilityDecision,
    pub(crate) hardware_supported: bool,
}

pub(crate) fn classify_model_executability(input: &ModelExecutabilityInput) -> ModelExecutability {
    if input.in_storage
        && input.in_registry
        && input.backend_availability.permits_real_inference()
        && input.hardware_supported
    {
        return ModelExecutability::Executable;
    }

    match (input.in_storage, input.in_registry) {
        (true, true) => ModelExecutability::StorageAndRegistryUnavailable,
        (true, false) => ModelExecutability::StorageOnly,
        (false, true) => ModelExecutability::RegistryOnly,
        (false, false) => ModelExecutability::Unknown,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerModelExecutionRejection {
    MissingLocalModel,
    HardwareUnsupported,
}

impl WorkerModelExecutionRejection {
    pub(crate) fn human_warning(self, model_id: &str) -> String {
        match self {
            Self::MissingLocalModel => {
                format!("   ⚠️ Modelo {} no instalado — ignorando", model_id)
            }
            Self::HardwareUnsupported => {
                format!("   ⚠️ Hardware insuficiente para {} — ignorando", model_id)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerModelExecutionGate {
    pub(crate) local_model_available: bool,
    pub(crate) mock_backend_enabled: bool,
    pub(crate) real_inference_available: bool,
    pub(crate) backend_availability: ModelBackendAvailabilityDecision,
    pub(crate) rejection: Option<WorkerModelExecutionRejection>,
}

pub(crate) fn evaluate_worker_model_execution_gate(
    model_id: &str,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
    startup_policy: Option<&WorkerStartupPolicy>,
) -> WorkerModelExecutionGate {
    let local_model_available = storage.has_model(model_id);
    let mock_backend_enabled = startup_policy
        .map(|policy| policy.mock_backend())
        .unwrap_or(false);
    let backend_availability = startup_policy
        .map(|policy| policy.backend_availability_decision())
        .unwrap_or_else(ModelBackendAvailabilityDecision::available);
    let real_inference_available = backend_availability.permits_real_inference();

    let rejection = if !local_model_available && !mock_backend_enabled {
        Some(WorkerModelExecutionRejection::MissingLocalModel)
    } else if !mock_backend_enabled && !worker_hardware_supports_model(model_id, node_caps) {
        Some(WorkerModelExecutionRejection::HardwareUnsupported)
    } else {
        None
    };

    WorkerModelExecutionGate {
        local_model_available,
        mock_backend_enabled,
        real_inference_available,
        backend_availability,
        rejection,
    }
}

fn worker_hardware_supports_model(model_id: &str, node_caps: &ModelNodeCapabilities) -> bool {
    evaluate_node_model_compatibility(model_id, node_caps).is_compatible()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_backend_availability::{
        evaluate_model_backend_availability, ModelBackendAvailabilityInput,
    };

    fn input(
        in_storage: bool,
        in_registry: bool,
        backend_is_mock: bool,
        real_inference_available: bool,
        hardware_supported: bool,
    ) -> ModelExecutabilityInput {
        ModelExecutabilityInput {
            in_storage,
            in_registry,
            backend_availability: evaluate_model_backend_availability(
                &ModelBackendAvailabilityInput {
                    backend_is_mock,
                    skip_model_load_on_startup: false,
                    cpu_feature_compatible: true,
                    real_inference_available,
                },
            ),
            hardware_supported,
        }
    }

    #[test]
    fn model_executability_mock_backend_excludes_real_llm() {
        let result = classify_model_executability(&input(true, true, true, true, true));

        assert_eq!(result, ModelExecutability::StorageAndRegistryUnavailable);
        assert!(!result.is_executable());
    }

    #[test]
    fn model_executability_false_when_real_inference_unavailable() {
        let result = classify_model_executability(&input(true, true, false, false, true));

        assert_eq!(result, ModelExecutability::StorageAndRegistryUnavailable);
        assert!(!result.is_executable());
    }

    #[test]
    fn model_executability_storage_only_is_not_executable() {
        let result = classify_model_executability(&input(true, false, false, true, true));

        assert_eq!(result, ModelExecutability::StorageOnly);
        assert!(!result.is_executable());
    }

    #[test]
    fn model_executability_registry_only_is_not_executable() {
        let result = classify_model_executability(&input(false, true, false, true, true));

        assert_eq!(result, ModelExecutability::RegistryOnly);
        assert!(!result.is_executable());
    }

    #[test]
    fn model_executability_real_backend_can_execute_available_supported_model() {
        let result = classify_model_executability(&input(true, true, false, true, true));

        assert_eq!(result, ModelExecutability::Executable);
        assert!(result.is_executable());
    }

    #[test]
    fn worker_model_rejection_messages_preserve_existing_human_text() {
        assert_eq!(
            WorkerModelExecutionRejection::MissingLocalModel.human_warning("tinyllama-1b"),
            "   ⚠️ Modelo tinyllama-1b no instalado — ignorando"
        );
        assert_eq!(
            WorkerModelExecutionRejection::HardwareUnsupported.human_warning("mistral-7b"),
            "   ⚠️ Hardware insuficiente para mistral-7b — ignorando"
        );
    }

    #[test]
    fn worker_hardware_support_rejects_unknown_model_requirements() {
        let node_caps = ModelNodeCapabilities {
            node_id: "test-node".to_string(),
            cpu_cores: 4,
            ram_gb: 16,
            gpu_type: None,
            npu_type: None,
            storage_available_gb: 20,
            worker_slots: 4,
            supported_models: Vec::new(),
            cpu_features: Vec::new(),
            accelerator: "CPU".to_string(),
        };

        assert!(!worker_hardware_supports_model("unknown-model", &node_caps));
    }
}
