use crate::model_backend_availability::ModelBackendAvailabilityDecision;
use crate::worker_startup_policy::WorkerStartupPolicy;
use iamine_models::{
    evaluate_descriptor_model_inference_eligibility, evaluate_node_model_compatibility,
    LicenseAcceptanceStore, ModelDescriptor, ModelInferenceBackendAvailability,
    ModelInferenceEligibilityDecision, ModelInferenceEligibilityReason, ModelNetworkPolicy,
    ModelNodeCapabilities, ModelStorage, NetworkPolicyDecision, NetworkPolicyOperation,
};

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
    RegistryAdmissionBlocked,
    HardwareUnsupported,
    BackendUnavailable,
    NetworkPolicyBlocked,
}

impl WorkerModelExecutionRejection {
    pub(crate) fn human_warning(self, model_id: &str) -> String {
        match self {
            Self::MissingLocalModel => {
                format!("   ⚠️ Modelo {} no instalado — ignorando", model_id)
            }
            Self::RegistryAdmissionBlocked => {
                format!(
                    "   ⚠️ Admisión de registry bloquea {} — ignorando",
                    model_id
                )
            }
            Self::HardwareUnsupported => {
                format!("   ⚠️ Hardware insuficiente para {} — ignorando", model_id)
            }
            Self::BackendUnavailable => {
                format!("   ⚠️ Backend no disponible para {} — ignorando", model_id)
            }
            Self::NetworkPolicyBlocked => {
                format!(
                    "   ⚠️ Política de red bloquea inferencia distribuida para {} — ignorando",
                    model_id
                )
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
    pub(crate) inference_eligibility: Option<ModelInferenceEligibilityDecision>,
    pub(crate) network_policy: Option<NetworkPolicyDecision>,
    pub(crate) rejection: Option<WorkerModelExecutionRejection>,
}

pub(crate) fn evaluate_worker_model_execution_gate(
    model_id: &str,
    storage: &ModelStorage,
    model_descriptor: Option<&ModelDescriptor>,
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
    let hardware_compatibility = evaluate_node_model_compatibility(model_id, node_caps);
    let inference_eligibility = model_descriptor.map(|model| {
        evaluate_descriptor_model_inference_eligibility(
            model,
            local_model_available,
            hardware_compatibility.clone(),
            model_inference_backend_availability(backend_availability),
            &LicenseAcceptanceStore::new(),
        )
    });
    let network_policy = inference_eligibility
        .as_ref()
        .map(|decision| decision.network_policy.clone());

    let rejection = if let Some(eligibility) = inference_eligibility.as_ref() {
        worker_rejection_from_eligibility(eligibility, mock_backend_enabled)
    } else if !local_model_available && !mock_backend_enabled {
        Some(WorkerModelExecutionRejection::MissingLocalModel)
    } else if !mock_backend_enabled && !hardware_compatibility.is_compatible() {
        Some(WorkerModelExecutionRejection::HardwareUnsupported)
    } else {
        None
    };

    WorkerModelExecutionGate {
        local_model_available,
        mock_backend_enabled,
        real_inference_available,
        backend_availability,
        inference_eligibility,
        network_policy,
        rejection,
    }
}

pub(crate) fn evaluate_model_network_inference_policy(
    model: &ModelDescriptor,
    installed: bool,
) -> NetworkPolicyDecision {
    ModelNetworkPolicy.evaluate_descriptor(
        model,
        NetworkPolicyOperation::NetworkInference,
        installed,
    )
}

fn model_inference_backend_availability(
    decision: ModelBackendAvailabilityDecision,
) -> ModelInferenceBackendAvailability {
    if decision.permits_real_inference() {
        ModelInferenceBackendAvailability::Available
    } else {
        ModelInferenceBackendAvailability::Unavailable
    }
}

fn worker_rejection_from_eligibility(
    decision: &ModelInferenceEligibilityDecision,
    mock_backend_enabled: bool,
) -> Option<WorkerModelExecutionRejection> {
    decision.reasons.iter().find_map(|reason| match reason {
        ModelInferenceEligibilityReason::ModelNotInstalled if !mock_backend_enabled => {
            Some(WorkerModelExecutionRejection::MissingLocalModel)
        }
        ModelInferenceEligibilityReason::RegistryAdmissionBlocked => {
            Some(WorkerModelExecutionRejection::RegistryAdmissionBlocked)
        }
        ModelInferenceEligibilityReason::HardwareIncompatible if !mock_backend_enabled => {
            Some(WorkerModelExecutionRejection::HardwareUnsupported)
        }
        ModelInferenceEligibilityReason::BackendUnavailable if !mock_backend_enabled => {
            Some(WorkerModelExecutionRejection::BackendUnavailable)
        }
        ModelInferenceEligibilityReason::NetworkPolicyBlocked => {
            Some(WorkerModelExecutionRejection::NetworkPolicyBlocked)
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_backend_availability::{
        evaluate_model_backend_availability, ModelBackendAvailabilityInput,
    };
    use iamine_models::{LicenseClass, LicenseMetadata, NetworkPolicyMetadata};
    use std::fs;
    use tempfile::TempDir;

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
                    legacy_cpu_daemon_only: false,
                    real_inference_available,
                },
            ),
            hardware_supported,
        }
    }

    fn descriptor(network_policy: NetworkPolicyMetadata) -> ModelDescriptor {
        ModelDescriptor {
            id: "tinyllama-1b".to_string(),
            version: "1.1".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 1_048_576,
            required_ram_gb: 1,
            required_vram_gb: 0,
            shards: 1,
            hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            download_url: "https://huggingface.co/iamine/test/resolve/main/test.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
            license: LicenseMetadata::missing(),
            network_policy,
        }
    }

    fn restricted_license_descriptor() -> ModelDescriptor {
        let mut model = descriptor(NetworkPolicyMetadata::distributed_allowed("test-fixture"));
        model.license = LicenseMetadata {
            license_id: Some("custom-restricted".to_string()),
            license_url: Some("https://example.com/license".to_string()),
            policy_class: Some(LicenseClass::Restricted),
            requires_acceptance: false,
            revision: Some("test-fixture".to_string()),
        };
        model
    }

    fn node_caps() -> ModelNodeCapabilities {
        ModelNodeCapabilities {
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
        }
    }

    fn storage_with_model(model_id: &str) -> Result<(TempDir, ModelStorage), std::io::Error> {
        let dir = TempDir::new()?;
        let storage = ModelStorage::new_in(dir.path().to_path_buf());
        fs::create_dir_all(storage.model_path(model_id))?;
        let mut bytes = vec![0u8; 2048];
        bytes[..4].copy_from_slice(b"GGUF");
        fs::write(storage.gguf_path(model_id), bytes)?;
        Ok((dir, storage))
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

        assert!(!evaluate_node_model_compatibility("unknown-model", &node_caps).is_compatible());
    }

    #[test]
    fn worker_execution_gate_allows_distributed_network_policy(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, storage) = storage_with_model("tinyllama-1b")?;
        let model = descriptor(NetworkPolicyMetadata::distributed_allowed("test-fixture"));

        let gate = evaluate_worker_model_execution_gate(
            "tinyllama-1b",
            &storage,
            Some(&model),
            &node_caps(),
            None,
        );

        assert_eq!(gate.rejection, None);
        assert!(gate
            .inference_eligibility
            .as_ref()
            .is_some_and(ModelInferenceEligibilityDecision::is_eligible));
        assert!(gate
            .network_policy
            .as_ref()
            .is_some_and(|decision| decision.permits_distributed_inference));
        Ok(())
    }

    #[test]
    fn worker_execution_gate_blocks_registry_admission_policy(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, storage) = storage_with_model("tinyllama-1b")?;
        let model = restricted_license_descriptor();

        let gate = evaluate_worker_model_execution_gate(
            "tinyllama-1b",
            &storage,
            Some(&model),
            &node_caps(),
            None,
        );

        assert_eq!(
            gate.rejection,
            Some(WorkerModelExecutionRejection::RegistryAdmissionBlocked)
        );
        assert!(gate
            .inference_eligibility
            .as_ref()
            .is_some_and(|decision| !decision.is_eligible()));
        Ok(())
    }

    #[test]
    fn worker_execution_gate_blocks_backend_unavailable_for_real_backend(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, storage) = storage_with_model("tinyllama-1b")?;
        let model = descriptor(NetworkPolicyMetadata::distributed_allowed("test-fixture"));
        let startup_policy = WorkerStartupPolicy::from_values(
            Some("real"),
            Some("1"),
            None,
            &["AVX2".to_string()],
            "CPU",
            "x86_64",
        );

        let gate = evaluate_worker_model_execution_gate(
            "tinyllama-1b",
            &storage,
            Some(&model),
            &node_caps(),
            Some(&startup_policy),
        );

        assert_eq!(
            gate.rejection,
            Some(WorkerModelExecutionRejection::BackendUnavailable)
        );
        Ok(())
    }

    #[test]
    fn worker_execution_gate_allows_legacy_cpu_daemon_only_backend(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, storage) = storage_with_model("tinyllama-1b")?;
        let model = descriptor(NetworkPolicyMetadata::distributed_allowed("test-fixture"));
        let startup_policy = WorkerStartupPolicy::from_values(
            Some("real"),
            None,
            Some("daemon_only"),
            &[],
            "CPU",
            "x86_64",
        );

        let gate = evaluate_worker_model_execution_gate(
            "tinyllama-1b",
            &storage,
            Some(&model),
            &node_caps(),
            Some(&startup_policy),
        );

        assert_eq!(gate.rejection, None);
        assert!(gate.real_inference_available);
        assert_eq!(
            gate.backend_availability.reason,
            crate::model_backend_availability::ModelBackendAvailabilityReason::LegacyCpuDaemonOnly
        );
        assert!(startup_policy.legacy_cpu_daemon_only_real_inference());
        Ok(())
    }

    #[test]
    fn worker_execution_gate_blocks_local_only_network_policy(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, storage) = storage_with_model("tinyllama-1b")?;
        let model = descriptor(NetworkPolicyMetadata::local_only("test-fixture"));

        let gate = evaluate_worker_model_execution_gate(
            "tinyllama-1b",
            &storage,
            Some(&model),
            &node_caps(),
            None,
        );

        assert_eq!(
            gate.rejection,
            Some(WorkerModelExecutionRejection::NetworkPolicyBlocked)
        );
        assert_eq!(
            gate.network_policy
                .as_ref()
                .map(NetworkPolicyDecision::policy_reason),
            Some("local_only")
        );
        Ok(())
    }

    #[test]
    fn worker_execution_gate_blocks_missing_network_policy_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, storage) = storage_with_model("tinyllama-1b")?;
        let model = descriptor(NetworkPolicyMetadata::missing());

        let gate = evaluate_worker_model_execution_gate(
            "tinyllama-1b",
            &storage,
            Some(&model),
            &node_caps(),
            None,
        );

        assert_eq!(
            gate.rejection,
            Some(WorkerModelExecutionRejection::NetworkPolicyBlocked)
        );
        assert_eq!(
            gate.network_policy
                .as_ref()
                .map(NetworkPolicyDecision::policy_reason),
            Some("network_policy_missing")
        );
        Ok(())
    }
}
