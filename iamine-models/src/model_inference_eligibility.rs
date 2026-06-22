use crate::{
    evaluate_model_registry_admission_with_license_acceptance_store, LicenseAcceptanceStore,
    LicenseOperation, ModelCompatibilityDecision, ModelDescriptor, ModelNetworkPolicy,
    ModelRegistryAdmissionDecision, NetworkPolicyDecision, NetworkPolicyOperation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInferenceBackendAvailability {
    Available,
    Unavailable,
}

impl ModelInferenceBackendAvailability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
        }
    }

    pub fn permits_inference(self) -> bool {
        self == Self::Available
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInferenceEligibilityStatus {
    Eligible,
    Ineligible,
}

impl ModelInferenceEligibilityStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eligible => "eligible",
            Self::Ineligible => "ineligible",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInferenceEligibilityReason {
    ModelNotInstalled,
    RegistryAdmissionBlocked,
    HardwareIncompatible,
    BackendUnavailable,
    NetworkPolicyBlocked,
}

impl ModelInferenceEligibilityReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ModelNotInstalled => "model_not_installed",
            Self::RegistryAdmissionBlocked => "registry_admission_blocked",
            Self::HardwareIncompatible => "hardware_incompatible",
            Self::BackendUnavailable => "backend_unavailable",
            Self::NetworkPolicyBlocked => "network_policy_blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInferenceEligibilityInput {
    pub installed: bool,
    pub registry_admission: ModelRegistryAdmissionDecision,
    pub hardware_compatibility: ModelCompatibilityDecision,
    pub backend_availability: ModelInferenceBackendAvailability,
    pub network_policy: NetworkPolicyDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInferenceEligibilityDecision {
    pub installed: bool,
    pub registry_admission: ModelRegistryAdmissionDecision,
    pub hardware_compatibility: ModelCompatibilityDecision,
    pub backend_availability: ModelInferenceBackendAvailability,
    pub network_policy: NetworkPolicyDecision,
    pub status: ModelInferenceEligibilityStatus,
    pub reasons: Vec<ModelInferenceEligibilityReason>,
}

impl ModelInferenceEligibilityDecision {
    pub fn is_eligible(&self) -> bool {
        self.status == ModelInferenceEligibilityStatus::Eligible
    }

    pub fn first_blocking_reason(&self) -> Option<ModelInferenceEligibilityReason> {
        self.reasons.first().copied()
    }
}

pub fn evaluate_model_inference_eligibility(
    input: ModelInferenceEligibilityInput,
) -> ModelInferenceEligibilityDecision {
    let mut reasons = Vec::new();

    if !input.installed {
        reasons.push(ModelInferenceEligibilityReason::ModelNotInstalled);
    }
    if !input.registry_admission.permits_operation {
        reasons.push(ModelInferenceEligibilityReason::RegistryAdmissionBlocked);
    }
    if !input.hardware_compatibility.is_compatible() {
        reasons.push(ModelInferenceEligibilityReason::HardwareIncompatible);
    }
    if !input.backend_availability.permits_inference() {
        reasons.push(ModelInferenceEligibilityReason::BackendUnavailable);
    }
    if !input.network_policy.permits_operation
        || !input.network_policy.permits_distributed_inference
    {
        reasons.push(ModelInferenceEligibilityReason::NetworkPolicyBlocked);
    }

    let status = if reasons.is_empty() {
        ModelInferenceEligibilityStatus::Eligible
    } else {
        ModelInferenceEligibilityStatus::Ineligible
    };

    ModelInferenceEligibilityDecision {
        installed: input.installed,
        registry_admission: input.registry_admission,
        hardware_compatibility: input.hardware_compatibility,
        backend_availability: input.backend_availability,
        network_policy: input.network_policy,
        status,
        reasons,
    }
}

pub fn evaluate_descriptor_model_inference_eligibility(
    model: &ModelDescriptor,
    installed: bool,
    hardware_compatibility: ModelCompatibilityDecision,
    backend_availability: ModelInferenceBackendAvailability,
    license_acceptance_store: &LicenseAcceptanceStore,
) -> ModelInferenceEligibilityDecision {
    let registry_admission = evaluate_model_registry_admission_with_license_acceptance_store(
        model,
        LicenseOperation::ExistingExecution,
        installed,
        license_acceptance_store,
    );
    let network_policy = ModelNetworkPolicy.evaluate_descriptor(
        model,
        NetworkPolicyOperation::NetworkInference,
        installed,
    );

    evaluate_model_inference_eligibility(ModelInferenceEligibilityInput {
        installed,
        registry_admission,
        hardware_compatibility,
        backend_availability,
        network_policy,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        evaluate_model_compatibility, LicenseClass, LicenseMetadata, ModelCompatibilityProfile,
        NetworkPolicyMetadata,
    };
    use tempfile::TempDir;

    fn eligible_model() -> ModelDescriptor {
        ModelDescriptor {
            id: "tinyllama-1b".to_string(),
            version: "1.1".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 669_000_000,
            required_ram_gb: 2,
            required_vram_gb: 0,
            shards: 1,
            hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            download_url: "https://huggingface.co/iamine/test/resolve/main/test.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
            license: LicenseMetadata {
                license_id: Some("MIT".to_string()),
                license_url: Some("https://opensource.org/license/mit".to_string()),
                policy_class: Some(LicenseClass::Allowed),
                requires_acceptance: false,
                revision: Some("test-fixture".to_string()),
            },
            network_policy: NetworkPolicyMetadata::distributed_allowed("test-fixture"),
        }
    }

    fn compatible_hardware(model_id: &str) -> ModelCompatibilityDecision {
        evaluate_model_compatibility(
            model_id,
            &ModelCompatibilityProfile {
                ram_gb: Some(8),
                storage_available_gb: Some(8),
                gpu_available: Some(false),
                cpu_features: vec!["AVX2".to_string(), "FMA".to_string()],
                accelerator: Some("CPU".to_string()),
            },
        )
    }

    fn incompatible_hardware(model_id: &str) -> ModelCompatibilityDecision {
        evaluate_model_compatibility(
            model_id,
            &ModelCompatibilityProfile {
                ram_gb: Some(1),
                storage_available_gb: Some(8),
                gpu_available: Some(false),
                cpu_features: vec!["AVX2".to_string(), "FMA".to_string()],
                accelerator: Some("CPU".to_string()),
            },
        )
    }

    fn evaluate(
        model: &ModelDescriptor,
        installed: bool,
        hardware: ModelCompatibilityDecision,
        backend_availability: ModelInferenceBackendAvailability,
    ) -> Result<ModelInferenceEligibilityDecision, Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let store = LicenseAcceptanceStore::new_in(dir.path().join("license_acceptance.json"));
        Ok(evaluate_descriptor_model_inference_eligibility(
            model,
            installed,
            hardware,
            backend_availability,
            &store,
        ))
    }

    #[test]
    fn all_gates_permit_network_inference_eligibility() -> Result<(), Box<dyn std::error::Error>> {
        let model = eligible_model();
        let decision = evaluate(
            &model,
            true,
            compatible_hardware(&model.id),
            ModelInferenceBackendAvailability::Available,
        )?;

        assert!(decision.is_eligible());
        assert_eq!(decision.status, ModelInferenceEligibilityStatus::Eligible);
        assert_eq!(decision.first_blocking_reason(), None);
        assert!(decision.registry_admission.permits_operation);
        assert!(decision.network_policy.permits_distributed_inference);
        Ok(())
    }

    #[test]
    fn missing_local_model_blocks_even_when_policy_allows() -> Result<(), Box<dyn std::error::Error>>
    {
        let model = eligible_model();
        let decision = evaluate(
            &model,
            false,
            compatible_hardware(&model.id),
            ModelInferenceBackendAvailability::Available,
        )?;

        assert!(!decision.is_eligible());
        assert_eq!(
            decision.first_blocking_reason(),
            Some(ModelInferenceEligibilityReason::ModelNotInstalled)
        );
        Ok(())
    }

    #[test]
    fn local_only_policy_blocks_network_inference_after_registry_admission(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut model = eligible_model();
        model.network_policy = NetworkPolicyMetadata::local_only("test-fixture");

        let decision = evaluate(
            &model,
            true,
            compatible_hardware(&model.id),
            ModelInferenceBackendAvailability::Available,
        )?;

        assert!(!decision.is_eligible());
        assert!(decision.registry_admission.permits_operation);
        assert_eq!(
            decision.first_blocking_reason(),
            Some(ModelInferenceEligibilityReason::NetworkPolicyBlocked)
        );
        Ok(())
    }

    #[test]
    fn legacy_missing_network_policy_does_not_permit_network_inference(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut model = eligible_model();
        model.network_policy = NetworkPolicyMetadata::missing();

        let decision = evaluate(
            &model,
            true,
            compatible_hardware(&model.id),
            ModelInferenceBackendAvailability::Available,
        )?;

        assert!(!decision.is_eligible());
        assert!(decision.registry_admission.permits_operation);
        assert_eq!(
            decision.first_blocking_reason(),
            Some(ModelInferenceEligibilityReason::NetworkPolicyBlocked)
        );
        Ok(())
    }

    #[test]
    fn backend_unavailable_blocks_after_static_gates_permit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let model = eligible_model();
        let decision = evaluate(
            &model,
            true,
            compatible_hardware(&model.id),
            ModelInferenceBackendAvailability::Unavailable,
        )?;

        assert!(!decision.is_eligible());
        assert_eq!(
            decision.first_blocking_reason(),
            Some(ModelInferenceEligibilityReason::BackendUnavailable)
        );
        Ok(())
    }

    #[test]
    fn incompatible_hardware_blocks_before_backend_and_network(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let model = eligible_model();
        let decision = evaluate(
            &model,
            true,
            incompatible_hardware(&model.id),
            ModelInferenceBackendAvailability::Available,
        )?;

        assert!(!decision.is_eligible());
        assert_eq!(
            decision.first_blocking_reason(),
            Some(ModelInferenceEligibilityReason::HardwareIncompatible)
        );
        Ok(())
    }
}
