use crate::license_policy::LicenseOperation;
use crate::{
    LicenseAcceptanceDecision, LicenseAcceptanceStatus, LicenseAcceptanceStore,
    LicensePolicyDecision, LicensePolicyStatus, ModelDescriptor, ModelDownloadDecision,
    ModelDownloadPolicy, ModelLicenseAcceptancePolicy, ModelLicensePolicy,
    ModelRegistryIntegrityPolicy, RegistryIntegrityDecision, RegistryIntegrityOperation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRegistryAdmissionDecision {
    pub download: ModelDownloadDecision,
    pub registry_integrity: RegistryIntegrityDecision,
    pub license: LicensePolicyDecision,
    pub license_acceptance: LicenseAcceptanceDecision,
    pub permits_operation: bool,
}

impl ModelRegistryAdmissionDecision {
    pub fn from_decisions(
        download: ModelDownloadDecision,
        registry_integrity: RegistryIntegrityDecision,
        license: LicensePolicyDecision,
        license_acceptance: LicenseAcceptanceDecision,
    ) -> Self {
        let permits_operation = download.permits_download()
            && registry_integrity.permits_operation
            && license_condition_permits_operation(&license, &license_acceptance)
            && license_acceptance.permits_operation;
        Self {
            download,
            registry_integrity,
            license,
            license_acceptance,
            permits_operation,
        }
    }

    pub fn first_blocking_error(&self) -> Option<String> {
        if !self.download.permits_download() {
            return Some(format!(
                "model download policy {}: {}",
                self.download.status.as_str(),
                self.download.policy_reason()
            ));
        }
        if !self.registry_integrity.permits_operation {
            return Some(format!(
                "model registry integrity policy {}: {}",
                self.registry_integrity.status.as_str(),
                self.registry_integrity.policy_reason()
            ));
        }
        if !license_condition_permits_operation(&self.license, &self.license_acceptance) {
            if self.license.status == LicensePolicyStatus::RequiresAcceptance {
                return Some(format!(
                    "model license acceptance policy {}: {}",
                    self.license_acceptance.status.as_str(),
                    self.license_acceptance.reason.as_str()
                ));
            }
            return Some(format!(
                "model license policy {}: {}",
                self.license.status.as_str(),
                self.license.reason.as_str()
            ));
        }
        if !self.license_acceptance.permits_operation {
            return Some(format!(
                "model license acceptance policy {}: {}",
                self.license_acceptance.status.as_str(),
                self.license_acceptance.reason.as_str()
            ));
        }
        None
    }
}

pub fn evaluate_model_registry_admission(
    model: &ModelDescriptor,
    operation: LicenseOperation,
    installed: bool,
) -> ModelRegistryAdmissionDecision {
    evaluate_model_registry_admission_with_license_acceptance_store(
        model,
        operation,
        installed,
        &LicenseAcceptanceStore::new(),
    )
}

pub fn evaluate_model_registry_admission_with_license_acceptance_store(
    model: &ModelDescriptor,
    operation: LicenseOperation,
    installed: bool,
    license_acceptance_store: &LicenseAcceptanceStore,
) -> ModelRegistryAdmissionDecision {
    let download = ModelDownloadPolicy::default().evaluate_descriptor(model);
    let registry_integrity = ModelRegistryIntegrityPolicy.evaluate_descriptor(
        model,
        registry_operation(operation),
        installed,
    );
    let license = ModelLicensePolicy.evaluate_descriptor(model, operation, installed);
    let license_acceptance = ModelLicenseAcceptancePolicy.evaluate_descriptor(
        model,
        operation,
        license_acceptance_store.has_accepted_descriptor(model),
    );
    ModelRegistryAdmissionDecision::from_decisions(
        download,
        registry_integrity,
        license,
        license_acceptance,
    )
}

fn registry_operation(operation: LicenseOperation) -> RegistryIntegrityOperation {
    match operation {
        LicenseOperation::List => RegistryIntegrityOperation::List,
        LicenseOperation::Download => RegistryIntegrityOperation::Download,
        LicenseOperation::Install => RegistryIntegrityOperation::Install,
        LicenseOperation::ExistingExecution => RegistryIntegrityOperation::ExistingExecution,
    }
}

fn license_condition_permits_operation(
    license: &LicensePolicyDecision,
    license_acceptance: &LicenseAcceptanceDecision,
) -> bool {
    license.permits_operation
        || (license.status == LicensePolicyStatus::RequiresAcceptance
            && license_acceptance.status == LicenseAcceptanceStatus::Accepted
            && license_acceptance.permits_operation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LicenseClass, LicenseMetadata};
    use tempfile::TempDir;

    fn acceptance_model() -> ModelDescriptor {
        ModelDescriptor {
            id: "acceptance-model".to_string(),
            version: "1.0".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 1_048_576,
            required_ram_gb: 1,
            required_vram_gb: 0,
            shards: 1,
            hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            download_url: "https://huggingface.co/iamine/test/resolve/main/test.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
            license: LicenseMetadata {
                license_id: Some("custom-requires-acceptance".to_string()),
                license_url: Some("https://example.com/license".to_string()),
                policy_class: Some(LicenseClass::RequiresAcceptance),
                requires_acceptance: true,
                revision: Some("2026-06-21".to_string()),
            },
        }
    }

    #[test]
    fn acceptance_gate_blocks_after_license_requires_acceptance(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let store = LicenseAcceptanceStore::new_in(dir.path().join("license_acceptance.json"));
        let decision = evaluate_model_registry_admission_with_license_acceptance_store(
            &acceptance_model(),
            LicenseOperation::Download,
            false,
            &store,
        );

        assert!(!decision.permits_operation);
        assert_eq!(
            decision.first_blocking_error(),
            Some(
                "model license acceptance policy required: license_acceptance_required".to_string()
            )
        );
        Ok(())
    }

    #[test]
    fn recorded_acceptance_satisfies_requires_acceptance_license(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let store = LicenseAcceptanceStore::new_in(dir.path().join("license_acceptance.json"));
        let model = acceptance_model();
        store.accept_descriptor(&model)?;

        let decision = evaluate_model_registry_admission_with_license_acceptance_store(
            &model,
            LicenseOperation::Download,
            false,
            &store,
        );

        assert!(decision.permits_operation);
        assert_eq!(
            decision.license_acceptance.status,
            LicenseAcceptanceStatus::Accepted
        );
        assert_eq!(decision.first_blocking_error(), None);
        Ok(())
    }
}
