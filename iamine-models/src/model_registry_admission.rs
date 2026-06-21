use crate::license_policy::LicenseOperation;
use crate::{
    LicensePolicyDecision, ModelDescriptor, ModelDownloadDecision, ModelDownloadPolicy,
    ModelLicensePolicy, ModelRegistryIntegrityPolicy, RegistryIntegrityDecision,
    RegistryIntegrityOperation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRegistryAdmissionDecision {
    pub download: ModelDownloadDecision,
    pub registry_integrity: RegistryIntegrityDecision,
    pub license: LicensePolicyDecision,
    pub permits_operation: bool,
}

impl ModelRegistryAdmissionDecision {
    pub fn from_decisions(
        download: ModelDownloadDecision,
        registry_integrity: RegistryIntegrityDecision,
        license: LicensePolicyDecision,
    ) -> Self {
        let permits_operation = download.permits_download()
            && registry_integrity.permits_operation
            && license.permits_operation;
        Self {
            download,
            registry_integrity,
            license,
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
        if !self.license.permits_operation {
            return Some(format!(
                "model license policy {}: {}",
                self.license.status.as_str(),
                self.license.reason.as_str()
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
    let download = ModelDownloadPolicy::default().evaluate_descriptor(model);
    let registry_integrity = ModelRegistryIntegrityPolicy.evaluate_descriptor(
        model,
        registry_operation(operation),
        installed,
    );
    let license = ModelLicensePolicy.evaluate_descriptor(model, operation, installed);
    ModelRegistryAdmissionDecision::from_decisions(download, registry_integrity, license)
}

fn registry_operation(operation: LicenseOperation) -> RegistryIntegrityOperation {
    match operation {
        LicenseOperation::List => RegistryIntegrityOperation::List,
        LicenseOperation::Download => RegistryIntegrityOperation::Download,
        LicenseOperation::Install => RegistryIntegrityOperation::Install,
        LicenseOperation::ExistingExecution => RegistryIntegrityOperation::ExistingExecution,
    }
}
