use crate::ModelDescriptor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryIntegrityOperation {
    List,
    Download,
    Install,
    ExistingExecution,
}

impl RegistryIntegrityOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Download => "download",
            Self::Install => "install",
            Self::ExistingExecution => "existing_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryIntegrityStatus {
    Trusted,
    PendingIntegrity,
    LegacyExecution,
    Blocked,
}

impl RegistryIntegrityStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trusted => "trusted",
            Self::PendingIntegrity => "pending_integrity",
            Self::LegacyExecution => "legacy_execution",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryIntegrityReason {
    TrustedRegistryDescriptor,
    ChecksumMissing,
    ChecksumPlaceholder,
    ChecksumInvalid,
    LegacyInstalledModel,
}

impl RegistryIntegrityReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrustedRegistryDescriptor => "trusted_registry_descriptor",
            Self::ChecksumMissing => "checksum_missing",
            Self::ChecksumPlaceholder => "checksum_placeholder",
            Self::ChecksumInvalid => "checksum_invalid",
            Self::LegacyInstalledModel => "legacy_installed_model",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryIntegrityDecision {
    pub model_id: String,
    pub status: RegistryIntegrityStatus,
    pub reasons: Vec<RegistryIntegrityReason>,
    pub permits_operation: bool,
}

impl RegistryIntegrityDecision {
    pub fn policy_reason(&self) -> String {
        self.reasons
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelRegistryIntegrityPolicy;

impl ModelRegistryIntegrityPolicy {
    pub fn evaluate_descriptor(
        &self,
        model: &ModelDescriptor,
        operation: RegistryIntegrityOperation,
        installed: bool,
    ) -> RegistryIntegrityDecision {
        let mut reasons = checksum_reasons(&model.hash);
        let status = status_for_reasons(&reasons, operation, installed);
        if status == RegistryIntegrityStatus::LegacyExecution {
            reasons.push(RegistryIntegrityReason::LegacyInstalledModel);
        }
        RegistryIntegrityDecision {
            model_id: model.id.clone(),
            status,
            permits_operation: permits_operation(status),
            reasons,
        }
    }
}

fn checksum_reasons(hash: &str) -> Vec<RegistryIntegrityReason> {
    let hash = hash.trim();
    if hash.is_empty() {
        return vec![RegistryIntegrityReason::ChecksumMissing];
    }
    if hash.eq_ignore_ascii_case("skip") || hash.ends_with("_placeholder") {
        return vec![RegistryIntegrityReason::ChecksumPlaceholder];
    }
    if !is_valid_sha256_hex(hash) {
        return vec![RegistryIntegrityReason::ChecksumInvalid];
    }
    vec![RegistryIntegrityReason::TrustedRegistryDescriptor]
}

fn status_for_reasons(
    reasons: &[RegistryIntegrityReason],
    operation: RegistryIntegrityOperation,
    installed: bool,
) -> RegistryIntegrityStatus {
    if reasons.len() == 1 && reasons[0] == RegistryIntegrityReason::TrustedRegistryDescriptor {
        return RegistryIntegrityStatus::Trusted;
    }
    if installed && operation == RegistryIntegrityOperation::ExistingExecution {
        return RegistryIntegrityStatus::LegacyExecution;
    }
    if operation == RegistryIntegrityOperation::List {
        return RegistryIntegrityStatus::PendingIntegrity;
    }
    RegistryIntegrityStatus::Blocked
}

fn permits_operation(status: RegistryIntegrityStatus) -> bool {
    matches!(
        status,
        RegistryIntegrityStatus::Trusted
            | RegistryIntegrityStatus::PendingIntegrity
            | RegistryIntegrityStatus::LegacyExecution
    )
}

fn is_valid_sha256_hex(hash: &str) -> bool {
    hash.len() == 64 && hash.chars().all(|ch| ch.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LicenseClass, LicenseMetadata};

    fn model_with_hash(hash: &str) -> ModelDescriptor {
        ModelDescriptor {
            id: "test-model".to_string(),
            version: "1.0".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 1_048_576,
            required_ram_gb: 1,
            required_vram_gb: 0,
            shards: 1,
            hash: hash.to_string(),
            download_url: "https://huggingface.co/iamine/test/resolve/main/test.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
            license: LicenseMetadata {
                license_id: Some("MIT".to_string()),
                license_url: Some("https://opensource.org/license/mit".to_string()),
                policy_class: Some(LicenseClass::Allowed),
                requires_acceptance: false,
                revision: Some("test-fixture".to_string()),
            },
        }
    }

    #[test]
    fn valid_sha256_is_trusted() {
        let model =
            model_with_hash("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

        let decision = ModelRegistryIntegrityPolicy.evaluate_descriptor(
            &model,
            RegistryIntegrityOperation::Download,
            false,
        );

        assert_eq!(decision.status, RegistryIntegrityStatus::Trusted);
        assert!(decision.permits_operation);
        assert_eq!(
            decision.reasons,
            vec![RegistryIntegrityReason::TrustedRegistryDescriptor]
        );
    }

    #[test]
    fn missing_checksum_blocks_new_download() {
        let model = model_with_hash("");

        let decision = ModelRegistryIntegrityPolicy.evaluate_descriptor(
            &model,
            RegistryIntegrityOperation::Download,
            false,
        );

        assert_eq!(decision.status, RegistryIntegrityStatus::Blocked);
        assert!(!decision.permits_operation);
        assert_eq!(
            decision.reasons,
            vec![RegistryIntegrityReason::ChecksumMissing]
        );
    }

    #[test]
    fn placeholder_checksum_blocks_new_install() {
        let model = model_with_hash("tinyllama_hash_placeholder");

        let decision = ModelRegistryIntegrityPolicy.evaluate_descriptor(
            &model,
            RegistryIntegrityOperation::Install,
            false,
        );

        assert_eq!(decision.status, RegistryIntegrityStatus::Blocked);
        assert_eq!(
            decision.reasons,
            vec![RegistryIntegrityReason::ChecksumPlaceholder]
        );
    }

    #[test]
    fn malformed_checksum_blocks_new_install() {
        let model = model_with_hash("not-a-sha256");

        let decision = ModelRegistryIntegrityPolicy.evaluate_descriptor(
            &model,
            RegistryIntegrityOperation::Install,
            false,
        );

        assert_eq!(decision.status, RegistryIntegrityStatus::Blocked);
        assert_eq!(
            decision.reasons,
            vec![RegistryIntegrityReason::ChecksumInvalid]
        );
    }

    #[test]
    fn list_exposes_pending_integrity_without_permitting_trust() {
        let model = model_with_hash("");

        let decision = ModelRegistryIntegrityPolicy.evaluate_descriptor(
            &model,
            RegistryIntegrityOperation::List,
            false,
        );

        assert_eq!(decision.status, RegistryIntegrityStatus::PendingIntegrity);
        assert!(decision.permits_operation);
    }

    #[test]
    fn existing_installed_model_gets_legacy_exception_without_trust() {
        let model = model_with_hash("");

        let decision = ModelRegistryIntegrityPolicy.evaluate_descriptor(
            &model,
            RegistryIntegrityOperation::ExistingExecution,
            true,
        );

        assert_eq!(decision.status, RegistryIntegrityStatus::LegacyExecution);
        assert!(decision.permits_operation);
        assert_eq!(
            decision.reasons,
            vec![
                RegistryIntegrityReason::ChecksumMissing,
                RegistryIntegrityReason::LegacyInstalledModel
            ]
        );
    }
}
