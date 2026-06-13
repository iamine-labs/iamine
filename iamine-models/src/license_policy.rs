use crate::download_policy::ModelDownloadDecision;
use crate::model_registry::ModelDescriptor;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicenseMetadata {
    pub license_id: Option<String>,
    pub license_url: Option<String>,
    pub policy_class: Option<LicenseClass>,
    pub requires_acceptance: bool,
    pub revision: Option<String>,
}

impl Default for LicenseMetadata {
    fn default() -> Self {
        Self::missing()
    }
}

impl LicenseMetadata {
    pub fn missing() -> Self {
        Self {
            license_id: None,
            license_url: None,
            policy_class: None,
            requires_acceptance: false,
            revision: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LicenseClass {
    Allowed,
    RequiresAcceptance,
    Restricted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicenseOperation {
    List,
    Download,
    Install,
    ExistingExecution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicensePolicyStatus {
    Allowed,
    RequiresAcceptance,
    PendingMetadata,
    PendingReview,
    Blocked,
}

impl LicensePolicyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::RequiresAcceptance => "requires_acceptance",
            Self::PendingMetadata => "pending_metadata",
            Self::PendingReview => "pending_review",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicensePolicyReason {
    LicenseAllowed,
    LicenseAcceptanceRequired,
    LicenseMissing,
    LicenseUnknown,
    LicenseBlocked,
    LicenseMetadataConflict,
    LicenseIdInvalid,
    LicenseUrlInvalid,
    LegacyInstalledModel,
}

impl LicensePolicyReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LicenseAllowed => "license_allowed",
            Self::LicenseAcceptanceRequired => "license_acceptance_required",
            Self::LicenseMissing => "license_missing",
            Self::LicenseUnknown => "license_unknown",
            Self::LicenseBlocked => "license_blocked",
            Self::LicenseMetadataConflict => "license_metadata_conflict",
            Self::LicenseIdInvalid => "license_id_invalid",
            Self::LicenseUrlInvalid => "license_url_invalid",
            Self::LegacyInstalledModel => "legacy_installed_model",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicensePolicyDecision {
    pub status: LicensePolicyStatus,
    pub reason: LicensePolicyReason,
    pub license_id: Option<String>,
    pub permits_operation: bool,
    pub requires_acceptance: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAdmissionDecision {
    pub download: ModelDownloadDecision,
    pub license: LicensePolicyDecision,
    pub permits_operation: bool,
}

impl ModelAdmissionDecision {
    pub fn from_decisions(download: ModelDownloadDecision, license: LicensePolicyDecision) -> Self {
        let permits_operation = download.permits_download() && license.permits_operation;
        Self {
            download,
            license,
            permits_operation,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelLicensePolicy;

impl ModelLicensePolicy {
    pub fn evaluate_descriptor(
        &self,
        model: &ModelDescriptor,
        operation: LicenseOperation,
        installed: bool,
    ) -> LicensePolicyDecision {
        self.evaluate(Some(&model.license), operation, installed)
    }

    pub fn evaluate(
        &self,
        metadata: Option<&LicenseMetadata>,
        operation: LicenseOperation,
        installed: bool,
    ) -> LicensePolicyDecision {
        let Some(metadata) = metadata else {
            return missing_metadata_decision(operation, installed);
        };

        if let Some(license_url) = metadata.license_url.as_deref() {
            if !valid_license_url(license_url) {
                return decision(
                    LicensePolicyStatus::Blocked,
                    LicensePolicyReason::LicenseUrlInvalid,
                    metadata.license_id.clone(),
                    false,
                    metadata.requires_acceptance,
                );
            }
        }

        let license_id = metadata.license_id.as_deref().map(str::trim);
        if license_id.is_some_and(str::is_empty) {
            return decision(
                LicensePolicyStatus::Blocked,
                LicensePolicyReason::LicenseIdInvalid,
                metadata.license_id.clone(),
                false,
                metadata.requires_acceptance,
            );
        }
        if let Some(license_id) = license_id {
            if !valid_license_id(license_id) {
                return decision(
                    LicensePolicyStatus::Blocked,
                    LicensePolicyReason::LicenseIdInvalid,
                    metadata.license_id.clone(),
                    false,
                    metadata.requires_acceptance,
                );
            }
        }

        if metadata.license_id.is_none() {
            return missing_metadata_decision(operation, installed);
        }

        let Some(policy_class) = metadata.policy_class else {
            return decision(
                LicensePolicyStatus::PendingReview,
                LicensePolicyReason::LicenseUnknown,
                metadata.license_id.clone(),
                list_permits(operation),
                metadata.requires_acceptance,
            );
        };

        if policy_class == LicenseClass::Allowed && metadata.requires_acceptance {
            return decision(
                LicensePolicyStatus::Blocked,
                LicensePolicyReason::LicenseMetadataConflict,
                metadata.license_id.clone(),
                false,
                metadata.requires_acceptance,
            );
        }

        match policy_class {
            LicenseClass::Allowed => decision(
                LicensePolicyStatus::Allowed,
                LicensePolicyReason::LicenseAllowed,
                metadata.license_id.clone(),
                true,
                false,
            ),
            LicenseClass::RequiresAcceptance => decision(
                LicensePolicyStatus::RequiresAcceptance,
                LicensePolicyReason::LicenseAcceptanceRequired,
                metadata.license_id.clone(),
                list_permits(operation),
                true,
            ),
            LicenseClass::Restricted => decision(
                LicensePolicyStatus::Blocked,
                LicensePolicyReason::LicenseBlocked,
                metadata.license_id.clone(),
                list_permits(operation),
                metadata.requires_acceptance,
            ),
        }
    }
}

fn missing_metadata_decision(
    operation: LicenseOperation,
    installed: bool,
) -> LicensePolicyDecision {
    let legacy_execution = installed && operation == LicenseOperation::ExistingExecution;
    let list_legacy = installed && operation == LicenseOperation::List;
    decision(
        LicensePolicyStatus::PendingMetadata,
        if legacy_execution || list_legacy {
            LicensePolicyReason::LegacyInstalledModel
        } else {
            LicensePolicyReason::LicenseMissing
        },
        None,
        legacy_execution || operation == LicenseOperation::List,
        false,
    )
}

fn decision(
    status: LicensePolicyStatus,
    reason: LicensePolicyReason,
    license_id: Option<String>,
    permits_operation: bool,
    requires_acceptance: bool,
) -> LicensePolicyDecision {
    LicensePolicyDecision {
        status,
        reason,
        license_id,
        permits_operation,
        requires_acceptance,
    }
}

fn list_permits(operation: LicenseOperation) -> bool {
    operation == LicenseOperation::List
}

fn valid_license_id(license_id: &str) -> bool {
    let trimmed = license_id.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 128
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '+'))
        && trimmed
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric())
        && trimmed
            .chars()
            .last()
            .is_some_and(|ch| ch.is_ascii_alphanumeric())
}

fn valid_license_url(url: &str) -> bool {
    let Some(rest) = url.trim().strip_prefix("https://") else {
        return false;
    };
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .split('@')
        .next_back()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default();
    !host.is_empty() && host.contains('.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download_policy::{ModelDownloadPolicyStatus, ModelDownloadRejectReason};

    fn allowed_metadata() -> LicenseMetadata {
        LicenseMetadata {
            license_id: Some("MIT".to_string()),
            license_url: Some("https://opensource.org/license/mit".to_string()),
            policy_class: Some(LicenseClass::Allowed),
            requires_acceptance: false,
            revision: Some("2026-06-13".to_string()),
        }
    }

    fn acceptance_metadata() -> LicenseMetadata {
        LicenseMetadata {
            license_id: Some("custom-requires-acceptance".to_string()),
            license_url: Some("https://example.com/license".to_string()),
            policy_class: Some(LicenseClass::RequiresAcceptance),
            requires_acceptance: true,
            revision: Some("2026-06-13".to_string()),
        }
    }

    fn restricted_metadata() -> LicenseMetadata {
        LicenseMetadata {
            license_id: Some("restricted-model-license".to_string()),
            license_url: Some("https://example.com/license".to_string()),
            policy_class: Some(LicenseClass::Restricted),
            requires_acceptance: false,
            revision: Some("2026-06-13".to_string()),
        }
    }

    fn evaluate(metadata: &LicenseMetadata, operation: LicenseOperation) -> LicensePolicyDecision {
        ModelLicensePolicy.evaluate(Some(metadata), operation, false)
    }

    #[test]
    fn allowed_license_permits_download() {
        let decision = evaluate(&allowed_metadata(), LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::Allowed);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseAllowed);
        assert!(decision.permits_operation);
    }

    #[test]
    fn allowed_license_permits_install() {
        let decision = evaluate(&allowed_metadata(), LicenseOperation::Install);

        assert!(decision.permits_operation);
    }

    #[test]
    fn allowed_license_permits_existing_execution() {
        let decision = ModelLicensePolicy.evaluate(
            Some(&allowed_metadata()),
            LicenseOperation::ExistingExecution,
            true,
        );

        assert!(decision.permits_operation);
    }

    #[test]
    fn restricted_license_blocks_download() {
        let decision = evaluate(&restricted_metadata(), LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::Blocked);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseBlocked);
        assert!(!decision.permits_operation);
    }

    #[test]
    fn restricted_license_blocks_install() {
        let decision = evaluate(&restricted_metadata(), LicenseOperation::Install);

        assert!(!decision.permits_operation);
    }

    #[test]
    fn acceptance_required_license_blocks_download() {
        let decision = evaluate(&acceptance_metadata(), LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::RequiresAcceptance);
        assert_eq!(
            decision.reason,
            LicensePolicyReason::LicenseAcceptanceRequired
        );
        assert!(!decision.permits_operation);
        assert!(decision.requires_acceptance);
    }

    #[test]
    fn acceptance_required_license_blocks_install() {
        let decision = evaluate(&acceptance_metadata(), LicenseOperation::Install);

        assert!(!decision.permits_operation);
    }

    #[test]
    fn missing_metadata_blocks_new_download() {
        let decision = ModelLicensePolicy.evaluate(None, LicenseOperation::Download, false);

        assert_eq!(decision.status, LicensePolicyStatus::PendingMetadata);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseMissing);
        assert!(!decision.permits_operation);
    }

    #[test]
    fn missing_metadata_blocks_new_install() {
        let decision = ModelLicensePolicy.evaluate(None, LicenseOperation::Install, false);

        assert!(!decision.permits_operation);
    }

    #[test]
    fn missing_metadata_allows_legacy_existing_execution() {
        let decision = ModelLicensePolicy.evaluate(None, LicenseOperation::ExistingExecution, true);

        assert_eq!(decision.status, LicensePolicyStatus::PendingMetadata);
        assert_eq!(decision.reason, LicensePolicyReason::LegacyInstalledModel);
        assert!(decision.permits_operation);
    }

    #[test]
    fn unknown_license_becomes_pending_review() {
        let mut metadata = allowed_metadata();
        metadata.policy_class = None;

        let decision = evaluate(&metadata, LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::PendingReview);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseUnknown);
        assert!(!decision.permits_operation);
    }

    #[test]
    fn invalid_license_id_is_blocked() {
        let mut metadata = allowed_metadata();
        metadata.license_id = Some("bad id!".to_string());

        let decision = evaluate(&metadata, LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::Blocked);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseIdInvalid);
    }

    #[test]
    fn invalid_license_url_is_blocked() {
        let mut metadata = allowed_metadata();
        metadata.license_url = Some("not-a-url".to_string());

        let decision = evaluate(&metadata, LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::Blocked);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseUrlInvalid);
    }

    #[test]
    fn http_license_url_is_rejected() {
        let mut metadata = allowed_metadata();
        metadata.license_url = Some("http://example.com/license".to_string());

        let decision = evaluate(&metadata, LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::Blocked);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseUrlInvalid);
    }

    #[test]
    fn url_without_license_id_remains_pending_metadata() {
        let metadata = LicenseMetadata {
            license_id: None,
            license_url: Some("https://example.com/license".to_string()),
            policy_class: None,
            requires_acceptance: false,
            revision: None,
        };

        let decision = evaluate(&metadata, LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::PendingMetadata);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseMissing);
    }

    #[test]
    fn conflicting_metadata_is_blocked() {
        let mut metadata = allowed_metadata();
        metadata.requires_acceptance = true;

        let decision = evaluate(&metadata, LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::Blocked);
        assert_eq!(
            decision.reason,
            LicensePolicyReason::LicenseMetadataConflict
        );
    }

    #[test]
    fn list_operation_exposes_status_without_side_effects() {
        let decision = ModelLicensePolicy.evaluate(None, LicenseOperation::List, false);

        assert_eq!(decision.status, LicensePolicyStatus::PendingMetadata);
        assert_eq!(decision.reason, LicensePolicyReason::LicenseMissing);
        assert!(decision.permits_operation);
    }

    #[test]
    fn license_allowed_cannot_override_blocked_download_policy() {
        let download = ModelDownloadDecision {
            model_id: "bad".to_string(),
            status: ModelDownloadPolicyStatus::Blocked,
            reasons: vec![ModelDownloadRejectReason::UnknownModel],
            source: None,
            format: None,
            source_trusted: false,
            format_allowed: false,
            checksum_status: crate::download_policy::ModelChecksumStatus::Missing,
        };
        let license = evaluate(&allowed_metadata(), LicenseOperation::Download);

        let admission = ModelAdmissionDecision::from_decisions(download, license);

        assert!(!admission.permits_operation);
    }

    #[test]
    fn download_allowed_cannot_override_blocked_license_policy() {
        let download = crate::download_policy::ModelDownloadPolicy::default().evaluate(
            &crate::download_policy::ModelDownloadRequest {
                model_id: "tinyllama-1b",
                version: "1.0",
                source_url: Some("https://huggingface.co/org/model/resolve/main/model.gguf"),
                source_kind: Some("registry"),
                format: None,
                size_bytes: Some(669_000_000),
                expected_sha256: Some(""),
                actual_sha256: None,
                registry_known: true,
                manual_model: false,
            },
        );
        let license = evaluate(&restricted_metadata(), LicenseOperation::Download);

        let admission = ModelAdmissionDecision::from_decisions(download, license);

        assert!(!admission.permits_operation);
    }

    #[test]
    fn pending_checksum_does_not_imply_license_allowed() {
        let download = crate::download_policy::ModelDownloadPolicy::default().evaluate(
            &crate::download_policy::ModelDownloadRequest {
                model_id: "tinyllama-1b",
                version: "1.0",
                source_url: Some("https://huggingface.co/org/model/resolve/main/model.gguf"),
                source_kind: Some("registry"),
                format: None,
                size_bytes: Some(669_000_000),
                expected_sha256: Some(""),
                actual_sha256: None,
                registry_known: true,
                manual_model: false,
            },
        );
        let license = ModelLicensePolicy.evaluate(None, LicenseOperation::Download, false);

        let admission = ModelAdmissionDecision::from_decisions(download, license);

        assert_eq!(
            admission.download.status,
            ModelDownloadPolicyStatus::PendingChecksum
        );
        assert_eq!(
            admission.license.status,
            LicensePolicyStatus::PendingMetadata
        );
        assert!(!admission.permits_operation);
    }

    #[test]
    fn pending_license_does_not_imply_trusted() {
        let decision = evaluate(&acceptance_metadata(), LicenseOperation::Download);

        assert_eq!(decision.status, LicensePolicyStatus::RequiresAcceptance);
        assert!(!decision.status.as_str().contains("trusted"));
        assert!(!decision.reason.as_str().contains("trusted"));
    }
}
