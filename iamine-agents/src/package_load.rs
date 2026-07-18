use crate::{parse_and_validate_yaml, ManifestError};

const UNAVAILABLE_PACKAGE_LOAD_GATES: [PackageLoadBlockerCode; 19] = [
    PackageLoadBlockerCode::ScopeManifestValidatorUnavailable,
    PackageLoadBlockerCode::CapabilityMetadataValidatorUnavailable,
    PackageLoadBlockerCode::ExpertiseMetadataValidatorUnavailable,
    PackageLoadBlockerCode::ResourceRequirementsValidatorUnavailable,
    PackageLoadBlockerCode::PermissionModelValidatorUnavailable,
    PackageLoadBlockerCode::AuditPolicyValidatorUnavailable,
    PackageLoadBlockerCode::BoundaryEvalValidatorUnavailable,
    PackageLoadBlockerCode::LocalRegistryReviewUnavailable,
    PackageLoadBlockerCode::LanguagePolicyReviewUnavailable,
    PackageLoadBlockerCode::DependencyPolicyReviewUnavailable,
    PackageLoadBlockerCode::RuntimeLanguageCompatibilityUnavailable,
    PackageLoadBlockerCode::ResourceCompatibilityUnavailable,
    PackageLoadBlockerCode::HumanReviewEvidenceUnavailable,
    PackageLoadBlockerCode::InputOutputEnforcementUnavailable,
    PackageLoadBlockerCode::SandboxEnforcementUnavailable,
    PackageLoadBlockerCode::ScopeEnforcementUnavailable,
    PackageLoadBlockerCode::PermissionEnforcementUnavailable,
    PackageLoadBlockerCode::AuditEventEnforcementUnavailable,
    PackageLoadBlockerCode::ExecutionAuthorizationUnavailable,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PackageLoadStatus {
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PackageLoadBlockerCode {
    ScopeManifestValidatorUnavailable,
    CapabilityMetadataValidatorUnavailable,
    ExpertiseMetadataValidatorUnavailable,
    ResourceRequirementsValidatorUnavailable,
    PermissionModelValidatorUnavailable,
    AuditPolicyValidatorUnavailable,
    BoundaryEvalValidatorUnavailable,
    LocalRegistryReviewUnavailable,
    LanguagePolicyReviewUnavailable,
    DependencyPolicyReviewUnavailable,
    RuntimeLanguageCompatibilityUnavailable,
    ResourceCompatibilityUnavailable,
    HumanReviewEvidenceUnavailable,
    InputOutputEnforcementUnavailable,
    SandboxEnforcementUnavailable,
    ScopeEnforcementUnavailable,
    PermissionEnforcementUnavailable,
    AuditEventEnforcementUnavailable,
    ExecutionAuthorizationUnavailable,
}

impl PackageLoadBlockerCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopeManifestValidatorUnavailable => "scope_manifest_validator_unavailable",
            Self::CapabilityMetadataValidatorUnavailable => {
                "capability_metadata_validator_unavailable"
            }
            Self::ExpertiseMetadataValidatorUnavailable => {
                "expertise_metadata_validator_unavailable"
            }
            Self::ResourceRequirementsValidatorUnavailable => {
                "resource_requirements_validator_unavailable"
            }
            Self::PermissionModelValidatorUnavailable => "permission_model_validator_unavailable",
            Self::AuditPolicyValidatorUnavailable => "audit_policy_validator_unavailable",
            Self::BoundaryEvalValidatorUnavailable => "boundary_eval_validator_unavailable",
            Self::LocalRegistryReviewUnavailable => "local_registry_review_unavailable",
            Self::LanguagePolicyReviewUnavailable => "language_policy_review_unavailable",
            Self::DependencyPolicyReviewUnavailable => "dependency_policy_review_unavailable",
            Self::RuntimeLanguageCompatibilityUnavailable => {
                "runtime_language_compatibility_unavailable"
            }
            Self::ResourceCompatibilityUnavailable => "resource_compatibility_unavailable",
            Self::HumanReviewEvidenceUnavailable => "human_review_evidence_unavailable",
            Self::InputOutputEnforcementUnavailable => "input_output_enforcement_unavailable",
            Self::SandboxEnforcementUnavailable => "sandbox_enforcement_unavailable",
            Self::ScopeEnforcementUnavailable => "scope_enforcement_unavailable",
            Self::PermissionEnforcementUnavailable => "permission_enforcement_unavailable",
            Self::AuditEventEnforcementUnavailable => "audit_event_enforcement_unavailable",
            Self::ExecutionAuthorizationUnavailable => "execution_authorization_unavailable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct PackageLoadReport {
    blockers: &'static [PackageLoadBlockerCode],
}

impl PackageLoadReport {
    pub const fn status(&self) -> PackageLoadStatus {
        PackageLoadStatus::Blocked
    }

    pub const fn load_allowed(&self) -> bool {
        false
    }

    pub const fn blockers(&self) -> &'static [PackageLoadBlockerCode] {
        self.blockers
    }
}

pub fn assess_package_load_yaml(input: &str) -> Result<PackageLoadReport, ManifestError> {
    parse_and_validate_yaml(input)?;

    Ok(PackageLoadReport {
        blockers: &UNAVAILABLE_PACKAGE_LOAD_GATES,
    })
}
