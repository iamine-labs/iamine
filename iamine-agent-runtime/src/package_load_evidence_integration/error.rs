use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PackageLoadEvidenceRequirement {
    ScopeManifestValidation,
    CapabilityMetadataValidation,
    ExpertiseMetadataValidation,
    ResourceRequirementsValidation,
    PermissionModelValidation,
    AuditPolicyValidation,
    BoundaryEvalValidation,
    ReferenceContract,
    ExecutionAuthorizationEvidence,
}

impl PackageLoadEvidenceRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopeManifestValidation => "scope_manifest_validation",
            Self::CapabilityMetadataValidation => "capability_metadata_validation",
            Self::ExpertiseMetadataValidation => "expertise_metadata_validation",
            Self::ResourceRequirementsValidation => "resource_requirements_validation",
            Self::PermissionModelValidation => "permission_model_validation",
            Self::AuditPolicyValidation => "audit_policy_validation",
            Self::BoundaryEvalValidation => "boundary_eval_validation",
            Self::ReferenceContract => "reference_contract",
            Self::ExecutionAuthorizationEvidence => "execution_authorization_evidence",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PackageLoadEvidenceErrorCode {
    ReferenceMissing,
    ReferenceEncodingInvalid,
    ReferenceValidationFailed,
    PackageIdentityMismatch,
    ReferenceContractMismatch,
    ExecutionAuthorizationNotVerified,
}

impl PackageLoadEvidenceErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReferenceMissing => "reference_missing",
            Self::ReferenceEncodingInvalid => "reference_encoding_invalid",
            Self::ReferenceValidationFailed => "reference_validation_failed",
            Self::PackageIdentityMismatch => "package_identity_mismatch",
            Self::ReferenceContractMismatch => "reference_contract_mismatch",
            Self::ExecutionAuthorizationNotVerified => "execution_authorization_not_verified",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ReferenceMissing => "required package reference is missing",
            Self::ReferenceEncodingInvalid => "package reference is not valid UTF-8",
            Self::ReferenceValidationFailed => {
                "package reference did not pass its canonical validator"
            }
            Self::PackageIdentityMismatch => {
                "package reference does not target the reviewed package"
            }
            Self::ReferenceContractMismatch => {
                "validated package references contain contradictory declarations"
            }
            Self::ExecutionAuthorizationNotVerified => {
                "execution authorization evidence was not verified for package loading"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageLoadEvidenceError {
    code: PackageLoadEvidenceErrorCode,
    requirement: PackageLoadEvidenceRequirement,
}

impl PackageLoadEvidenceError {
    pub(crate) const fn new(
        code: PackageLoadEvidenceErrorCode,
        requirement: PackageLoadEvidenceRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> PackageLoadEvidenceErrorCode {
        self.code
    }

    pub const fn requirement(self) -> PackageLoadEvidenceRequirement {
        self.requirement
    }
}

impl fmt::Display for PackageLoadEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for PackageLoadEvidenceError {}
