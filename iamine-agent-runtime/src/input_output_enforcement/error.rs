use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InputOutputRequirement {
    RuntimeCompatibilityEvidence,
    EnforcementEvidence,
    ScopeMetadata,
    PackageIdentity,
    TaskType,
    RedactionAttestation,
    RecordContent,
    RecordLimit,
}

impl InputOutputRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeCompatibilityEvidence => "runtime_compatibility_evidence",
            Self::EnforcementEvidence => "input_output_enforcement_evidence",
            Self::ScopeMetadata => "scope_metadata",
            Self::PackageIdentity => "package_identity",
            Self::TaskType => "task_type",
            Self::RedactionAttestation => "redaction_attestation",
            Self::RecordContent => "record_content",
            Self::RecordLimit => "record_limit",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputOutputEnforcementErrorCode {
    RuntimeCompatibilityNotVerified,
    EnforcementEvidenceNotVerified,
    ScopeMetadataMissing,
    ScopeMetadataInvalid,
    ScopePackageMismatch,
    ScopeTaskTypeMismatch,
    RedactionAttestationNotVerified,
    EmptyContent,
    InputTooLarge,
    OutputTooLarge,
    ControlCharacter,
}

impl InputOutputEnforcementErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeCompatibilityNotVerified => "runtime_compatibility_not_verified",
            Self::EnforcementEvidenceNotVerified => "enforcement_evidence_not_verified",
            Self::ScopeMetadataMissing => "scope_metadata_missing",
            Self::ScopeMetadataInvalid => "scope_metadata_invalid",
            Self::ScopePackageMismatch => "scope_package_mismatch",
            Self::ScopeTaskTypeMismatch => "scope_task_type_mismatch",
            Self::RedactionAttestationNotVerified => "redaction_attestation_not_verified",
            Self::EmptyContent => "empty_content",
            Self::InputTooLarge => "input_too_large",
            Self::OutputTooLarge => "output_too_large",
            Self::ControlCharacter => "control_character",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::RuntimeCompatibilityNotVerified => {
                "runtime compatibility evidence is not verified"
            }
            Self::EnforcementEvidenceNotVerified => {
                "input/output enforcement evidence is not verified"
            }
            Self::ScopeMetadataMissing => "scope metadata is unavailable",
            Self::ScopeMetadataInvalid => "scope metadata is invalid",
            Self::ScopePackageMismatch => "scope metadata package does not match",
            Self::ScopeTaskTypeMismatch => "scope metadata task type does not match",
            Self::RedactionAttestationNotVerified => {
                "operator redaction attestation is not verified"
            }
            Self::EmptyContent => "redacted record content must not be empty",
            Self::InputTooLarge => "redacted input exceeds the configured limit",
            Self::OutputTooLarge => "redacted output exceeds the configured limit",
            Self::ControlCharacter => "redacted record content contains a control character",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputOutputEnforcementError {
    code: InputOutputEnforcementErrorCode,
    requirement: InputOutputRequirement,
}

impl InputOutputEnforcementError {
    pub(crate) const fn new(
        code: InputOutputEnforcementErrorCode,
        requirement: InputOutputRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> InputOutputEnforcementErrorCode {
        self.code
    }

    pub const fn requirement(self) -> InputOutputRequirement {
        self.requirement
    }
}

impl fmt::Display for InputOutputEnforcementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for InputOutputEnforcementError {}
