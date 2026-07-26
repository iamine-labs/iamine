use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SandboxEnforcementRequirement {
    RuntimeCompatibilityEvidence,
    InputOutputEnforcementEvidence,
    EvidenceChain,
    CurrentPlatform,
    RuntimeMode,
    OperatingMode,
    SecurityPolicy,
    ResourceMetadata,
    FilesystemIsolation,
    NetworkIsolation,
    ResourceLimits,
    CleanupOwnership,
}

impl SandboxEnforcementRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeCompatibilityEvidence => "runtime_compatibility_evidence",
            Self::InputOutputEnforcementEvidence => "input_output_enforcement_evidence",
            Self::EvidenceChain => "evidence_chain",
            Self::CurrentPlatform => "current_platform",
            Self::RuntimeMode => "runtime_mode",
            Self::OperatingMode => "operating_mode",
            Self::SecurityPolicy => "security_policy",
            Self::ResourceMetadata => "resource_metadata",
            Self::FilesystemIsolation => "filesystem_isolation",
            Self::NetworkIsolation => "network_isolation",
            Self::ResourceLimits => "resource_limits",
            Self::CleanupOwnership => "cleanup_ownership",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxEnforcementErrorCode {
    RuntimeCompatibilityNotVerified,
    InputOutputEnforcementNotVerified,
    EvidenceChainMismatch,
    RuntimeModeUnsupported,
    OperatingModeUnsupported,
    PrivateDataRequested,
    UnsafeSecurityPolicy,
    NetworkAccessUnsupported,
    ResourceMetadataInvalid,
}

impl SandboxEnforcementErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeCompatibilityNotVerified => "runtime_compatibility_not_verified",
            Self::InputOutputEnforcementNotVerified => "input_output_enforcement_not_verified",
            Self::EvidenceChainMismatch => "evidence_chain_mismatch",
            Self::RuntimeModeUnsupported => "runtime_mode_unsupported",
            Self::OperatingModeUnsupported => "operating_mode_unsupported",
            Self::PrivateDataRequested => "private_data_requested",
            Self::UnsafeSecurityPolicy => "unsafe_security_policy",
            Self::NetworkAccessUnsupported => "network_access_unsupported",
            Self::ResourceMetadataInvalid => "resource_metadata_invalid",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::RuntimeCompatibilityNotVerified => {
                "runtime compatibility evidence was not verified"
            }
            Self::InputOutputEnforcementNotVerified => {
                "input/output enforcement evidence was not verified"
            }
            Self::EvidenceChainMismatch => "the enforcement evidence chain does not match",
            Self::RuntimeModeUnsupported => "the runtime mode is not sandbox-plan eligible",
            Self::OperatingModeUnsupported => "the operating mode is not sandbox-plan eligible",
            Self::PrivateDataRequested => "the package requests private operator data",
            Self::UnsafeSecurityPolicy => "the package security policy is unsafe",
            Self::NetworkAccessUnsupported => "network access is not supported by this plan",
            Self::ResourceMetadataInvalid => "resource metadata is invalid for sandbox planning",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxEnforcementError {
    code: SandboxEnforcementErrorCode,
    requirement: SandboxEnforcementRequirement,
}

impl SandboxEnforcementError {
    pub(crate) const fn new(
        code: SandboxEnforcementErrorCode,
        requirement: SandboxEnforcementRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> SandboxEnforcementErrorCode {
        self.code
    }

    pub const fn requirement(self) -> SandboxEnforcementRequirement {
        self.requirement
    }
}

impl fmt::Display for SandboxEnforcementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for SandboxEnforcementError {}
