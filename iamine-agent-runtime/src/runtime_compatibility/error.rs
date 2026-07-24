use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RuntimeCompatibilityRequirement {
    PackageReviewEvidence,
    RuntimeLanguage,
    ResourceMetadata,
    OperatingMode,
    Cpu,
    Memory,
    Storage,
    Network,
}

impl RuntimeCompatibilityRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PackageReviewEvidence => "package_review_evidence",
            Self::RuntimeLanguage => "runtime_language",
            Self::ResourceMetadata => "resource_metadata",
            Self::OperatingMode => "operating_mode",
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::Storage => "storage",
            Self::Network => "network",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeCompatibilityErrorCode {
    ReviewEvidenceNotVerified,
    RuntimeModeUnsupported,
    RuntimeUnavailable,
    RuntimeDeferred,
    RuntimeBlocked,
    ResourceMetadataMissing,
    ResourceMetadataInvalid,
    ResourcePackageMismatch,
    OperatingModeMissing,
    CpuInsufficient,
    MemoryInsufficient,
    StorageInsufficient,
    NetworkInsufficient,
}

impl RuntimeCompatibilityErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReviewEvidenceNotVerified => "review_evidence_not_verified",
            Self::RuntimeModeUnsupported => "runtime_mode_unsupported",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::RuntimeDeferred => "runtime_deferred",
            Self::RuntimeBlocked => "runtime_blocked",
            Self::ResourceMetadataMissing => "resource_metadata_missing",
            Self::ResourceMetadataInvalid => "resource_metadata_invalid",
            Self::ResourcePackageMismatch => "resource_package_mismatch",
            Self::OperatingModeMissing => "operating_mode_missing",
            Self::CpuInsufficient => "cpu_insufficient",
            Self::MemoryInsufficient => "memory_insufficient",
            Self::StorageInsufficient => "storage_insufficient",
            Self::NetworkInsufficient => "network_insufficient",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ReviewEvidenceNotVerified => "package review evidence is not verified",
            Self::RuntimeModeUnsupported => "runtime language mode is not executable",
            Self::RuntimeUnavailable => "runtime language mode is unavailable",
            Self::RuntimeDeferred => "runtime language mode is deferred",
            Self::RuntimeBlocked => "runtime language mode is blocked",
            Self::ResourceMetadataMissing => "resource metadata is unavailable",
            Self::ResourceMetadataInvalid => "resource metadata is invalid",
            Self::ResourcePackageMismatch => "resource metadata package does not match",
            Self::OperatingModeMissing => "resource operating mode is unavailable",
            Self::CpuInsufficient => "CPU resource envelope is insufficient",
            Self::MemoryInsufficient => "memory resource envelope is insufficient",
            Self::StorageInsufficient => "storage resource envelope is insufficient",
            Self::NetworkInsufficient => "network resource envelope is insufficient",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCompatibilityError {
    code: RuntimeCompatibilityErrorCode,
    requirement: RuntimeCompatibilityRequirement,
}

impl RuntimeCompatibilityError {
    pub(crate) const fn new(
        code: RuntimeCompatibilityErrorCode,
        requirement: RuntimeCompatibilityRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> RuntimeCompatibilityErrorCode {
        self.code
    }

    pub const fn requirement(self) -> RuntimeCompatibilityRequirement {
        self.requirement
    }
}

impl fmt::Display for RuntimeCompatibilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for RuntimeCompatibilityError {}
