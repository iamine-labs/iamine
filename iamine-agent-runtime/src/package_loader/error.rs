use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PackageLoaderRequirement {
    PackageLoadEvidence,
    BoundedReferenceSnapshot,
    ValidatedReferenceContract,
}

impl PackageLoaderRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PackageLoadEvidence => "package_load_evidence",
            Self::BoundedReferenceSnapshot => "bounded_reference_snapshot",
            Self::ValidatedReferenceContract => "validated_reference_contract",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PackageLoaderErrorCode {
    PackageLoadEvidenceNotVerified,
}

impl PackageLoaderErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PackageLoadEvidenceNotVerified => "package_load_evidence_not_verified",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::PackageLoadEvidenceNotVerified => {
                "package-load evidence was not verified for the current authorization"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageLoaderError {
    code: PackageLoaderErrorCode,
    requirement: PackageLoaderRequirement,
}

impl PackageLoaderError {
    pub(crate) const fn new(
        code: PackageLoaderErrorCode,
        requirement: PackageLoaderRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> PackageLoaderErrorCode {
        self.code
    }

    pub const fn requirement(self) -> PackageLoaderRequirement {
        self.requirement
    }
}

impl fmt::Display for PackageLoaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for PackageLoaderError {}
