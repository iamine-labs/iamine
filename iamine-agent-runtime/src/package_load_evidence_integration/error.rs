use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PackageLoadEvidenceRequirement {
    ExecutionAuthorizationEvidence,
}

impl PackageLoadEvidenceRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionAuthorizationEvidence => "execution_authorization_evidence",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PackageLoadEvidenceErrorCode {
    ExecutionAuthorizationNotVerified,
}

impl PackageLoadEvidenceErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionAuthorizationNotVerified => "execution_authorization_not_verified",
        }
    }

    const fn message(self) -> &'static str {
        match self {
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
