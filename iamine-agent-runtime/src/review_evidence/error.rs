use std::fmt;

use super::PackageReviewRequirement;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReviewEvidenceErrorCode {
    RegistryNotReady,
    LanguageNotAllowed,
    DependenciesNotApproved,
    HumanReviewNotApproved,
}

impl ReviewEvidenceErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RegistryNotReady => "registry_not_ready",
            Self::LanguageNotAllowed => "language_not_allowed",
            Self::DependenciesNotApproved => "dependencies_not_approved",
            Self::HumanReviewNotApproved => "human_review_not_approved",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::RegistryNotReady => "local registry review is not ready",
            Self::LanguageNotAllowed => "language policy review is not allowed",
            Self::DependenciesNotApproved => "dependency policy review is not approved",
            Self::HumanReviewNotApproved => "independent human review is not approved",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewEvidenceError {
    code: ReviewEvidenceErrorCode,
    requirement: PackageReviewRequirement,
}

impl ReviewEvidenceError {
    pub(crate) const fn new(
        code: ReviewEvidenceErrorCode,
        requirement: PackageReviewRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> ReviewEvidenceErrorCode {
        self.code
    }

    pub const fn requirement(self) -> PackageReviewRequirement {
        self.requirement
    }
}

impl fmt::Display for ReviewEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for ReviewEvidenceError {}
