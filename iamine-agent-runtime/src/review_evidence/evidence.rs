use std::{fmt, sync::Arc};

use super::PackageReviewSubject;

#[derive(Debug)]
pub(crate) struct ReviewAuthorityIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PackageReviewRequirement {
    LocalRegistry,
    LanguagePolicy,
    DependencyPolicy,
    IndependentHumanReview,
}

impl PackageReviewRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalRegistry => "local_registry",
            Self::LanguagePolicy => "language_policy",
            Self::DependencyPolicy => "dependency_policy",
            Self::IndependentHumanReview => "independent_human_review",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PackageReviewEvidenceStatus {
    Established,
}

const ESTABLISHED_REQUIREMENTS: [PackageReviewRequirement; 4] = [
    PackageReviewRequirement::LocalRegistry,
    PackageReviewRequirement::LanguagePolicy,
    PackageReviewRequirement::DependencyPolicy,
    PackageReviewRequirement::IndependentHumanReview,
];

#[must_use]
pub struct PackageReviewEvidence<'a> {
    authority: Arc<ReviewAuthorityIdentity>,
    subject: PackageReviewSubject<'a>,
}

impl<'a> PackageReviewEvidence<'a> {
    pub(crate) fn new(
        authority: Arc<ReviewAuthorityIdentity>,
        subject: PackageReviewSubject<'a>,
    ) -> Self {
        Self { authority, subject }
    }

    pub const fn status(&self) -> PackageReviewEvidenceStatus {
        PackageReviewEvidenceStatus::Established
    }

    pub const fn requirements(&self) -> &'static [PackageReviewRequirement] {
        &ESTABLISHED_REQUIREMENTS
    }

    pub const fn load_allowed(&self) -> bool {
        false
    }

    pub const fn execution_allowed(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<ReviewAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'a> {
        self.subject
    }
}

impl fmt::Debug for PackageReviewEvidence<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackageReviewEvidence")
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("load_allowed", &false)
            .field("execution_allowed", &false)
            .finish()
    }
}
