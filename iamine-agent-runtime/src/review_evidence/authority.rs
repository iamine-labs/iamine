use std::{fmt, sync::Arc};

use super::evidence::ReviewAuthorityIdentity;
use super::{
    DependencyPolicyReviewDecision, HumanReviewDecision, LanguagePolicyReviewDecision,
    LocalRegistryReviewDecision, PackageReviewDecisions, PackageReviewEvidence,
    PackageReviewRequirement, PackageReviewSubject, ReviewEvidenceError, ReviewEvidenceErrorCode,
};

/// Operator-local capability that establishes package review evidence.
///
/// Package-controlled bytes are never parsed into this capability or its
/// decisions. Consumers must retain and use the same authority instance when
/// verifying evidence.
pub struct PackageReviewAuthority {
    identity: Arc<ReviewAuthorityIdentity>,
}

impl PackageReviewAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(ReviewAuthorityIdentity),
        }
    }

    pub fn issue<'a>(
        &self,
        subject: PackageReviewSubject<'a>,
        decisions: PackageReviewDecisions,
    ) -> Result<PackageReviewEvidence<'a>, ReviewEvidenceError> {
        validate_decisions(decisions)?;
        Ok(PackageReviewEvidence::new(
            Arc::clone(&self.identity),
            subject,
        ))
    }

    pub fn verifies(
        &self,
        evidence: &PackageReviewEvidence<'_>,
        subject: PackageReviewSubject<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority()) && evidence.subject().same_as(subject)
    }
}

impl fmt::Debug for PackageReviewAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PackageReviewAuthority { identity: [redacted] }")
    }
}

fn validate_decisions(decisions: PackageReviewDecisions) -> Result<(), ReviewEvidenceError> {
    if decisions.registry() != LocalRegistryReviewDecision::RegistryReviewReady {
        return Err(ReviewEvidenceError::new(
            ReviewEvidenceErrorCode::RegistryNotReady,
            PackageReviewRequirement::LocalRegistry,
        ));
    }
    if decisions.language() != LanguagePolicyReviewDecision::RustOfficialAllowed {
        return Err(ReviewEvidenceError::new(
            ReviewEvidenceErrorCode::LanguageNotAllowed,
            PackageReviewRequirement::LanguagePolicy,
        ));
    }
    if decisions.dependencies() != DependencyPolicyReviewDecision::Allowed {
        return Err(ReviewEvidenceError::new(
            ReviewEvidenceErrorCode::DependenciesNotApproved,
            PackageReviewRequirement::DependencyPolicy,
        ));
    }
    if decisions.human() != HumanReviewDecision::IndependentApproved {
        return Err(ReviewEvidenceError::new(
            ReviewEvidenceErrorCode::HumanReviewNotApproved,
            PackageReviewRequirement::IndependentHumanReview,
        ));
    }
    Ok(())
}
