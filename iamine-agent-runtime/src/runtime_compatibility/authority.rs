use std::{fmt, sync::Arc};

use crate::{
    PackageReviewAuthority, PackageReviewEvidence, PackageReviewSubject, RuntimeCompatibilityError,
    RuntimeCompatibilityErrorCode, RuntimeCompatibilityEvidence, RuntimeCompatibilityRequirement,
};

use super::configuration::{RuntimeLanguageDecision, RuntimeResourceEnvelope};
use super::evaluation::evaluate_subject;
use super::evidence::RuntimeCompatibilityAuthorityIdentity;

/// Operator-local capability that establishes passive compatibility evidence.
///
/// Consumers must retain this configured authority and the package review
/// authority. Package-controlled bytes cannot construct either authority.
pub struct RuntimeCompatibilityAuthority {
    identity: Arc<RuntimeCompatibilityAuthorityIdentity>,
    language: RuntimeLanguageDecision,
    resources: RuntimeResourceEnvelope,
}

impl RuntimeCompatibilityAuthority {
    pub fn new_operator_local(
        language: RuntimeLanguageDecision,
        resources: RuntimeResourceEnvelope,
    ) -> Self {
        Self {
            identity: Arc::new(RuntimeCompatibilityAuthorityIdentity),
            language,
            resources,
        }
    }

    pub fn evaluate<'a>(
        &self,
        review_authority: &PackageReviewAuthority,
        review_evidence: &PackageReviewEvidence<'a>,
        subject: PackageReviewSubject<'a>,
    ) -> Result<RuntimeCompatibilityEvidence<'a>, RuntimeCompatibilityError> {
        if !review_authority.verifies(review_evidence, subject) {
            return Err(RuntimeCompatibilityError::new(
                RuntimeCompatibilityErrorCode::ReviewEvidenceNotVerified,
                RuntimeCompatibilityRequirement::PackageReviewEvidence,
            ));
        }
        let result = evaluate_subject(subject, self.language, self.resources)?;
        Ok(RuntimeCompatibilityEvidence::new(
            Arc::clone(&self.identity),
            subject,
            self.language.mode(),
            result.operating_mode,
        ))
    }

    pub fn verifies(
        &self,
        evidence: &RuntimeCompatibilityEvidence<'_>,
        subject: PackageReviewSubject<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority()) && evidence.subject().same_as(subject)
    }
}

impl fmt::Debug for RuntimeCompatibilityAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeCompatibilityAuthority")
            .field("identity", &"[redacted]")
            .field("language", &"[redacted]")
            .field("resources", &"[redacted]")
            .finish()
    }
}
