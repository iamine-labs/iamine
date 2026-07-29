use std::{fmt, sync::Arc};

use crate::{
    ExecutionAuthorizationAuthority, ExecutionAuthorizationEvidence, ExecutionAuthorizationRequest,
};

use super::evidence::PackageLoadEvidenceAuthorityIdentity;
use super::validation::validate_subject;
use super::{
    PackageLoadEvidence, PackageLoadEvidenceError, PackageLoadEvidenceErrorCode,
    PackageLoadEvidenceRequirement,
};

/// Operator-local capability that integrates current authorization evidence
/// into a passive package-load eligibility decision.
pub struct PackageLoadEvidenceAuthority {
    identity: Arc<PackageLoadEvidenceAuthorityIdentity>,
}

impl PackageLoadEvidenceAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(PackageLoadEvidenceAuthorityIdentity),
        }
    }

    pub fn integrate<'subject>(
        &self,
        authorization_authority: &ExecutionAuthorizationAuthority,
        authorization_evidence: &ExecutionAuthorizationEvidence<'subject>,
        authorization_request: &ExecutionAuthorizationRequest<'_, 'subject>,
    ) -> Result<PackageLoadEvidence<'subject>, PackageLoadEvidenceError> {
        if !authorization_authority.verifies(authorization_evidence, authorization_request) {
            return Err(PackageLoadEvidenceError::new(
                PackageLoadEvidenceErrorCode::ExecutionAuthorizationNotVerified,
                PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence,
            ));
        }
        validate_subject(authorization_evidence.subject())?;

        Ok(PackageLoadEvidence::new(
            Arc::clone(&self.identity),
            authorization_evidence,
        ))
    }

    pub fn verifies(
        &self,
        evidence: &PackageLoadEvidence<'_>,
        authorization_authority: &ExecutionAuthorizationAuthority,
        authorization_evidence: &ExecutionAuthorizationEvidence<'_>,
        authorization_request: &ExecutionAuthorizationRequest<'_, '_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
            && authorization_authority.verifies(authorization_evidence, authorization_request)
            && evidence.matches_authorization(authorization_evidence)
    }
}

impl Default for PackageLoadEvidenceAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for PackageLoadEvidenceAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackageLoadEvidenceAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
