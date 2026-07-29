use std::{fmt, sync::Arc};

use crate::{
    ExecutionAuthorizationAuthority, ExecutionAuthorizationEvidence, ExecutionAuthorizationRequest,
    PackageLoadEvidence, PackageLoadEvidenceAuthority,
};

use super::loaded::PackageLoaderAuthorityIdentity;
use super::{
    LoadedAgentPackage, PackageLoaderError, PackageLoaderErrorCode, PackageLoaderRequirement,
};

/// Operator-local capability that materializes an eligible package snapshot.
///
/// Loading retains the exact bounded subject already validated by package-load
/// evidence. It does not reopen package paths or start runtime execution.
pub struct PackageLoaderAuthority {
    identity: Arc<PackageLoaderAuthorityIdentity>,
}

impl PackageLoaderAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(PackageLoaderAuthorityIdentity),
        }
    }

    pub fn load<'subject>(
        &self,
        evidence_authority: &PackageLoadEvidenceAuthority,
        evidence: &PackageLoadEvidence<'subject>,
        authorization_authority: &ExecutionAuthorizationAuthority,
        authorization_evidence: &ExecutionAuthorizationEvidence<'subject>,
        authorization_request: &ExecutionAuthorizationRequest<'_, 'subject>,
    ) -> Result<LoadedAgentPackage<'subject>, PackageLoaderError> {
        if !evidence_authority.verifies(
            evidence,
            authorization_authority,
            authorization_evidence,
            authorization_request,
        ) {
            return Err(PackageLoaderError::new(
                PackageLoaderErrorCode::PackageLoadEvidenceNotVerified,
                PackageLoaderRequirement::PackageLoadEvidence,
            ));
        }

        Ok(LoadedAgentPackage::new(
            Arc::clone(&self.identity),
            evidence,
        ))
    }

    pub fn verifies(
        &self,
        loaded: &LoadedAgentPackage<'_>,
        evidence_authority: &PackageLoadEvidenceAuthority,
        evidence: &PackageLoadEvidence<'_>,
        authorization_authority: &ExecutionAuthorizationAuthority,
        authorization_evidence: &ExecutionAuthorizationEvidence<'_>,
        authorization_request: &ExecutionAuthorizationRequest<'_, '_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, loaded.authority())
            && evidence_authority.verifies(
                evidence,
                authorization_authority,
                authorization_evidence,
                authorization_request,
            )
            && loaded.matches_evidence(evidence)
    }
}

impl Default for PackageLoaderAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for PackageLoaderAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackageLoaderAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
