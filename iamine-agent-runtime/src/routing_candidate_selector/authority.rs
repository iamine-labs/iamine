use std::{fmt, sync::Arc};

use crate::{RuntimeCompatibilityAuthority, SandboxEnforcementAuthority};

use super::{
    evaluation::evaluate_candidates, RoutingCandidateRef, RoutingCandidateSelectionEvidence,
    RoutingCandidateSelectorError, RoutingSelectionRequestRef,
};

#[derive(Debug)]
pub(crate) struct RoutingCandidateSelectionAuthorityIdentity;

/// Operator-local capability that establishes passive candidate-selection evidence.
///
/// Selection does not create a route, mutate a scheduler, or authorize execution.
pub struct RoutingCandidateSelectionAuthority {
    identity: Arc<RoutingCandidateSelectionAuthorityIdentity>,
}

impl RoutingCandidateSelectionAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(RoutingCandidateSelectionAuthorityIdentity),
        }
    }

    pub fn select(
        &self,
        request: RoutingSelectionRequestRef<'_>,
        candidates: &[RoutingCandidateRef<'_>],
        compatibility_authority: &RuntimeCompatibilityAuthority,
        sandbox_authority: &SandboxEnforcementAuthority,
    ) -> Result<RoutingCandidateSelectionEvidence, RoutingCandidateSelectorError> {
        let result = evaluate_candidates(
            request,
            candidates,
            compatibility_authority,
            sandbox_authority,
        )?;
        Ok(RoutingCandidateSelectionEvidence::new(
            Arc::clone(&self.identity),
            result,
        ))
    }

    pub fn verifies(&self, evidence: &RoutingCandidateSelectionEvidence) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
    }
}

impl Default for RoutingCandidateSelectionAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for RoutingCandidateSelectionAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutingCandidateSelectionAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
