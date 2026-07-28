use std::{fmt, sync::Arc};

use iamine_agents::{PermissionEvaluation, ScopeEvaluation};

use crate::HandoffDispatchEvidence;

use super::{
    policy::{plan_handoff_response, plan_permission_response, plan_scope_response},
    OutOfScopeResponseError, OutOfScopeResponseEvidence,
};

#[derive(Debug)]
pub(crate) struct OutOfScopeResponseAuthorityIdentity;

/// Operator-local authority for recording deterministic non-execution responses.
///
/// This authority records only a fixed, typed response classification. It
/// cannot deliver a response, grant permissions, broaden scope, route work,
/// perform transport, authorize execution, persist data, or emit audit events.
pub struct OutOfScopeResponseAuthority {
    identity: Arc<OutOfScopeResponseAuthorityIdentity>,
}

impl OutOfScopeResponseAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(OutOfScopeResponseAuthorityIdentity),
        }
    }

    pub fn respond_to_scope(
        &self,
        evaluation: &ScopeEvaluation,
    ) -> Result<OutOfScopeResponseEvidence, OutOfScopeResponseError> {
        Ok(OutOfScopeResponseEvidence::new(
            Arc::clone(&self.identity),
            plan_scope_response(evaluation)?,
            false,
            false,
        ))
    }

    pub fn respond_to_permission(
        &self,
        evaluation: &PermissionEvaluation,
    ) -> Result<OutOfScopeResponseEvidence, OutOfScopeResponseError> {
        Ok(OutOfScopeResponseEvidence::new(
            Arc::clone(&self.identity),
            plan_permission_response(evaluation)?,
            false,
            false,
        ))
    }

    pub fn respond_to_handoff(
        &self,
        evidence: &HandoffDispatchEvidence,
    ) -> OutOfScopeResponseEvidence {
        OutOfScopeResponseEvidence::new(
            Arc::clone(&self.identity),
            plan_handoff_response(evidence),
            evidence.dispatch_recorded(),
            evidence.local_execution_cancelled(),
        )
    }

    pub fn verifies_response(&self, evidence: &OutOfScopeResponseEvidence) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
    }
}

impl Default for OutOfScopeResponseAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for OutOfScopeResponseAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutOfScopeResponseAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
