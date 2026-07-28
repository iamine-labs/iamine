use std::{fmt, sync::Arc};

use super::evaluation::evaluate_request;
use super::evidence::ExecutionAuthorizationAuthorityIdentity;
use super::{
    ExecutionAuthorizationError, ExecutionAuthorizationEvidence, ExecutionAuthorizationRequest,
};

/// Operator-local capability that emits a passive execution-authorization decision.
///
/// The decision does not load a package, start a sandbox, mutate lifecycle
/// state, activate a runtime, dispatch a task, or perform an external action.
pub struct ExecutionAuthorizationAuthority {
    identity: Arc<ExecutionAuthorizationAuthorityIdentity>,
}

impl ExecutionAuthorizationAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(ExecutionAuthorizationAuthorityIdentity),
        }
    }

    pub fn authorize<'subject>(
        &self,
        request: &ExecutionAuthorizationRequest<'_, 'subject>,
    ) -> Result<ExecutionAuthorizationEvidence<'subject>, ExecutionAuthorizationError> {
        let facts = evaluate_request(request)?;
        Ok(ExecutionAuthorizationEvidence::new(
            Arc::clone(&self.identity),
            facts,
        ))
    }

    pub fn verifies(
        &self,
        evidence: &ExecutionAuthorizationEvidence<'_>,
        request: &ExecutionAuthorizationRequest<'_, '_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
            && evaluate_request(request).is_ok_and(|facts| evidence.matches(&facts))
    }
}

impl Default for ExecutionAuthorizationAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for ExecutionAuthorizationAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionAuthorizationAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
