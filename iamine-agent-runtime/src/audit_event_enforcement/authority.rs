use std::{fmt, sync::Arc};

use iamine_agents::{
    audit_permission_evaluation, audit_scope_evaluation, PermissionEvaluation, ScopeEvaluation,
};

use crate::{ExecutionLifecycleAuthority, ExecutionLifecycleRecord};

use super::evidence::AuditEventEnforcementAuthorityIdentity;
use super::lifecycle::audit_runtime_lifecycle_state;
use super::{
    AuditEventEnforcementError, AuditEventEnforcementErrorCode, AuditEventEnforcementEvidence,
    AuditEventEnforcementRequirement,
};

/// Operator-local authority for enforcing bounded in-memory audit projections.
///
/// Audit evidence cannot authorize execution or prove an external side effect.
pub struct AuditEventEnforcementAuthority {
    identity: Arc<AuditEventEnforcementAuthorityIdentity>,
}

impl AuditEventEnforcementAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(AuditEventEnforcementAuthorityIdentity),
        }
    }

    pub fn enforce_scope(&self, evaluation: &ScopeEvaluation) -> AuditEventEnforcementEvidence {
        AuditEventEnforcementEvidence::typed_gate(
            Arc::clone(&self.identity),
            audit_scope_evaluation(evaluation),
        )
    }

    pub fn enforce_permission(
        &self,
        evaluation: &PermissionEvaluation,
    ) -> AuditEventEnforcementEvidence {
        AuditEventEnforcementEvidence::typed_gate(
            Arc::clone(&self.identity),
            audit_permission_evaluation(evaluation),
        )
    }

    pub fn enforce_lifecycle(
        &self,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> Result<AuditEventEnforcementEvidence, AuditEventEnforcementError> {
        if !lifecycle_authority.verifies_record_identity(lifecycle_record) {
            return Err(AuditEventEnforcementError::new(
                AuditEventEnforcementErrorCode::LifecycleRecordNotVerified,
                AuditEventEnforcementRequirement::LifecycleAuthority,
            ));
        }

        Ok(AuditEventEnforcementEvidence::authority_bound_lifecycle(
            Arc::clone(&self.identity),
            Arc::clone(lifecycle_record.execution()),
            audit_runtime_lifecycle_state(lifecycle_record.state()),
            lifecycle_record.revision(),
        ))
    }

    pub fn verifies(&self, evidence: &AuditEventEnforcementEvidence) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
    }

    pub fn verifies_scope(
        &self,
        evidence: &AuditEventEnforcementEvidence,
        evaluation: &ScopeEvaluation,
    ) -> bool {
        self.verifies(evidence)
            && !evidence.upstream_authority_bound()
            && evidence.events() == &audit_scope_evaluation(evaluation)
    }

    pub fn verifies_permission(
        &self,
        evidence: &AuditEventEnforcementEvidence,
        evaluation: &PermissionEvaluation,
    ) -> bool {
        self.verifies(evidence)
            && !evidence.upstream_authority_bound()
            && evidence.events() == &audit_permission_evaluation(evaluation)
    }

    pub fn verifies_lifecycle(
        &self,
        evidence: &AuditEventEnforcementEvidence,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        self.verifies(evidence)
            && evidence.upstream_authority_bound()
            && evidence.lifecycle_revision() == Some(lifecycle_record.revision())
            && lifecycle_authority.verifies_record_identity(lifecycle_record)
            && evidence.events() == &audit_runtime_lifecycle_state(lifecycle_record.state())
            && evidence
                .execution()
                .is_some_and(|execution| Arc::ptr_eq(execution, lifecycle_record.execution()))
    }
}

impl Default for AuditEventEnforcementAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for AuditEventEnforcementAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditEventEnforcementAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
