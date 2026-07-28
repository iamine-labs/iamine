use std::{fmt, sync::Arc};

use crate::{
    ExecutionLifecycleAuthority, ExecutionLifecycleRecord, ExecutionLifecycleState,
    ExecutionLifecycleTransitionEvidence,
};

use super::policy::target_supports_reason;
use super::{
    HandoffAuthorityIdentity, HandoffControl, HandoffDispatchEvidence, HandoffError,
    HandoffErrorCode, HandoffRequest, HandoffRequirement,
};

/// Operator-local capability for recording bounded handoff dispatch evidence.
///
/// The authority returns local execution ownership to a typed target class. It
/// cannot select a concrete agent, perform transport, grant permission, broaden
/// scope, approve a human action, or authorize execution.
pub struct HandoffEnforcementAuthority {
    identity: Arc<HandoffAuthorityIdentity>,
}

impl HandoffEnforcementAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(HandoffAuthorityIdentity),
        }
    }

    pub fn prepare(
        &self,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        handoff_transition: &ExecutionLifecycleTransitionEvidence,
        request: HandoffRequest,
    ) -> Result<HandoffControl, HandoffError> {
        if !lifecycle_authority.verifies_record_identity(lifecycle_record) {
            return Err(HandoffError::new(
                HandoffErrorCode::LifecycleRecordNotVerified,
                HandoffRequirement::LifecycleRecord,
            ));
        }
        if lifecycle_record.state() != ExecutionLifecycleState::HandoffRequired {
            return Err(HandoffError::new(
                HandoffErrorCode::HandoffStateRequired,
                HandoffRequirement::HandoffRequiredState,
            ));
        }
        if !lifecycle_authority.verifies_transition(handoff_transition, lifecycle_record)
            || handoff_transition.from() != ExecutionLifecycleState::ScopeCheck
            || handoff_transition.to() != ExecutionLifecycleState::HandoffRequired
            || handoff_transition.revision() != lifecycle_record.revision()
        {
            return Err(HandoffError::new(
                HandoffErrorCode::HandoffTransitionNotVerified,
                HandoffRequirement::HandoffTransitionEvidence,
            ));
        }
        if !target_supports_reason(request.target(), request.reason()) {
            return Err(HandoffError::new(
                HandoffErrorCode::TargetReasonMismatch,
                HandoffRequirement::CompatibleTargetReason,
            ));
        }

        Ok(HandoffControl::new(
            Arc::clone(&self.identity),
            Arc::clone(lifecycle_record.authority()),
            Arc::clone(lifecycle_record.execution()),
            request,
            lifecycle_record.revision(),
        ))
    }

    pub fn verifies_control(
        &self,
        control: &HandoffControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, control.authority())
            && Arc::ptr_eq(control.lifecycle_authority(), lifecycle_record.authority())
            && Arc::ptr_eq(control.execution(), lifecycle_record.execution())
            && lifecycle_authority.verifies_record_identity(lifecycle_record)
    }

    pub fn dispatch(
        &self,
        control: &HandoffControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &mut ExecutionLifecycleRecord<'_>,
    ) -> Result<HandoffDispatchEvidence, HandoffError> {
        self.require_control(control, lifecycle_authority, lifecycle_record)?;
        if lifecycle_record.revision() != control.lifecycle_revision() {
            return Err(HandoffError::new(
                HandoffErrorCode::StaleRevision,
                HandoffRequirement::CurrentRevision,
            ));
        }
        if lifecycle_record.state() != ExecutionLifecycleState::HandoffRequired {
            return Err(HandoffError::new(
                HandoffErrorCode::HandoffStateRequired,
                HandoffRequirement::HandoffRequiredState,
            ));
        }

        let transition = lifecycle_authority
            .transition(
                lifecycle_record,
                control.lifecycle_revision(),
                ExecutionLifecycleState::Cancelled,
            )
            .map_err(|_| {
                HandoffError::new(
                    HandoffErrorCode::LifecycleTransitionRejected,
                    HandoffRequirement::CanonicalTerminalTransition,
                )
            })?;

        Ok(HandoffDispatchEvidence::new(
            Arc::clone(&self.identity),
            control,
            transition,
        ))
    }

    pub fn verifies_dispatch(
        &self,
        evidence: &HandoffDispatchEvidence,
        control: &HandoffControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
            && Arc::ptr_eq(control.identity(), evidence.control())
            && Arc::ptr_eq(control.execution(), evidence.execution())
            && self.verifies_control(control, lifecycle_authority, lifecycle_record)
            && lifecycle_authority
                .verifies_transition(evidence.lifecycle_transition(), lifecycle_record)
            && evidence.target() == control.target()
            && evidence.reason() == control.reason()
            && evidence.lifecycle_revision() == lifecycle_record.revision()
            && evidence.terminal_state() == ExecutionLifecycleState::Cancelled
            && lifecycle_record.state() == ExecutionLifecycleState::Cancelled
    }

    fn require_control(
        &self,
        control: &HandoffControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> Result<(), HandoffError> {
        if !Arc::ptr_eq(&self.identity, control.authority()) {
            return Err(HandoffError::new(
                HandoffErrorCode::ForeignAuthority,
                HandoffRequirement::HandoffAuthority,
            ));
        }
        if !self.verifies_control(control, lifecycle_authority, lifecycle_record) {
            return Err(HandoffError::new(
                HandoffErrorCode::LifecycleRecordNotVerified,
                HandoffRequirement::LifecycleRecord,
            ));
        }
        Ok(())
    }
}

impl Default for HandoffEnforcementAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for HandoffEnforcementAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandoffEnforcementAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
