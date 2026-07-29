use std::{fmt, sync::Arc};

use crate::{PackageReviewSubject, SandboxEnforcementAuthority, SandboxEnforcementEvidence};

use super::transition::{classify_transition, TransitionDisposition};
use super::{
    ExecutionLifecycleError, ExecutionLifecycleErrorCode, ExecutionLifecycleRecord,
    ExecutionLifecycleRequirement, ExecutionLifecycleState, ExecutionLifecycleTransitionEvidence,
    LifecycleAuthorityIdentity, MAX_EXECUTION_LIFECYCLE_TRANSITIONS,
};

/// Operator-local capability that owns in-memory lifecycle records.
///
/// The authority validates state progression but cannot authorize or start
/// agent execution.
pub struct ExecutionLifecycleAuthority {
    identity: Arc<LifecycleAuthorityIdentity>,
}

impl ExecutionLifecycleAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(LifecycleAuthorityIdentity),
        }
    }

    pub fn queue<'a>(
        &self,
        sandbox_authority: &SandboxEnforcementAuthority,
        sandbox_evidence: &SandboxEnforcementEvidence<'a>,
        subject: PackageReviewSubject<'a>,
    ) -> Result<ExecutionLifecycleRecord<'a>, ExecutionLifecycleError> {
        if !sandbox_authority.verifies(sandbox_evidence, subject) {
            return Err(ExecutionLifecycleError::new(
                ExecutionLifecycleErrorCode::SandboxEvidenceNotVerified,
                ExecutionLifecycleRequirement::SandboxEnforcementEvidence,
            ));
        }

        Ok(ExecutionLifecycleRecord::new(
            Arc::clone(&self.identity),
            Arc::clone(sandbox_evidence.authority()),
            Arc::clone(sandbox_evidence.identity()),
            subject,
        ))
    }

    pub fn transition(
        &self,
        record: &mut ExecutionLifecycleRecord<'_>,
        expected_revision: u8,
        target: ExecutionLifecycleState,
    ) -> Result<ExecutionLifecycleTransitionEvidence, ExecutionLifecycleError> {
        self.transition_inner(record, expected_revision, target, false)
    }

    pub(crate) fn transition_authorized_to_running(
        &self,
        record: &mut ExecutionLifecycleRecord<'_>,
        expected_revision: u8,
    ) -> Result<ExecutionLifecycleTransitionEvidence, ExecutionLifecycleError> {
        self.transition_inner(
            record,
            expected_revision,
            ExecutionLifecycleState::Running,
            true,
        )
    }

    fn transition_inner(
        &self,
        record: &mut ExecutionLifecycleRecord<'_>,
        expected_revision: u8,
        target: ExecutionLifecycleState,
        authorized_running: bool,
    ) -> Result<ExecutionLifecycleTransitionEvidence, ExecutionLifecycleError> {
        if !Arc::ptr_eq(&self.identity, record.authority()) {
            return Err(ExecutionLifecycleError::new(
                ExecutionLifecycleErrorCode::ForeignLifecycleAuthority,
                ExecutionLifecycleRequirement::LifecycleAuthority,
            ));
        }
        if expected_revision != record.revision() {
            return Err(ExecutionLifecycleError::new(
                ExecutionLifecycleErrorCode::StaleRevision,
                ExecutionLifecycleRequirement::CurrentRevision,
            ));
        }
        if record.is_terminal() {
            return Err(ExecutionLifecycleError::new(
                ExecutionLifecycleErrorCode::TerminalState,
                ExecutionLifecycleRequirement::NonTerminalState,
            ));
        }

        match classify_transition(record.state(), target) {
            TransitionDisposition::Rejected => {
                return Err(ExecutionLifecycleError::new(
                    ExecutionLifecycleErrorCode::InvalidTransition,
                    ExecutionLifecycleRequirement::CanonicalTransition,
                ));
            }
            TransitionDisposition::ExecutionAuthorizationRequired => {
                if !authorized_running {
                    return Err(ExecutionLifecycleError::new(
                        ExecutionLifecycleErrorCode::ExecutionAuthorizationRequired,
                        ExecutionLifecycleRequirement::ExecutionAuthorizationEvidence,
                    ));
                }
            }
            TransitionDisposition::Recordable => {}
        }

        if record.revision() >= MAX_EXECUTION_LIFECYCLE_TRANSITIONS {
            return Err(ExecutionLifecycleError::new(
                ExecutionLifecycleErrorCode::TransitionLimitExceeded,
                ExecutionLifecycleRequirement::TransitionBound,
            ));
        }

        let from = record.state();
        record.record_transition(target);
        Ok(ExecutionLifecycleTransitionEvidence::new(record, from))
    }

    pub fn verifies_record(
        &self,
        record: &ExecutionLifecycleRecord<'_>,
        sandbox_authority: &SandboxEnforcementAuthority,
        sandbox_evidence: &SandboxEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, record.authority())
            && sandbox_authority.verifies(sandbox_evidence, subject)
            && Arc::ptr_eq(record.sandbox_authority(), sandbox_evidence.authority())
            && Arc::ptr_eq(record.sandbox_evidence(), sandbox_evidence.identity())
            && record.subject().same_as(subject)
    }

    pub fn verifies_transition(
        &self,
        evidence: &ExecutionLifecycleTransitionEvidence,
        record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
            && Arc::ptr_eq(&self.identity, record.authority())
            && Arc::ptr_eq(evidence.execution(), record.execution())
            && evidence.revision() <= record.revision()
    }

    pub(crate) fn verifies_record_identity(&self, record: &ExecutionLifecycleRecord<'_>) -> bool {
        Arc::ptr_eq(&self.identity, record.authority())
    }
}

impl Default for ExecutionLifecycleAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for ExecutionLifecycleAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionLifecycleAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}
