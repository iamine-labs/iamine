use std::{
    fmt,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use crate::{
    ExecutionLifecycleAuthority, ExecutionLifecycleRecord, ExecutionLifecycleState,
    PackageReviewSubject, SandboxCleanupOwner, SandboxCleanupTrigger, SandboxEnforcementAuthority,
    SandboxEnforcementEvidence,
};

use super::{
    AgentTimeoutClass, AgentTimeoutHandle, CancellationHandle, CancellationRequestEvidence,
    CancellationSource, CleanupTimeoutEvidence, TimeoutCancelAuthorityIdentity,
    TimeoutCancelControl, TimeoutCancelError, TimeoutCancelErrorCode, TimeoutCancelEvent,
    TimeoutCancelPolicy, TimeoutCancelRequirement, TimeoutCancelTerminalEvidence,
};

/// Operator-local capability for bounded timeout and cancellation enforcement.
///
/// It may request canonical lifecycle terminal transitions, but it cannot
/// authorize execution or perform sandbox cleanup.
pub struct TimeoutCancelAuthority {
    identity: Arc<TimeoutCancelAuthorityIdentity>,
}

impl TimeoutCancelAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(TimeoutCancelAuthorityIdentity),
        }
    }

    pub fn establish(
        &self,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        sandbox_authority: &SandboxEnforcementAuthority,
        sandbox_evidence: &SandboxEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
        policy: TimeoutCancelPolicy,
    ) -> Result<TimeoutCancelControl, TimeoutCancelError> {
        if !lifecycle_authority.verifies_record(
            lifecycle_record,
            sandbox_authority,
            sandbox_evidence,
            subject,
        ) {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::LifecycleRecordNotVerified,
                TimeoutCancelRequirement::LifecycleRecord,
            ));
        }
        if lifecycle_record.is_terminal() {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TerminalState,
                TimeoutCancelRequirement::NonTerminalState,
            ));
        }
        if policy.timeout_ms(AgentTimeoutClass::Execution)
            > sandbox_evidence.resource_limits().max_wall_time_ms()
        {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutPolicyExceedsSandbox,
                TimeoutCancelRequirement::SandboxWallTime,
            ));
        }
        if sandbox_evidence.cleanup_owner() != SandboxCleanupOwner::RuntimeSandboxAdapter {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::CleanupOwnershipMismatch,
                TimeoutCancelRequirement::SandboxCleanupOwner,
            ));
        }
        for trigger in [
            SandboxCleanupTrigger::Cancellation,
            SandboxCleanupTrigger::Timeout,
        ] {
            if !sandbox_evidence.cleanup_triggers().contains(&trigger) {
                return Err(TimeoutCancelError::new(
                    TimeoutCancelErrorCode::CleanupTriggerMissing,
                    TimeoutCancelRequirement::SandboxCleanupTrigger,
                ));
            }
        }

        Ok(TimeoutCancelControl::new(
            Arc::clone(&self.identity),
            Arc::clone(lifecycle_record.authority()),
            Arc::clone(lifecycle_record.execution()),
            policy,
        ))
    }

    pub fn verifies_control(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, control.authority())
            && Arc::ptr_eq(control.lifecycle_authority(), lifecycle_record.authority())
            && Arc::ptr_eq(control.execution(), lifecycle_record.execution())
            && lifecycle_authority.verifies_record_identity(lifecycle_record)
    }

    pub fn arm_timeout(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        timeout_class: AgentTimeoutClass,
    ) -> Result<AgentTimeoutHandle, TimeoutCancelError> {
        self.arm_timeout_at(
            control,
            lifecycle_authority,
            lifecycle_record,
            timeout_class,
            Instant::now(),
        )
    }

    pub fn arm_timeout_at(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        timeout_class: AgentTimeoutClass,
        started_at: Instant,
    ) -> Result<AgentTimeoutHandle, TimeoutCancelError> {
        self.require_control(control, lifecycle_authority, lifecycle_record)?;
        if timeout_class == AgentTimeoutClass::Cleanup {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutClassStateMismatch,
                TimeoutCancelRequirement::StateTimeoutClass,
            ));
        }
        if timeout_class_for_state(lifecycle_record.state()) != timeout_class {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutClassStateMismatch,
                TimeoutCancelRequirement::StateTimeoutClass,
            ));
        }
        AgentTimeoutHandle::new(
            control,
            timeout_class,
            lifecycle_record.revision(),
            started_at,
        )
        .ok_or_else(|| {
            TimeoutCancelError::new(
                TimeoutCancelErrorCode::DeadlineOverflow,
                TimeoutCancelRequirement::BoundedDeadline,
            )
        })
    }

    pub fn arm_cleanup_timeout(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        terminal_evidence: &TimeoutCancelTerminalEvidence,
    ) -> Result<AgentTimeoutHandle, TimeoutCancelError> {
        self.arm_cleanup_timeout_at(
            control,
            lifecycle_authority,
            lifecycle_record,
            terminal_evidence,
            Instant::now(),
        )
    }

    pub fn arm_cleanup_timeout_at(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        terminal_evidence: &TimeoutCancelTerminalEvidence,
        started_at: Instant,
    ) -> Result<AgentTimeoutHandle, TimeoutCancelError> {
        self.require_control(control, lifecycle_authority, lifecycle_record)?;
        if !self.verifies_terminal(
            terminal_evidence,
            control,
            lifecycle_authority,
            lifecycle_record,
        ) || terminal_evidence.cleanup_result() != super::SandboxCleanupResult::Pending
        {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TerminalEvidenceNotVerified,
                TimeoutCancelRequirement::TerminalEvidence,
            ));
        }
        AgentTimeoutHandle::new(
            control,
            AgentTimeoutClass::Cleanup,
            lifecycle_record.revision(),
            started_at,
        )
        .ok_or_else(|| {
            TimeoutCancelError::new(
                TimeoutCancelErrorCode::DeadlineOverflow,
                TimeoutCancelRequirement::BoundedDeadline,
            )
        })
    }

    pub fn enforce_timeout(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &mut ExecutionLifecycleRecord<'_>,
        timeout: &AgentTimeoutHandle,
    ) -> Result<TimeoutCancelTerminalEvidence, TimeoutCancelError> {
        self.enforce_timeout_at(
            control,
            lifecycle_authority,
            lifecycle_record,
            timeout,
            Instant::now(),
        )
    }

    pub fn enforce_timeout_at(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &mut ExecutionLifecycleRecord<'_>,
        timeout: &AgentTimeoutHandle,
        observed_at: Instant,
    ) -> Result<TimeoutCancelTerminalEvidence, TimeoutCancelError> {
        self.require_timeout(control, lifecycle_authority, lifecycle_record, timeout)?;
        if timeout.timeout_class() == AgentTimeoutClass::Cleanup {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutClassStateMismatch,
                TimeoutCancelRequirement::StateTimeoutClass,
            ));
        }
        if !timeout.expired_at(observed_at) {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutNotExpired,
                TimeoutCancelRequirement::ExpiredDeadline,
            ));
        }

        let target = terminal_target(
            lifecycle_record.state(),
            TimeoutCancelEvent::Timeout(timeout.timeout_class()),
        )?;
        let transition = lifecycle_authority
            .transition(lifecycle_record, timeout.lifecycle_revision(), target)
            .map_err(|_| {
                TimeoutCancelError::new(
                    TimeoutCancelErrorCode::LifecycleTransitionRejected,
                    TimeoutCancelRequirement::CanonicalTerminalTransition,
                )
            })?;
        Ok(TimeoutCancelTerminalEvidence::new(
            Arc::clone(&self.identity),
            Arc::clone(control.identity()),
            Arc::clone(control.execution()),
            TimeoutCancelEvent::Timeout(timeout.timeout_class()),
            transition,
            SandboxCleanupTrigger::Timeout,
        ))
    }

    pub fn record_cleanup_timeout_at(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        timeout: &AgentTimeoutHandle,
        observed_at: Instant,
    ) -> Result<CleanupTimeoutEvidence, TimeoutCancelError> {
        self.require_timeout(control, lifecycle_authority, lifecycle_record, timeout)?;
        if timeout.timeout_class() != AgentTimeoutClass::Cleanup {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutClassStateMismatch,
                TimeoutCancelRequirement::StateTimeoutClass,
            ));
        }
        if !timeout.expired_at(observed_at) {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutNotExpired,
                TimeoutCancelRequirement::ExpiredDeadline,
            ));
        }
        Ok(CleanupTimeoutEvidence::new(
            Arc::clone(&self.identity),
            Arc::clone(control.identity()),
            Arc::clone(control.execution()),
            lifecycle_record.state(),
            lifecycle_record.revision(),
        ))
    }

    pub fn request_cancellation(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        handle: &CancellationHandle,
        expected_revision: u8,
        source: CancellationSource,
    ) -> Result<CancellationRequestEvidence, TimeoutCancelError> {
        self.require_control(control, lifecycle_authority, lifecycle_record)?;
        self.require_cancellation_handle(control, handle)?;
        if lifecycle_record.is_terminal() {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TerminalState,
                TimeoutCancelRequirement::NonTerminalState,
            ));
        }
        if expected_revision != lifecycle_record.revision() {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::StaleRevision,
                TimeoutCancelRequirement::CurrentRevision,
            ));
        }
        handle
            .state()
            .compare_exchange(0, source.encode(), Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                TimeoutCancelError::new(
                    TimeoutCancelErrorCode::CancellationAlreadyRequested,
                    TimeoutCancelRequirement::SingleCancellationRequest,
                )
            })?;

        Ok(CancellationRequestEvidence::new(
            Arc::clone(&self.identity),
            Arc::clone(control.identity()),
            Arc::clone(control.execution()),
            source,
            expected_revision,
        ))
    }

    pub fn enforce_cancellation(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &mut ExecutionLifecycleRecord<'_>,
        handle: &CancellationHandle,
        request: &CancellationRequestEvidence,
    ) -> Result<TimeoutCancelTerminalEvidence, TimeoutCancelError> {
        self.require_control(control, lifecycle_authority, lifecycle_record)?;
        self.require_cancellation_handle(control, handle)?;
        if !self.verifies_cancellation_request(request, control, lifecycle_record) {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::CancellationRequestNotVerified,
                TimeoutCancelRequirement::CancellationRequest,
            ));
        }

        let source = request.source();
        let requested = source.encode();
        if handle.state().load(Ordering::Acquire) != requested {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::CancellationRequestNotVerified,
                TimeoutCancelRequirement::CancellationRequest,
            ));
        }
        let target = terminal_target(
            lifecycle_record.state(),
            TimeoutCancelEvent::Cancellation(source),
        )?;
        let transition = lifecycle_authority
            .transition(lifecycle_record, request.lifecycle_revision(), target)
            .map_err(|_| {
                TimeoutCancelError::new(
                    TimeoutCancelErrorCode::LifecycleTransitionRejected,
                    TimeoutCancelRequirement::CanonicalTerminalTransition,
                )
            })?;
        handle.state().store(
            requested | CancellationSource::ENFORCED_MASK,
            Ordering::Release,
        );

        Ok(TimeoutCancelTerminalEvidence::new(
            Arc::clone(&self.identity),
            Arc::clone(control.identity()),
            Arc::clone(control.execution()),
            TimeoutCancelEvent::Cancellation(source),
            transition,
            SandboxCleanupTrigger::Cancellation,
        ))
    }

    pub fn verifies_terminal(
        &self,
        evidence: &TimeoutCancelTerminalEvidence,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
            && Arc::ptr_eq(control.identity(), evidence.control())
            && Arc::ptr_eq(control.execution(), evidence.execution())
            && self.verifies_control(control, lifecycle_authority, lifecycle_record)
            && lifecycle_authority
                .verifies_transition(evidence.lifecycle_transition(), lifecycle_record)
            && evidence.terminal_state() == lifecycle_record.state()
            && lifecycle_record.is_terminal()
    }

    pub fn verifies_cleanup_timeout(
        &self,
        evidence: &CleanupTimeoutEvidence,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority())
            && Arc::ptr_eq(control.identity(), evidence.control())
            && Arc::ptr_eq(control.execution(), evidence.execution())
            && self.verifies_control(control, lifecycle_authority, lifecycle_record)
            && evidence.terminal_state() == lifecycle_record.state()
            && evidence.lifecycle_revision() == lifecycle_record.revision()
            && lifecycle_record.is_terminal()
    }

    fn verifies_cancellation_request(
        &self,
        request: &CancellationRequestEvidence,
        control: &TimeoutCancelControl,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, request.authority())
            && Arc::ptr_eq(control.identity(), request.control())
            && Arc::ptr_eq(control.execution(), request.execution())
            && Arc::ptr_eq(control.execution(), lifecycle_record.execution())
            && request.lifecycle_revision() == lifecycle_record.revision()
    }

    fn require_control(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
    ) -> Result<(), TimeoutCancelError> {
        if self.verifies_control(control, lifecycle_authority, lifecycle_record) {
            return Ok(());
        }
        Err(TimeoutCancelError::new(
            TimeoutCancelErrorCode::ForeignAuthority,
            TimeoutCancelRequirement::TimeoutCancelAuthority,
        ))
    }

    fn require_timeout(
        &self,
        control: &TimeoutCancelControl,
        lifecycle_authority: &ExecutionLifecycleAuthority,
        lifecycle_record: &ExecutionLifecycleRecord<'_>,
        timeout: &AgentTimeoutHandle,
    ) -> Result<(), TimeoutCancelError> {
        self.require_control(control, lifecycle_authority, lifecycle_record)?;
        if !Arc::ptr_eq(&self.identity, timeout.authority())
            || !Arc::ptr_eq(control.identity(), timeout.control())
            || !Arc::ptr_eq(control.execution(), timeout.execution())
        {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutHandleNotVerified,
                TimeoutCancelRequirement::TimeoutHandle,
            ));
        }
        if timeout.lifecycle_revision() != lifecycle_record.revision() {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::StaleRevision,
                TimeoutCancelRequirement::CurrentRevision,
            ));
        }
        if timeout_class_for_state(lifecycle_record.state()) != timeout.timeout_class() {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TimeoutClassStateMismatch,
                TimeoutCancelRequirement::StateTimeoutClass,
            ));
        }
        Ok(())
    }

    fn require_cancellation_handle(
        &self,
        control: &TimeoutCancelControl,
        handle: &CancellationHandle,
    ) -> Result<(), TimeoutCancelError> {
        if Arc::ptr_eq(&self.identity, handle.authority())
            && Arc::ptr_eq(control.identity(), handle.control())
            && Arc::ptr_eq(control.execution(), handle.execution())
            && Arc::ptr_eq(control.cancellation(), handle.state())
        {
            return Ok(());
        }
        Err(TimeoutCancelError::new(
            TimeoutCancelErrorCode::CancellationHandleNotVerified,
            TimeoutCancelRequirement::CancellationHandle,
        ))
    }
}

impl Default for TimeoutCancelAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for TimeoutCancelAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TimeoutCancelAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}

fn timeout_class_for_state(state: ExecutionLifecycleState) -> AgentTimeoutClass {
    match state {
        ExecutionLifecycleState::Queued => AgentTimeoutClass::SandboxStart,
        ExecutionLifecycleState::PermissionPending => AgentTimeoutClass::PermissionWait,
        ExecutionLifecycleState::ScopeCheck => AgentTimeoutClass::ScopeCheck,
        ExecutionLifecycleState::HandoffRequired => AgentTimeoutClass::Handoff,
        ExecutionLifecycleState::Running => AgentTimeoutClass::Execution,
        ExecutionLifecycleState::Completed
        | ExecutionLifecycleState::Failed
        | ExecutionLifecycleState::Cancelled
        | ExecutionLifecycleState::Timeout
        | ExecutionLifecycleState::Blocked => AgentTimeoutClass::Cleanup,
    }
}

fn terminal_target(
    state: ExecutionLifecycleState,
    event: TimeoutCancelEvent,
) -> Result<ExecutionLifecycleState, TimeoutCancelError> {
    let target = match state {
        ExecutionLifecycleState::Queued
        | ExecutionLifecycleState::PermissionPending
        | ExecutionLifecycleState::ScopeCheck => ExecutionLifecycleState::Blocked,
        ExecutionLifecycleState::HandoffRequired => ExecutionLifecycleState::Cancelled,
        ExecutionLifecycleState::Running => match event {
            TimeoutCancelEvent::Timeout(_) => ExecutionLifecycleState::Timeout,
            TimeoutCancelEvent::Cancellation(_) => ExecutionLifecycleState::Cancelled,
        },
        ExecutionLifecycleState::Completed
        | ExecutionLifecycleState::Failed
        | ExecutionLifecycleState::Cancelled
        | ExecutionLifecycleState::Timeout
        | ExecutionLifecycleState::Blocked => {
            return Err(TimeoutCancelError::new(
                TimeoutCancelErrorCode::TerminalState,
                TimeoutCancelRequirement::NonTerminalState,
            ));
        }
    };
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_lifecycle_state_has_one_timeout_class() {
        let expected = [
            (
                ExecutionLifecycleState::Queued,
                AgentTimeoutClass::SandboxStart,
            ),
            (
                ExecutionLifecycleState::PermissionPending,
                AgentTimeoutClass::PermissionWait,
            ),
            (
                ExecutionLifecycleState::ScopeCheck,
                AgentTimeoutClass::ScopeCheck,
            ),
            (
                ExecutionLifecycleState::HandoffRequired,
                AgentTimeoutClass::Handoff,
            ),
            (
                ExecutionLifecycleState::Running,
                AgentTimeoutClass::Execution,
            ),
            (
                ExecutionLifecycleState::Completed,
                AgentTimeoutClass::Cleanup,
            ),
            (ExecutionLifecycleState::Failed, AgentTimeoutClass::Cleanup),
            (
                ExecutionLifecycleState::Cancelled,
                AgentTimeoutClass::Cleanup,
            ),
            (ExecutionLifecycleState::Timeout, AgentTimeoutClass::Cleanup),
            (ExecutionLifecycleState::Blocked, AgentTimeoutClass::Cleanup),
        ];
        for (state, timeout_class) in expected {
            assert_eq!(timeout_class_for_state(state), timeout_class);
        }
    }

    #[test]
    fn running_outcomes_preserve_timeout_and_cancellation_distinction() {
        assert_eq!(
            terminal_target(
                ExecutionLifecycleState::Running,
                TimeoutCancelEvent::Timeout(AgentTimeoutClass::Execution)
            ),
            Ok(ExecutionLifecycleState::Timeout)
        );
        assert_eq!(
            terminal_target(
                ExecutionLifecycleState::Running,
                TimeoutCancelEvent::Cancellation(CancellationSource::Shutdown)
            ),
            Ok(ExecutionLifecycleState::Cancelled)
        );
    }
}
