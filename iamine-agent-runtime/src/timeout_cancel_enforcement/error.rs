use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeoutCancelErrorCode {
    LifecycleRecordNotVerified,
    ForeignAuthority,
    StaleRevision,
    TerminalState,
    TimeoutClassStateMismatch,
    DeadlineOverflow,
    TimeoutPolicyExceedsSandbox,
    CleanupOwnershipMismatch,
    CleanupTriggerMissing,
    TimeoutHandleNotVerified,
    TimeoutNotExpired,
    CancellationHandleNotVerified,
    CancellationAlreadyRequested,
    CancellationRequestNotVerified,
    TerminalEvidenceNotVerified,
    LifecycleTransitionRejected,
}

impl TimeoutCancelErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LifecycleRecordNotVerified => "lifecycle_record_not_verified",
            Self::ForeignAuthority => "foreign_authority",
            Self::StaleRevision => "stale_revision",
            Self::TerminalState => "terminal_state",
            Self::TimeoutClassStateMismatch => "timeout_class_state_mismatch",
            Self::DeadlineOverflow => "deadline_overflow",
            Self::TimeoutPolicyExceedsSandbox => "timeout_policy_exceeds_sandbox",
            Self::CleanupOwnershipMismatch => "cleanup_ownership_mismatch",
            Self::CleanupTriggerMissing => "cleanup_trigger_missing",
            Self::TimeoutHandleNotVerified => "timeout_handle_not_verified",
            Self::TimeoutNotExpired => "timeout_not_expired",
            Self::CancellationHandleNotVerified => "cancellation_handle_not_verified",
            Self::CancellationAlreadyRequested => "cancellation_already_requested",
            Self::CancellationRequestNotVerified => "cancellation_request_not_verified",
            Self::TerminalEvidenceNotVerified => "terminal_evidence_not_verified",
            Self::LifecycleTransitionRejected => "lifecycle_transition_rejected",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::LifecycleRecordNotVerified => "lifecycle record is not verified",
            Self::ForeignAuthority => "timeout/cancel authority does not own this control",
            Self::StaleRevision => "lifecycle revision is stale",
            Self::TerminalState => "operation requires a non-terminal lifecycle state",
            Self::TimeoutClassStateMismatch => {
                "timeout class does not match the current lifecycle state"
            }
            Self::DeadlineOverflow => "timeout deadline exceeds the monotonic clock range",
            Self::TimeoutPolicyExceedsSandbox => {
                "execution timeout exceeds the sandbox wall-time limit"
            }
            Self::CleanupOwnershipMismatch => "sandbox cleanup owner is not verified",
            Self::CleanupTriggerMissing => "sandbox cleanup trigger is missing",
            Self::TimeoutHandleNotVerified => "timeout handle is not verified",
            Self::TimeoutNotExpired => "timeout deadline has not expired",
            Self::CancellationHandleNotVerified => "cancellation handle is not verified",
            Self::CancellationAlreadyRequested => "cancellation was already requested",
            Self::CancellationRequestNotVerified => "cancellation request is not verified",
            Self::TerminalEvidenceNotVerified => "terminal evidence is not verified",
            Self::LifecycleTransitionRejected => "lifecycle rejected the terminal transition",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeoutCancelRequirement {
    LifecycleRecord,
    TimeoutCancelAuthority,
    CurrentRevision,
    NonTerminalState,
    StateTimeoutClass,
    BoundedDeadline,
    SandboxWallTime,
    SandboxCleanupOwner,
    SandboxCleanupTrigger,
    TimeoutHandle,
    ExpiredDeadline,
    CancellationHandle,
    SingleCancellationRequest,
    CancellationRequest,
    TerminalEvidence,
    CanonicalTerminalTransition,
}

impl TimeoutCancelRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LifecycleRecord => "lifecycle_record",
            Self::TimeoutCancelAuthority => "timeout_cancel_authority",
            Self::CurrentRevision => "current_revision",
            Self::NonTerminalState => "non_terminal_state",
            Self::StateTimeoutClass => "state_timeout_class",
            Self::BoundedDeadline => "bounded_deadline",
            Self::SandboxWallTime => "sandbox_wall_time",
            Self::SandboxCleanupOwner => "sandbox_cleanup_owner",
            Self::SandboxCleanupTrigger => "sandbox_cleanup_trigger",
            Self::TimeoutHandle => "timeout_handle",
            Self::ExpiredDeadline => "expired_deadline",
            Self::CancellationHandle => "cancellation_handle",
            Self::SingleCancellationRequest => "single_cancellation_request",
            Self::CancellationRequest => "cancellation_request",
            Self::TerminalEvidence => "terminal_evidence",
            Self::CanonicalTerminalTransition => "canonical_terminal_transition",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutCancelError {
    code: TimeoutCancelErrorCode,
    requirement: TimeoutCancelRequirement,
}

impl TimeoutCancelError {
    pub(crate) const fn new(
        code: TimeoutCancelErrorCode,
        requirement: TimeoutCancelRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> TimeoutCancelErrorCode {
        self.code
    }

    pub const fn requirement(self) -> TimeoutCancelRequirement {
        self.requirement
    }
}

impl fmt::Display for TimeoutCancelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for TimeoutCancelError {}
