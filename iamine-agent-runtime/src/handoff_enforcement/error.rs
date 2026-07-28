use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HandoffRequirement {
    HandoffAuthority,
    LifecycleRecord,
    HandoffTransitionEvidence,
    HandoffRequiredState,
    CurrentRevision,
    CompatibleTargetReason,
    CanonicalTerminalTransition,
}

impl HandoffRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HandoffAuthority => "handoff_authority",
            Self::LifecycleRecord => "lifecycle_record",
            Self::HandoffTransitionEvidence => "handoff_transition_evidence",
            Self::HandoffRequiredState => "handoff_required_state",
            Self::CurrentRevision => "current_revision",
            Self::CompatibleTargetReason => "compatible_target_reason",
            Self::CanonicalTerminalTransition => "canonical_terminal_transition",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandoffErrorCode {
    ForeignAuthority,
    LifecycleRecordNotVerified,
    HandoffTransitionNotVerified,
    HandoffStateRequired,
    StaleRevision,
    TargetReasonMismatch,
    LifecycleTransitionRejected,
}

impl HandoffErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ForeignAuthority => "foreign_authority",
            Self::LifecycleRecordNotVerified => "lifecycle_record_not_verified",
            Self::HandoffTransitionNotVerified => "handoff_transition_not_verified",
            Self::HandoffStateRequired => "handoff_state_required",
            Self::StaleRevision => "stale_revision",
            Self::TargetReasonMismatch => "target_reason_mismatch",
            Self::LifecycleTransitionRejected => "lifecycle_transition_rejected",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ForeignAuthority => "handoff control belongs to another authority",
            Self::LifecycleRecordNotVerified => "lifecycle record was not verified",
            Self::HandoffTransitionNotVerified => "handoff transition evidence was not verified",
            Self::HandoffStateRequired => "lifecycle is not waiting for handoff",
            Self::StaleRevision => "handoff control does not match the current lifecycle revision",
            Self::TargetReasonMismatch => "handoff target is not safe for the declared reason",
            Self::LifecycleTransitionRejected => {
                "lifecycle rejected the canonical handoff terminal transition"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandoffError {
    code: HandoffErrorCode,
    requirement: HandoffRequirement,
}

impl HandoffError {
    pub(crate) const fn new(code: HandoffErrorCode, requirement: HandoffRequirement) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> HandoffErrorCode {
        self.code
    }

    pub const fn requirement(self) -> HandoffRequirement {
        self.requirement
    }
}

impl fmt::Display for HandoffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for HandoffError {}
