#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HandoffTarget {
    Operator,
    Orchestrator,
    SpecializedAgent,
    ArchitectureReview,
    SecurityReview,
    QaReview,
    BlockedState,
}

pub const HANDOFF_TARGETS: [HandoffTarget; 7] = [
    HandoffTarget::Operator,
    HandoffTarget::Orchestrator,
    HandoffTarget::SpecializedAgent,
    HandoffTarget::ArchitectureReview,
    HandoffTarget::SecurityReview,
    HandoffTarget::QaReview,
    HandoffTarget::BlockedState,
];

impl HandoffTarget {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Orchestrator => "orchestrator",
            Self::SpecializedAgent => "specialized_agent",
            Self::ArchitectureReview => "architecture_review",
            Self::SecurityReview => "security_review",
            Self::QaReview => "qa_review",
            Self::BlockedState => "blocked_state",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HandoffReason {
    OutOfScope,
    PermissionMissing,
    RiskTooHigh,
    InputAmbiguous,
    OutputRequiresReview,
    SandboxUnavailable,
    TimeoutOrCancelled,
    PolicyConflict,
}

pub const HANDOFF_REASONS: [HandoffReason; 8] = [
    HandoffReason::OutOfScope,
    HandoffReason::PermissionMissing,
    HandoffReason::RiskTooHigh,
    HandoffReason::InputAmbiguous,
    HandoffReason::OutputRequiresReview,
    HandoffReason::SandboxUnavailable,
    HandoffReason::TimeoutOrCancelled,
    HandoffReason::PolicyConflict,
];

impl HandoffReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutOfScope => "out_of_scope",
            Self::PermissionMissing => "permission_missing",
            Self::RiskTooHigh => "risk_too_high",
            Self::InputAmbiguous => "input_ambiguous",
            Self::OutputRequiresReview => "output_requires_review",
            Self::SandboxUnavailable => "sandbox_unavailable",
            Self::TimeoutOrCancelled => "timeout_or_cancelled",
            Self::PolicyConflict => "policy_conflict",
        }
    }

    pub const fn operator_summary(self) -> HandoffOperatorSummary {
        match self {
            Self::OutOfScope => HandoffOperatorSummary::OutsideDeclaredScope,
            Self::PermissionMissing => HandoffOperatorSummary::RequiredPermissionMissing,
            Self::RiskTooHigh => HandoffOperatorSummary::IndependentRiskReviewRequired,
            Self::InputAmbiguous => HandoffOperatorSummary::InputClarificationRequired,
            Self::OutputRequiresReview => HandoffOperatorSummary::IndependentOutputReviewRequired,
            Self::SandboxUnavailable => HandoffOperatorSummary::SandboxUnavailable,
            Self::TimeoutOrCancelled => HandoffOperatorSummary::TimedOutOrCancelled,
            Self::PolicyConflict => HandoffOperatorSummary::PolicyConflictRequiresReview,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HandoffOperatorSummary {
    OutsideDeclaredScope,
    RequiredPermissionMissing,
    IndependentRiskReviewRequired,
    InputClarificationRequired,
    IndependentOutputReviewRequired,
    SandboxUnavailable,
    TimedOutOrCancelled,
    PolicyConflictRequiresReview,
}

impl HandoffOperatorSummary {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutsideDeclaredScope => "task_is_outside_declared_scope",
            Self::RequiredPermissionMissing => "required_permission_is_missing",
            Self::IndependentRiskReviewRequired => "risk_requires_independent_review",
            Self::InputClarificationRequired => "input_requires_clarification",
            Self::IndependentOutputReviewRequired => "output_requires_independent_review",
            Self::SandboxUnavailable => "sandbox_is_unavailable",
            Self::TimedOutOrCancelled => "operation_timed_out_or_was_cancelled",
            Self::PolicyConflictRequiresReview => "policy_conflict_requires_review",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HandoffBlockedAction {
    ContinueLocalExecution,
}

impl HandoffBlockedAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ContinueLocalExecution => "continue_local_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct HandoffRequest {
    target: HandoffTarget,
    reason: HandoffReason,
}

impl HandoffRequest {
    pub const fn new(target: HandoffTarget, reason: HandoffReason) -> Self {
        Self { target, reason }
    }

    pub const fn target(self) -> HandoffTarget {
        self.target
    }

    pub const fn reason(self) -> HandoffReason {
        self.reason
    }

    pub const fn operator_summary(self) -> HandoffOperatorSummary {
        self.reason.operator_summary()
    }

    pub const fn blocked_action(self) -> HandoffBlockedAction {
        HandoffBlockedAction::ContinueLocalExecution
    }
}

pub(crate) const fn target_supports_reason(target: HandoffTarget, reason: HandoffReason) -> bool {
    match reason {
        HandoffReason::RiskTooHigh => matches!(
            target,
            HandoffTarget::Operator
                | HandoffTarget::ArchitectureReview
                | HandoffTarget::SecurityReview
                | HandoffTarget::BlockedState
        ),
        HandoffReason::OutputRequiresReview => matches!(
            target,
            HandoffTarget::Operator
                | HandoffTarget::ArchitectureReview
                | HandoffTarget::SecurityReview
                | HandoffTarget::QaReview
                | HandoffTarget::BlockedState
        ),
        HandoffReason::OutOfScope
        | HandoffReason::PermissionMissing
        | HandoffReason::InputAmbiguous
        | HandoffReason::SandboxUnavailable
        | HandoffReason::TimeoutOrCancelled
        | HandoffReason::PolicyConflict => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elevated_risk_requires_an_explicit_review_or_blocked_target() {
        assert!(target_supports_reason(
            HandoffTarget::SecurityReview,
            HandoffReason::RiskTooHigh
        ));
        assert!(!target_supports_reason(
            HandoffTarget::Orchestrator,
            HandoffReason::RiskTooHigh
        ));
        assert!(!target_supports_reason(
            HandoffTarget::SpecializedAgent,
            HandoffReason::RiskTooHigh
        ));
    }

    #[test]
    fn output_review_requires_an_operator_or_review_target() {
        assert!(target_supports_reason(
            HandoffTarget::QaReview,
            HandoffReason::OutputRequiresReview
        ));
        assert!(!target_supports_reason(
            HandoffTarget::Orchestrator,
            HandoffReason::OutputRequiresReview
        ));
    }
}
