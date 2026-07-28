use iamine_agents::{
    PermissionDecision, PermissionEvaluation, PermissionReasonCode, ScopeDecision, ScopeEvaluation,
    ScopeReasonCode,
};

use crate::{HandoffDispatchEvidence, HandoffReason, HandoffTarget};

use super::{OutOfScopeResponseError, OutOfScopeResponseErrorCode, OutOfScopeResponseRequirement};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutOfScopeResponseClass {
    Refuse,
    Clarify,
    Handoff,
    Blocked,
}

pub const OUT_OF_SCOPE_RESPONSE_CLASSES: [OutOfScopeResponseClass; 4] = [
    OutOfScopeResponseClass::Refuse,
    OutOfScopeResponseClass::Clarify,
    OutOfScopeResponseClass::Handoff,
    OutOfScopeResponseClass::Blocked,
];

impl OutOfScopeResponseClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Refuse => "refuse",
            Self::Clarify => "clarify",
            Self::Handoff => "handoff",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutOfScopeResponseReason {
    ScopeMismatch,
    PermissionMissing,
    InputUnsafe,
    InputAmbiguous,
    RiskTooHigh,
    ResourceUnavailable,
    SandboxUnavailable,
    PolicyConflict,
}

pub const OUT_OF_SCOPE_RESPONSE_REASONS: [OutOfScopeResponseReason; 8] = [
    OutOfScopeResponseReason::ScopeMismatch,
    OutOfScopeResponseReason::PermissionMissing,
    OutOfScopeResponseReason::InputUnsafe,
    OutOfScopeResponseReason::InputAmbiguous,
    OutOfScopeResponseReason::RiskTooHigh,
    OutOfScopeResponseReason::ResourceUnavailable,
    OutOfScopeResponseReason::SandboxUnavailable,
    OutOfScopeResponseReason::PolicyConflict,
];

impl OutOfScopeResponseReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopeMismatch => "scope_mismatch",
            Self::PermissionMissing => "permission_missing",
            Self::InputUnsafe => "input_unsafe",
            Self::InputAmbiguous => "input_ambiguous",
            Self::RiskTooHigh => "risk_too_high",
            Self::ResourceUnavailable => "resource_unavailable",
            Self::SandboxUnavailable => "sandbox_unavailable",
            Self::PolicyConflict => "policy_conflict",
        }
    }

    pub const fn operator_summary(self) -> OutOfScopeOperatorSummary {
        match self {
            Self::ScopeMismatch => OutOfScopeOperatorSummary::OutsideDeclaredScope,
            Self::PermissionMissing => OutOfScopeOperatorSummary::RequiredPermissionMissing,
            Self::InputUnsafe => OutOfScopeOperatorSummary::UnsafeInputRejected,
            Self::InputAmbiguous => OutOfScopeOperatorSummary::InputClarificationRequired,
            Self::RiskTooHigh => OutOfScopeOperatorSummary::IndependentRiskReviewRequired,
            Self::ResourceUnavailable => OutOfScopeOperatorSummary::RequiredResourceUnavailable,
            Self::SandboxUnavailable => OutOfScopeOperatorSummary::SandboxUnavailable,
            Self::PolicyConflict => OutOfScopeOperatorSummary::PolicyConflictRequiresReview,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutOfScopeOperatorSummary {
    OutsideDeclaredScope,
    RequiredPermissionMissing,
    UnsafeInputRejected,
    InputClarificationRequired,
    IndependentRiskReviewRequired,
    RequiredResourceUnavailable,
    SandboxUnavailable,
    PolicyConflictRequiresReview,
}

impl OutOfScopeOperatorSummary {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutsideDeclaredScope => "task_is_outside_declared_scope",
            Self::RequiredPermissionMissing => "required_permission_is_missing",
            Self::UnsafeInputRejected => "unsafe_input_was_rejected",
            Self::InputClarificationRequired => "input_requires_clarification",
            Self::IndependentRiskReviewRequired => "risk_requires_independent_review",
            Self::RequiredResourceUnavailable => "required_resource_is_unavailable",
            Self::SandboxUnavailable => "sandbox_is_unavailable",
            Self::PolicyConflictRequiresReview => "policy_conflict_requires_review",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutOfScopeBlockedAction {
    ContinueLocalExecution,
}

impl OutOfScopeBlockedAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ContinueLocalExecution => "continue_local_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutOfScopeResponseSource {
    Scope,
    Permission,
    Handoff,
}

impl OutOfScopeResponseSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scope => "scope",
            Self::Permission => "permission",
            Self::Handoff => "handoff",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutOfScopeSourceReason {
    Scope(ScopeReasonCode),
    Permission(PermissionReasonCode),
    Handoff(HandoffReason),
}

impl OutOfScopeSourceReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scope(reason) => reason.as_str(),
            Self::Permission(reason) => reason.as_str(),
            Self::Handoff(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutOfScopeResponsePlan {
    source: OutOfScopeResponseSource,
    source_reason: OutOfScopeSourceReason,
    response_class: OutOfScopeResponseClass,
    response_reason: OutOfScopeResponseReason,
    handoff_target: Option<HandoffTarget>,
}

impl OutOfScopeResponsePlan {
    const fn new(
        source: OutOfScopeResponseSource,
        source_reason: OutOfScopeSourceReason,
        response_class: OutOfScopeResponseClass,
        response_reason: OutOfScopeResponseReason,
        handoff_target: Option<HandoffTarget>,
    ) -> Self {
        Self {
            source,
            source_reason,
            response_class,
            response_reason,
            handoff_target,
        }
    }

    pub const fn source(self) -> OutOfScopeResponseSource {
        self.source
    }

    pub const fn source_reason(self) -> OutOfScopeSourceReason {
        self.source_reason
    }

    pub const fn response_class(self) -> OutOfScopeResponseClass {
        self.response_class
    }

    pub const fn response_reason(self) -> OutOfScopeResponseReason {
        self.response_reason
    }

    pub const fn handoff_target(self) -> Option<HandoffTarget> {
        self.handoff_target
    }
}

pub(crate) fn plan_scope_response(
    evaluation: &ScopeEvaluation,
) -> Result<OutOfScopeResponsePlan, OutOfScopeResponseError> {
    let source_reason = OutOfScopeSourceReason::Scope(evaluation.reason());
    match evaluation.decision() {
        ScopeDecision::Allow => Err(response_not_required()),
        ScopeDecision::HandoffToOrchestrator => Err(handoff_dispatch_required()),
        ScopeDecision::Clarify if evaluation.reason() == ScopeReasonCode::AmbiguousTask => {
            Ok(OutOfScopeResponsePlan::new(
                OutOfScopeResponseSource::Scope,
                source_reason,
                OutOfScopeResponseClass::Clarify,
                OutOfScopeResponseReason::InputAmbiguous,
                None,
            ))
        }
        ScopeDecision::Refuse => {
            let response_reason = match evaluation.reason() {
                ScopeReasonCode::DangerousTask => OutOfScopeResponseReason::RiskTooHigh,
                ScopeReasonCode::PermissionEscalation
                | ScopeReasonCode::PromptInjection
                | ScopeReasonCode::RoleConfusion
                | ScopeReasonCode::BlockedAction
                | ScopeReasonCode::ForbiddenInput => OutOfScopeResponseReason::InputUnsafe,
                _ => return Err(unsupported_decision_reason()),
            };
            Ok(OutOfScopeResponsePlan::new(
                OutOfScopeResponseSource::Scope,
                source_reason,
                OutOfScopeResponseClass::Refuse,
                response_reason,
                None,
            ))
        }
        _ => Err(unsupported_decision_reason()),
    }
}

pub(crate) fn plan_permission_response(
    evaluation: &PermissionEvaluation,
) -> Result<OutOfScopeResponsePlan, OutOfScopeResponseError> {
    let source_reason = OutOfScopeSourceReason::Permission(evaluation.reason());
    match evaluation.decision() {
        PermissionDecision::Allow => Err(response_not_required()),
        PermissionDecision::HandoffToOrchestrator => Err(handoff_dispatch_required()),
        PermissionDecision::RequireConfirmation
            if evaluation.reason() == PermissionReasonCode::ConfirmationRequired =>
        {
            Ok(OutOfScopeResponsePlan::new(
                OutOfScopeResponseSource::Permission,
                source_reason,
                OutOfScopeResponseClass::Blocked,
                OutOfScopeResponseReason::PermissionMissing,
                None,
            ))
        }
        PermissionDecision::Refuse => {
            let response_reason = match evaluation.reason() {
                PermissionReasonCode::UndeclaredAction
                | PermissionReasonCode::UndeclaredCategory => {
                    OutOfScopeResponseReason::PermissionMissing
                }
                PermissionReasonCode::InvalidRequest
                | PermissionReasonCode::PackageMismatch
                | PermissionReasonCode::BlockedAction
                | PermissionReasonCode::ForbiddenCategory => {
                    OutOfScopeResponseReason::PolicyConflict
                }
                _ => return Err(unsupported_decision_reason()),
            };
            Ok(OutOfScopeResponsePlan::new(
                OutOfScopeResponseSource::Permission,
                source_reason,
                OutOfScopeResponseClass::Refuse,
                response_reason,
                None,
            ))
        }
        _ => Err(unsupported_decision_reason()),
    }
}

pub(crate) const fn plan_handoff_response(
    evidence: &HandoffDispatchEvidence,
) -> OutOfScopeResponsePlan {
    let response_reason = match evidence.reason() {
        HandoffReason::OutOfScope => OutOfScopeResponseReason::ScopeMismatch,
        HandoffReason::PermissionMissing => OutOfScopeResponseReason::PermissionMissing,
        HandoffReason::RiskTooHigh => OutOfScopeResponseReason::RiskTooHigh,
        HandoffReason::InputAmbiguous => OutOfScopeResponseReason::InputAmbiguous,
        HandoffReason::OutputRequiresReview => OutOfScopeResponseReason::PolicyConflict,
        HandoffReason::SandboxUnavailable => OutOfScopeResponseReason::SandboxUnavailable,
        HandoffReason::TimeoutOrCancelled => OutOfScopeResponseReason::ResourceUnavailable,
        HandoffReason::PolicyConflict => OutOfScopeResponseReason::PolicyConflict,
    };
    OutOfScopeResponsePlan::new(
        OutOfScopeResponseSource::Handoff,
        OutOfScopeSourceReason::Handoff(evidence.reason()),
        OutOfScopeResponseClass::Handoff,
        response_reason,
        Some(evidence.target()),
    )
}

const fn response_not_required() -> OutOfScopeResponseError {
    OutOfScopeResponseError::new(
        OutOfScopeResponseErrorCode::ResponseNotRequired,
        OutOfScopeResponseRequirement::NonAllowDecision,
    )
}

const fn unsupported_decision_reason() -> OutOfScopeResponseError {
    OutOfScopeResponseError::new(
        OutOfScopeResponseErrorCode::UnsupportedDecisionReason,
        OutOfScopeResponseRequirement::DeterministicDecisionReason,
    )
}

const fn handoff_dispatch_required() -> OutOfScopeResponseError {
    OutOfScopeResponseError::new(
        OutOfScopeResponseErrorCode::HandoffDispatchRequired,
        OutOfScopeResponseRequirement::HandoffDispatchEvidence,
    )
}
