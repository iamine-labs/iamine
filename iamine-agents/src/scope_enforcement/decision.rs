#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopeDecision {
    Allow,
    Clarify,
    Refuse,
    HandoffToOrchestrator,
}

impl ScopeDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Clarify => "clarify",
            Self::Refuse => "refuse",
            Self::HandoffToOrchestrator => "handoff_to_orchestrator",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopeReasonCode {
    InScope,
    InvalidRequest,
    AmbiguousTask,
    DangerousTask,
    CrossDomainTask,
    PermissionEscalation,
    PromptInjection,
    RoleConfusion,
    PackageMismatch,
    UnsupportedTaskType,
    OutsideDeclaredScope,
    BlockedAction,
    ForbiddenInput,
    UnsupportedOperation,
    UnsupportedInput,
}

impl ScopeReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InScope => "in_scope",
            Self::InvalidRequest => "invalid_request",
            Self::AmbiguousTask => "ambiguous_task",
            Self::DangerousTask => "dangerous_task",
            Self::CrossDomainTask => "cross_domain_task",
            Self::PermissionEscalation => "permission_escalation",
            Self::PromptInjection => "prompt_injection",
            Self::RoleConfusion => "role_confusion",
            Self::PackageMismatch => "package_mismatch",
            Self::UnsupportedTaskType => "unsupported_task_type",
            Self::OutsideDeclaredScope => "outside_declared_scope",
            Self::BlockedAction => "blocked_action",
            Self::ForbiddenInput => "forbidden_input",
            Self::UnsupportedOperation => "unsupported_operation",
            Self::UnsupportedInput => "unsupported_input",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct ScopeEvaluation {
    decision: ScopeDecision,
    reason: ScopeReasonCode,
}

impl ScopeEvaluation {
    pub(crate) const fn new(decision: ScopeDecision, reason: ScopeReasonCode) -> Self {
        Self { decision, reason }
    }

    pub const fn decision(&self) -> ScopeDecision {
        self.decision
    }

    pub const fn reason(&self) -> ScopeReasonCode {
        self.reason
    }

    pub const fn allowed(&self) -> bool {
        matches!(self.decision, ScopeDecision::Allow)
    }
}
