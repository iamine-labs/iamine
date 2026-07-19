#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PermissionDecision {
    Allow,
    RequireConfirmation,
    Refuse,
    HandoffToOrchestrator,
}

impl PermissionDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::RequireConfirmation => "require_confirmation",
            Self::Refuse => "refuse",
            Self::HandoffToOrchestrator => "handoff_to_orchestrator",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PermissionReasonCode {
    Permitted,
    ScopeGateNotPassed,
    InvalidRequest,
    PackageMismatch,
    BlockedAction,
    ForbiddenCategory,
    UndeclaredAction,
    UndeclaredCategory,
    ConfirmationRequired,
}

impl PermissionReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Permitted => "permitted",
            Self::ScopeGateNotPassed => "scope_gate_not_passed",
            Self::InvalidRequest => "invalid_request",
            Self::PackageMismatch => "package_mismatch",
            Self::BlockedAction => "blocked_action",
            Self::ForbiddenCategory => "forbidden_category",
            Self::UndeclaredAction => "undeclared_action",
            Self::UndeclaredCategory => "undeclared_category",
            Self::ConfirmationRequired => "confirmation_required",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct PermissionEvaluation {
    decision: PermissionDecision,
    reason: PermissionReasonCode,
}

impl PermissionEvaluation {
    pub(crate) const fn new(decision: PermissionDecision, reason: PermissionReasonCode) -> Self {
        Self { decision, reason }
    }

    pub const fn decision(&self) -> PermissionDecision {
        self.decision
    }

    pub const fn reason(&self) -> PermissionReasonCode {
        self.reason
    }

    pub const fn allowed(&self) -> bool {
        matches!(self.decision, PermissionDecision::Allow)
    }

    pub const fn confirmation_required(&self) -> bool {
        matches!(self.decision, PermissionDecision::RequireConfirmation)
    }
}
