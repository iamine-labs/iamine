use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ExecutionLifecycleRequirement {
    SandboxEnforcementEvidence,
    LifecycleAuthority,
    CurrentRevision,
    NonTerminalState,
    CanonicalTransition,
    ExecutionAuthorizationEvidence,
    TransitionBound,
}

impl ExecutionLifecycleRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SandboxEnforcementEvidence => "sandbox_enforcement_evidence",
            Self::LifecycleAuthority => "lifecycle_authority",
            Self::CurrentRevision => "current_revision",
            Self::NonTerminalState => "non_terminal_state",
            Self::CanonicalTransition => "canonical_transition",
            Self::ExecutionAuthorizationEvidence => "execution_authorization_evidence",
            Self::TransitionBound => "transition_bound",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionLifecycleErrorCode {
    SandboxEvidenceNotVerified,
    ForeignLifecycleAuthority,
    StaleRevision,
    TerminalState,
    InvalidTransition,
    ExecutionAuthorizationRequired,
    TransitionLimitExceeded,
}

impl ExecutionLifecycleErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SandboxEvidenceNotVerified => "sandbox_evidence_not_verified",
            Self::ForeignLifecycleAuthority => "foreign_lifecycle_authority",
            Self::StaleRevision => "stale_revision",
            Self::TerminalState => "terminal_state",
            Self::InvalidTransition => "invalid_transition",
            Self::ExecutionAuthorizationRequired => "execution_authorization_required",
            Self::TransitionLimitExceeded => "transition_limit_exceeded",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::SandboxEvidenceNotVerified => "sandbox enforcement evidence was not verified",
            Self::ForeignLifecycleAuthority => "lifecycle record belongs to another authority",
            Self::StaleRevision => "lifecycle revision is not current",
            Self::TerminalState => "terminal lifecycle state cannot transition",
            Self::InvalidTransition => "lifecycle transition is not canonical",
            Self::ExecutionAuthorizationRequired => {
                "running requires independent execution authorization"
            }
            Self::TransitionLimitExceeded => "lifecycle transition bound was reached",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLifecycleError {
    code: ExecutionLifecycleErrorCode,
    requirement: ExecutionLifecycleRequirement,
}

impl ExecutionLifecycleError {
    pub(crate) const fn new(
        code: ExecutionLifecycleErrorCode,
        requirement: ExecutionLifecycleRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> ExecutionLifecycleErrorCode {
        self.code
    }

    pub const fn requirement(self) -> ExecutionLifecycleRequirement {
        self.requirement
    }
}

impl fmt::Display for ExecutionLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for ExecutionLifecycleError {}
