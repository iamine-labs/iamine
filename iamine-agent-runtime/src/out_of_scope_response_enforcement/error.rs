use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutOfScopeResponseRequirement {
    NonAllowDecision,
    DeterministicDecisionReason,
    HandoffDispatchEvidence,
}

impl OutOfScopeResponseRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NonAllowDecision => "non_allow_decision",
            Self::DeterministicDecisionReason => "deterministic_decision_reason",
            Self::HandoffDispatchEvidence => "handoff_dispatch_evidence",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutOfScopeResponseErrorCode {
    ResponseNotRequired,
    UnsupportedDecisionReason,
    HandoffDispatchRequired,
}

impl OutOfScopeResponseErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResponseNotRequired => "response_not_required",
            Self::UnsupportedDecisionReason => "unsupported_decision_reason",
            Self::HandoffDispatchRequired => "handoff_dispatch_required",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ResponseNotRequired => {
                "an allowed decision does not require an out-of-scope response"
            }
            Self::UnsupportedDecisionReason => {
                "the decision and reason do not have a deterministic response mapping"
            }
            Self::HandoffDispatchRequired => {
                "a handoff response requires recorded handoff dispatch evidence"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfScopeResponseError {
    code: OutOfScopeResponseErrorCode,
    requirement: OutOfScopeResponseRequirement,
}

impl OutOfScopeResponseError {
    pub(crate) const fn new(
        code: OutOfScopeResponseErrorCode,
        requirement: OutOfScopeResponseRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> OutOfScopeResponseErrorCode {
        self.code
    }

    pub const fn requirement(self) -> OutOfScopeResponseRequirement {
        self.requirement
    }
}

impl fmt::Display for OutOfScopeResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for OutOfScopeResponseError {}
