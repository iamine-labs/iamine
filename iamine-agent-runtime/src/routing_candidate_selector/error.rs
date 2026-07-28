use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoutingCandidateSelectorRequirement {
    TaskType,
    ResourceRequirements,
    CandidateCount,
    CandidateIdentity,
    ScopeEvaluation,
    PermissionEvaluation,
    RuntimeCompatibilityEvidence,
    SandboxEnforcementEvidence,
    DeterministicSelection,
}

impl RoutingCandidateSelectorRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskType => "task_type",
            Self::ResourceRequirements => "resource_requirements",
            Self::CandidateCount => "candidate_count",
            Self::CandidateIdentity => "candidate_identity",
            Self::ScopeEvaluation => "scope_evaluation",
            Self::PermissionEvaluation => "permission_evaluation",
            Self::RuntimeCompatibilityEvidence => "runtime_compatibility_evidence",
            Self::SandboxEnforcementEvidence => "sandbox_enforcement_evidence",
            Self::DeterministicSelection => "deterministic_selection",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoutingCandidateSelectorErrorCode {
    EmptyTaskType,
    InvalidTaskType,
    ZeroLogicalCoreRequirement,
    ZeroMemoryRequirement,
    ZeroStorageRequirement,
    TooManyCandidates,
    EmptyCandidateId,
    InvalidCandidateId,
    InvalidCandidateTaskType,
    DuplicateCandidateId,
    RuntimeCompatibilityNotVerified,
    SandboxEnforcementNotVerified,
}

impl RoutingCandidateSelectorErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EmptyTaskType => "empty_task_type",
            Self::InvalidTaskType => "invalid_task_type",
            Self::ZeroLogicalCoreRequirement => "zero_logical_core_requirement",
            Self::ZeroMemoryRequirement => "zero_memory_requirement",
            Self::ZeroStorageRequirement => "zero_storage_requirement",
            Self::TooManyCandidates => "too_many_candidates",
            Self::EmptyCandidateId => "empty_candidate_id",
            Self::InvalidCandidateId => "invalid_candidate_id",
            Self::InvalidCandidateTaskType => "invalid_candidate_task_type",
            Self::DuplicateCandidateId => "duplicate_candidate_id",
            Self::RuntimeCompatibilityNotVerified => "runtime_compatibility_not_verified",
            Self::SandboxEnforcementNotVerified => "sandbox_enforcement_not_verified",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::EmptyTaskType => "routing task type must not be empty",
            Self::InvalidTaskType => "routing task type is not a bounded identifier",
            Self::ZeroLogicalCoreRequirement => "routing logical core requirement must be non-zero",
            Self::ZeroMemoryRequirement => "routing memory requirement must be non-zero",
            Self::ZeroStorageRequirement => "routing storage requirement must be non-zero",
            Self::TooManyCandidates => "routing candidate count exceeds the bounded maximum",
            Self::EmptyCandidateId => "routing candidate id must not be empty",
            Self::InvalidCandidateId => "routing candidate id is not a bounded identifier",
            Self::InvalidCandidateTaskType => {
                "routing candidate task type is not a bounded identifier"
            }
            Self::DuplicateCandidateId => "routing candidate ids must be unique",
            Self::RuntimeCompatibilityNotVerified => {
                "routing candidate runtime compatibility evidence was not verified"
            }
            Self::SandboxEnforcementNotVerified => {
                "routing candidate sandbox enforcement evidence was not verified"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingCandidateSelectorError {
    code: RoutingCandidateSelectorErrorCode,
    requirement: RoutingCandidateSelectorRequirement,
}

impl RoutingCandidateSelectorError {
    pub(crate) const fn new(
        code: RoutingCandidateSelectorErrorCode,
        requirement: RoutingCandidateSelectorRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> RoutingCandidateSelectorErrorCode {
        self.code
    }

    pub const fn requirement(self) -> RoutingCandidateSelectorRequirement {
        self.requirement
    }
}

impl fmt::Display for RoutingCandidateSelectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for RoutingCandidateSelectorError {}
