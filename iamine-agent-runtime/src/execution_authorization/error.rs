use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionAuthorizationRequirement {
    PackageIdentity,
    PackageReviewEvidence,
    RuntimeCompatibilityEvidence,
    InputOutputEnforcementEvidence,
    SandboxEnforcementEvidence,
    LifecycleRecord,
    LifecycleState,
    TimeoutCancelControl,
    ScopeEvaluation,
    PermissionEvaluation,
    RoutingCandidateSelectionEvidence,
    AuditScopeEvidence,
    AuditPermissionEvidence,
    AuditLifecycleEvidence,
}

impl ExecutionAuthorizationRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PackageIdentity => "package_identity",
            Self::PackageReviewEvidence => "package_review_evidence",
            Self::RuntimeCompatibilityEvidence => "runtime_compatibility_evidence",
            Self::InputOutputEnforcementEvidence => "input_output_enforcement_evidence",
            Self::SandboxEnforcementEvidence => "sandbox_enforcement_evidence",
            Self::LifecycleRecord => "lifecycle_record",
            Self::LifecycleState => "lifecycle_state",
            Self::TimeoutCancelControl => "timeout_cancel_control",
            Self::ScopeEvaluation => "scope_evaluation",
            Self::PermissionEvaluation => "permission_evaluation",
            Self::RoutingCandidateSelectionEvidence => "routing_candidate_selection_evidence",
            Self::AuditScopeEvidence => "audit_scope_evidence",
            Self::AuditPermissionEvidence => "audit_permission_evidence",
            Self::AuditLifecycleEvidence => "audit_lifecycle_evidence",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionAuthorizationErrorCode {
    PackageIdentityMismatch,
    PackageReviewNotVerified,
    RuntimeCompatibilityNotVerified,
    InputOutputEnforcementNotVerified,
    SandboxEnforcementNotVerified,
    LifecycleRecordNotVerified,
    LifecycleNotReady,
    TimeoutCancelControlNotVerified,
    CancellationAlreadyRequested,
    ScopeNotAllowed,
    PermissionNotAllowed,
    RoutingSelectionNotVerified,
    CandidateNotSelected,
    RoutingSubjectNotVerified,
    ScopeAuditNotVerified,
    PermissionAuditNotVerified,
    LifecycleAuditNotVerified,
}

impl ExecutionAuthorizationErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PackageIdentityMismatch => "package_identity_mismatch",
            Self::PackageReviewNotVerified => "package_review_not_verified",
            Self::RuntimeCompatibilityNotVerified => "runtime_compatibility_not_verified",
            Self::InputOutputEnforcementNotVerified => "input_output_enforcement_not_verified",
            Self::SandboxEnforcementNotVerified => "sandbox_enforcement_not_verified",
            Self::LifecycleRecordNotVerified => "lifecycle_record_not_verified",
            Self::LifecycleNotReady => "lifecycle_not_ready",
            Self::TimeoutCancelControlNotVerified => "timeout_cancel_control_not_verified",
            Self::CancellationAlreadyRequested => "cancellation_already_requested",
            Self::ScopeNotAllowed => "scope_not_allowed",
            Self::PermissionNotAllowed => "permission_not_allowed",
            Self::RoutingSelectionNotVerified => "routing_selection_not_verified",
            Self::CandidateNotSelected => "candidate_not_selected",
            Self::RoutingSubjectNotVerified => "routing_subject_not_verified",
            Self::ScopeAuditNotVerified => "scope_audit_not_verified",
            Self::PermissionAuditNotVerified => "permission_audit_not_verified",
            Self::LifecycleAuditNotVerified => "lifecycle_audit_not_verified",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::PackageIdentityMismatch => {
                "authorization policy inputs do not target the reviewed package"
            }
            Self::PackageReviewNotVerified => {
                "package review evidence was not verified for authorization"
            }
            Self::RuntimeCompatibilityNotVerified => {
                "runtime compatibility evidence was not verified for authorization"
            }
            Self::InputOutputEnforcementNotVerified => {
                "input/output enforcement evidence was not verified for authorization"
            }
            Self::SandboxEnforcementNotVerified => {
                "sandbox enforcement evidence was not verified for authorization"
            }
            Self::LifecycleRecordNotVerified => {
                "execution lifecycle record was not verified for authorization"
            }
            Self::LifecycleNotReady => "execution lifecycle is not at the authorization boundary",
            Self::TimeoutCancelControlNotVerified => {
                "timeout/cancel control was not verified for authorization"
            }
            Self::CancellationAlreadyRequested => {
                "execution cancellation was requested before authorization"
            }
            Self::ScopeNotAllowed => "scope evaluation did not allow execution",
            Self::PermissionNotAllowed => "permission evaluation did not allow execution",
            Self::RoutingSelectionNotVerified => {
                "routing selection evidence was not verified for authorization"
            }
            Self::CandidateNotSelected => {
                "routing evidence does not contain one selected candidate"
            }
            Self::RoutingSubjectNotVerified => {
                "selected routing candidate is not bound to the authorized sandbox"
            }
            Self::ScopeAuditNotVerified => {
                "scope audit evidence does not match the authorization evaluation"
            }
            Self::PermissionAuditNotVerified => {
                "permission audit evidence does not match the authorization evaluation"
            }
            Self::LifecycleAuditNotVerified => {
                "lifecycle audit evidence does not match the authorization record"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionAuthorizationError {
    code: ExecutionAuthorizationErrorCode,
    requirement: ExecutionAuthorizationRequirement,
}

impl ExecutionAuthorizationError {
    pub(crate) const fn new(
        code: ExecutionAuthorizationErrorCode,
        requirement: ExecutionAuthorizationRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> ExecutionAuthorizationErrorCode {
        self.code
    }

    pub const fn requirement(self) -> ExecutionAuthorizationRequirement {
        self.requirement
    }
}

impl fmt::Display for ExecutionAuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for ExecutionAuthorizationError {}
