#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RuntimeOwner {
    PackageReferenceResolver,
    PackageReviewEvidence,
    RuntimeCompatibility,
    InputOutputEnforcement,
    SandboxEnforcement,
    ExecutionLifecycle,
    TimeoutCancelEnforcement,
    HandoffEnforcement,
    OutOfScopeResponseEnforcement,
    RoutingCandidateSelector,
    AuditEventEnforcement,
    ExecutionAuthorization,
    PackageLoadEvidenceIntegration,
    PackageLoader,
    RuntimeExecutor,
}

impl RuntimeOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PackageReferenceResolver => "package_reference_resolver",
            Self::PackageReviewEvidence => "package_review_evidence",
            Self::RuntimeCompatibility => "runtime_compatibility",
            Self::InputOutputEnforcement => "input_output_enforcement",
            Self::SandboxEnforcement => "sandbox_enforcement",
            Self::ExecutionLifecycle => "execution_lifecycle",
            Self::TimeoutCancelEnforcement => "timeout_cancel_enforcement",
            Self::HandoffEnforcement => "handoff_enforcement",
            Self::OutOfScopeResponseEnforcement => "out_of_scope_response_enforcement",
            Self::RoutingCandidateSelector => "routing_candidate_selector",
            Self::AuditEventEnforcement => "audit_event_enforcement",
            Self::ExecutionAuthorization => "execution_authorization",
            Self::PackageLoadEvidenceIntegration => "package_load_evidence_integration",
            Self::PackageLoader => "package_loader",
            Self::RuntimeExecutor => "runtime_executor",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeOwnerState {
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeOwnerStatus {
    owner: RuntimeOwner,
    state: RuntimeOwnerState,
}

impl RuntimeOwnerStatus {
    const fn unavailable(owner: RuntimeOwner) -> Self {
        Self {
            owner,
            state: RuntimeOwnerState::Unavailable,
        }
    }

    pub const fn owner(self) -> RuntimeOwner {
        self.owner
    }

    pub const fn state(self) -> RuntimeOwnerState {
        self.state
    }
}

const RUNTIME_OWNER_STATUSES: [RuntimeOwnerStatus; 15] = [
    RuntimeOwnerStatus::unavailable(RuntimeOwner::PackageReferenceResolver),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::PackageReviewEvidence),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::RuntimeCompatibility),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::InputOutputEnforcement),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::SandboxEnforcement),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::ExecutionLifecycle),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::TimeoutCancelEnforcement),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::HandoffEnforcement),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::OutOfScopeResponseEnforcement),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::RoutingCandidateSelector),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::AuditEventEnforcement),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::ExecutionAuthorization),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::PackageLoadEvidenceIntegration),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::PackageLoader),
    RuntimeOwnerStatus::unavailable(RuntimeOwner::RuntimeExecutor),
];

pub(crate) const fn runtime_owner_statuses() -> &'static [RuntimeOwnerStatus] {
    &RUNTIME_OWNER_STATUSES
}
