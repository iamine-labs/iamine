use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RuntimeExecutorRequirement {
    ExecutorAuthority,
    ExecutionPermit,
    LoadedPackage,
    ExecutionAuthorization,
    OfficialProgram,
    LifecycleRecord,
    SandboxEvidence,
    SandboxRestrictions,
    TimeoutCancelControl,
    EnforcedInput,
    RuntimeProgram,
    EnforcedOutput,
    AuditEvidence,
}

impl RuntimeExecutorRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutorAuthority => "executor_authority",
            Self::ExecutionPermit => "execution_permit",
            Self::LoadedPackage => "loaded_package",
            Self::ExecutionAuthorization => "execution_authorization",
            Self::OfficialProgram => "official_program",
            Self::LifecycleRecord => "lifecycle_record",
            Self::SandboxEvidence => "sandbox_evidence",
            Self::SandboxRestrictions => "sandbox_restrictions",
            Self::TimeoutCancelControl => "timeout_cancel_control",
            Self::EnforcedInput => "enforced_input",
            Self::RuntimeProgram => "runtime_program",
            Self::EnforcedOutput => "enforced_output",
            Self::AuditEvidence => "audit_evidence",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RuntimeExecutorErrorCode {
    LoadedPackageNotVerified,
    OfficialProgramNotVerified,
    ForeignExecutorAuthority,
    StaleExecutionPermit,
    LifecycleRecordNotVerified,
    SandboxEvidenceNotVerified,
    SandboxRestrictionsUnsupported,
    TimeoutControlNotVerified,
    CancellationPending,
    EnforcedInputNotVerified,
    LifecycleTransitionRejected,
    TimeoutArmFailed,
    RuntimeProgramFailed,
    RuntimeTimedOut,
    RuntimeOutputRejected,
    AuditProjectionFailed,
}

impl RuntimeExecutorErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoadedPackageNotVerified => "loaded_package_not_verified",
            Self::OfficialProgramNotVerified => "official_program_not_verified",
            Self::ForeignExecutorAuthority => "foreign_executor_authority",
            Self::StaleExecutionPermit => "stale_execution_permit",
            Self::LifecycleRecordNotVerified => "lifecycle_record_not_verified",
            Self::SandboxEvidenceNotVerified => "sandbox_evidence_not_verified",
            Self::SandboxRestrictionsUnsupported => "sandbox_restrictions_unsupported",
            Self::TimeoutControlNotVerified => "timeout_control_not_verified",
            Self::CancellationPending => "cancellation_pending",
            Self::EnforcedInputNotVerified => "enforced_input_not_verified",
            Self::LifecycleTransitionRejected => "lifecycle_transition_rejected",
            Self::TimeoutArmFailed => "timeout_arm_failed",
            Self::RuntimeProgramFailed => "runtime_program_failed",
            Self::RuntimeTimedOut => "runtime_timed_out",
            Self::RuntimeOutputRejected => "runtime_output_rejected",
            Self::AuditProjectionFailed => "audit_projection_failed",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::LoadedPackageNotVerified => "loaded package evidence was not verified",
            Self::OfficialProgramNotVerified => "official program registration was not verified",
            Self::ForeignExecutorAuthority => "execution permit belongs to another authority",
            Self::StaleExecutionPermit => "execution permit is stale or does not match",
            Self::LifecycleRecordNotVerified => "lifecycle record was not verified",
            Self::SandboxEvidenceNotVerified => "sandbox evidence was not verified",
            Self::SandboxRestrictionsUnsupported => {
                "sandbox restrictions are not supported by this adapter"
            }
            Self::TimeoutControlNotVerified => "timeout and cancellation control was not verified",
            Self::CancellationPending => "cancellation must be enforced before execution",
            Self::EnforcedInputNotVerified => "enforced input was not verified",
            Self::LifecycleTransitionRejected => "runtime lifecycle transition was rejected",
            Self::TimeoutArmFailed => "execution timeout could not be armed",
            Self::RuntimeProgramFailed => "official runtime program failed",
            Self::RuntimeTimedOut => "official runtime program exceeded its deadline",
            Self::RuntimeOutputRejected => "runtime output enforcement rejected the output",
            Self::AuditProjectionFailed => "runtime audit projection failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeExecutorError {
    code: RuntimeExecutorErrorCode,
    requirement: RuntimeExecutorRequirement,
}

impl RuntimeExecutorError {
    pub(crate) const fn new(
        code: RuntimeExecutorErrorCode,
        requirement: RuntimeExecutorRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> RuntimeExecutorErrorCode {
        self.code
    }

    pub const fn requirement(self) -> RuntimeExecutorRequirement {
        self.requirement
    }
}

impl fmt::Display for RuntimeExecutorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for RuntimeExecutorError {}
