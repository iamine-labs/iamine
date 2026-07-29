use std::{fmt, sync::Arc, time::Instant};

use crate::{AuditEventEnforcementAuthority, ExecutionLifecycleState};

use super::adapter::ActiveOfficialRustSandbox;
use super::result::RuntimeExecutionResultParts;
use super::{
    RuntimeExecutionContext, RuntimeExecutionPermit, RuntimeExecutionPreparation,
    RuntimeExecutionRequest, RuntimeExecutionResult, RuntimeExecutionVerification,
    RuntimeExecutorError, RuntimeExecutorErrorCode, RuntimeExecutorRequirement,
};

#[derive(Debug)]
pub(crate) struct RuntimeExecutorAuthorityIdentity;

/// Operator-local authority for one-shot execution of registered official Rust programs.
///
/// Package bytes are never interpreted as code. The executor consumes an exact
/// loader and authorization chain, then coordinates existing runtime owners.
pub struct RuntimeExecutorAuthority {
    identity: Arc<RuntimeExecutorAuthorityIdentity>,
}

impl RuntimeExecutorAuthority {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(RuntimeExecutorAuthorityIdentity),
        }
    }

    pub fn prepare<'subject>(
        &self,
        preparation: RuntimeExecutionPreparation<'_, '_, 'subject>,
    ) -> Result<RuntimeExecutionPermit<'subject>, RuntimeExecutorError> {
        let (loader_authority, evidence_authority, evidence) =
            preparation.loader.ok_or_else(|| {
                RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::LoadedPackageNotVerified,
                    RuntimeExecutorRequirement::LoadedPackage,
                )
            })?;
        let (authorization_authority, authorization) =
            preparation.authorization.ok_or_else(|| {
                RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::LoadedPackageNotVerified,
                    RuntimeExecutorRequirement::ExecutionAuthorization,
                )
            })?;
        let (program_registry, program) = preparation.program.ok_or_else(|| {
            RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::OfficialProgramNotVerified,
                RuntimeExecutorRequirement::OfficialProgram,
            )
        })?;
        if !loader_authority.verifies(
            preparation.loaded,
            evidence_authority,
            evidence,
            authorization_authority,
            authorization,
            preparation.authorization_request,
        ) {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::LoadedPackageNotVerified,
                RuntimeExecutorRequirement::LoadedPackage,
            ));
        }
        if !program_registry.verifies(program, preparation.loaded.subject()) {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::OfficialProgramNotVerified,
                RuntimeExecutorRequirement::OfficialProgram,
            ));
        }
        if preparation.loaded.lifecycle_revision() != authorization.lifecycle_revision() {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::StaleExecutionPermit,
                RuntimeExecutorRequirement::ExecutionAuthorization,
            ));
        }

        Ok(RuntimeExecutionPermit::new(
            Arc::clone(&self.identity),
            preparation.loaded,
            authorization,
            program,
        ))
    }

    pub fn execute<'subject>(
        &self,
        request: RuntimeExecutionRequest<'_, 'subject>,
    ) -> Result<RuntimeExecutionResult<'subject>, RuntimeExecutorError> {
        let (program_registry, program) = request.program.ok_or_else(|| {
            RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::OfficialProgramNotVerified,
                RuntimeExecutorRequirement::OfficialProgram,
            )
        })?;
        let (sandbox_authority, sandbox_evidence) = request.sandbox.ok_or_else(|| {
            RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::SandboxEvidenceNotVerified,
                RuntimeExecutorRequirement::SandboxEvidence,
            )
        })?;
        let (timeout_authority, timeout_control) = request.timeout_cancel.ok_or_else(|| {
            RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::TimeoutControlNotVerified,
                RuntimeExecutorRequirement::TimeoutCancelControl,
            )
        })?;
        let (input_output_authority, input_output_evidence) =
            request.input_output.ok_or_else(|| {
                RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::EnforcedInputNotVerified,
                    RuntimeExecutorRequirement::EnforcedInput,
                )
            })?;
        let audit_authority = request.audit.ok_or_else(|| {
            RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::AuditProjectionFailed,
                RuntimeExecutorRequirement::AuditEvidence,
            )
        })?;

        self.require_permit(
            &request.permit,
            program_registry,
            program,
            request.lifecycle_authority,
            request.lifecycle_record,
        )?;
        if !sandbox_authority.verifies(sandbox_evidence, request.permit.subject)
            || !Arc::ptr_eq(sandbox_evidence.identity(), &request.permit.sandbox)
        {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::SandboxEvidenceNotVerified,
                RuntimeExecutorRequirement::SandboxEvidence,
            ));
        }
        if !timeout_authority.verifies_control(
            timeout_control,
            request.lifecycle_authority,
            request.lifecycle_record,
        ) {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::TimeoutControlNotVerified,
                RuntimeExecutorRequirement::TimeoutCancelControl,
            ));
        }
        let cancellation = timeout_control.cancellation_handle();
        if cancellation.cancellation_requested() {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::CancellationPending,
                RuntimeExecutorRequirement::TimeoutCancelControl,
            ));
        }
        if !input_output_authority.verifies(input_output_evidence, request.permit.subject)
            || !Arc::ptr_eq(request.input.evidence(), input_output_evidence.identity())
            || !request.input.matches_subject(
                request.permit.subject.package_id(),
                request.permit.subject.task_type(),
            )
        {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::EnforcedInputNotVerified,
                RuntimeExecutorRequirement::EnforcedInput,
            ));
        }

        let sandbox = ActiveOfficialRustSandbox::activate(sandbox_evidence).ok_or_else(|| {
            RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::SandboxRestrictionsUnsupported,
                RuntimeExecutorRequirement::SandboxRestrictions,
            )
        })?;
        let started = request
            .lifecycle_authority
            .transition_authorized_to_running(
                request.lifecycle_record,
                request.permit.lifecycle_revision,
            )
            .map_err(|_| {
                RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::LifecycleTransitionRejected,
                    RuntimeExecutorRequirement::LifecycleRecord,
                )
            })?;
        let started_audit = match audit_authority
            .enforce_lifecycle(request.lifecycle_authority, request.lifecycle_record)
        {
            Ok(evidence) => evidence,
            Err(_) => {
                fail_running_execution(
                    request.lifecycle_authority,
                    request.lifecycle_record,
                    audit_authority,
                );
                return Err(RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::AuditProjectionFailed,
                    RuntimeExecutorRequirement::AuditEvidence,
                ));
            }
        };
        let timeout = match timeout_authority.arm_timeout(
            timeout_control,
            request.lifecycle_authority,
            request.lifecycle_record,
            crate::AgentTimeoutClass::Execution,
        ) {
            Ok(timeout) => timeout,
            Err(_) => {
                fail_running_execution(
                    request.lifecycle_authority,
                    request.lifecycle_record,
                    audit_authority,
                );
                return Err(RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::TimeoutArmFailed,
                    RuntimeExecutorRequirement::TimeoutCancelControl,
                ));
            }
        };

        let context = RuntimeExecutionContext::new(
            &cancellation,
            timeout.deadline(),
            sandbox_evidence.resource_limits(),
        );
        let program_result = program.invoke(&context, request.input.redacted_content());
        let observed_at = Instant::now();
        let cleanup_completed = sandbox.close();

        if timeout.expired_at(observed_at) {
            let _terminal = timeout_authority
                .enforce_timeout_at(
                    timeout_control,
                    request.lifecycle_authority,
                    request.lifecycle_record,
                    &timeout,
                    observed_at,
                )
                .map_err(|_| {
                    RuntimeExecutorError::new(
                        RuntimeExecutorErrorCode::LifecycleTransitionRejected,
                        RuntimeExecutorRequirement::TimeoutCancelControl,
                    )
                })?;
            let _ = audit_authority
                .enforce_lifecycle(request.lifecycle_authority, request.lifecycle_record);
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::RuntimeTimedOut,
                RuntimeExecutorRequirement::RuntimeProgram,
            ));
        }
        if cancellation.cancellation_requested() {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::CancellationPending,
                RuntimeExecutorRequirement::TimeoutCancelControl,
            ));
        }

        let program_output = match program_result {
            Ok(output) => output,
            Err(_) => {
                fail_running_execution(
                    request.lifecycle_authority,
                    request.lifecycle_record,
                    audit_authority,
                );
                return Err(RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::RuntimeProgramFailed,
                    RuntimeExecutorRequirement::RuntimeProgram,
                ));
            }
        };
        let redacted_output = match input_output_authority.attest_redacted_output(
            input_output_evidence,
            request.permit.subject,
            program_output.redacted_content(),
        ) {
            Ok(output) => output,
            Err(_) => {
                fail_running_execution(
                    request.lifecycle_authority,
                    request.lifecycle_record,
                    audit_authority,
                );
                return Err(RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::RuntimeOutputRejected,
                    RuntimeExecutorRequirement::EnforcedOutput,
                ));
            }
        };
        let output = match input_output_authority.enforce_output(
            input_output_evidence,
            request.permit.subject,
            program_output.classification(),
            redacted_output,
        ) {
            Ok(output) => output,
            Err(_) => {
                fail_running_execution(
                    request.lifecycle_authority,
                    request.lifecycle_record,
                    audit_authority,
                );
                return Err(RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::RuntimeOutputRejected,
                    RuntimeExecutorRequirement::EnforcedOutput,
                ));
            }
        };
        let completed = request
            .lifecycle_authority
            .transition(
                request.lifecycle_record,
                request.lifecycle_record.revision(),
                ExecutionLifecycleState::Completed,
            )
            .map_err(|_| {
                RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::LifecycleTransitionRejected,
                    RuntimeExecutorRequirement::LifecycleRecord,
                )
            })?;
        let completed_audit = audit_authority
            .enforce_lifecycle(request.lifecycle_authority, request.lifecycle_record)
            .map_err(|_| {
                RuntimeExecutorError::new(
                    RuntimeExecutorErrorCode::AuditProjectionFailed,
                    RuntimeExecutorRequirement::AuditEvidence,
                )
            })?;

        debug_assert!(cleanup_completed);
        Ok(RuntimeExecutionResult::new(RuntimeExecutionResultParts {
            executor: Arc::clone(&self.identity),
            loader: Arc::clone(&request.permit.loader),
            load_evidence: Arc::clone(&request.permit.load_evidence),
            program: Arc::clone(&request.permit.program),
            execution: Arc::clone(&request.permit.execution),
            subject: request.permit.subject,
            started,
            completed,
            started_audit,
            completed_audit,
            output,
        }))
    }

    pub fn verifies_result(&self, verification: RuntimeExecutionVerification<'_, '_>) -> bool {
        let Some((program_registry, program)) = verification.program else {
            return false;
        };
        let Some(audit_authority) = verification.audit else {
            return false;
        };
        let result = verification.result;
        let loaded = verification.loaded;
        let lifecycle_authority = verification.lifecycle_authority;
        let lifecycle_record = verification.lifecycle_record;
        Arc::ptr_eq(&self.identity, &result.executor)
            && Arc::ptr_eq(loaded.authority(), &result.loader)
            && Arc::ptr_eq(loaded.evidence(), &result.load_evidence)
            && loaded.subject().same_as(result.subject)
            && program_registry.verifies(program, result.subject)
            && Arc::ptr_eq(program.identity(), &result.program)
            && Arc::ptr_eq(lifecycle_record.execution(), &result.execution)
            && lifecycle_record.state() == ExecutionLifecycleState::Completed
            && lifecycle_record.revision() == result.completed.revision()
            && lifecycle_authority.verifies_transition(&result.started, lifecycle_record)
            && lifecycle_authority.verifies_transition(&result.completed, lifecycle_record)
            && audit_authority.verifies(&result.started_audit)
            && audit_authority.verifies_lifecycle(
                &result.completed_audit,
                lifecycle_authority,
                lifecycle_record,
            )
    }

    fn require_permit(
        &self,
        permit: &RuntimeExecutionPermit<'_>,
        program_registry: &super::OfficialRustProgramRegistry,
        program: &super::OfficialRustProgram<'_>,
        lifecycle_authority: &crate::ExecutionLifecycleAuthority,
        lifecycle_record: &crate::ExecutionLifecycleRecord<'_>,
    ) -> Result<(), RuntimeExecutorError> {
        if !Arc::ptr_eq(&self.identity, &permit.executor) {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::ForeignExecutorAuthority,
                RuntimeExecutorRequirement::ExecutorAuthority,
            ));
        }
        if !program_registry.verifies(program, permit.subject)
            || !Arc::ptr_eq(program.identity(), &permit.program)
        {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::OfficialProgramNotVerified,
                RuntimeExecutorRequirement::OfficialProgram,
            ));
        }
        if !lifecycle_authority.verifies_record_identity(lifecycle_record)
            || !Arc::ptr_eq(lifecycle_record.execution(), &permit.execution)
        {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::LifecycleRecordNotVerified,
                RuntimeExecutorRequirement::LifecycleRecord,
            ));
        }
        if lifecycle_record.state() != ExecutionLifecycleState::ScopeCheck
            || lifecycle_record.revision() != permit.lifecycle_revision
        {
            return Err(RuntimeExecutorError::new(
                RuntimeExecutorErrorCode::StaleExecutionPermit,
                RuntimeExecutorRequirement::ExecutionPermit,
            ));
        }
        Ok(())
    }
}

impl Default for RuntimeExecutorAuthority {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for RuntimeExecutorAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeExecutorAuthority")
            .field("identity", &"[redacted]")
            .finish()
    }
}

fn fail_running_execution(
    lifecycle_authority: &crate::ExecutionLifecycleAuthority,
    lifecycle_record: &mut crate::ExecutionLifecycleRecord<'_>,
    audit_authority: &AuditEventEnforcementAuthority,
) {
    let revision = lifecycle_record.revision();
    if lifecycle_authority
        .transition(lifecycle_record, revision, ExecutionLifecycleState::Failed)
        .is_ok()
    {
        let _ = audit_authority.enforce_lifecycle(lifecycle_authority, lifecycle_record);
    }
}
