use std::fmt;

use crate::{
    AuditEventEnforcementAuthority, EnforcedInputRecord, ExecutionLifecycleAuthority,
    ExecutionLifecycleRecord, InputOutputEnforcementAuthority, InputOutputEnforcementEvidence,
    SandboxEnforcementAuthority, SandboxEnforcementEvidence, TimeoutCancelAuthority,
    TimeoutCancelControl,
};

use super::{OfficialRustProgram, OfficialRustProgramRegistry, RuntimeExecutionPermit};

#[must_use]
pub struct RuntimeExecutionRequest<'context, 'subject> {
    pub(super) permit: RuntimeExecutionPermit<'subject>,
    pub(super) lifecycle_authority: &'context ExecutionLifecycleAuthority,
    pub(super) lifecycle_record: &'context mut ExecutionLifecycleRecord<'subject>,
    pub(super) input: &'context EnforcedInputRecord,
    pub(super) program: Option<(
        &'context OfficialRustProgramRegistry,
        &'context OfficialRustProgram<'subject>,
    )>,
    pub(super) sandbox: Option<(
        &'context SandboxEnforcementAuthority,
        &'context SandboxEnforcementEvidence<'subject>,
    )>,
    pub(super) timeout_cancel: Option<(
        &'context TimeoutCancelAuthority,
        &'context TimeoutCancelControl,
    )>,
    pub(super) input_output: Option<(
        &'context InputOutputEnforcementAuthority,
        &'context InputOutputEnforcementEvidence<'subject>,
    )>,
    pub(super) audit: Option<&'context AuditEventEnforcementAuthority>,
}

impl<'context, 'subject> RuntimeExecutionRequest<'context, 'subject> {
    pub fn new(
        permit: RuntimeExecutionPermit<'subject>,
        lifecycle_authority: &'context ExecutionLifecycleAuthority,
        lifecycle_record: &'context mut ExecutionLifecycleRecord<'subject>,
        input: &'context EnforcedInputRecord,
    ) -> Self {
        Self {
            permit,
            lifecycle_authority,
            lifecycle_record,
            input,
            program: None,
            sandbox: None,
            timeout_cancel: None,
            input_output: None,
            audit: None,
        }
    }

    pub fn with_program(
        mut self,
        registry: &'context OfficialRustProgramRegistry,
        program: &'context OfficialRustProgram<'subject>,
    ) -> Self {
        self.program = Some((registry, program));
        self
    }

    pub fn with_sandbox(
        mut self,
        authority: &'context SandboxEnforcementAuthority,
        evidence: &'context SandboxEnforcementEvidence<'subject>,
    ) -> Self {
        self.sandbox = Some((authority, evidence));
        self
    }

    pub fn with_timeout_cancel(
        mut self,
        authority: &'context TimeoutCancelAuthority,
        control: &'context TimeoutCancelControl,
    ) -> Self {
        self.timeout_cancel = Some((authority, control));
        self
    }

    pub fn with_input_output(
        mut self,
        authority: &'context InputOutputEnforcementAuthority,
        evidence: &'context InputOutputEnforcementEvidence<'subject>,
    ) -> Self {
        self.input_output = Some((authority, evidence));
        self
    }

    pub fn with_audit(mut self, authority: &'context AuditEventEnforcementAuthority) -> Self {
        self.audit = Some(authority);
        self
    }
}

impl fmt::Debug for RuntimeExecutionRequest<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionRequest")
            .field("permit", &"[redacted]")
            .field("lifecycle", &"[redacted]")
            .field("input", &"[redacted]")
            .field("program_present", &self.program.is_some())
            .field("sandbox_present", &self.sandbox.is_some())
            .field("timeout_cancel_present", &self.timeout_cancel.is_some())
            .field("input_output_present", &self.input_output.is_some())
            .field("audit_present", &self.audit.is_some())
            .finish()
    }
}
