use std::fmt;

use crate::{
    AuditEventEnforcementAuthority, ExecutionLifecycleAuthority, ExecutionLifecycleRecord,
    LoadedAgentPackage,
};

use super::{OfficialRustProgram, OfficialRustProgramRegistry, RuntimeExecutionResult};

#[must_use]
pub struct RuntimeExecutionVerification<'context, 'subject> {
    pub(super) result: &'context RuntimeExecutionResult<'subject>,
    pub(super) loaded: &'context LoadedAgentPackage<'subject>,
    pub(super) lifecycle_authority: &'context ExecutionLifecycleAuthority,
    pub(super) lifecycle_record: &'context ExecutionLifecycleRecord<'subject>,
    pub(super) program: Option<(
        &'context OfficialRustProgramRegistry,
        &'context OfficialRustProgram<'subject>,
    )>,
    pub(super) audit: Option<&'context AuditEventEnforcementAuthority>,
}

impl<'context, 'subject> RuntimeExecutionVerification<'context, 'subject> {
    pub const fn new(
        result: &'context RuntimeExecutionResult<'subject>,
        loaded: &'context LoadedAgentPackage<'subject>,
        lifecycle_authority: &'context ExecutionLifecycleAuthority,
        lifecycle_record: &'context ExecutionLifecycleRecord<'subject>,
    ) -> Self {
        Self {
            result,
            loaded,
            lifecycle_authority,
            lifecycle_record,
            program: None,
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

    pub fn with_audit(mut self, authority: &'context AuditEventEnforcementAuthority) -> Self {
        self.audit = Some(authority);
        self
    }
}

impl fmt::Debug for RuntimeExecutionVerification<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionVerification")
            .field("result", &"[redacted]")
            .field("loaded", &"[redacted]")
            .field("lifecycle", &"[redacted]")
            .field("program_present", &self.program.is_some())
            .field("audit_present", &self.audit.is_some())
            .finish()
    }
}
