use std::{fmt, sync::Arc};

use crate::{
    execution_lifecycle::ExecutionIdentity,
    package_load_evidence_integration::PackageLoadEvidenceIdentity,
    package_loader::PackageLoaderAuthorityIdentity, AuditEventEnforcementEvidence,
    EnforcedOutputRecord, ExecutionLifecycleTransitionEvidence, PackageReviewSubject,
};

use super::authority::RuntimeExecutorAuthorityIdentity;
use super::program::OfficialRustProgramIdentity;

pub const RUNTIME_EXECUTION_RESULT_SCHEMA_VERSION: &str =
    "iamine.agent.runtime_executor.result-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeExecutionStatus {
    Completed,
}

#[must_use]
pub struct RuntimeExecutionResult<'subject> {
    pub(super) executor: Arc<RuntimeExecutorAuthorityIdentity>,
    pub(super) loader: Arc<PackageLoaderAuthorityIdentity>,
    pub(super) load_evidence: Arc<PackageLoadEvidenceIdentity>,
    pub(super) program: Arc<OfficialRustProgramIdentity>,
    pub(super) execution: Arc<ExecutionIdentity>,
    pub(super) subject: PackageReviewSubject<'subject>,
    pub(super) started: ExecutionLifecycleTransitionEvidence,
    pub(super) completed: ExecutionLifecycleTransitionEvidence,
    pub(super) started_audit: AuditEventEnforcementEvidence,
    pub(super) completed_audit: AuditEventEnforcementEvidence,
    output: EnforcedOutputRecord,
}

impl RuntimeExecutionResult<'_> {
    pub const fn schema_version(&self) -> &'static str {
        RUNTIME_EXECUTION_RESULT_SCHEMA_VERSION
    }

    pub const fn status(&self) -> RuntimeExecutionStatus {
        RuntimeExecutionStatus::Completed
    }

    pub const fn started_revision(&self) -> u8 {
        self.started.revision()
    }

    pub const fn completed_revision(&self) -> u8 {
        self.completed.revision()
    }

    pub const fn output(&self) -> &EnforcedOutputRecord {
        &self.output
    }

    pub const fn execution_authorized(&self) -> bool {
        true
    }

    pub const fn package_loaded(&self) -> bool {
        true
    }

    pub const fn execution_started(&self) -> bool {
        true
    }

    pub const fn runtime_was_active(&self) -> bool {
        true
    }

    pub const fn sandbox_adapter_was_active(&self) -> bool {
        true
    }

    pub const fn os_isolation_claimed(&self) -> bool {
        false
    }

    pub const fn cleanup_completed(&self) -> bool {
        true
    }

    pub const fn audit_recorded(&self) -> bool {
        true
    }

    pub const fn scheduler_mutated(&self) -> bool {
        false
    }

    pub const fn transport_started(&self) -> bool {
        false
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn external_event_emitted(&self) -> bool {
        false
    }
}

impl fmt::Debug for RuntimeExecutionResult<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionResult")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("executor", &"[redacted]")
            .field("loader", &"[redacted]")
            .field("load_evidence", &"[redacted]")
            .field("program", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("started_revision", &self.started_revision())
            .field("completed_revision", &self.completed_revision())
            .field("audit", &"[redacted]")
            .field("output", &"[redacted]")
            .field("execution_authorized", &true)
            .field("package_loaded", &true)
            .field("execution_started", &true)
            .field("runtime_was_active", &true)
            .field("sandbox_adapter_was_active", &true)
            .field("os_isolation_claimed", &false)
            .field("cleanup_completed", &true)
            .field("scheduler_mutated", &false)
            .field("transport_started", &false)
            .field("persisted", &false)
            .field("external_event_emitted", &false)
            .finish()
    }
}

pub(super) struct RuntimeExecutionResultParts<'subject> {
    pub executor: Arc<RuntimeExecutorAuthorityIdentity>,
    pub loader: Arc<PackageLoaderAuthorityIdentity>,
    pub load_evidence: Arc<PackageLoadEvidenceIdentity>,
    pub program: Arc<OfficialRustProgramIdentity>,
    pub execution: Arc<ExecutionIdentity>,
    pub subject: PackageReviewSubject<'subject>,
    pub started: ExecutionLifecycleTransitionEvidence,
    pub completed: ExecutionLifecycleTransitionEvidence,
    pub started_audit: AuditEventEnforcementEvidence,
    pub completed_audit: AuditEventEnforcementEvidence,
    pub output: EnforcedOutputRecord,
}

impl<'subject> RuntimeExecutionResult<'subject> {
    pub(super) fn new(parts: RuntimeExecutionResultParts<'subject>) -> Self {
        Self {
            executor: parts.executor,
            loader: parts.loader,
            load_evidence: parts.load_evidence,
            program: parts.program,
            execution: parts.execution,
            subject: parts.subject,
            started: parts.started,
            completed: parts.completed,
            started_audit: parts.started_audit,
            completed_audit: parts.completed_audit,
            output: parts.output,
        }
    }
}
