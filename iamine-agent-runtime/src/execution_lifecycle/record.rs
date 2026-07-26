use std::{fmt, sync::Arc};

use crate::{
    sandbox_enforcement::{SandboxAuthorityIdentity, SandboxEvidenceIdentity},
    PackageReviewSubject,
};

use super::{ExecutionIdentity, ExecutionLifecycleState, LifecycleAuthorityIdentity};

pub const EXECUTION_LIFECYCLE_RECORD_SCHEMA_VERSION: &str =
    "iamine.agent.execution_lifecycle.record-0.1";
pub const MAX_EXECUTION_LIFECYCLE_TRANSITIONS: u8 = 4;

#[must_use]
pub struct ExecutionLifecycleRecord<'a> {
    authority: Arc<LifecycleAuthorityIdentity>,
    execution: Arc<ExecutionIdentity>,
    sandbox_authority: Arc<SandboxAuthorityIdentity>,
    sandbox_evidence: Arc<SandboxEvidenceIdentity>,
    subject: PackageReviewSubject<'a>,
    state: ExecutionLifecycleState,
    revision: u8,
}

impl<'a> ExecutionLifecycleRecord<'a> {
    pub(crate) fn new(
        authority: Arc<LifecycleAuthorityIdentity>,
        sandbox_authority: Arc<SandboxAuthorityIdentity>,
        sandbox_evidence: Arc<SandboxEvidenceIdentity>,
        subject: PackageReviewSubject<'a>,
    ) -> Self {
        Self {
            authority,
            execution: Arc::new(ExecutionIdentity),
            sandbox_authority,
            sandbox_evidence,
            subject,
            state: ExecutionLifecycleState::Queued,
            revision: 0,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        EXECUTION_LIFECYCLE_RECORD_SCHEMA_VERSION
    }

    pub const fn state(&self) -> ExecutionLifecycleState {
        self.state
    }

    pub const fn revision(&self) -> u8 {
        self.revision
    }

    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    pub const fn execution_authorized(&self) -> bool {
        false
    }

    pub const fn runtime_active(&self) -> bool {
        false
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<LifecycleAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }

    pub(crate) const fn sandbox_authority(&self) -> &Arc<SandboxAuthorityIdentity> {
        &self.sandbox_authority
    }

    pub(crate) const fn sandbox_evidence(&self) -> &Arc<SandboxEvidenceIdentity> {
        &self.sandbox_evidence
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'a> {
        self.subject
    }

    pub(crate) fn record_transition(&mut self, target: ExecutionLifecycleState) {
        self.state = target;
        self.revision += 1;
    }
}

impl fmt::Debug for ExecutionLifecycleRecord<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionLifecycleRecord")
            .field("schema_version", &self.schema_version())
            .field("authority", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("sandbox_authority", &"[redacted]")
            .field("sandbox_evidence", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("state", &self.state.as_str())
            .field("revision", &self.revision)
            .field("execution_authorized", &false)
            .field("runtime_active", &false)
            .field("persisted", &false)
            .finish()
    }
}
