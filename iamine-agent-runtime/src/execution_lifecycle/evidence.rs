use std::{fmt, sync::Arc};

use super::{ExecutionLifecycleRecord, ExecutionLifecycleState};

#[derive(Debug)]
pub(crate) struct LifecycleAuthorityIdentity;

#[derive(Debug)]
pub(crate) struct ExecutionIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionLifecycleTransitionEvidenceStatus {
    Recorded,
}

pub const EXECUTION_LIFECYCLE_TRANSITION_SCHEMA_VERSION: &str =
    "iamine.agent.execution_lifecycle.transition-0.1";

#[must_use]
pub struct ExecutionLifecycleTransitionEvidence {
    authority: Arc<LifecycleAuthorityIdentity>,
    execution: Arc<ExecutionIdentity>,
    from: ExecutionLifecycleState,
    to: ExecutionLifecycleState,
    revision: u8,
}

impl ExecutionLifecycleTransitionEvidence {
    pub(crate) fn new(
        record: &ExecutionLifecycleRecord<'_>,
        from: ExecutionLifecycleState,
    ) -> Self {
        Self {
            authority: Arc::clone(record.authority()),
            execution: Arc::clone(record.execution()),
            from,
            to: record.state(),
            revision: record.revision(),
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        EXECUTION_LIFECYCLE_TRANSITION_SCHEMA_VERSION
    }

    pub const fn status(&self) -> ExecutionLifecycleTransitionEvidenceStatus {
        ExecutionLifecycleTransitionEvidenceStatus::Recorded
    }

    pub const fn from(&self) -> ExecutionLifecycleState {
        self.from
    }

    pub const fn to(&self) -> ExecutionLifecycleState {
        self.to
    }

    pub const fn revision(&self) -> u8 {
        self.revision
    }

    pub const fn transition_recorded(&self) -> bool {
        true
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

    pub const fn audit_emitted(&self) -> bool {
        false
    }

    pub const fn cleanup_completed(&self) -> bool {
        false
    }

    pub const fn transport_allowed(&self) -> bool {
        false
    }

    pub const fn package_loaded(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<LifecycleAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl fmt::Debug for ExecutionLifecycleTransitionEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionLifecycleTransitionEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("authority", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("from", &self.from.as_str())
            .field("to", &self.to.as_str())
            .field("revision", &self.revision)
            .field("execution_authorized", &false)
            .field("runtime_active", &false)
            .field("persisted", &false)
            .finish()
    }
}
