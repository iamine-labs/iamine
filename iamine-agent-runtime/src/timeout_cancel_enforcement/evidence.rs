use std::{fmt, sync::Arc};

use crate::{
    execution_lifecycle::ExecutionIdentity, ExecutionLifecycleState,
    ExecutionLifecycleTransitionEvidence, SandboxCleanupOwner, SandboxCleanupTrigger,
};

use super::{AgentTimeoutClass, TimeoutCancelAuthorityIdentity, TimeoutCancelControlIdentity};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CancellationSource {
    Operator,
    Orchestrator,
    PermissionRevoked,
    ScopeViolation,
    SandboxFailure,
    Timeout,
    Shutdown,
}

pub const CANCELLATION_SOURCES: [CancellationSource; 7] = [
    CancellationSource::Operator,
    CancellationSource::Orchestrator,
    CancellationSource::PermissionRevoked,
    CancellationSource::ScopeViolation,
    CancellationSource::SandboxFailure,
    CancellationSource::Timeout,
    CancellationSource::Shutdown,
];

impl CancellationSource {
    pub(crate) const ENFORCED_MASK: u8 = 0x80;

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator_cancelled",
            Self::Orchestrator => "orchestrator_cancelled",
            Self::PermissionRevoked => "permission_revoked",
            Self::ScopeViolation => "scope_violation_cancelled",
            Self::SandboxFailure => "sandbox_failure_cancelled",
            Self::Timeout => "timeout_cancelled",
            Self::Shutdown => "shutdown_cancelled",
        }
    }

    pub(crate) const fn encode(self) -> u8 {
        match self {
            Self::Operator => 1,
            Self::Orchestrator => 2,
            Self::PermissionRevoked => 3,
            Self::ScopeViolation => 4,
            Self::SandboxFailure => 5,
            Self::Timeout => 6,
            Self::Shutdown => 7,
        }
    }

    pub(crate) const fn decode(value: u8) -> Option<Self> {
        match value & !Self::ENFORCED_MASK {
            1 => Some(Self::Operator),
            2 => Some(Self::Orchestrator),
            3 => Some(Self::PermissionRevoked),
            4 => Some(Self::ScopeViolation),
            5 => Some(Self::SandboxFailure),
            6 => Some(Self::Timeout),
            7 => Some(Self::Shutdown),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeoutCancelEvent {
    Timeout(AgentTimeoutClass),
    Cancellation(CancellationSource),
}

impl TimeoutCancelEvent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout(timeout_class) => timeout_class.as_str(),
            Self::Cancellation(source) => source.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxCleanupResult {
    Pending,
    TimedOut,
}

impl SandboxCleanupResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::TimedOut => "timed_out",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CancellationRequestEvidenceStatus {
    Requested,
}

pub const CANCELLATION_REQUEST_SCHEMA_VERSION: &str =
    "iamine.agent.timeout_cancel.cancellation_request-0.1";

#[must_use]
pub struct CancellationRequestEvidence {
    authority: Arc<TimeoutCancelAuthorityIdentity>,
    control: Arc<TimeoutCancelControlIdentity>,
    execution: Arc<ExecutionIdentity>,
    source: CancellationSource,
    lifecycle_revision: u8,
}

impl CancellationRequestEvidence {
    pub(crate) fn new(
        authority: Arc<TimeoutCancelAuthorityIdentity>,
        control: Arc<TimeoutCancelControlIdentity>,
        execution: Arc<ExecutionIdentity>,
        source: CancellationSource,
        lifecycle_revision: u8,
    ) -> Self {
        Self {
            authority,
            control,
            execution,
            source,
            lifecycle_revision,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        CANCELLATION_REQUEST_SCHEMA_VERSION
    }

    pub const fn status(&self) -> CancellationRequestEvidenceStatus {
        CancellationRequestEvidenceStatus::Requested
    }

    pub const fn source(&self) -> CancellationSource {
        self.source
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn terminal_state_recorded(&self) -> bool {
        false
    }

    pub const fn cleanup_completed(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<TimeoutCancelAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn control(&self) -> &Arc<TimeoutCancelControlIdentity> {
        &self.control
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl fmt::Debug for CancellationRequestEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationRequestEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("authority", &"[redacted]")
            .field("control", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("source", &self.source.as_str())
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("terminal_state_recorded", &false)
            .field("cleanup_completed", &false)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeoutCancelTerminalEvidenceStatus {
    TerminalRecordedCleanupPending,
}

pub const TIMEOUT_CANCEL_TERMINAL_SCHEMA_VERSION: &str = "iamine.agent.timeout_cancel.terminal-0.1";

#[must_use]
pub struct TimeoutCancelTerminalEvidence {
    authority: Arc<TimeoutCancelAuthorityIdentity>,
    control: Arc<TimeoutCancelControlIdentity>,
    execution: Arc<ExecutionIdentity>,
    event: TimeoutCancelEvent,
    transition: ExecutionLifecycleTransitionEvidence,
    cleanup_trigger: SandboxCleanupTrigger,
}

impl TimeoutCancelTerminalEvidence {
    pub(crate) fn new(
        authority: Arc<TimeoutCancelAuthorityIdentity>,
        control: Arc<TimeoutCancelControlIdentity>,
        execution: Arc<ExecutionIdentity>,
        event: TimeoutCancelEvent,
        transition: ExecutionLifecycleTransitionEvidence,
        cleanup_trigger: SandboxCleanupTrigger,
    ) -> Self {
        Self {
            authority,
            control,
            execution,
            event,
            transition,
            cleanup_trigger,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        TIMEOUT_CANCEL_TERMINAL_SCHEMA_VERSION
    }

    pub const fn status(&self) -> TimeoutCancelTerminalEvidenceStatus {
        TimeoutCancelTerminalEvidenceStatus::TerminalRecordedCleanupPending
    }

    pub const fn event(&self) -> TimeoutCancelEvent {
        self.event
    }

    pub const fn terminal_state(&self) -> ExecutionLifecycleState {
        self.transition.to()
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.transition.revision()
    }

    pub const fn cleanup_owner(&self) -> SandboxCleanupOwner {
        SandboxCleanupOwner::RuntimeSandboxAdapter
    }

    pub const fn cleanup_trigger(&self) -> SandboxCleanupTrigger {
        self.cleanup_trigger
    }

    pub const fn cleanup_result(&self) -> SandboxCleanupResult {
        SandboxCleanupResult::Pending
    }

    pub const fn terminal_state_recorded(&self) -> bool {
        true
    }

    pub const fn cleanup_completed(&self) -> bool {
        false
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

    pub const fn lifecycle_transition(&self) -> &ExecutionLifecycleTransitionEvidence {
        &self.transition
    }

    pub(crate) const fn authority(&self) -> &Arc<TimeoutCancelAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn control(&self) -> &Arc<TimeoutCancelControlIdentity> {
        &self.control
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl fmt::Debug for TimeoutCancelTerminalEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TimeoutCancelTerminalEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("authority", &"[redacted]")
            .field("control", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("event", &self.event.as_str())
            .field("terminal_state", &self.terminal_state().as_str())
            .field("lifecycle_revision", &self.lifecycle_revision())
            .field("cleanup_owner", &self.cleanup_owner())
            .field("cleanup_trigger", &self.cleanup_trigger())
            .field("cleanup_result", &self.cleanup_result().as_str())
            .field("execution_authorized", &false)
            .field("runtime_active", &false)
            .field("persisted", &false)
            .field("audit_emitted", &false)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CleanupTimeoutEvidenceStatus {
    Recorded,
}

pub const CLEANUP_TIMEOUT_SCHEMA_VERSION: &str = "iamine.agent.timeout_cancel.cleanup_timeout-0.1";

#[must_use]
pub struct CleanupTimeoutEvidence {
    authority: Arc<TimeoutCancelAuthorityIdentity>,
    control: Arc<TimeoutCancelControlIdentity>,
    execution: Arc<ExecutionIdentity>,
    terminal_state: ExecutionLifecycleState,
    lifecycle_revision: u8,
}

impl CleanupTimeoutEvidence {
    pub(crate) fn new(
        authority: Arc<TimeoutCancelAuthorityIdentity>,
        control: Arc<TimeoutCancelControlIdentity>,
        execution: Arc<ExecutionIdentity>,
        terminal_state: ExecutionLifecycleState,
        lifecycle_revision: u8,
    ) -> Self {
        Self {
            authority,
            control,
            execution,
            terminal_state,
            lifecycle_revision,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        CLEANUP_TIMEOUT_SCHEMA_VERSION
    }

    pub const fn status(&self) -> CleanupTimeoutEvidenceStatus {
        CleanupTimeoutEvidenceStatus::Recorded
    }

    pub const fn terminal_state(&self) -> ExecutionLifecycleState {
        self.terminal_state
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn cleanup_owner(&self) -> SandboxCleanupOwner {
        SandboxCleanupOwner::RuntimeSandboxAdapter
    }

    pub const fn cleanup_result(&self) -> SandboxCleanupResult {
        SandboxCleanupResult::TimedOut
    }

    pub const fn lifecycle_state_changed(&self) -> bool {
        false
    }

    pub const fn cleanup_completed(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<TimeoutCancelAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn control(&self) -> &Arc<TimeoutCancelControlIdentity> {
        &self.control
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl fmt::Debug for CleanupTimeoutEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CleanupTimeoutEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("authority", &"[redacted]")
            .field("control", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("terminal_state", &self.terminal_state.as_str())
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("cleanup_owner", &self.cleanup_owner())
            .field("cleanup_result", &self.cleanup_result().as_str())
            .field("lifecycle_state_changed", &false)
            .finish()
    }
}
