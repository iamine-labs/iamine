use super::{
    AgentAuditEvent, AuditEventClass, AuditEventSet, AuditEventSource, AuditOutcome,
    AuditReasonCode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditLifecycleState {
    Queued,
    PermissionPending,
    ScopeCheck,
    HandoffRequired,
    Running,
    Completed,
    Failed,
    Cancelled,
    Timeout,
    Blocked,
}

impl AuditLifecycleState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::PermissionPending => "permission_pending",
            Self::ScopeCheck => "scope_check",
            Self::HandoffRequired => "handoff_required",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Blocked => "blocked",
        }
    }
}

pub const fn audit_lifecycle_state(state: AuditLifecycleState) -> AuditEventSet {
    let observed = AgentAuditEvent::new(
        AuditEventClass::LifecycleObserved,
        AuditEventSource::Lifecycle,
        AuditOutcome::Observed,
        AuditReasonCode::LifecycleStateObserved,
        Some(state),
    );

    if matches!(state, AuditLifecycleState::HandoffRequired) {
        let handoff = AgentAuditEvent::new(
            AuditEventClass::HandoffRequired,
            AuditEventSource::Lifecycle,
            AuditOutcome::HandedOff,
            AuditReasonCode::LifecycleStateObserved,
            Some(state),
        );
        AuditEventSet::pair(observed, handoff)
    } else {
        AuditEventSet::single(observed)
    }
}
