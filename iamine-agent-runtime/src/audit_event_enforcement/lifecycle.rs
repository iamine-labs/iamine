use iamine_agents::{audit_lifecycle_state, AuditEventSet, AuditLifecycleState};

use crate::ExecutionLifecycleState;

pub(crate) const fn audit_runtime_lifecycle_state(state: ExecutionLifecycleState) -> AuditEventSet {
    let audit_state = match state {
        ExecutionLifecycleState::Queued => AuditLifecycleState::Queued,
        ExecutionLifecycleState::PermissionPending => AuditLifecycleState::PermissionPending,
        ExecutionLifecycleState::ScopeCheck => AuditLifecycleState::ScopeCheck,
        ExecutionLifecycleState::HandoffRequired => AuditLifecycleState::HandoffRequired,
        ExecutionLifecycleState::Running => AuditLifecycleState::Running,
        ExecutionLifecycleState::Completed => AuditLifecycleState::Completed,
        ExecutionLifecycleState::Failed => AuditLifecycleState::Failed,
        ExecutionLifecycleState::Cancelled => AuditLifecycleState::Cancelled,
        ExecutionLifecycleState::Timeout => AuditLifecycleState::Timeout,
        ExecutionLifecycleState::Blocked => AuditLifecycleState::Blocked,
    };
    audit_lifecycle_state(audit_state)
}
