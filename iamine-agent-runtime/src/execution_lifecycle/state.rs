#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ExecutionLifecycleState {
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

pub const EXECUTION_LIFECYCLE_STATES: [ExecutionLifecycleState; 10] = [
    ExecutionLifecycleState::Queued,
    ExecutionLifecycleState::PermissionPending,
    ExecutionLifecycleState::ScopeCheck,
    ExecutionLifecycleState::HandoffRequired,
    ExecutionLifecycleState::Running,
    ExecutionLifecycleState::Completed,
    ExecutionLifecycleState::Failed,
    ExecutionLifecycleState::Cancelled,
    ExecutionLifecycleState::Timeout,
    ExecutionLifecycleState::Blocked,
];

impl ExecutionLifecycleState {
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

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Timeout | Self::Blocked
        )
    }

    pub const fn has_canonical_transition_to(self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::Queued, Self::PermissionPending | Self::Blocked)
                | (Self::PermissionPending, Self::ScopeCheck | Self::Blocked)
                | (
                    Self::ScopeCheck,
                    Self::HandoffRequired | Self::Running | Self::Blocked
                )
                | (Self::HandoffRequired, Self::Cancelled)
                | (
                    Self::Running,
                    Self::Completed | Self::Failed | Self::Cancelled | Self::Timeout
                )
        )
    }
}
