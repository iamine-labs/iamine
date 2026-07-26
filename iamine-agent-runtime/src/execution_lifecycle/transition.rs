use super::ExecutionLifecycleState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransitionDisposition {
    Recordable,
    ExecutionAuthorizationRequired,
    Rejected,
}

pub(super) const fn classify_transition(
    from: ExecutionLifecycleState,
    target: ExecutionLifecycleState,
) -> TransitionDisposition {
    if !from.has_canonical_transition_to(target) {
        TransitionDisposition::Rejected
    } else if matches!(target, ExecutionLifecycleState::Running) {
        TransitionDisposition::ExecutionAuthorizationRequired
    } else {
        TransitionDisposition::Recordable
    }
}
