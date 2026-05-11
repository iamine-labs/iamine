#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultRejectionReason {
    UnknownTaskId,
    InvalidTaskId,
    DuplicateResult,
    SuccessFalse,
    WrongWorker,
    TaskNotAssigned,
}

impl ResultRejectionReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnknownTaskId => "unknown_task_id",
            Self::InvalidTaskId => "invalid_task_id",
            Self::DuplicateResult => "duplicate_result",
            Self::SuccessFalse => "success_false",
            Self::WrongWorker => "wrong_worker",
            Self::TaskNotAssigned => "task_not_assigned",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultAcceptanceDecision {
    Accept,
    Reject(ResultRejectionReason),
}

impl ResultAcceptanceDecision {
    #[cfg(test)]
    pub(crate) fn is_accept(self) -> bool {
        matches!(self, Self::Accept)
    }

    #[cfg(test)]
    pub(crate) fn rejection_reason(self) -> Option<&'static str> {
        match self {
            Self::Accept => None,
            Self::Reject(reason) => Some(reason.as_str()),
        }
    }

    pub(crate) fn into_result(self) -> Result<(), &'static str> {
        match self {
            Self::Accept => Ok(()),
            Self::Reject(reason) => Err(reason.as_str()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BroadcastResultAcceptanceContext<'a> {
    pub(crate) expected_task_id: Option<&'a str>,
    pub(crate) assigned_worker_peer_id: Option<&'a str>,
    pub(crate) result_already_accepted: bool,
    pub(crate) result_task_id: &'a str,
    pub(crate) result_worker_peer_id: &'a str,
    pub(crate) success: bool,
}

pub(crate) fn decide_broadcast_result_acceptance(
    context: BroadcastResultAcceptanceContext<'_>,
) -> ResultAcceptanceDecision {
    let Some(expected_task_id) = context.expected_task_id else {
        return ResultAcceptanceDecision::Reject(ResultRejectionReason::UnknownTaskId);
    };
    if context.result_task_id.trim().is_empty() {
        return ResultAcceptanceDecision::Reject(ResultRejectionReason::InvalidTaskId);
    }
    if expected_task_id != context.result_task_id {
        return ResultAcceptanceDecision::Reject(ResultRejectionReason::UnknownTaskId);
    }
    if context.result_already_accepted {
        return ResultAcceptanceDecision::Reject(ResultRejectionReason::DuplicateResult);
    }
    if !context.success {
        return ResultAcceptanceDecision::Reject(ResultRejectionReason::SuccessFalse);
    }
    match context.assigned_worker_peer_id {
        Some(assigned_worker) if assigned_worker == context.result_worker_peer_id => {
            ResultAcceptanceDecision::Accept
        }
        Some(_) => ResultAcceptanceDecision::Reject(ResultRejectionReason::WrongWorker),
        None => ResultAcceptanceDecision::Reject(ResultRejectionReason::TaskNotAssigned),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context<'a>(
        result_task_id: &'a str,
        assigned_worker_peer_id: Option<&'a str>,
        result_worker_peer_id: &'a str,
        result_already_accepted: bool,
        success: bool,
    ) -> BroadcastResultAcceptanceContext<'a> {
        BroadcastResultAcceptanceContext {
            expected_task_id: Some("task-1"),
            assigned_worker_peer_id,
            result_already_accepted,
            result_task_id,
            result_worker_peer_id,
            success,
        }
    }

    #[test]
    fn result_acceptance_accepts_assigned_worker_result() {
        let decision = decide_broadcast_result_acceptance(context(
            "task-1",
            Some("worker-a"),
            "worker-a",
            false,
            true,
        ));

        assert_eq!(decision, ResultAcceptanceDecision::Accept);
        assert!(decision.is_accept());
    }

    #[test]
    fn result_acceptance_rejects_wrong_worker() {
        let decision = decide_broadcast_result_acceptance(context(
            "task-1",
            Some("worker-a"),
            "worker-b",
            false,
            true,
        ));

        assert_eq!(
            decision,
            ResultAcceptanceDecision::Reject(ResultRejectionReason::WrongWorker)
        );
        assert_eq!(decision.rejection_reason(), Some("wrong_worker"));
    }

    #[test]
    fn result_acceptance_rejects_duplicate_result() {
        let decision = decide_broadcast_result_acceptance(context(
            "task-1",
            Some("worker-a"),
            "worker-a",
            true,
            true,
        ));

        assert_eq!(
            decision,
            ResultAcceptanceDecision::Reject(ResultRejectionReason::DuplicateResult)
        );
        assert_eq!(decision.rejection_reason(), Some("duplicate_result"));
    }

    #[test]
    fn result_acceptance_rejects_unknown_task_id() {
        let decision = decide_broadcast_result_acceptance(BroadcastResultAcceptanceContext {
            expected_task_id: None,
            assigned_worker_peer_id: Some("worker-a"),
            result_already_accepted: false,
            result_task_id: "task-1",
            result_worker_peer_id: "worker-a",
            success: true,
        });

        assert_eq!(
            decision,
            ResultAcceptanceDecision::Reject(ResultRejectionReason::UnknownTaskId)
        );
    }

    #[test]
    fn result_acceptance_rejects_mismatched_task_id() {
        let decision = decide_broadcast_result_acceptance(context(
            "other-task",
            Some("worker-a"),
            "worker-a",
            false,
            true,
        ));

        assert_eq!(
            decision,
            ResultAcceptanceDecision::Reject(ResultRejectionReason::UnknownTaskId)
        );
        assert_eq!(decision.rejection_reason(), Some("unknown_task_id"));
    }

    #[test]
    fn result_acceptance_rejects_invalid_task_id() {
        let decision = decide_broadcast_result_acceptance(context(
            "",
            Some("worker-a"),
            "worker-a",
            false,
            true,
        ));

        assert_eq!(
            decision,
            ResultAcceptanceDecision::Reject(ResultRejectionReason::InvalidTaskId)
        );
        assert_eq!(decision.rejection_reason(), Some("invalid_task_id"));
    }

    #[test]
    fn result_acceptance_rejects_success_false() {
        let decision = decide_broadcast_result_acceptance(context(
            "task-1",
            Some("worker-a"),
            "worker-a",
            false,
            false,
        ));

        assert_eq!(
            decision,
            ResultAcceptanceDecision::Reject(ResultRejectionReason::SuccessFalse)
        );
        assert_eq!(decision.rejection_reason(), Some("success_false"));
    }
}
