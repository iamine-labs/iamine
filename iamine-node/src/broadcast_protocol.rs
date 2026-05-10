use serde_json::Value;

#[derive(Debug, Clone)]
pub(crate) struct BroadcastResultToPublish {
    pub(crate) task_id: String,
    pub(crate) task_type: String,
    pub(crate) worker_peer_id: String,
    pub(crate) origin_peer: String,
    pub(crate) success: bool,
    pub(crate) output: String,
    pub(crate) elapsed_ms: u64,
    pub(crate) error: Option<String>,
    pub(crate) attempts: u32,
    pub(crate) source: &'static str,
}

pub(crate) fn broadcast_payload_preview(data: &str) -> String {
    const PREVIEW_CHARS: usize = 64;
    let mut preview: String = data.chars().take(PREVIEW_CHARS).collect();
    if data.chars().count() > PREVIEW_CHARS {
        preview.push_str("...");
    }
    preview
}

pub(crate) fn build_broadcast_task_offer_payload(
    task_id: &str,
    task_type: &str,
    data: &str,
    requester_peer_id: &str,
) -> Value {
    serde_json::json!({
        "type": "TaskOffer",
        "task_id": task_id,
        "task_type": task_type,
        "data": data,
        "requester_id": requester_peer_id,
        "origin_peer": requester_peer_id,
        "is_retry": false,
    })
}

pub(crate) fn build_task_bid_payload(
    task_id: &str,
    worker_id: &str,
    origin_peer: &str,
    reputation_score: u32,
    available_slots: usize,
    estimated_ms: u64,
) -> Value {
    serde_json::json!({
        "type": "TaskBid",
        "task_id": task_id,
        "worker_id": worker_id,
        "origin_peer": origin_peer,
        "reputation_score": reputation_score,
        "available_slots": available_slots,
        "estimated_ms": estimated_ms,
    })
}

pub(crate) fn build_broadcast_task_assign_payload(
    task_id: &str,
    assigned_worker: &str,
    origin_peer: &str,
    deadline_ms: u64,
    task_type: &str,
    data: &str,
) -> Value {
    serde_json::json!({
        "type": "TaskAssign",
        "task_id": task_id,
        "assigned_worker": assigned_worker,
        "origin_peer": origin_peer,
        "deadline_ms": deadline_ms,
        "task_type": task_type,
        "data": data,
    })
}

pub(crate) fn build_broadcast_task_result_payload(result: &BroadcastResultToPublish) -> Value {
    serde_json::json!({
        "type": "TaskResult",
        "task_id": result.task_id,
        "task_type": result.task_type,
        "worker_id": result.worker_peer_id,
        "worker_peer_id": result.worker_peer_id,
        "origin_peer": result.origin_peer,
        "controller_peer_id": result.origin_peer,
        "success": result.success,
        "output": result.output,
        "elapsed_ms": result.elapsed_ms,
        "execution_ms": result.elapsed_ms,
        "attempts": result.attempts,
        "error": result.error,
        "source": result.source,
    })
}

#[cfg(test)]
pub(crate) fn test_broadcast_result(
    task_id: &str,
    worker_peer_id: &str,
) -> BroadcastResultToPublish {
    BroadcastResultToPublish {
        task_id: task_id.to_string(),
        task_type: "reverse_string".to_string(),
        worker_peer_id: worker_peer_id.to_string(),
        origin_peer: "controller-peer".to_string(),
        success: true,
        output: "tset-tluser-tsacdaorb-aq".to_string(),
        elapsed_ms: 42,
        error: None,
        attempts: 1,
        source: "broadcast_task_assign",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast_runtime::{evaluate_broadcast_result_acceptance, BroadcastOfferState};
    use crate::broadcast_worker::should_execute_task_assignment;

    #[test]
    fn broadcast_mode_prepares_initial_task_offer() {
        let payload = build_broadcast_task_offer_payload(
            "broadcast-task-1",
            "reverse_string",
            "qa-payload",
            "controller-peer",
        );

        assert_eq!(payload["type"], "TaskOffer");
        assert_eq!(payload["task_id"], "broadcast-task-1");
        assert_eq!(payload["task_type"], "reverse_string");
        assert_eq!(payload["data"], "qa-payload");
        assert_eq!(payload["requester_id"], "controller-peer");
        assert_eq!(payload["origin_peer"], "controller-peer");
        assert_eq!(payload["is_retry"], false);
    }

    #[test]
    fn worker_receives_task_offer_and_publishes_bid() {
        let bid = build_task_bid_payload(
            "broadcast-task-bid",
            "worker-peer",
            "controller-peer",
            91,
            2,
            10,
        );

        assert_eq!(bid["type"], "TaskBid");
        assert_eq!(bid["task_id"], "broadcast-task-bid");
        assert_eq!(bid["worker_id"], "worker-peer");
        assert_eq!(bid["origin_peer"], "controller-peer");
        assert_eq!(bid["reputation_score"], 91);
        assert_eq!(bid["available_slots"], 2);
    }

    #[test]
    fn broadcaster_receives_bid_and_publishes_assignment() {
        let assign = build_broadcast_task_assign_payload(
            "broadcast-task-assign",
            "worker-peer",
            "controller-peer",
            30_000,
            "reverse_string",
            "qa-payload",
        );

        assert_eq!(assign["type"], "TaskAssign");
        assert_eq!(assign["task_id"], "broadcast-task-assign");
        assert_eq!(assign["assigned_worker"], "worker-peer");
        assert_eq!(assign["origin_peer"], "controller-peer");
        assert_eq!(assign["deadline_ms"], 30_000);
        assert_eq!(assign["task_type"], "reverse_string");
        assert_eq!(assign["data"], "qa-payload");
    }

    #[test]
    fn broadcast_flow_taskoffer_to_bid_to_assign_still_works() {
        let offer = build_broadcast_task_offer_payload(
            "broadcast-flow-task",
            "reverse_string",
            "abc",
            "controller-peer",
        );
        let bid = build_task_bid_payload(
            offer["task_id"].as_str().unwrap(),
            "worker-peer",
            offer["origin_peer"].as_str().unwrap(),
            90,
            1,
            10,
        );
        let assign = build_broadcast_task_assign_payload(
            bid["task_id"].as_str().unwrap(),
            bid["worker_id"].as_str().unwrap(),
            "controller-peer",
            30_000,
            offer["task_type"].as_str().unwrap(),
            offer["data"].as_str().unwrap(),
        );

        assert_eq!(offer["type"], "TaskOffer");
        assert_eq!(bid["type"], "TaskBid");
        assert_eq!(assign["type"], "TaskAssign");
        assert!(should_execute_task_assignment(
            assign["assigned_worker"].as_str().unwrap(),
            "worker-peer",
            false
        ));
        assert!(!should_execute_task_assignment(
            assign["assigned_worker"].as_str().unwrap(),
            "other-peer",
            false
        ));
    }

    #[test]
    fn broadcast_result_contains_required_fields() {
        let result = test_broadcast_result("broadcast-result-fields", "assigned-worker");
        let payload = build_broadcast_task_result_payload(&result);

        assert_eq!(payload["type"], "TaskResult");
        assert_eq!(payload["task_id"], "broadcast-result-fields");
        assert_eq!(payload["task_type"], "reverse_string");
        assert_eq!(payload["worker_id"], "assigned-worker");
        assert_eq!(payload["worker_peer_id"], "assigned-worker");
        assert_eq!(payload["origin_peer"], "controller-peer");
        assert_eq!(payload["success"], true);
        assert_eq!(payload["output"], "tset-tluser-tsacdaorb-aq");
        assert_eq!(payload["source"], "broadcast_task_assign");
    }

    #[test]
    fn broadcast_flow_taskoffer_to_bid_to_assign_to_result_still_works() {
        let task_id = "broadcast-flow-result";
        let offer =
            build_broadcast_task_offer_payload(task_id, "reverse_string", "abc", "controller-peer");
        let bid = build_task_bid_payload(
            offer["task_id"].as_str().unwrap(),
            "assigned-worker",
            offer["origin_peer"].as_str().unwrap(),
            90,
            1,
            10,
        );
        let assign = build_broadcast_task_assign_payload(
            bid["task_id"].as_str().unwrap(),
            bid["worker_id"].as_str().unwrap(),
            "controller-peer",
            30_000,
            offer["task_type"].as_str().unwrap(),
            offer["data"].as_str().unwrap(),
        );
        let result = test_broadcast_result(task_id, assign["assigned_worker"].as_str().unwrap());
        let payload = build_broadcast_task_result_payload(&result);
        let mut state = BroadcastOfferState::new(task_id.to_string());
        state.mark_assigned(assign["assigned_worker"].as_str().unwrap());

        assert_eq!(offer["type"], "TaskOffer");
        assert_eq!(bid["type"], "TaskBid");
        assert_eq!(assign["type"], "TaskAssign");
        assert_eq!(payload["type"], "TaskResult");
        assert!(evaluate_broadcast_result_acceptance(
            Some(&state),
            task_id,
            "assigned-worker",
            true,
        )
        .is_ok());
    }
}
