use crate::broadcast_runtime::{evaluate_broadcast_result_acceptance, BroadcastOfferState};
use crate::result_observability::{
    emit_broadcast_final_outcome_success_event, emit_broadcast_recovery_cancelled_event,
    emit_broadcast_result_received_events, emit_broadcast_result_rejected_event,
    should_print_result_output, BroadcastResultReceived,
};
use crate::result_protocol::BroadcastTaskResultMessage;
use crate::task_events::{
    emit_task_lifecycle_completed, emit_task_lifecycle_finalized,
    emit_task_lifecycle_result_received,
};
use crate::task_scheduler::TaskScheduler;
use crate::{log_observability_event, RESULTS_TOPIC};
use iamine_network::LogLevel;
use serde_json::{Map, Value};
use std::sync::Arc;

pub(crate) async fn handle_controller_task_result(
    msg: &Value,
    message_topic: &str,
    scheduler: &Arc<TaskScheduler>,
    broadcast_offer_state: &mut Option<BroadcastOfferState>,
) -> bool {
    if message_topic != RESULTS_TOPIC {
        return false;
    }

    let result_message = BroadcastTaskResultMessage::from_pubsub_value(msg);
    let assigned_worker = broadcast_offer_state
        .as_ref()
        .and_then(|state| state.assigned_worker.as_deref())
        .map(str::to_string);
    let acceptance = evaluate_broadcast_result_acceptance(
        broadcast_offer_state.as_ref(),
        &result_message.task_id,
        &result_message.worker_peer_id,
        result_message.success,
    );
    let accepted = acceptance.is_ok();
    let rejection_reason = acceptance.err();
    let received = BroadcastResultReceived {
        task_id: &result_message.task_id,
        task_type: &result_message.task_type,
        worker_peer_id: &result_message.worker_peer_id,
        assigned_worker_peer_id: assigned_worker.as_deref(),
        origin_peer: result_message.origin_peer.as_deref(),
        success: result_message.success,
        output: &result_message.output,
        elapsed_ms: result_message.elapsed_ms,
        transport: "pubsub",
        accepted,
        rejection_reason,
    };
    emit_broadcast_result_received_events(&received);

    if let Some(reason) = rejection_reason {
        emit_broadcast_result_rejected_event(&received, reason);
        return false;
    }

    emit_task_lifecycle_result_received(
        &result_message.task_id,
        &result_message.task_type,
        &result_message.worker_peer_id,
        result_message.success,
        &result_message.output,
        result_message.elapsed_ms,
    );
    if let Some(state) = broadcast_offer_state.as_mut() {
        if state.task_id == result_message.task_id {
            state.mark_result_accepted();
        }
    }
    scheduler
        .mark_completed(&result_message.task_id, &result_message.worker_peer_id)
        .await;
    emit_broadcast_recovery_cancelled_event(
        &result_message.task_id,
        &result_message.worker_peer_id,
    );
    emit_broadcast_final_outcome_success_event(
        &result_message.task_id,
        &result_message.worker_peer_id,
        &result_message.output,
    );
    emit_task_lifecycle_completed(
        &result_message.task_id,
        &result_message.task_type,
        &result_message.worker_peer_id,
        &result_message.output,
        result_message.elapsed_ms,
    );
    emit_task_lifecycle_finalized(
        &result_message.task_id,
        &result_message.task_type,
        &result_message.worker_peer_id,
        "success",
        &result_message.output,
    );
    log_controller_task_completed(&result_message);
    if should_print_result_output(&result_message.output) {
        println!("{}", result_message.output);
    }
    println!(
        "✅ [Broadcast] Resultado aceptado: task_id={} worker={} success=true",
        result_message.task_id, result_message.worker_peer_id
    );
    true
}

fn log_controller_task_completed(result_message: &BroadcastTaskResultMessage) {
    log_observability_event(
        LogLevel::Info,
        "task_completed",
        &result_message.task_id,
        Some(&result_message.task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("success".to_string(), true.into());
            fields.insert(
                "worker_peer_id".to_string(),
                result_message.worker_peer_id.clone().into(),
            );
            fields.insert("transport".to_string(), "pubsub".into());
            fields.insert("source".to_string(), "broadcast_task_result".into());
            fields
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast_runtime::BroadcastOfferState;

    #[test]
    fn controller_result_accepts_assigned_worker() {
        let mut state = BroadcastOfferState::new("task-1".to_string());
        state.mark_assigned("worker-a");

        let accepted =
            evaluate_broadcast_result_acceptance(Some(&state), "task-1", "worker-a", true);

        assert!(accepted.is_ok());
    }

    #[test]
    fn controller_result_rejects_wrong_worker() {
        let mut state = BroadcastOfferState::new("task-1".to_string());
        state.mark_assigned("worker-a");

        let accepted =
            evaluate_broadcast_result_acceptance(Some(&state), "task-1", "worker-b", true);

        assert_eq!(accepted, Err("wrong_worker"));
    }

    #[test]
    fn controller_result_rejects_duplicate_result() {
        let mut state = BroadcastOfferState::new("task-1".to_string());
        state.mark_assigned("worker-a");
        state.mark_result_accepted();

        let accepted =
            evaluate_broadcast_result_acceptance(Some(&state), "task-1", "worker-a", true);

        assert_eq!(accepted, Err("duplicate_result"));
    }
}
