use crate::broadcast_protocol::{build_broadcast_task_result_payload, BroadcastResultToPublish};
use crate::broadcast_worker::{
    emit_broadcast_result_publish_attempt_event, emit_broadcast_result_publish_failed_event,
    emit_broadcast_result_published_event,
};
use crate::{IamineBehaviour, RESULTS_TOPIC};
use libp2p::{gossipsub, swarm::Swarm};

pub(crate) struct WorkerBroadcastResultPayload {
    pub(crate) payload_bytes: Vec<u8>,
    pub(crate) display_size: usize,
}

pub(crate) fn prepare_worker_broadcast_result_payload(
    result: &BroadcastResultToPublish,
) -> WorkerBroadcastResultPayload {
    let payload = build_broadcast_task_result_payload(result);
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let display_size = payload.to_string().len();

    WorkerBroadcastResultPayload {
        payload_bytes,
        display_size,
    }
}

pub(crate) fn publish_worker_broadcast_task_result(
    swarm: &mut Swarm<IamineBehaviour>,
    broadcast_result: BroadcastResultToPublish,
) {
    let prepared = prepare_worker_broadcast_result_payload(&broadcast_result);
    emit_broadcast_result_publish_attempt_event(&broadcast_result, prepared.payload_bytes.len());
    match swarm.behaviour_mut().gossipsub.publish(
        gossipsub::IdentTopic::new(RESULTS_TOPIC),
        prepared.payload_bytes,
    ) {
        Ok(message_id) => {
            emit_broadcast_result_published_event(
                &broadcast_result,
                &message_id.to_string(),
                prepared.display_size,
            );
            println!(
                "📤 [Worker] TaskResult publicado: task_id={} success={}",
                broadcast_result.task_id, broadcast_result.success
            );
        }
        Err(error) => {
            emit_broadcast_result_publish_failed_event(&broadcast_result, &error.to_string(), true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn worker_result_prepare_contains_required_fields() {
        let result = BroadcastResultToPublish {
            task_id: "task-1".to_string(),
            task_type: "reverse_string".to_string(),
            worker_peer_id: "worker-a".to_string(),
            origin_peer: "controller".to_string(),
            success: true,
            output: "cba".to_string(),
            elapsed_ms: 12,
            error: None,
            attempts: 1,
            source: "broadcast_task_assign",
        };

        let prepared = prepare_worker_broadcast_result_payload(&result);

        let payload: Value = serde_json::from_slice(&prepared.payload_bytes).unwrap();
        assert_eq!(payload["type"], "TaskResult");
        assert_eq!(payload["task_id"], "task-1");
        assert_eq!(payload["worker_peer_id"], "worker-a");
        assert_eq!(payload["success"], true);
        assert_eq!(payload["output"], "cba");
        assert!(!prepared.payload_bytes.is_empty());
    }
}
