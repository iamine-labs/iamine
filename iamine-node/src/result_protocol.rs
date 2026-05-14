use crate::{log_observability_event, IamineBehaviour, RESULTS_TOPIC, TASK_TOPIC};
use iamine_models::InferenceTaskResult;
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use libp2p::{gossipsub, swarm::Swarm, PeerId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::str::FromStr;

/// Protocolo directo para enviar resultados al origin_peer
/// Evita gossip broadcast — va directo al nodo que originó la tarea
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultRequest {
    pub task_id: String,
    pub attempt_id: String,
    pub model_id: String,
    pub worker_id: String,
    pub success: bool,
    pub result: String,
    pub tokens_generated: u64,
    pub execution_ms: u64,
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultResponse {
    pub acknowledged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BroadcastTaskResultMessage {
    pub(crate) task_id: String,
    pub(crate) task_type: String,
    pub(crate) worker_peer_id: String,
    pub(crate) success: bool,
    pub(crate) output: String,
    pub(crate) origin_peer: Option<String>,
    pub(crate) elapsed_ms: u64,
}

impl BroadcastTaskResultMessage {
    pub(crate) fn from_pubsub_value(value: &Value) -> Self {
        Self {
            task_id: value["task_id"].as_str().unwrap_or("").to_string(),
            task_type: value["task_type"].as_str().unwrap_or("").to_string(),
            worker_peer_id: value["worker_peer_id"]
                .as_str()
                .or_else(|| value["worker_id"].as_str())
                .unwrap_or("")
                .to_string(),
            success: value["success"].as_bool().unwrap_or(false),
            output: value["output"].as_str().unwrap_or("").to_string(),
            origin_peer: value["origin_peer"]
                .as_str()
                .or_else(|| value["controller_peer_id"].as_str())
                .map(str::to_string),
            elapsed_ms: value["elapsed_ms"]
                .as_u64()
                .or_else(|| value["execution_ms"].as_u64())
                .unwrap_or(0),
        }
    }
}

pub(super) fn publish_worker_result_payload(
    swarm: &mut Swarm<IamineBehaviour>,
    task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    result_payload: &serde_json::Value,
) -> Option<String> {
    let mut first_message_id = None;
    for topic in [RESULTS_TOPIC, TASK_TOPIC] {
        match swarm.behaviour_mut().gossipsub.publish(
            gossipsub::IdentTopic::new(topic),
            serde_json::to_vec(result_payload).unwrap_or_default(),
        ) {
            Ok(message_id) => {
                if first_message_id.is_none() {
                    first_message_id = Some(message_id.to_string());
                }
            }
            Err(error) => log_observability_event(
                LogLevel::Error,
                "result_publish_failed",
                task_id,
                Some(task_id),
                Some(model_id),
                Some(TASK_DISPATCH_UNCONFIRMED_001),
                {
                    let mut fields = Map::new();
                    fields.insert("attempt_id".to_string(), attempt_id.into());
                    fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                    fields.insert("topic".to_string(), topic.into());
                    fields.insert("error".to_string(), error.to_string().into());
                    fields
                },
            ),
        }
    }
    first_message_id
}

pub(super) fn send_worker_result_direct_response(
    swarm: &mut Swarm<IamineBehaviour>,
    origin_peer_id: &str,
    task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    result: &InferenceTaskResult,
) {
    if origin_peer_id.trim().is_empty() || origin_peer_id == worker_peer_id {
        return;
    }

    match PeerId::from_str(origin_peer_id) {
        Ok(origin_peer) => {
            let request = TaskResultRequest {
                task_id: task_id.to_string(),
                attempt_id: attempt_id.to_string(),
                model_id: model_id.to_string(),
                worker_id: worker_peer_id.to_string(),
                success: result.success,
                result: result.output.clone(),
                tokens_generated: result.tokens_generated as u64,
                execution_ms: result.execution_ms,
                attempts: 1,
            };
            let request_id = swarm
                .behaviour_mut()
                .result_response
                .send_request(&origin_peer, request);
            log_observability_event(
                LogLevel::Info,
                "remote_result_direct_publish_attempt",
                task_id,
                Some(task_id),
                Some(model_id),
                None,
                {
                    let mut fields = Map::new();
                    fields.insert("attempt_id".to_string(), attempt_id.into());
                    fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                    fields.insert("origin_peer_id".to_string(), origin_peer_id.into());
                    fields.insert("request_id".to_string(), format!("{:?}", request_id).into());
                    fields
                },
            );
        }
        Err(error) => {
            log_observability_event(
                LogLevel::Warn,
                "remote_result_direct_publish_failed",
                task_id,
                Some(task_id),
                Some(model_id),
                Some(TASK_DISPATCH_UNCONFIRMED_001),
                {
                    let mut fields = Map::new();
                    fields.insert("attempt_id".to_string(), attempt_id.into());
                    fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                    fields.insert("origin_peer_id".to_string(), origin_peer_id.into());
                    fields.insert("error".to_string(), error.to_string().into());
                    fields
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcast_task_result_message_preserves_wire_aliases() {
        let payload = serde_json::json!({
            "type": "TaskResult",
            "task_id": "task-1",
            "task_type": "reverse_string",
            "worker_id": "worker-a",
            "controller_peer_id": "controller",
            "success": true,
            "output": "abc",
            "execution_ms": 12,
        });

        let result = BroadcastTaskResultMessage::from_pubsub_value(&payload);

        assert_eq!(result.task_id, "task-1");
        assert_eq!(result.task_type, "reverse_string");
        assert_eq!(result.worker_peer_id, "worker-a");
        assert_eq!(result.origin_peer.as_deref(), Some("controller"));
        assert!(result.success);
        assert_eq!(result.output, "abc");
        assert_eq!(result.elapsed_ms, 12);
    }
}
