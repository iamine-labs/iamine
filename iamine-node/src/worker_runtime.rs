use crate::daemon_runtime::{daemon_is_available, daemon_socket_path, infer_via_daemon};
use crate::infer_observability::{
    elapsed_ms_since, emit_direct_inference_request_received_event,
    emit_worker_task_message_received_event, publish_worker_progress_message,
};
use crate::infer_runtime::mock_real_inference_result;
use crate::metrics::NodeMetrics;
use crate::model_executability::evaluate_worker_model_execution_gate;
use crate::result_observability::{
    emit_worker_result_published_event, emit_worker_task_completed_event,
};
use crate::result_protocol::{publish_worker_result_payload, send_worker_result_direct_response};
use crate::worker_startup_policy::WorkerStartupPolicy;
use crate::{IamineBehaviour, RESULTS_TOPIC};
use iamine_models::{
    InferenceTaskResult, ModelNodeCapabilities, ModelRegistry, ModelStorage, RealInferenceEngine,
    RealInferenceRequest, StreamedToken,
};
use libp2p::{gossipsub, swarm::Swarm, PeerId};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerInferenceMessageKind {
    PubsubInferenceRequest,
    DirectInferenceRequest,
}

impl WorkerInferenceMessageKind {
    fn completion_label(self) -> &'static str {
        match self {
            Self::PubsubInferenceRequest => "Inference",
            Self::DirectInferenceRequest => "Direct inference",
        }
    }

    fn emits_direct_request_received(self) -> bool {
        matches!(self, Self::PubsubInferenceRequest)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WorkerInferenceRuntimeRequest {
    pub(crate) kind: WorkerInferenceMessageKind,
    pub(crate) request_id: String,
    pub(crate) task_id: String,
    pub(crate) attempt_id: String,
    pub(crate) model_id: String,
    pub(crate) prompt: String,
    pub(crate) max_tokens: u32,
    pub(crate) temperature: f32,
    pub(crate) requester_peer: String,
}

impl WorkerInferenceRuntimeRequest {
    pub(crate) fn from_inference_request_value(value: &Value) -> Self {
        let request_id = value["request_id"].as_str().unwrap_or("").to_string();
        Self {
            kind: WorkerInferenceMessageKind::PubsubInferenceRequest,
            task_id: value["task_id"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(request_id.as_str())
                .to_string(),
            attempt_id: value["attempt_id"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(request_id.as_str())
                .to_string(),
            model_id: value["model_id"].as_str().unwrap_or("").to_string(),
            prompt: value["prompt"].as_str().unwrap_or("").to_string(),
            max_tokens: value["max_tokens"].as_u64().unwrap_or(200) as u32,
            temperature: value["temperature"].as_f64().unwrap_or(0.7) as f32,
            requester_peer: value["requester_peer"].as_str().unwrap_or("").to_string(),
            request_id,
        }
    }

    pub(crate) fn from_direct_inference_request_value(
        value: &Value,
        local_peer_id: &str,
        requester_peer: &str,
    ) -> Option<Self> {
        let target = value["target_peer"].as_str().unwrap_or("");
        if target != local_peer_id {
            return None;
        }

        let request_id = value["request_id"].as_str().unwrap_or("").to_string();
        Some(Self {
            kind: WorkerInferenceMessageKind::DirectInferenceRequest,
            task_id: value["task_id"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(request_id.as_str())
                .to_string(),
            attempt_id: value["attempt_id"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(request_id.as_str())
                .to_string(),
            model_id: value["model"]
                .as_str()
                .unwrap_or("tinyllama-1b")
                .to_string(),
            prompt: value["prompt"].as_str().unwrap_or("").to_string(),
            max_tokens: value["max_tokens"].as_u64().unwrap_or(200) as u32,
            temperature: 0.7,
            requester_peer: requester_peer.to_string(),
            request_id,
        })
    }
}

pub(crate) struct WorkerInferenceRuntimeContext<'a> {
    pub(crate) peer_id: &'a PeerId,
    pub(crate) message_topic: &'a str,
    pub(crate) from_peer: &'a str,
    pub(crate) model_storage: &'a ModelStorage,
    pub(crate) node_caps: &'a ModelNodeCapabilities,
    pub(crate) worker_startup_policy: Option<&'a WorkerStartupPolicy>,
    pub(crate) inference_engine: Option<Arc<RealInferenceEngine>>,
    pub(crate) metrics: Arc<RwLock<NodeMetrics>>,
}

pub(crate) async fn handle_worker_inference_request(
    swarm: &mut Swarm<IamineBehaviour>,
    request: WorkerInferenceRuntimeRequest,
    context: WorkerInferenceRuntimeContext<'_>,
) {
    let remote_attempt_started_at = tokio::time::Instant::now();
    let peer_id_string = context.peer_id.to_string();
    let model_execution_gate = evaluate_worker_model_execution_gate(
        &request.model_id,
        context.model_storage,
        context.node_caps,
        context.worker_startup_policy,
    );
    let local_model_available = model_execution_gate.local_model_available;
    emit_worker_task_message_received_event(
        &request.task_id,
        &request.attempt_id,
        context.message_topic,
        context.from_peer,
        "ok",
        &request.model_id,
        local_model_available,
    );
    if request.kind.emits_direct_request_received() {
        emit_direct_inference_request_received_event(
            &request.task_id,
            &request.attempt_id,
            &request.model_id,
            context.from_peer,
            &peer_id_string,
            local_model_available,
        );
    }
    println!(
        "🧠 [Worker] {}: model={} prompt='{}'",
        match request.kind {
            WorkerInferenceMessageKind::PubsubInferenceRequest => "InferenceRequest",
            WorkerInferenceMessageKind::DirectInferenceRequest => "DirectInferenceRequest",
        },
        request.model_id,
        &request.prompt[..request.prompt.len().min(40)]
    );

    if let Some(rejection) = model_execution_gate.rejection {
        println!("{}", rejection.human_warning(&request.model_id));
        return;
    }

    for stage in [
        "task_received",
        "task_message_received",
        "worker_claimed",
        "attempt_started",
    ] {
        publish_worker_progress_message(
            swarm,
            &request.task_id,
            &request.attempt_id,
            &request.request_id,
            &request.model_id,
            &peer_id_string,
            stage,
            elapsed_ms_since(remote_attempt_started_at),
            None,
        );
    }

    let engine_ref = context.inference_engine.clone();
    let metrics_ref = Arc::clone(&context.metrics);
    let registry_clone = ModelRegistry::new();
    let request_id_clone = request.request_id.clone();
    let model_id_for_inference = request.model_id.clone();
    let mock_backend_for_task = model_execution_gate.mock_backend_enabled;
    let real_inference_available_for_task = model_execution_gate.real_inference_available;
    let peer_id_for_task = peer_id_string.clone();
    let prompt_for_task = request.prompt.clone();
    let max_tokens = request.max_tokens;
    let temperature = request.temperature;

    let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);
    let (worker_progress_tx, mut worker_progress_rx) =
        tokio::sync::mpsc::channel::<&'static str>(32);

    let inference_handle = tokio::spawn(async move {
        let progress_tx = worker_progress_tx;
        let req = RealInferenceRequest {
            task_id: request_id_clone.clone(),
            model_id: model_id_for_inference.clone(),
            prompt: prompt_for_task,
            max_tokens,
            temperature,
        };
        let daemon_socket = daemon_socket_path();
        let result = if mock_backend_for_task {
            let _ = progress_tx.try_send("inference_started");
            mock_real_inference_result(
                request_id_clone.clone(),
                model_id_for_inference.clone(),
                req.prompt.clone(),
                req.max_tokens,
            )
        } else if !real_inference_available_for_task {
            return InferenceTaskResult::failure(
                request_id_clone.clone(),
                model_id_for_inference.clone(),
                peer_id_for_task.clone(),
                "real inference unavailable by worker startup policy".to_string(),
            );
        } else if daemon_is_available(&daemon_socket).await {
            let _ = progress_tx.try_send("model_loading");
            let _ = progress_tx.try_send("inference_warmup");
            let _ = progress_tx.try_send("inference_started");
            match infer_via_daemon(&daemon_socket, req, |token| {
                let _ = token_tx.try_send(token);
            })
            .await
            {
                Ok(response) => response.result,
                Err(e) => {
                    return InferenceTaskResult::failure(
                        request_id_clone.clone(),
                        model_id_for_inference.clone(),
                        peer_id_for_task.clone(),
                        e,
                    );
                }
            }
        } else {
            let Some(eng) = engine_ref.clone() else {
                return InferenceTaskResult::failure(
                    request_id_clone.clone(),
                    model_id_for_inference.clone(),
                    peer_id_for_task.clone(),
                    "real inference engine unavailable".to_string(),
                );
            };
            let hash = registry_clone
                .get(&model_id_for_inference)
                .map(|m| m.hash.clone())
                .unwrap_or_default();
            let _ = progress_tx.try_send("model_loading");
            if let Err(e) = eng.load_model(&model_id_for_inference, &hash) {
                return InferenceTaskResult::failure(
                    request_id_clone.clone(),
                    model_id_for_inference.clone(),
                    peer_id_for_task.clone(),
                    e,
                );
            }
            let _ = progress_tx.try_send("model_loaded");
            let _ = progress_tx.try_send("inference_warmup");
            let _ = progress_tx.try_send("inference_started");

            eng.run_inference(req, Some(token_tx)).await
        };

        InferenceTaskResult::success(
            request_id_clone,
            model_id_for_inference,
            result.output,
            result.tokens_generated,
            result.truncated,
            result.continuation_steps,
            result.execution_ms,
            peer_id_for_task,
            result.accelerator_used,
        )
    });

    let mut token_idx = 0u32;
    let mut produced_tokens = 0u64;
    let mut first_token_emitted = false;
    let mut last_token_progress_published_at: Option<tokio::time::Instant> = None;
    while !inference_handle.is_finished() || !token_rx.is_empty() || !worker_progress_rx.is_empty()
    {
        tokio::select! {
            maybe_stage = worker_progress_rx.recv() => {
                if let Some(stage) = maybe_stage {
                    publish_worker_progress_message(
                        swarm,
                        &request.task_id,
                        &request.attempt_id,
                        &request.request_id,
                        &request.model_id,
                        &peer_id_string,
                        stage,
                        elapsed_ms_since(remote_attempt_started_at),
                        None,
                    );
                }
            }
            maybe_token = token_rx.recv() => {
                if let Some(token) = maybe_token {
                    let st = StreamedToken {
                        request_id: request.request_id.clone(),
                        token,
                        index: token_idx,
                        is_final: false,
                    };
                    let _ = swarm.behaviour_mut().gossipsub.publish(
                        gossipsub::IdentTopic::new(RESULTS_TOPIC),
                        serde_json::to_vec(&st.to_gossip_json()).unwrap(),
                    );
                    produced_tokens = produced_tokens.saturating_add(1);
                    let should_publish_token_count = produced_tokens.is_multiple_of(16)
                        || last_token_progress_published_at
                            .map(|last| last.elapsed() >= Duration::from_secs(5))
                            .unwrap_or(true);
                    if !first_token_emitted {
                        first_token_emitted = true;
                        last_token_progress_published_at =
                            Some(tokio::time::Instant::now());
                        publish_worker_progress_message(
                            swarm,
                            &request.task_id,
                            &request.attempt_id,
                            &request.request_id,
                            &request.model_id,
                            &peer_id_string,
                            "first_token_generated",
                            elapsed_ms_since(remote_attempt_started_at),
                            Some(produced_tokens),
                        );
                    } else if should_publish_token_count {
                        last_token_progress_published_at =
                            Some(tokio::time::Instant::now());
                        publish_worker_progress_message(
                            swarm,
                            &request.task_id,
                            &request.attempt_id,
                            &request.request_id,
                            &request.model_id,
                            &peer_id_string,
                            "tokens_generated_count",
                            elapsed_ms_since(remote_attempt_started_at),
                            Some(produced_tokens),
                        );
                    }
                    token_idx += 1;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)), if !first_token_emitted => {
                publish_worker_progress_message(
                    swarm,
                    &request.task_id,
                    &request.attempt_id,
                    &request.request_id,
                    &request.model_id,
                    &peer_id_string,
                    "still_running",
                    elapsed_ms_since(remote_attempt_started_at),
                    None,
                );
            }
        }
    }

    if let Ok(result) = inference_handle.await {
        {
            let mut m = metrics_ref.write().await;
            if result.success {
                m.inference_success(result.execution_ms, result.tokens_generated as u64);
            } else {
                m.inference_failed();
            }
        }
        publish_worker_progress_message(
            swarm,
            &request.task_id,
            &request.attempt_id,
            &request.request_id,
            &request.model_id,
            &peer_id_string,
            "inference_finished",
            elapsed_ms_since(remote_attempt_started_at),
            Some(result.tokens_generated as u64),
        );
        let result_size = result.output.len();
        emit_worker_task_completed_event(
            &request.task_id,
            &request.attempt_id,
            &request.model_id,
            result_size,
            result.success,
            &peer_id_string,
        );
        let mut result_payload = result.to_gossip_json();
        if let Some(map) = result_payload.as_object_mut() {
            map.insert("task_id".to_string(), request.task_id.clone().into());
            map.insert("attempt_id".to_string(), request.attempt_id.clone().into());
            map.insert("worker_peer_id".to_string(), peer_id_string.clone().into());
        }
        publish_worker_progress_message(
            swarm,
            &request.task_id,
            &request.attempt_id,
            &request.request_id,
            &request.model_id,
            &peer_id_string,
            "result_serialized",
            elapsed_ms_since(remote_attempt_started_at),
            Some(result.tokens_generated as u64),
        );
        let payload_size = result_payload.to_string().len();
        let message_id = publish_worker_result_payload(
            swarm,
            &request.task_id,
            &request.attempt_id,
            &request.model_id,
            &peer_id_string,
            &result_payload,
        );
        if message_id.is_some() {
            publish_worker_progress_message(
                swarm,
                &request.task_id,
                &request.attempt_id,
                &request.request_id,
                &request.model_id,
                &peer_id_string,
                "result_published",
                elapsed_ms_since(remote_attempt_started_at),
                Some(result.tokens_generated as u64),
            );
            emit_worker_result_published_event(
                &request.task_id,
                &request.attempt_id,
                &request.model_id,
                RESULTS_TOPIC,
                payload_size,
                message_id.as_deref(),
                &peer_id_string,
            )
        }
        send_worker_result_direct_response(
            swarm,
            &request.requester_peer,
            &request.task_id,
            &request.attempt_id,
            &request.model_id,
            &peer_id_string,
            &result,
        );
        println!(
            "✅ [Worker] {} completada: {} tokens en {}ms",
            request.kind.completion_label(),
            result.tokens_generated,
            result.execution_ms
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_runtime_parses_pubsub_inference_request() {
        let value = serde_json::json!({
            "request_id": "req-1",
            "task_id": "task-1",
            "attempt_id": "attempt-1",
            "model_id": "tinyllama-1b",
            "prompt": "2+2",
            "max_tokens": 42,
            "temperature": 0.2,
            "requester_peer": "controller"
        });

        let request = WorkerInferenceRuntimeRequest::from_inference_request_value(&value);

        assert_eq!(
            request.kind,
            WorkerInferenceMessageKind::PubsubInferenceRequest
        );
        assert_eq!(request.task_id, "task-1");
        assert_eq!(request.attempt_id, "attempt-1");
        assert_eq!(request.model_id, "tinyllama-1b");
        assert_eq!(request.max_tokens, 42);
        assert_eq!(request.temperature, 0.2);
        assert_eq!(request.requester_peer, "controller");
    }

    #[test]
    fn worker_runtime_direct_request_ignores_other_target() {
        let value = serde_json::json!({
            "target_peer": "other-peer",
            "request_id": "req-1",
            "prompt": "2+2"
        });

        assert!(
            WorkerInferenceRuntimeRequest::from_direct_inference_request_value(
                &value,
                "local-peer",
                "controller"
            )
            .is_none()
        );
    }

    #[test]
    fn worker_runtime_parses_direct_inference_request_defaults() {
        let value = serde_json::json!({
            "target_peer": "local-peer",
            "request_id": "req-1",
            "prompt": "2+2"
        });

        let request = WorkerInferenceRuntimeRequest::from_direct_inference_request_value(
            &value,
            "local-peer",
            "controller",
        )
        .expect("direct request");

        assert_eq!(
            request.kind,
            WorkerInferenceMessageKind::DirectInferenceRequest
        );
        assert_eq!(request.task_id, "req-1");
        assert_eq!(request.attempt_id, "req-1");
        assert_eq!(request.model_id, "tinyllama-1b");
        assert_eq!(request.max_tokens, 200);
        assert_eq!(request.temperature, 0.7);
        assert_eq!(request.requester_peer, "controller");
    }
}
