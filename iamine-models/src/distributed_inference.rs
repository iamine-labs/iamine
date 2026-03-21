use serde::{Deserialize, Serialize};

/// Petición de inferencia distribuida
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceTask {
    pub request_id: String,
    pub model_id: String,
    pub prompt: String,
    pub max_tokens: u32,
    pub temperature: f32,
    pub requester_peer: String,
}

/// Resultado de inferencia distribuida
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceTaskResult {
    pub request_id: String,
    pub model_id: String,
    pub output: String,
    pub tokens_generated: u32,
    pub truncated: bool,
    pub continuation_steps: usize,
    pub execution_ms: u64,
    pub worker_peer: String,
    pub accelerator: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Token individual streamed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamedToken {
    pub request_id: String,
    pub token: String,
    pub index: u32,
    pub is_final: bool,
}

/// Petición de inferencia directa
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectInferenceRequest {
    pub request_id: String,
    pub target_peer: String,
    pub model: String,
    pub prompt: String,
    pub max_tokens: u32,
}

impl InferenceTask {
    pub fn new(
        request_id: String,
        model_id: String,
        prompt: String,
        max_tokens: u32,
        requester_peer: String,
    ) -> Self {
        Self {
            request_id,
            model_id,
            prompt,
            max_tokens,
            temperature: 0.7,
            requester_peer,
        }
    }

    /// Serializar para gossipsub
    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "InferenceRequest",
            "request_id": self.request_id,
            "model_id": self.model_id,
            "prompt": self.prompt,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "requester_peer": self.requester_peer,
        })
    }
}

impl InferenceTaskResult {
    pub fn success(
        request_id: String,
        model_id: String,
        output: String,
        tokens: u32,
        truncated: bool,
        continuation_steps: usize,
        ms: u64,
        worker_peer: String,
        accelerator: String,
    ) -> Self {
        Self {
            request_id, model_id, output, tokens_generated: tokens, truncated, continuation_steps,
            execution_ms: ms, worker_peer, accelerator,
            success: true, error: None,
        }
    }

    pub fn failure(request_id: String, model_id: String, worker_peer: String, error: String) -> Self {
        Self {
            request_id, model_id, output: String::new(), tokens_generated: 0, truncated: false, continuation_steps: 0,
            execution_ms: 0, worker_peer, accelerator: "none".to_string(),
            success: false, error: Some(error),
        }
    }

    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "InferenceResult",
            "request_id": self.request_id,
            "model_id": self.model_id,
            "output": self.output,
            "tokens_generated": self.tokens_generated,
            "truncated": self.truncated,
            "continuation_steps": self.continuation_steps,
            "execution_ms": self.execution_ms,
            "worker_peer": self.worker_peer,
            "accelerator": self.accelerator,
            "success": self.success,
            "error": self.error,
        })
    }
}

impl StreamedToken {
    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "InferenceToken",
            "request_id": self.request_id,
            "token": self.token,
            "index": self.index,
            "is_final": self.is_final,
        })
    }
}

impl DirectInferenceRequest {
    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "DirectInferenceRequest",
            "request_id": self.request_id,
            "target_peer": self.target_peer,
            "model": self.model,
            "prompt": self.prompt,
            "max_tokens": self.max_tokens,
        })
    }
}
