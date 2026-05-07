use serde::{Deserialize, Serialize};

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
