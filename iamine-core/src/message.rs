use crate::node::NodeCapabilities;
use crate::result::TaskResult;
use crate::task::Task;
use serde::{Deserialize, Serialize};

/// Mensajes P2P entre nodos
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IaMineMessage {
    /// Worker anuncia sus capacidades al conectarse
    WorkerAnnounce {
        peer_id: String,
        capabilities: NodeCapabilities,
    },

    /// Cliente broadcast tarea a la red
    TaskOffer {
        task: Task,
        requester_id: String,
        origin_peer: String, // ← nuevo: PeerId del broadcaster para result routing
    },

    /// Worker hace bid (se postula para ejecutar)
    TaskBid {
        task_id: String,
        worker_id: String,
        reputation_score: u32,
        available_slots: usize,
        estimated_ms: u64,
    },

    /// Scheduler/cliente asigna tarea a UN worker
    TaskAssign {
        task_id: String,
        assigned_worker: String,
        origin_peer: String, // ← nuevo: para que el worker sepa a quién enviar el resultado
    },

    /// Worker rechaza (ya ocupado, cambió de estado)
    TaskRejected {
        task_id: String,
        worker_id: String,
        reason: String,
    },

    /// Inference dirigida a un worker específico (v0.6.1 smart routing)
    DirectInferenceRequest {
        request_id: String,
        target_peer: String,
        model: String,
        prompt: String,
        max_tokens: u32,
    },

    /// Worker envía resultado
    TaskResult(TaskResult),

    /// Heartbeat periódico del worker cada 5s
    Heartbeat {
        peer_id: String,
        active_tasks: usize,
        available_slots: usize,
        reputation_score: u32,
        uptime_secs: u64,
        avg_latency_ms: f64,
    },

    /// Cancelación de tarea por timeout
    TaskCancelled { task_id: String, reason: String },
}
