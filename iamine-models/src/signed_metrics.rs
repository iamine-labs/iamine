use serde::{Deserialize, Serialize};

/// Métricas que el nodo envía — SIN reputación (la calcula el servidor)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetricsPayload {
    pub node_id: String,
    pub timestamp: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub uptime_secs: u64,
    pub avg_execution_ms: f64,
    pub cpu_score: f64,
    pub ram_gb: u64,
    pub active_inference_slots: usize,
    // ← NO incluye reputation_score — lo calcula el servidor
}

/// Métricas firmadas con Ed25519
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedNodeMetrics {
    pub node_id: String,
    pub timestamp: u64,
    pub payload: NodeMetricsPayload,
    pub signature: String,  // hex-encoded Ed25519 signature
}

impl SignedNodeMetrics {
    /// Crear y firmar métricas con el keypair del nodo
    pub fn sign(
        payload: NodeMetricsPayload,
        keypair: &libp2p::identity::Keypair,
    ) -> Result<Self, String> {
        let payload_bytes = serde_json::to_vec(&payload)
            .map_err(|e| e.to_string())?;

        let signature = keypair.sign(&payload_bytes)
            .map_err(|e| format!("Sign error: {}", e))?;

        Ok(Self {
            node_id: payload.node_id.clone(),
            timestamp: payload.timestamp,
            payload,
            signature: hex::encode(signature),
        })
    }

    /// Verificar firma usando la public key del nodo
    pub fn verify(&self, public_key: &libp2p::identity::PublicKey) -> bool {
        let Ok(payload_bytes) = serde_json::to_vec(&self.payload) else { return false };
        let Ok(sig_bytes) = hex::decode(&self.signature) else { return false };
        public_key.verify(&payload_bytes, &sig_bytes)
    }
}

impl NodeMetricsPayload {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            timestamp: now_secs(),
            tasks_completed: 0,
            tasks_failed: 0,
            uptime_secs: 0,
            avg_execution_ms: 0.0,
            cpu_score: 0.0,
            ram_gb: 0,
            active_inference_slots: 0,
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
