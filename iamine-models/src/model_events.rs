use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInstalledEvent {
    pub node_id: String,
    pub model_id: String,
    pub timestamp: u64,
    pub size_bytes: u64,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRemovedEvent {
    pub node_id: String,
    pub model_id: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesUpdatedEvent {
    pub node_id: String,
    pub timestamp: u64,
    pub cpu_cores: u32,
    pub ram_gb: u32,
    pub gpu_type: Option<String>,
    pub worker_slots: u32,
    pub supported_models: Vec<String>,
    pub storage_available_gb: u32,
}

impl ModelInstalledEvent {
    pub fn new(node_id: &str, model_id: &str, size_bytes: u64, hash: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            model_id: model_id.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            size_bytes,
            hash: hash.to_string(),
        }
    }

    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "ModelInstalled",
            "node_id": self.node_id,
            "model_id": self.model_id,
            "timestamp": self.timestamp,
            "size_bytes": self.size_bytes,
            "hash": self.hash,
        })
    }
}

impl ModelRemovedEvent {
    pub fn new(node_id: &str, model_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            model_id: model_id.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "ModelRemoved",
            "node_id": self.node_id,
            "model_id": self.model_id,
            "timestamp": self.timestamp,
        })
    }
}

impl CapabilitiesUpdatedEvent {
    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "CapabilitiesUpdated",
            "node_id": self.node_id,
            "timestamp": self.timestamp,
            "cpu_cores": self.cpu_cores,
            "ram_gb": self.ram_gb,
            "gpu_type": self.gpu_type,
            "worker_slots": self.worker_slots,
            "supported_models": self.supported_models,
            "storage_available_gb": self.storage_available_gb,
        })
    }
}
