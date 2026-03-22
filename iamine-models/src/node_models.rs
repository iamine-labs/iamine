use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelId {
    pub id: String,
    pub version: String,
    pub sha256: String,
    pub size_bytes: u64,
}

/// Modelos disponibles en un nodo peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeModels {
    pub node_id: String,
    pub models: Vec<ModelId>,
    pub timestamp: u64,
}

impl NodeModels {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            models: vec![],
            timestamp: now_secs(),
        }
    }

    pub fn has_model(&self, model_id: &str) -> bool {
        self.models.iter().any(|m| m.id == model_id)
    }
}

/// Registry de modelos disponibles en la red P2P
pub struct PeerModelRegistry {
    /// node_id → modelos disponibles
    peers: HashMap<String, NodeModels>,
}

impl PeerModelRegistry {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn update_peer(&mut self, node_models: NodeModels) {
        println!(
            "📦 [PeerRegistry] {} tiene {} modelos",
            &node_models.node_id[..8.min(node_models.node_id.len())],
            node_models.models.len()
        );
        self.peers.insert(node_models.node_id.clone(), node_models);
    }

    /// Peers que tienen un modelo específico
    pub fn peers_with_model(&self, model_id: &str) -> Vec<&NodeModels> {
        self.peers
            .values()
            .filter(|n| n.has_model(model_id))
            .collect()
    }

    /// Estrategia de descarga: oficial primero, luego peers
    pub fn download_strategy(&self, model_id: &str, official_url: &str) -> DownloadStrategy {
        let peers = self.peers_with_model(model_id);
        if peers.is_empty() {
            DownloadStrategy::Official(official_url.to_string())
        } else {
            let peer_ids: Vec<String> = peers.iter().map(|n| n.node_id.clone()).collect();
            DownloadStrategy::PeerFirst {
                peers: peer_ids,
                fallback: official_url.to_string(),
            }
        }
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }
}

#[derive(Debug, Clone)]
pub enum DownloadStrategy {
    Official(String),
    PeerFirst {
        peers: Vec<String>,
        fallback: String,
    },
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
