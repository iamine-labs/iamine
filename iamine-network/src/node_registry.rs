use iamine_models::AcceleratorType;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::RwLock;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NodeCapability {
    pub peer_id: String,
    pub cpu_score: u64,
    pub ram_gb: u32,
    pub accelerator: AcceleratorType,
    pub models: Vec<String>,
    pub worker_slots: u32,
    pub active_tasks: u32,
    pub latency_ms: u32,
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilityHeartbeat {
    pub peer_id: String,
    pub cpu_score: u64,
    pub ram_gb: u32,
    pub accelerator: String,
    pub models: Vec<String>,
    pub worker_slots: u32,
    pub active_tasks: u32,
    pub latency_ms: u32,
}

impl NodeCapabilityHeartbeat {
    pub fn into_capability(self) -> NodeCapability {
        let accelerator = match self.accelerator.as_str() {
            "Metal" => AcceleratorType::Metal,
            "CUDA" => AcceleratorType::CUDA,
            "ROCm" => AcceleratorType::ROCm,
            "Vulkan" => AcceleratorType::Vulkan,
            _ => AcceleratorType::CPU,
        };

        NodeCapability {
            peer_id: self.peer_id,
            cpu_score: self.cpu_score,
            ram_gb: self.ram_gb,
            accelerator,
            models: self.models,
            worker_slots: self.worker_slots,
            active_tasks: self.active_tasks,
            latency_ms: self.latency_ms.max(1),
            last_seen: Instant::now(),
        }
    }
}

#[derive(Default)]
pub struct NodeRegistry {
    nodes: HashMap<PeerId, NodeCapability>,
}

pub type SharedNodeRegistry = Arc<RwLock<NodeRegistry>>;

impl NodeRegistry {
    pub fn new() -> Self {
        Self { nodes: HashMap::new() }
    }

    pub fn register_node(&mut self, cap: NodeCapability) -> Option<PeerId> {
        let pid: PeerId = cap.peer_id.parse().ok()?;
        self.nodes.insert(pid.clone(), cap);
        Some(pid)
    }

    pub fn update_node(&mut self, cap: NodeCapability) -> Option<PeerId> {
        self.register_node(cap)
    }

    pub fn update_from_heartbeat(&mut self, hb: NodeCapabilityHeartbeat) -> Option<PeerId> {
        self.update_node(hb.into_capability())
    }

    pub fn remove_node(&mut self, peer_id: &PeerId) {
        self.nodes.remove(peer_id);
    }

    pub fn get_nodes_with_model(&self, model: &str) -> Vec<(PeerId, NodeCapability)> {
        self.nodes
            .iter()
            .filter(|(_, c)| c.models.iter().any(|m| m == model))
            .map(|(p, c)| (p.clone(), c.clone()))
            .collect()
    }

    pub fn all_nodes(&self) -> Vec<(PeerId, NodeCapability)> {
        self.nodes.iter().map(|(p, c)| (p.clone(), c.clone())).collect()
    }

    pub fn select_best_node(&self, model: &str) -> Option<PeerId> {
        let candidates = self.get_nodes_with_model(model);
        if candidates.is_empty() {
            return None;
        }

        candidates
            .into_iter()
            .filter(|(_, c)| c.worker_slots > c.active_tasks)
            .max_by(|(_, a), (_, b)| score(a).partial_cmp(&score(b)).unwrap())
            .map(|(p, _)| p)
    }
}

fn score(c: &NodeCapability) -> f64 {
    let free_slots = c.worker_slots.saturating_sub(c.active_tasks) as f64;
    let cpu = c.cpu_score as f64 * 0.5;
    let lat = (1.0 / (c.latency_ms.max(1) as f64)) * 1000.0 * 0.3;
    let cap = free_slots * 0.2;
    cpu + lat + cap
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(peer: &str, cpu: u64, model: &str, slots: u32, active: u32, latency_ms: u32) -> NodeCapability {
        NodeCapability {
            peer_id: peer.to_string(),
            cpu_score: cpu,
            ram_gb: 16,
            accelerator: AcceleratorType::CPU,
            models: vec![model.to_string()],
            worker_slots: slots,
            active_tasks: active,
            latency_ms,
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn test_node_registry_register() {
        let mut r = NodeRegistry::new();
        let p = PeerId::random();
        let _ = r.register_node(cap(&p.to_string(), 100, "tinyllama-1b", 8, 0, 10));
        assert_eq!(r.all_nodes().len(), 1);
    }

    #[test]
    fn test_node_registry_update() {
        let mut r = NodeRegistry::new();
        let p = PeerId::random().to_string();
        let _ = r.register_node(cap(&p, 100, "tinyllama-1b", 8, 0, 20));
        let _ = r.update_node(cap(&p, 200, "tinyllama-1b", 8, 1, 5));
        assert_eq!(r.all_nodes().len(), 1);
        assert_eq!(r.all_nodes()[0].1.cpu_score, 200);
    }

    #[test]
    fn test_node_filter_by_model() {
        let mut r = NodeRegistry::new();
        let p1 = PeerId::random().to_string();
        let p2 = PeerId::random().to_string();
        let _ = r.register_node(cap(&p1, 100, "tinyllama-1b", 8, 0, 20));
        let _ = r.register_node(cap(&p2, 100, "llama3-3b", 8, 0, 20));
        assert_eq!(r.get_nodes_with_model("tinyllama-1b").len(), 1);
    }

    #[test]
    fn test_select_best_node() {
        let mut r = NodeRegistry::new();
        let p1 = PeerId::random().to_string();
        let p2 = PeerId::random().to_string();

        let _ = r.register_node(cap(&p1, 120_000, "tinyllama-1b", 8, 6, 30));
        let _ = r.register_node(cap(&p2, 180_000, "tinyllama-1b", 8, 1, 8));

        let best = r.select_best_node("tinyllama-1b");
        assert!(best.is_some());
        assert_eq!(best.unwrap().to_string(), p2);
    }
}
