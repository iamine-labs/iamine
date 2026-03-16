use std::collections::HashMap;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use crate::model_capability_matcher::{
    is_node_compatible_with_model, ModelHardwareRequirements, NodeHardwareProfile,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilityHeartbeat {
    pub peer_id: String,
    pub cpu_score: u64,
    pub ram_gb: u32,
    pub gpu_available: bool,
    pub storage_available_gb: u32,
    pub accelerator: String,
    pub models: Vec<String>,
    pub worker_slots: u32,
    pub active_tasks: u32,
    pub latency_ms: u32,
}

#[derive(Debug, Clone)]
pub struct NodeCapability {
    pub peer_id: String,
    pub cpu_score: u64,
    pub ram_gb: u32,
    pub gpu_available: bool,
    pub storage_available_gb: u32,
    pub accelerator: String,
    pub models: Vec<String>,
    pub worker_slots: u32,
    pub active_tasks: u32,
    pub latency_ms: u32,
    pub last_seen: Instant,
}

pub struct NodeRegistry {
    nodes: HashMap<String, NodeCapability>,
}

pub type SharedNodeRegistry = std::sync::Arc<tokio::sync::RwLock<NodeRegistry>>;

impl NodeRegistry {
    pub fn new() -> Self {
        Self { nodes: HashMap::new() }
    }

    pub fn update_from_heartbeat(&mut self, hb: NodeCapabilityHeartbeat) -> &NodeCapability {
        let cap = NodeCapability {
            peer_id: hb.peer_id.clone(),
            cpu_score: hb.cpu_score,
            ram_gb: hb.ram_gb,
            gpu_available: hb.gpu_available,
            storage_available_gb: hb.storage_available_gb,
            accelerator: hb.accelerator,
            models: hb.models,
            worker_slots: hb.worker_slots,
            active_tasks: hb.active_tasks,
            latency_ms: hb.latency_ms,
            last_seen: Instant::now(),
        };
        self.nodes.insert(hb.peer_id.clone(), cap);
        self.nodes.get(&hb.peer_id).unwrap()
    }

    pub fn register_node(&mut self, cap: NodeCapability) {
        self.nodes.insert(cap.peer_id.clone(), cap);
    }

    pub fn select_best_node(&self, model_id: &str) -> Option<String> {
        self.select_best_node_for_model(model_id)
    }

    pub fn select_best_node_for_model(&self, model_id: &str) -> Option<String> {
        let requirements = ModelHardwareRequirements::for_model(model_id)?;

        let mut candidates: Vec<&NodeCapability> = self.nodes
            .values()
            .filter(|node| node.models.iter().any(|m| m == model_id))
            .filter(|node| {
                let profile = NodeHardwareProfile {
                    ram_gb: node.ram_gb,
                    gpu_available: node.gpu_available,
                    storage_available_gb: node.storage_available_gb,
                };
                is_node_compatible_with_model(&profile, &requirements)
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by_key(|node| {
            let free_slots = node.worker_slots.saturating_sub(node.active_tasks) as u64;
            let slot_score = free_slots * 40;
            let cpu_score = (node.cpu_score / 10_000).min(40);
            let latency_score = if node.latency_ms == 0 { 20u64 }
                else { (1000 / node.latency_ms.max(1) as u64).min(20) };
            std::cmp::Reverse(slot_score + cpu_score + latency_score)
        });

        candidates.first().map(|n| n.peer_id.clone())
    }

    pub fn all_nodes(&self) -> Vec<(String, NodeCapability)> {
        self.nodes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn nodes_with_model(&self, model: &str) -> Vec<(String, NodeCapability)> {
        self.nodes
            .iter()
            .filter(|(_, c)| c.models.iter().any(|m| m == model))
            .map(|(p, c)| (p.clone(), c.clone()))
            .collect()
    }

    pub fn remove_node(&mut self, peer_id: &str) {
        self.nodes.remove(peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cap(peer_id: &str, cpu_score: u64, model: &str, ram_gb: u32, active_tasks: u32, latency_ms: u32) -> NodeCapability {
        NodeCapability {
            peer_id: peer_id.to_string(),
            cpu_score,
            ram_gb,
            gpu_available: false,
            storage_available_gb: 50,
            accelerator: "CPU".to_string(),
            models: vec![model.to_string()],
            worker_slots: 8,
            active_tasks,
            latency_ms,
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn test_node_registry_register() {
        let mut r = NodeRegistry::new();
        r.register_node(make_cap("peer1", 100, "tinyllama-1b", 8, 0, 10));
        assert_eq!(r.nodes.len(), 1);
    }

    #[test]
    fn test_node_registry_update() {
        let mut r = NodeRegistry::new();
        r.register_node(make_cap("peer1", 100, "tinyllama-1b", 8, 0, 20));
        r.register_node(make_cap("peer1", 200, "tinyllama-1b", 8, 1, 5));
        assert_eq!(r.nodes.len(), 1);
        assert_eq!(r.nodes["peer1"].cpu_score, 200);
    }

    #[test]
    fn test_node_filter_by_model() {
        let mut r = NodeRegistry::new();
        r.register_node(make_cap("peer1", 100, "tinyllama-1b", 8, 0, 20));
        r.register_node(make_cap("peer2", 100, "llama3-3b", 8, 0, 20));
        let found = r.nodes_with_model("tinyllama-1b");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, "peer1");
    }

    #[test]
    fn test_select_best_node() {
        let mut r = NodeRegistry::new();
        r.register_node(make_cap("peer1", 120_000, "tinyllama-1b", 8, 6, 30));
        r.register_node(make_cap("peer2", 180_000, "tinyllama-1b", 8, 1, 8));
        let best = r.select_best_node_for_model("tinyllama-1b");
        assert!(best.is_some());
        assert_eq!(best.unwrap(), "peer2"); // más slots libres y menor latencia
    }
}
