use crate::node_health::NodeHealth;
use crate::scheduler::IntelligentScheduler;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

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
    pub cluster_id: Option<String>, // ← NEW
    pub health: NodeHealth,
}

pub struct NodeRegistry {
    nodes: HashMap<String, NodeCapability>,
}

pub type SharedNodeRegistry = std::sync::Arc<tokio::sync::RwLock<NodeRegistry>>;

impl NodeRegistry {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub fn update_from_heartbeat(&mut self, hb: NodeCapabilityHeartbeat) -> &NodeCapability {
        let existing_cluster = self
            .nodes
            .get(&hb.peer_id)
            .and_then(|n| n.cluster_id.clone());
        let existing_health = self
            .nodes
            .get(&hb.peer_id)
            .map(|n| n.health.clone())
            .unwrap_or_default();

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
            cluster_id: existing_cluster, // ← preserve
            health: existing_health,
        };
        self.nodes.insert(hb.peer_id.clone(), cap);
        self.nodes.get(&hb.peer_id).unwrap()
    }

    pub fn register_node(&mut self, cap: NodeCapability) {
        self.nodes.insert(cap.peer_id.clone(), cap);
    }

    pub fn set_cluster(&mut self, peer_id: &str, cluster_id: &str) {
        if let Some(node) = self.nodes.get_mut(peer_id) {
            node.cluster_id = Some(cluster_id.to_string());
        }
    }

    pub fn record_success(&mut self, peer_id: &str, latency_ms: u64) {
        if let Some(node) = self.nodes.get_mut(peer_id) {
            node.health.record_success(latency_ms);
        }
    }

    pub fn record_failure(&mut self, peer_id: &str) {
        if let Some(node) = self.nodes.get_mut(peer_id) {
            node.health.record_failure();
        }
    }

    pub fn record_timeout(&mut self, peer_id: &str) {
        if let Some(node) = self.nodes.get_mut(peer_id) {
            node.health.record_timeout();
        }
    }

    pub fn select_best_node(&self, model_id: &str) -> Option<String> {
        self.select_best_node_for_model(model_id)
    }

    pub fn select_best_node_for_model(&self, model_id: &str) -> Option<String> {
        self.select_best_node_for_model_with_cluster(model_id, None)
    }

    pub fn select_best_node_for_model_with_cluster(
        &self,
        model_id: &str,
        local_cluster_id: Option<&str>,
    ) -> Option<String> {
        IntelligentScheduler::new()
            .select_best_node(self, model_id, local_cluster_id)
            .map(|decision| decision.peer_id)
    }

    pub fn available_models(&self) -> Vec<String> {
        let mut models = Vec::new();
        for node in self.nodes.values() {
            for model in &node.models {
                if !models.contains(model) {
                    models.push(model.clone());
                }
            }
        }
        models
    }

    pub fn has_model_available(&self, model_id: &str) -> bool {
        self.nodes
            .values()
            .any(|node| node.models.iter().any(|model| model == model_id))
    }

    pub fn select_best_node_for_models_with_cluster(
        &self,
        model_ids: &[String],
        local_cluster_id: Option<&str>,
    ) -> Option<(String, String)> {
        IntelligentScheduler::new()
            .select_best_node_for_models(self, model_ids, local_cluster_id)
            .map(|decision| (decision.peer_id, decision.model_id))
    }

    pub fn all_nodes(&self) -> Vec<(String, NodeCapability)> {
        self.nodes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
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

    fn make_cap(
        peer_id: &str,
        cpu_score: u64,
        model: &str,
        ram_gb: u32,
        active_tasks: u32,
        latency_ms: u32,
    ) -> NodeCapability {
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
            cluster_id: None, // ← NEW
            health: NodeHealth::default(),
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

    #[test]
    fn test_cluster_routing_preference() {
        let mut r = NodeRegistry::new();

        // Make peer1 and peer2 very similar so cluster bonus tips the scale
        let mut cap1 = make_cap("peer1", 150_000, "tinyllama-1b", 8, 2, 10);
        cap1.cluster_id = Some("cluster-local".to_string());
        r.register_node(cap1);

        let mut cap2 = make_cap("peer2", 150_000, "tinyllama-1b", 8, 2, 10);
        cap2.cluster_id = Some("cluster-remote".to_string());
        r.register_node(cap2);

        // Without cluster preference → either could win (deterministic by HashMap order isn't guaranteed, but both equal)
        // With cluster preference → peer1 wins (same cluster bonus)
        let best = r.select_best_node_for_model_with_cluster("tinyllama-1b", Some("cluster-local"));
        assert_eq!(best.unwrap(), "peer1");
    }

    #[test]
    fn test_available_models_union() {
        let mut r = NodeRegistry::new();
        r.register_node(make_cap("peer1", 100, "tinyllama-1b", 8, 0, 20));
        r.register_node(make_cap("peer2", 100, "llama3-3b", 8, 0, 20));
        let models = r.available_models();
        assert!(models.contains(&"tinyllama-1b".to_string()));
        assert!(models.contains(&"llama3-3b".to_string()));
    }
}
