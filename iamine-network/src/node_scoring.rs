use crate::cluster::{relation_for_cluster, ClusterRelation};
use crate::node_registry::NodeCapability;

#[derive(Debug, Clone, PartialEq)]
pub struct NodeScore {
    pub peer_id: String,
    pub model_id: String,
    pub score: f64,
    pub free_slots: u32,
    pub model_karma_score: f32,
}

pub fn free_slots(node: &NodeCapability) -> u32 {
    node.worker_slots.saturating_sub(node.active_tasks)
}

pub fn latency_score(latency_ms: u32) -> f64 {
    if latency_ms == 0 {
        1.0
    } else {
        1.0 / (1.0 + (latency_ms as f64 / 10.0))
    }
}

pub fn cluster_bonus(node: &NodeCapability, local_cluster_id: Option<&str>) -> f64 {
    match relation_for_cluster(node.cluster_id.as_deref(), local_cluster_id) {
        ClusterRelation::Same => 1.0,
        ClusterRelation::Nearby => 0.5,
        ClusterRelation::Remote => 0.0,
    }
}

pub fn cluster_priority(node: &NodeCapability, local_cluster_id: Option<&str>) -> u8 {
    match relation_for_cluster(node.cluster_id.as_deref(), local_cluster_id) {
        ClusterRelation::Same => 3,
        ClusterRelation::Nearby => 2,
        ClusterRelation::Remote => 1,
    }
}

pub fn score_node(node: &NodeCapability, local_cluster_id: Option<&str>) -> f64 {
    let free_slots_component = if node.worker_slots == 0 {
        0.0
    } else {
        free_slots(node) as f64 / node.worker_slots as f64
    };
    let cpu_component = (node.cpu_score as f64 / 200_000.0).min(1.0);
    let latency_component = latency_score(node.latency_ms);
    let cluster_component = cluster_bonus(node, local_cluster_id);

    (free_slots_component * 0.4)
        + (cpu_component * 0.3)
        + (latency_component * 0.2)
        + (cluster_component * 0.1)
}

#[cfg(test)]
mod tests {
    use super::{cluster_bonus, cluster_priority, free_slots, latency_score, score_node};
    use crate::node_registry::NodeCapability;
    use std::time::Instant;

    fn make_node(
        peer_id: &str,
        cpu_score: u64,
        worker_slots: u32,
        active_tasks: u32,
        latency_ms: u32,
        cluster_id: Option<&str>,
    ) -> NodeCapability {
        NodeCapability {
            peer_id: peer_id.to_string(),
            cpu_score,
            ram_gb: 16,
            gpu_available: false,
            storage_available_gb: 100,
            accelerator: "CPU".to_string(),
            models: vec!["llama3-3b".to_string()],
            worker_slots,
            active_tasks,
            latency_ms,
            last_seen: Instant::now(),
            cluster_id: cluster_id.map(|value| value.to_string()),
        }
    }

    #[test]
    fn test_free_slots() {
        let node = make_node("peer-a", 120_000, 8, 3, 20, None);
        assert_eq!(free_slots(&node), 5);
    }

    #[test]
    fn test_latency_score_prefers_lower_rtt() {
        assert!(latency_score(5) > latency_score(40));
    }

    #[test]
    fn test_cluster_bonus_same_cluster() {
        let node = make_node("peer-a", 120_000, 8, 0, 20, Some("local"));
        assert_eq!(cluster_bonus(&node, Some("local")), 1.0);
        assert_eq!(cluster_bonus(&node, Some("remote")), 0.0);
        assert_eq!(cluster_priority(&node, Some("local")), 3);
    }

    #[test]
    fn test_score_node_prefers_capacity_and_latency() {
        let fast = make_node("fast", 150_000, 8, 1, 8, Some("local"));
        let loaded = make_node("loaded", 180_000, 8, 7, 50, Some("remote"));
        assert!(score_node(&fast, Some("local")) > score_node(&loaded, Some("local")));
    }
}
