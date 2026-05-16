use crate::cluster_health::{ClusterHealth, ClusterHealthThresholds};
use crate::cluster_registry::{
    ClusterBackend, ClusterExecutionMode, ClusterMetricsStatus, ClusterRegistry, ClusterRole,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterStatusSnapshot {
    pub(crate) cluster_id: String,
    pub(crate) nodes_detected: usize,
    pub(crate) healthy_nodes: usize,
    pub(crate) stale_nodes: usize,
    pub(crate) degraded_nodes: usize,
    pub(crate) offline_nodes: usize,
    pub(crate) nodes: Vec<ClusterStatusNodeRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterStatusNodeRow {
    pub(crate) peer_id: String,
    pub(crate) peer_id_short: String,
    pub(crate) hostname: String,
    pub(crate) observed_addresses: Vec<String>,
    pub(crate) role: ClusterRole,
    pub(crate) execution_mode: ClusterExecutionMode,
    pub(crate) backend: ClusterBackend,
    pub(crate) health: ClusterHealth,
    pub(crate) last_seen_ms: Option<u64>,
    pub(crate) age_ms: Option<u64>,
    pub(crate) latency_ms: Option<u32>,
    pub(crate) metrics_status: ClusterMetricsStatus,
    pub(crate) real_inference_available: Option<bool>,
    pub(crate) supported_task_types: Vec<String>,
    pub(crate) models_in_storage: Vec<String>,
    pub(crate) models_in_registry: Vec<String>,
    pub(crate) executable_models: Vec<String>,
}

pub(crate) fn build_cluster_status_snapshot(
    registry: &ClusterRegistry,
    now_ms: u64,
    thresholds: ClusterHealthThresholds,
) -> ClusterStatusSnapshot {
    let mut rows: Vec<_> = registry
        .list_nodes()
        .into_iter()
        .map(|node| {
            let health = node.health_at(now_ms, thresholds);
            ClusterStatusNodeRow {
                peer_id: node.peer_id,
                peer_id_short: node.peer_id_short,
                hostname: node.hostname,
                observed_addresses: node.observed_addresses,
                role: node.role,
                execution_mode: node.execution_mode,
                backend: node.backend,
                health,
                last_seen_ms: node.last_seen_ms,
                age_ms: node
                    .last_seen_ms
                    .map(|last_seen| now_ms.saturating_sub(last_seen)),
                latency_ms: node.latency_ms,
                metrics_status: node.metrics_status,
                real_inference_available: node.capabilities.real_inference_available,
                supported_task_types: node.capabilities.supported_task_types,
                models_in_storage: node.capabilities.models_in_storage,
                models_in_registry: node.capabilities.models_in_registry,
                executable_models: node.capabilities.executable_models,
            }
        })
        .collect();
    rows.sort_by(|left, right| {
        left.hostname
            .cmp(&right.hostname)
            .then_with(|| left.peer_id.cmp(&right.peer_id))
    });

    ClusterStatusSnapshot {
        cluster_id: registry.cluster_id().to_string(),
        nodes_detected: rows.len(),
        healthy_nodes: registry.healthy_nodes(now_ms).len(),
        stale_nodes: registry.stale_nodes(now_ms).len(),
        degraded_nodes: registry.degraded_nodes(now_ms).len(),
        offline_nodes: registry.offline_nodes(now_ms).len(),
        nodes: rows,
    }
}

pub(crate) fn cluster_status_wait_ms_from_env() -> u64 {
    std::env::var("IAMINE_CLUSTER_STATUS_WAIT_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(|value| value.clamp(250, 30_000))
        .unwrap_or(6_500)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_registry::{
        ClusterExecutionMode, ClusterNodeStatusMessage, ClusterRegistry, ClusterRole,
    };

    #[test]
    fn cluster_status_empty_registry_prints_zero_counts() {
        let registry = ClusterRegistry::new("test-lan");
        let snapshot =
            build_cluster_status_snapshot(&registry, 100, ClusterHealthThresholds::default());

        assert_eq!(snapshot.cluster_id, "test-lan");
        assert_eq!(snapshot.nodes_detected, 0);
        assert!(snapshot.nodes.is_empty());
    }

    #[test]
    fn cluster_status_counts_health_buckets() {
        let mut registry = ClusterRegistry::new("test-lan");
        let mut healthy = ClusterNodeStatusMessage::worker(
            "test-lan",
            "peer-a",
            "wrk-a",
            ClusterExecutionMode::Mock,
            ClusterBackend::Mock,
            false,
            vec!["reverse_string".to_string()],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            ClusterMetricsStatus::Fallback,
            Some(10),
        );
        healthy.role = ClusterRole::Worker;
        registry.update_from_cluster_status_message(healthy, 100_000);
        registry.add_or_update_discovered_node("peer-stale", None, 60_000);

        let snapshot = build_cluster_status_snapshot(
            &registry,
            100_000,
            ClusterHealthThresholds {
                stale_after_ms: 30_000,
                offline_after_ms: 90_000,
            },
        );

        assert_eq!(snapshot.nodes_detected, 2);
        assert_eq!(snapshot.healthy_nodes, 1);
        assert_eq!(snapshot.stale_nodes, 1);
    }
}
