use crate::cluster_events::{
    emit_cluster_status_requested, render_and_emit_cluster_status,
    update_cluster_status_message_and_emit,
};
use crate::cluster_registry::{ClusterNodeStatusMessage, ClusterRegistry};
use serde_json::Value;

pub(crate) fn emit_controller_cluster_status_requested(cluster_id: &str, wait_ms: u64) {
    emit_cluster_status_requested(cluster_id, wait_ms);
}

pub(crate) fn render_controller_cluster_status(
    registry: &ClusterRegistry,
    cluster_id: &str,
    render_json: bool,
    discovery_status: Option<&str>,
    reason: Option<String>,
) -> Result<(), serde_json::Error> {
    render_and_emit_cluster_status(registry, cluster_id, render_json, discovery_status, reason)
}

pub(crate) fn handle_controller_cluster_status_message(
    registry: &mut ClusterRegistry,
    cluster_id: &str,
    msg: &Value,
    topic: &str,
) {
    if let Ok(cluster_message) = serde_json::from_value::<ClusterNodeStatusMessage>(msg.clone()) {
        update_cluster_status_message_and_emit(registry, cluster_id, cluster_message, topic);
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster_registry::ClusterRegistry;

    #[test]
    fn controller_cluster_status_human_preserved() {
        let registry = ClusterRegistry::with_default_cluster_id();
        let rendered = crate::cluster_cli::render_cluster_status_human(
            &crate::cluster_status::build_cluster_status_snapshot(
                &registry,
                crate::unix_now_ms(),
                crate::cluster_health::ClusterHealthThresholds::default(),
            ),
        );

        assert!(rendered.contains("cluster_id: default-lan"));
        assert!(rendered.contains("nodes_detected: 0"));
        assert!(rendered.contains("ready_nodes: 0"));
    }
}
