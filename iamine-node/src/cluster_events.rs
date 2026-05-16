use crate::cluster_cli::{render_cluster_status_human, render_cluster_status_json};
use crate::cluster_health::ClusterHealthThresholds;
use crate::cluster_registry::{
    ClusterNodeStatusMessage, ClusterRegistry, CLUSTER_NODE_STATUS_TYPE,
};
use crate::cluster_status::build_cluster_status_snapshot;
use crate::{log_observability_event, unix_now_ms};
use iamine_network::LogLevel;
use serde_json::Map;

pub(crate) fn emit_cluster_event(
    event: &str,
    cluster_id: &str,
    peer_id: Option<&str>,
    mut fields: Map<String, serde_json::Value>,
) {
    fields.insert("cluster_id".to_string(), cluster_id.into());
    if let Some(peer_id) = peer_id {
        fields.insert("peer_id".to_string(), peer_id.into());
    }
    log_observability_event(LogLevel::Info, event, "cluster", None, None, None, fields);
}

pub(crate) fn emit_cluster_status_requested(cluster_id: &str, wait_ms: u64) {
    emit_cluster_event("cluster_registry_created", cluster_id, None, Map::new());
    emit_cluster_event("cluster_status_requested", cluster_id, None, {
        let mut fields = Map::new();
        fields.insert("wait_ms".to_string(), wait_ms.into());
        fields
    });
}

pub(crate) fn render_and_emit_cluster_status(
    registry: &ClusterRegistry,
    cluster_id: &str,
    render_json: bool,
    discovery_status: Option<&str>,
    reason: Option<String>,
) -> Result<(), serde_json::Error> {
    let snapshot =
        build_cluster_status_snapshot(registry, unix_now_ms(), ClusterHealthThresholds::default());
    let rendered = if render_json {
        render_cluster_status_json(&snapshot)?
    } else {
        render_cluster_status_human(&snapshot)
    };
    println!("{}", rendered);

    emit_cluster_event("cluster_status_rendered", cluster_id, None, {
        let mut fields = Map::new();
        fields.insert("nodes_detected".to_string(), snapshot.nodes_detected.into());
        fields.insert("healthy_nodes".to_string(), snapshot.healthy_nodes.into());
        fields.insert("stale_nodes".to_string(), snapshot.stale_nodes.into());
        fields.insert("degraded_nodes".to_string(), snapshot.degraded_nodes.into());
        fields.insert("offline_nodes".to_string(), snapshot.offline_nodes.into());
        fields.insert("ready_nodes".to_string(), snapshot.ready_nodes.into());
        fields.insert(
            "format".to_string(),
            if render_json { "json" } else { "human" }.into(),
        );
        if let Some(status) = discovery_status {
            fields.insert("discovery_status".to_string(), status.into());
        }
        if let Some(reason) = reason {
            fields.insert("reason".to_string(), reason.into());
        }
        fields
    });

    Ok(())
}

pub(crate) fn emit_cluster_discovery_update(
    inserted: bool,
    cluster_id: &str,
    peer_id: &str,
    source: &str,
    address: Option<String>,
) {
    emit_cluster_event(
        if inserted {
            "cluster_node_discovered"
        } else {
            "cluster_node_updated"
        },
        cluster_id,
        Some(peer_id),
        {
            let mut fields = Map::new();
            fields.insert("source".to_string(), source.into());
            if let Some(address) = address {
                fields.insert("address".to_string(), address.into());
            }
            fields
        },
    );
}

pub(crate) fn emit_cluster_capabilities_updated(
    registry: &ClusterRegistry,
    cluster_id: &str,
    peer_id: &str,
    source: &str,
    executable_models_count: usize,
    latency_ms: Option<u32>,
) {
    emit_cluster_event("cluster_capabilities_updated", cluster_id, Some(peer_id), {
        let mut fields = Map::new();
        fields.insert("source".to_string(), source.into());
        fields.insert(
            "executable_models_count".to_string(),
            executable_models_count.into(),
        );
        if let Some(latency_ms) = latency_ms {
            fields.insert("latency_ms".to_string(), latency_ms.into());
        }
        if let Some(node) = registry.node_by_peer_id(peer_id) {
            let readiness = node.readiness_at(
                unix_now_ms(),
                crate::cluster_health::ClusterHealthThresholds::default(),
            );
            fields.insert(
                "ready_for_tasks".to_string(),
                readiness.ready_for_tasks.into(),
            );
            fields.insert(
                "readiness_reason".to_string(),
                readiness.readiness_reason.as_str().into(),
            );
        }
        fields
    });
}

pub(crate) fn emit_cluster_node_updated(cluster_id: &str, peer_id: &str, source: &str) {
    emit_cluster_event("cluster_node_updated", cluster_id, Some(peer_id), {
        let mut fields = Map::new();
        fields.insert("source".to_string(), source.into());
        fields
    });
}

pub(crate) fn update_cluster_status_message_and_emit(
    registry: &mut ClusterRegistry,
    cluster_id: &str,
    message: ClusterNodeStatusMessage,
    topic: &str,
) {
    let observed_peer = message.peer_id.clone();
    let now_ms = unix_now_ms();
    let health_before = registry
        .node_by_peer_id(&observed_peer)
        .map(|node| node.health_at(now_ms, ClusterHealthThresholds::default()));
    registry.update_from_cluster_status_message(message, now_ms);
    let health_after = registry
        .node_by_peer_id(&observed_peer)
        .map(|node| node.health_at(now_ms, ClusterHealthThresholds::default()));

    emit_cluster_event("cluster_node_updated", cluster_id, Some(&observed_peer), {
        let mut fields = Map::new();
        fields.insert("source".to_string(), CLUSTER_NODE_STATUS_TYPE.into());
        fields.insert("topic".to_string(), topic.into());
        if let Some(node) = registry.node_by_peer_id(&observed_peer) {
            let readiness =
                node.readiness_at(now_ms, crate::cluster_health::ClusterHealthThresholds::default());
            fields.insert(
                "ready_for_tasks".to_string(),
                readiness.ready_for_tasks.into(),
            );
            fields.insert(
                "readiness_reason".to_string(),
                readiness.readiness_reason.as_str().into(),
            );
        }
        fields
    });

    if health_before != health_after {
        emit_cluster_event(
            "cluster_node_health_changed",
            cluster_id,
            Some(&observed_peer),
            {
                let mut fields = Map::new();
                fields.insert(
                    "previous_health".to_string(),
                    health_before
                        .map(|health| health.as_str())
                        .unwrap_or("unknown")
                        .into(),
                );
                fields.insert(
                    "health".to_string(),
                    health_after
                        .map(|health| health.as_str())
                        .unwrap_or("unknown")
                        .into(),
                );
                fields
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_event_helpers_preserve_event_names() {
        assert_eq!(CLUSTER_NODE_STATUS_TYPE, "ClusterNodeStatus");
    }
}
