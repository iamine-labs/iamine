use crate::cluster_status::{ClusterStatusNodeRow, ClusterStatusSnapshot};

pub(crate) fn render_cluster_status_human(snapshot: &ClusterStatusSnapshot) -> String {
    let mut output = String::new();
    output.push_str("Cluster LAN:\n");
    output.push_str(&format!("cluster_id: {}\n", snapshot.cluster_id));
    output.push_str(&format!("nodes_detected: {}\n", snapshot.nodes_detected));
    output.push_str(&format!("healthy_nodes: {}\n", snapshot.healthy_nodes));
    output.push_str(&format!("stale_nodes: {}\n", snapshot.stale_nodes));
    output.push_str(&format!("degraded_nodes: {}\n", snapshot.degraded_nodes));
    output.push_str(&format!("offline_nodes: {}\n", snapshot.offline_nodes));
    output.push_str(&format!("ready_nodes: {}\n", snapshot.ready_nodes));
    output.push_str("\nNodes:\n");

    if snapshot.nodes.is_empty() {
        output.push_str("No LAN nodes discovered yet.\n");
        return output;
    }

    for node in &snapshot.nodes {
        output.push_str(&render_node_row(node));
        output.push('\n');
    }

    output
}

pub(crate) fn render_cluster_status_json(
    snapshot: &ClusterStatusSnapshot,
) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(snapshot)
}

fn render_node_row(node: &ClusterStatusNodeRow) -> String {
    let tasks = if node.supported_task_types.is_empty() {
        "-".to_string()
    } else {
        node.supported_task_types.join(",")
    };
    let executable_models = if node.executable_models.is_empty() {
        "-".to_string()
    } else {
        node.executable_models.join(",")
    };
    let metadata_only_models = if node.metadata_only_models.is_empty() {
        "-".to_string()
    } else {
        node.metadata_only_models.join(",")
    };
    let unavailable_models = if node.unavailable_models.is_empty() {
        "-".to_string()
    } else {
        node.unavailable_models
            .iter()
            .map(|model| format!("{}({})", model.model_id, model.reason))
            .collect::<Vec<_>>()
            .join(",")
    };
    let last_seen = node
        .age_ms
        .map(|age_ms| format!("{}ms ago", age_ms))
        .unwrap_or_else(|| "unknown".to_string());
    let latency = node
        .latency_ms
        .map(|latency_ms| format!("{}ms", latency_ms))
        .unwrap_or_else(|| "-".to_string());
    let real_available = node
        .real_inference_available
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    format!(
        "- {:20} {:12} {}/{} {} backend={} ready={} reason={} last_seen={} latency={} real_inference_available={} tasks={} models_in_registry={} models_in_storage={} executable_models={} metadata_only_models={} unavailable_models={} metrics={}",
        node.hostname,
        node.peer_id_short,
        node.role.as_str(),
        node.execution_mode.as_str(),
        node.health.as_str(),
        node.backend.as_str(),
        node.ready_for_tasks,
        node.readiness_reason.as_str(),
        last_seen,
        latency,
        real_available,
        tasks,
        list_or_dash(&node.models_in_registry),
        list_or_dash(&node.models_in_storage),
        executable_models,
        metadata_only_models,
        unavailable_models,
        node.metrics_status.as_str()
    )
}

fn list_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_health::ClusterHealth;
    use crate::cluster_readiness::ClusterReadinessReason;
    use crate::cluster_registry::{
        ClusterBackend, ClusterExecutionMode, ClusterMetricsStatus, ClusterRole,
    };
    use crate::model_capability_consistency::{ModelCapabilityStatus, ModelExecutionStatus};

    fn snapshot_with_node() -> ClusterStatusSnapshot {
        ClusterStatusSnapshot {
            cluster_id: "default-lan".to_string(),
            nodes_detected: 1,
            healthy_nodes: 1,
            stale_nodes: 0,
            degraded_nodes: 0,
            offline_nodes: 0,
            ready_nodes: 1,
            nodes: vec![ClusterStatusNodeRow {
                peer_id: "12D3KooWTest".to_string(),
                peer_id_short: "12D3KooWTest".to_string(),
                hostname: "wrk-01".to_string(),
                observed_addresses: vec!["/ip4/127.0.0.1/tcp/4101".to_string()],
                role: ClusterRole::Worker,
                execution_mode: ClusterExecutionMode::Mock,
                backend: ClusterBackend::Mock,
                health: ClusterHealth::Healthy,
                last_seen_ms: Some(100),
                age_ms: Some(25),
                latency_ms: Some(12),
                metrics_status: ClusterMetricsStatus::Fallback,
                ready_for_tasks: true,
                readiness_reason: ClusterReadinessReason::MockSimpleTasksReady,
                real_inference_available: Some(false),
                supported_task_types: vec![
                    "reverse_string".to_string(),
                    "test".to_string(),
                    "echo".to_string(),
                ],
                models_in_storage: vec!["tinyllama-1b".to_string()],
                models_in_registry: vec!["tinyllama-1b".to_string()],
                executable_models: Vec::new(),
                metadata_only_models: vec!["tinyllama-1b".to_string()],
                unavailable_models: vec![ModelCapabilityStatus {
                    model_id: "tinyllama-1b".to_string(),
                    known_by_registry: true,
                    present_in_storage: true,
                    executable: false,
                    execution_status: ModelExecutionStatus::DisabledByMock,
                    reason: "disabled_by_mock".to_string(),
                }],
            }],
        }
    }

    #[test]
    fn cluster_status_cli_prints_summary() {
        let rendered = render_cluster_status_human(&snapshot_with_node());

        assert!(rendered.contains("cluster_id: default-lan"));
        assert!(rendered.contains("nodes_detected: 1"));
        assert!(rendered.contains("healthy_nodes: 1"));
        assert!(rendered.contains("ready_nodes: 1"));
        assert!(rendered.contains("wrk-01"));
        assert!(rendered.contains("backend=mock"));
        assert!(rendered.contains("ready=true"));
        assert!(rendered.contains("reason=mock_simple_tasks_ready"));
        assert!(rendered.contains("models_in_registry=tinyllama-1b"));
        assert!(rendered.contains("models_in_storage=tinyllama-1b"));
        assert!(rendered.contains("executable_models=-"));
        assert!(rendered.contains("metadata_only_models=tinyllama-1b"));
        assert!(rendered.contains("unavailable_models=tinyllama-1b(disabled_by_mock)"));
    }

    #[test]
    fn cluster_status_empty_registry_prints_clear_message() {
        let rendered = render_cluster_status_human(&ClusterStatusSnapshot {
            cluster_id: "default-lan".to_string(),
            nodes_detected: 0,
            healthy_nodes: 0,
            stale_nodes: 0,
            degraded_nodes: 0,
            offline_nodes: 0,
            ready_nodes: 0,
            nodes: Vec::new(),
        });

        assert!(rendered.contains("nodes_detected: 0"));
        assert!(rendered.contains("ready_nodes: 0"));
        assert!(rendered.contains("No LAN nodes discovered yet."));
    }

    #[test]
    fn cluster_status_json_if_added_is_valid() {
        let rendered = render_cluster_status_json(&snapshot_with_node()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(value["cluster_id"], "default-lan");
        assert_eq!(value["nodes_detected"], 1);
        assert_eq!(value["ready_nodes"], 1);
        assert_eq!(value["nodes"][0]["ready_for_tasks"], true);
        assert_eq!(
            value["nodes"][0]["readiness_reason"],
            "mock_simple_tasks_ready"
        );
        assert_eq!(value["nodes"][0]["models_in_registry"][0], "tinyllama-1b");
        assert_eq!(value["nodes"][0]["models_in_storage"][0], "tinyllama-1b");
        assert_eq!(
            value["nodes"][0]["executable_models"],
            serde_json::json!([])
        );
        assert_eq!(value["nodes"][0]["metadata_only_models"][0], "tinyllama-1b");
        assert_eq!(
            value["nodes"][0]["unavailable_models"][0]["execution_status"],
            "disabled_by_mock"
        );
    }
}
