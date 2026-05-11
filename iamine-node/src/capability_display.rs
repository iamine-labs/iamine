use crate::benchmark::NodeBenchmark;
use crate::worker_capabilities::WorkerCapabilities;
use iamine_models::{ModelInstaller, ModelNodeCapabilities};
use iamine_network::NodeCapabilityHeartbeat;
use serde_json::{json, Value};
use std::collections::HashSet;

pub(crate) const SIMPLE_TASK_CAPABILITIES: [&str; 3] = ["reverse_string", "test", "echo"];

pub(crate) fn display_node_startup_summary(
    peer_id: &str,
    wallet_address: &str,
    benchmark: Option<&NodeBenchmark>,
    worker_slots: usize,
    mode: Option<String>,
    balance: u64,
    tasks_completed: u64,
    reputation: f32,
) {
    println!("\n═══════════════════════════════════");
    println!("  Peer ID:      {}", peer_id);
    println!("  Wallet:       {}", wallet_address);
    if let Some(b) = benchmark {
        println!("  CPU Score:    {:.0}", b.cpu_score);
        println!("  RAM:          {} GB", b.ram_available_gb);
        println!(
            "  GPU:          {}",
            if b.gpu_available { "✅" } else { "❌" }
        );
    }
    println!("  Worker Slots: {}", worker_slots);
    if let Some(mode) = mode {
        println!("  Modo:         {}", mode);
    }
    println!("  Balance:      {} $MIND", balance);
    println!("  Tareas:       {}", tasks_completed);
    println!("  Reputación:   {:.1}/100", reputation);
    println!("═══════════════════════════════════\n");
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapabilityDisplaySnapshot {
    pub(crate) backend: String,
    pub(crate) real_inference_available: bool,
    pub(crate) executable_models_count: usize,
    pub(crate) storage_models_count: usize,
    pub(crate) registry_models_count: usize,
    pub(crate) simple_tasks: Vec<String>,
}

pub(crate) fn build_capability_display_snapshot(
    backend: impl Into<String>,
    real_inference_available: bool,
    executable_models_count: usize,
    storage_models_count: usize,
    registry_models_count: usize,
    supported_tasks: &[String],
) -> CapabilityDisplaySnapshot {
    CapabilityDisplaySnapshot {
        backend: backend.into(),
        real_inference_available,
        executable_models_count,
        storage_models_count,
        registry_models_count,
        simple_tasks: supported_tasks
            .iter()
            .filter(|task| SIMPLE_TASK_CAPABILITIES.contains(&task.as_str()))
            .cloned()
            .collect(),
    }
}

pub(crate) fn build_worker_heartbeat_payload(
    peer_id: &str,
    available_slots: usize,
    reputation_score: u32,
    uptime_secs: u64,
    avg_latency_ms: f64,
    capabilities: &WorkerCapabilities,
) -> Value {
    json!({
        "type": "Heartbeat",
        "peer_id": peer_id,
        "available_slots": available_slots,
        "reputation_score": reputation_score,
        "uptime_secs": uptime_secs,
        "avg_latency_ms": avg_latency_ms,
        "capabilities": {
            "cpu_cores": capabilities.cpu_cores,
            "ram_gb": capabilities.ram_gb,
            "gpu_available": capabilities.gpu_available,
            "supported_tasks": capabilities.supported_tasks,
        }
    })
}

pub(crate) fn build_node_models_broadcast_payload(
    peer_id: &str,
    validated_advertised_models: &[String],
) -> Option<Value> {
    let installer = ModelInstaller::new();
    let allowed_model_ids: HashSet<_> = validated_advertised_models.iter().cloned().collect();
    let mut node_models = installer.build_node_models(peer_id);
    node_models
        .models
        .retain(|model| allowed_model_ids.contains(&model.id));
    if node_models.models.is_empty() {
        return None;
    }
    Some(json!({
        "type": "NodeModelsBroadcast",
        "node_id": peer_id,
        "models": node_models.models,
    }))
}

pub(crate) fn build_node_capability_heartbeat(
    peer_id: &str,
    benchmark: Option<&NodeBenchmark>,
    node_caps: &ModelNodeCapabilities,
    validated_advertised_models: &[String],
    worker_slots: usize,
    active_tasks: usize,
    latency_ms: f64,
) -> NodeCapabilityHeartbeat {
    NodeCapabilityHeartbeat {
        peer_id: peer_id.to_string(),
        cpu_score: benchmark.map(|b| b.cpu_score as u64).unwrap_or(0),
        ram_gb: benchmark.map(|b| b.ram_available_gb as u32).unwrap_or(8),
        gpu_available: benchmark.map(|b| b.gpu_available).unwrap_or(false),
        storage_available_gb: node_caps.storage_available_gb,
        accelerator: node_caps.accelerator.clone(),
        models: validated_advertised_models.to_vec(),
        worker_slots: worker_slots as u32,
        active_tasks: active_tasks as u32,
        latency_ms: latency_ms.max(1.0) as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_caps() -> WorkerCapabilities {
        WorkerCapabilities {
            cpu_cores: 4,
            ram_gb: 8,
            gpu_available: false,
            disk_available_gb: 20,
            supported_tasks: vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string(),
                "inference".to_string(),
            ],
            avg_latency_ms: 12.0,
        }
    }

    #[test]
    fn capability_display_reports_backend_and_real_availability() {
        let snapshot = build_capability_display_snapshot(
            "mock",
            false,
            0,
            2,
            3,
            &worker_caps().supported_tasks,
        );

        assert_eq!(snapshot.backend, "mock");
        assert!(!snapshot.real_inference_available);
        assert_eq!(snapshot.executable_models_count, 0);
        assert_eq!(snapshot.storage_models_count, 2);
        assert_eq!(snapshot.registry_models_count, 3);
    }

    #[test]
    fn capability_display_preserves_simple_tasks_under_mock() {
        let snapshot = build_capability_display_snapshot(
            "mock",
            false,
            0,
            0,
            3,
            &worker_caps().supported_tasks,
        );

        assert_eq!(
            snapshot.simple_tasks,
            vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string()
            ]
        );
    }

    #[test]
    fn worker_heartbeat_payload_preserves_shape() {
        let payload = build_worker_heartbeat_payload("peer-1", 2, 99, 42, 15.0, &worker_caps());

        assert_eq!(payload["type"], "Heartbeat");
        assert_eq!(payload["peer_id"], "peer-1");
        assert_eq!(payload["available_slots"], 2);
        assert_eq!(payload["reputation_score"], 99);
        assert_eq!(payload["capabilities"]["cpu_cores"], 4);
        assert_eq!(
            payload["capabilities"]["supported_tasks"][0],
            "reverse_string"
        );
    }
}
