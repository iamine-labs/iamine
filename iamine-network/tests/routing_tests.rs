use iamine_network::model_capability_matcher::{
    is_node_compatible_with_model, ModelHardwareRequirements, NodeHardwareProfile,
};
use iamine_network::{NodeCapabilityHeartbeat, NodeRegistry};

#[test]
fn test_model_ram_filter() {
    let req = ModelHardwareRequirements::for_model("mistral-7b").unwrap();

    let low_ram = NodeHardwareProfile { ram_gb: 4, gpu_available: false, storage_available_gb: 20 };
    let high_ram = NodeHardwareProfile { ram_gb: 16, gpu_available: false, storage_available_gb: 20 };

    assert!(!is_node_compatible_with_model(&low_ram, &req));
    assert!(is_node_compatible_with_model(&high_ram, &req));
}

#[test]
fn test_gpu_requirement_filter() {
    let req = ModelHardwareRequirements {
        model_id: "gpu-model".to_string(),
        ram_required_gb: 4,
        gpu_required: true,
        disk_required_gb: 2,
    };

    let no_gpu = NodeHardwareProfile { ram_gb: 16, gpu_available: false, storage_available_gb: 20 };
    let with_gpu = NodeHardwareProfile { ram_gb: 16, gpu_available: true, storage_available_gb: 20 };

    assert!(!is_node_compatible_with_model(&no_gpu, &req));
    assert!(is_node_compatible_with_model(&with_gpu, &req));
}

#[test]
fn test_select_best_node_with_capabilities() {
    let mut registry = NodeRegistry::new();

    // Nodo con RAM insuficiente para mistral-7b
    registry.update_from_heartbeat(NodeCapabilityHeartbeat {
        peer_id: "peer_low_ram".to_string(),
        cpu_score: 200_000,
        ram_gb: 4,
        gpu_available: false,
        storage_available_gb: 50,
        accelerator: "CPU".to_string(),
        models: vec!["mistral-7b".to_string()],
        worker_slots: 8,
        active_tasks: 0,
        latency_ms: 10,
    });

    // Nodo con RAM suficiente
    registry.update_from_heartbeat(NodeCapabilityHeartbeat {
        peer_id: "peer_high_ram".to_string(),
        cpu_score: 150_000,
        ram_gb: 16,
        gpu_available: true,
        storage_available_gb: 50,
        accelerator: "Metal".to_string(),
        models: vec!["mistral-7b".to_string()],
        worker_slots: 8,
        active_tasks: 0,
        latency_ms: 10,
    });

    let selected = registry.select_best_node_for_model("mistral-7b");
    assert!(selected.is_some());
    assert_eq!(selected.unwrap(), "peer_high_ram");
}

#[test]
fn test_tinyllama_any_node_compatible() {
    let req = ModelHardwareRequirements::for_model("tinyllama-1b").unwrap();
    let minimal = NodeHardwareProfile { ram_gb: 2, gpu_available: false, storage_available_gb: 1 };
    assert!(is_node_compatible_with_model(&minimal, &req));
}
