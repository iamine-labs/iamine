// ─── Test 1: Model Registry ───────────────────────────────────────────────
#[test]
fn test_registry_has_default_models() {
    let registry = ModelRegistry::new();
    assert!(registry.get("tinyllama-1b").is_some());
    assert!(registry.get("llama3-3b").is_some());
    assert!(registry.get("mistral-7b").is_some());
    assert!(registry.get("unknown-model").is_none());
}

#[test]
fn test_registry_can_run_check() {
    let registry = ModelRegistry::new();
    // tinyllama necesita 2GB RAM
    assert!(registry.can_run("tinyllama-1b", 4, false).is_ok());
    assert!(registry.can_run("tinyllama-1b", 1, false).is_err()); // RAM insuficiente
                                                                  // mistral necesita 8GB
    assert!(registry.can_run("mistral-7b", 4, false).is_err());
    assert!(registry.can_run("mistral-7b", 8, false).is_ok());
}

// ─── Test 2: Storage Config ───────────────────────────────────────────────
#[test]
fn test_storage_config_space_check() {
    let cfg = StorageConfig {
        max_storage_gb: 10,
        models_path: "/tmp".to_string(),
    };
    let ten_gb = 10 * 1_073_741_824u64;
    let one_gb = 1_073_741_824u64;

    assert!(cfg.has_space_for(one_gb, 0)); // 1GB en 10GB → ok
    assert!(!cfg.has_space_for(ten_gb + 1, 0)); // 10GB+1 → no cabe
    assert!(!cfg.has_space_for(one_gb, ten_gb)); // ya lleno → no cabe
}

// ─── Test 3: NodeModels P2P ───────────────────────────────────────────────
#[test]
fn test_peer_model_registry() {
    let mut registry = PeerModelRegistry::new();

    let mut peer_a = NodeModels::new("peer_a_12345678".to_string());
    peer_a.models.push(ModelId {
        id: "tinyllama-1b".to_string(),
        version: "1.1".to_string(),
        sha256: "abc123".to_string(),
        size_bytes: 637_000_000,
    });
    registry.update_peer(peer_a);

    let peers = registry.peers_with_model("tinyllama-1b");
    assert_eq!(peers.len(), 1);

    let peers_none = registry.peers_with_model("mistral-7b");
    assert_eq!(peers_none.len(), 0);
}

#[test]
fn test_download_strategy() {
    let mut registry = PeerModelRegistry::new();

    // Sin peers → oficial
    let strategy = registry.download_strategy("tinyllama-1b", "https://example.com/model");
    assert!(matches!(
        strategy,
        iamine_models::node_models::DownloadStrategy::Official(_)
    ));

    // Con peer → peer first
    let mut peer = NodeModels::new("peer_xyz".to_string());
    peer.models.push(ModelId {
        id: "tinyllama-1b".to_string(),
        version: "1.1".to_string(),
        sha256: "abc".to_string(),
        size_bytes: 100,
    });
    registry.update_peer(peer);

    let strategy = registry.download_strategy("tinyllama-1b", "https://example.com/model");
    assert!(matches!(
        strategy,
        iamine_models::node_models::DownloadStrategy::PeerFirst { .. }
    ));
}

// ─── Test 4: NodeCapabilities ─────────────────────────────────────────────
#[test]
fn test_node_capabilities() {
    use iamine_models::{can_node_run_model, ModelNodeCapabilities, ModelRequirements};

    let cap = ModelNodeCapabilities {
        node_id: "test-node".to_string(),
        cpu_cores: 8,
        ram_gb: 16,
        gpu_type: Some("Metal".to_string()),
        npu_type: None,
        storage_available_gb: 50,
        worker_slots: 8,
        supported_models: vec!["tinyllama-1b".to_string()],
        cpu_features: vec!["NEON".to_string()],
        accelerator: "Metal".to_string(),
    };

    assert!(cap.has_model("tinyllama-1b"));
    assert!(!cap.has_model("mistral-7b"));

    // Validar con requirements
    let req = ModelRequirements::for_model("tinyllama-1b").unwrap();
    assert!(can_node_run_model(&cap, &req));

    let low_ram = ModelNodeCapabilities {
        node_id: "test-low".to_string(),
        cpu_cores: 2,
        ram_gb: 2,
        gpu_type: None,
        npu_type: None,
        storage_available_gb: 5,
        worker_slots: 2,
        supported_models: vec!["tinyllama-1b".to_string()],
        cpu_features: vec![],
        accelerator: "CPU".to_string(),
    };

    let req_tiny = ModelRequirements::for_model("tinyllama-1b").unwrap();
    assert!(can_node_run_model(&low_ram, &req_tiny));

    let req_llama = ModelRequirements::for_model("llama3-3b").unwrap();
    assert!(!can_node_run_model(&low_ram, &req_llama)); // necesita 4GB RAM
}
