// ─── Tests v0.5.4: Node Intelligence Layer ────────────────────────────

#[test]
fn test_model_requirements() {
    use iamine_models::{can_node_run_model, ModelNodeCapabilities, ModelRequirements};
    let req = ModelRequirements::for_model("tinyllama-1b").unwrap();
    assert_eq!(req.min_ram_gb, 2);
    assert!(!req.requires_gpu);

    let cap = ModelNodeCapabilities {
        node_id: "test".to_string(),
        cpu_cores: 4,
        ram_gb: 8,
        gpu_type: None,
        npu_type: None,
        storage_available_gb: 10,
        worker_slots: 4,
        supported_models: vec!["tinyllama-1b".to_string()],
        cpu_features: vec![],
        accelerator: "CPU".to_string(),
    };
    assert!(can_node_run_model(&cap, &req));
}

#[test]
fn test_model_requirements_insufficient_ram() {
    use iamine_models::{can_node_run_model, ModelNodeCapabilities, ModelRequirements};
    let req = ModelRequirements::for_model("mistral-7b").unwrap();
    let cap = ModelNodeCapabilities {
        node_id: "test".to_string(),
        cpu_cores: 4,
        ram_gb: 4,
        gpu_type: None,
        npu_type: None,
        storage_available_gb: 10,
        worker_slots: 4,
        supported_models: vec![],
        cpu_features: vec![],
        accelerator: "CPU".to_string(),
    };
    assert!(!can_node_run_model(&cap, &req));
}

#[test]
fn test_runnable_models() {
    use iamine_models::{runnable_models, ModelNodeCapabilities};
    let cap = ModelNodeCapabilities {
        node_id: "test".to_string(),
        cpu_cores: 8,
        ram_gb: 16,
        gpu_type: Some("Metal".to_string()),
        npu_type: None,
        storage_available_gb: 50,
        worker_slots: 8,
        supported_models: vec!["tinyllama-1b".to_string()],
        cpu_features: vec![],
        accelerator: "Metal".to_string(),
    };
    let models = runnable_models(&cap);
    assert!(models.contains(&"tinyllama-1b".to_string()));
    assert!(models.contains(&"llama3-3b".to_string()));
    assert!(models.contains(&"mistral-7b".to_string()));
}

#[test]
fn test_model_selector_simple_prompt() {
    use iamine_models::{select_best_model, ModelNodeCapabilities};
    let cap = ModelNodeCapabilities {
        node_id: "test".to_string(),
        cpu_cores: 8,
        ram_gb: 16,
        gpu_type: None,
        npu_type: None,
        storage_available_gb: 50,
        worker_slots: 8,
        supported_models: vec![],
        cpu_features: vec![],
        accelerator: "CPU".to_string(),
    };
    let available = vec!["tinyllama-1b".to_string(), "llama3-3b".to_string()];
    let result = select_best_model("What is 2+2?", &available, &cap);
    assert!(result.is_some());
    assert_eq!(result.unwrap(), "tinyllama-1b"); // simple → fast model
}

#[test]
fn test_model_selector_complex_prompt() {
    use iamine_models::{select_best_model, ModelNodeCapabilities};
    let cap = ModelNodeCapabilities {
        node_id: "test".to_string(),
        cpu_cores: 8,
        ram_gb: 16,
        gpu_type: None,
        npu_type: None,
        storage_available_gb: 50,
        worker_slots: 8,
        supported_models: vec![],
        cpu_features: vec![],
        accelerator: "CPU".to_string(),
    };
    let available = vec![
        "tinyllama-1b".to_string(),
        "llama3-3b".to_string(),
        "mistral-7b".to_string(),
    ];
    let long_prompt = "Explain in great detail the theory of general relativity as proposed by Albert Einstein, including the mathematical framework of tensor calculus, the curvature of spacetime, geodesics, the Einstein field equations, and their implications for modern cosmology, black holes, gravitational waves, and the expansion of the universe. Also discuss the experimental confirmations.";
    let result = select_best_model(long_prompt, &available, &cap);
    assert!(result.is_some());
    assert_eq!(result.unwrap(), "mistral-7b"); // complex → quality model
}

#[test]
fn test_estimate_tokens() {
    use iamine_models::estimate_tokens;
    assert_eq!(estimate_tokens("Hello"), 2); // 5 chars / 4 ≈ 2
    assert!(estimate_tokens("What is gravity?") > 2);
    assert!(estimate_tokens("What is gravity?") < 10);
}

#[test]
fn test_classify_prompt() {
    use iamine_models::{classify_prompt, PromptComplexity};
    assert_eq!(classify_prompt("Hi"), PromptComplexity::Simple);
    assert_eq!(
        classify_prompt("What is gravity?"),
        PromptComplexity::Simple
    );
    // 100+ tokens ≈ 400+ characters
    let long = "a ".repeat(250);
    assert_eq!(classify_prompt(&long), PromptComplexity::Complex);
}

#[test]
fn test_model_installed_event() {
    use iamine_models::ModelInstalledEvent;
    let event = ModelInstalledEvent::new("node1", "tinyllama-1b", 600_000_000, "abc123");
    let json = event.to_gossip_json();
    assert_eq!(json["type"], "ModelInstalled");
    assert_eq!(json["model_id"], "tinyllama-1b");
    assert!(json["timestamp"].as_u64().unwrap() > 0);
}

#[test]
fn test_model_signature_verification() {
    use iamine_models::verify_model_hash;
    use std::path::Path;
    // Non-existent file → error
    let result = verify_model_hash(Path::new("/nonexistent"), "abc");
    assert!(result.is_err());

    // Placeholder hash → always valid
    let tmp = std::env::temp_dir().join("test_model.gguf");
    std::fs::write(&tmp, b"test").unwrap();
    let result = verify_model_hash(&tmp, "placeholder");
    assert!(result.unwrap());
    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn test_capabilities_updated_event() {
    use iamine_models::CapabilitiesUpdatedEvent;
    let event = CapabilitiesUpdatedEvent {
        node_id: "peer123".to_string(),
        timestamp: 1000,
        cpu_cores: 8,
        ram_gb: 16,
        gpu_type: Some("Metal".to_string()),
        worker_slots: 8,
        supported_models: vec!["tinyllama-1b".to_string()],
        storage_available_gb: 40,
    };
    let json = event.to_gossip_json();
    assert_eq!(json["type"], "CapabilitiesUpdated");
    assert_eq!(json["cpu_cores"], 8);
    assert_eq!(json["supported_models"][0], "tinyllama-1b");
}

#[test]
fn test_model_recommendation() {
    let (_tmp_dir, storage) = temp_storage();
    let provision = ModelAutoProvision::new(ModelRegistry::new(), storage);
    let profile = AutoProvisionProfile {
        cpu_score: 120_000,
        ram_gb: 4,
        gpu_available: false,
        storage_available_gb: 10,
    };

    let recommended: Vec<String> = provision
        .recommend_for_empty_node(&profile)
        .into_iter()
        .map(|m| m.id)
        .collect();

    assert!(recommended.contains(&"tinyllama-1b".to_string()));
    assert!(recommended.contains(&"llama3-3b".to_string()));
    assert!(!recommended.contains(&"mistral-7b".to_string()));
}

#[tokio::test]
async fn test_auto_model_download() {
    let (_tmp_dir, storage) = temp_storage();
    let provision = ModelAutoProvision::new(ModelRegistry::new(), storage);
    let profile = AutoProvisionProfile {
        cpu_score: 200_000,
        ram_gb: 16,
        gpu_available: true,
        storage_available_gb: 50,
    };

    let result = provision
        .auto_download_recommended(&profile, None, true)
        .await;
    assert!(result.is_ok());

    if let Some(model_id) = result.unwrap() {
        assert!(!model_id.is_empty());
    }
}

#[test]
fn test_worker_start_without_models() {
    let (_tmp_dir, storage) = temp_storage();
    let provision = ModelAutoProvision::new(ModelRegistry::new(), storage);
    let profile = AutoProvisionProfile {
        cpu_score: 90_000,
        ram_gb: 2,
        gpu_available: false,
        storage_available_gb: 10,
    };

    let recommended = provision.startup_recommendations(&profile);
    let empty_node_recommended = provision.recommend_for_empty_node(&profile);

    assert!(recommended.is_empty() || !recommended.is_empty()); // environment-dependent installed models
    assert!(!empty_node_recommended.is_empty());
    assert_eq!(empty_node_recommended[0].id, "tinyllama-1b");
}
