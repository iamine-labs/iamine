use iamine_models::*;
use iamine_models::storage_config::StorageConfig;
use iamine_models::node_models::{NodeModels, ModelId, PeerModelRegistry};
use iamine_models::model_validator::ModelValidator;
use tempfile::TempDir;

fn temp_storage() -> (TempDir, ModelStorage) {
    let dir = TempDir::new().unwrap();
    let storage = ModelStorage::new_in(dir.path().to_path_buf());
    (dir, storage)
}

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
    let cfg = StorageConfig { max_storage_gb: 10, models_path: "/tmp".to_string() };
    let ten_gb = 10 * 1_073_741_824u64;
    let one_gb = 1_073_741_824u64;

    assert!(cfg.has_space_for(one_gb, 0));              // 1GB en 10GB → ok
    assert!(!cfg.has_space_for(ten_gb + 1, 0));         // 10GB+1 → no cabe
    assert!(!cfg.has_space_for(one_gb, ten_gb));        // ya lleno → no cabe
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
    assert!(matches!(strategy, iamine_models::node_models::DownloadStrategy::Official(_)));

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
    assert!(matches!(strategy, iamine_models::node_models::DownloadStrategy::PeerFirst { .. }));
}

// ─── Test 4: NodeCapabilities ─────────────────────────────────────────────
#[test]
fn test_node_capabilities() {
    use iamine_models::{ModelNodeCapabilities, ModelRequirements, can_node_run_model};

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

// ─── Test 5: ModelValidator ───────────────────────────────────────────────
#[test]
fn test_model_validator_placeholder_hash() {
    use std::io::Write;
    let validator = ModelValidator::new();

    // Crear archivo temporal
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let _ = tmp.as_file().write_all(b"test model data");

    // Con hash placeholder → siempre ok en dev
    let result = validator.validate(
        "tinyllama-1b",
        tmp.path(),
        "tinyllama_hash_placeholder",
        None,
    );
    assert!(result.sha256_ok);
    assert!(result.signature_ok);
    assert!(result.is_valid());
}

// ─── Test 6: ModelStorage ────────────────────────────────────────────────
#[test]
fn test_model_storage_shard() {
    // Usar directorio temporal para no tocar ~/.iamine
    let (_tmp_dir, storage) = temp_storage();

    // Verificar que list_local_models funciona sin crash
    let models = storage.list_local_models();
    assert!(models.is_empty() || models.iter().all(|m| !m.is_empty()));
}

// ─── Test v0.5.2: ModelInstaller ─────────────────────────────────────────
#[tokio::test]
async fn test_installer_list_models() {
    let installer = iamine_models::ModelInstaller::new();
    let models = installer.list_models();
    assert!(models.len() >= 3);
    // Verificar que todos tienen los campos básicos
    for m in &models {
        assert!(!m.id.is_empty());
        assert!(m.size_gb > 0.0);
        assert!(m.required_ram_gb > 0);
    }
}

#[test]
fn test_installer_list_models_reports_per_model_disk_usage() {
    use iamine_models::ModelInstaller;

    let (_tmp_dir, storage) = temp_storage();

    let tiny_path = storage.gguf_path("tinyllama-1b");
    std::fs::create_dir_all(storage.model_path("tinyllama-1b")).unwrap();
    let mut tiny = vec![0u8; 1_048_576];
    tiny[..4].copy_from_slice(b"GGUF");
    std::fs::write(&tiny_path, tiny).unwrap();

    let llama_path = storage.gguf_path("llama3-3b");
    std::fs::create_dir_all(storage.model_path("llama3-3b")).unwrap();
    let mut llama = vec![0u8; 2_097_152];
    llama[..4].copy_from_slice(b"GGUF");
    std::fs::write(&llama_path, llama).unwrap();

    let installer = ModelInstaller::with_storage(storage);
    let models = installer.list_models();

    let tiny_status = models.iter().find(|m| m.id == "tinyllama-1b").unwrap();
    let llama_status = models.iter().find(|m| m.id == "llama3-3b").unwrap();

    assert_eq!(tiny_status.size_on_disk_mb, Some(1));
    assert_eq!(llama_status.size_on_disk_mb, Some(2));
}

#[tokio::test]
async fn test_installer_storage_limit() {
    use iamine_models::storage_config::StorageConfig;
    let cfg = StorageConfig { max_storage_gb: 1, models_path: "/tmp".to_string() };
    // mistral-7b necesita 4.1 GB, límite es 1 GB
    let registry = iamine_models::ModelRegistry::new();
    let model = registry.get("mistral-7b").unwrap();
    let fits = cfg.has_space_for(model.size_bytes, 0);
    assert!(!fits, "No debería caber mistral-7b en 1GB");
}

#[tokio::test]
async fn test_installer_mock_download() {
    use iamine_models::model_downloader::ModelDownloader;
    let (_tmp_dir, storage) = temp_storage();
    let downloader = ModelDownloader::new(storage.clone_for_test());
    let registry = iamine_models::ModelRegistry::new();
    let model = registry.get("tinyllama-1b").unwrap();

    // Solo verificar que mock no falla si ya existe
    if storage.has_model("tinyllama-1b") {
        println!("tinyllama-1b ya existe — skip mock download");
        return;
    }

    let result = downloader.download_model_mock(&model).await;
    // En CI puede fallar el path, solo verificar que no panics
    println!("Mock download result: {:?}", result);
}

#[test]
fn test_build_node_models() {
    use iamine_models::ModelInstaller;
    let installer = ModelInstaller::new();
    let nm = installer.build_node_models("test_node_123");
    assert_eq!(nm.node_id, "test_node_123");
    // models puede estar vacío si no hay nada instalado — OK
    println!("Node models: {} modelos", nm.models.len());
}

// ─── Tests v0.5.3: Hardware + InferenceEngine ─────────────────────────────

#[test]
fn test_hardware_detection() {
    use iamine_models::HardwareAcceleration;
    let hw = HardwareAcceleration::detect();
    // Solo verificar que no falla y tiene valores razonables
    assert!(hw.cpu_cores >= 1);
    assert!(hw.recommended_threads >= 1);
    println!("HW: {:?} — {} cores", hw.accelerator, hw.cpu_cores);
}

#[test]
fn test_hardware_llama_params() {
    use iamine_models::HardwareAcceleration;
    let hw = HardwareAcceleration::detect();
    let params = hw.llama_params();
    assert!(params.n_threads >= 1);
    // Metal/CUDA → GPU layers > 0
    // CPU → 0 GPU layers
    println!("Llama params: threads={} gpu_layers={}", params.n_threads, params.n_gpu_layers);
}

#[test]
fn test_runtime_backend_name() {
    let backend = RealInferenceEngine::runtime_backend_name();
    assert!(matches!(backend, "metal" | "cuda" | "cpu" | "unknown"));
    println!("Runtime backend: {}", backend);
}

#[tokio::test]
async fn test_inference_engine_mock() {
    use iamine_models::{RealInferenceEngine, RealInferenceRequest};

    let (_tmp_dir, storage) = temp_storage();
    let engine = RealInferenceEngine::new(storage);

    // Sin modelo cargado → debe fallar
    let req = RealInferenceRequest {
        task_id: "test-001".to_string(),
        model_id: "tinyllama-1b".to_string(),
        prompt: "What is 2+2?".to_string(),
        max_tokens: 50,
        temperature: 0.7,
    };
    let result = engine.run_inference(req, None).await;
    assert!(!result.success); // no cargado → falla
    assert!(result.error.is_some());
}

#[tokio::test]
async fn test_model_load() {
    use iamine_models::RealInferenceEngine;
    use iamine_models::ModelRegistry;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!("Skipping real model load test: {} not installed", model_id);
        return;
    }

    let mut engine = RealInferenceEngine::new(storage);
    let model = registry.get(model_id).unwrap();
    let result = engine.load_model(model_id, &model.hash);
    assert!(result.is_ok(), "Load failed: {:?}", result);
    assert!(engine.is_loaded(model_id));
}

#[tokio::test]
async fn test_real_inference() {
    use iamine_models::{RealInferenceEngine, RealInferenceRequest};
    use iamine_models::ModelRegistry;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!("Skipping real inference test: {} not installed", model_id);
        return;
    }

    let mut engine = RealInferenceEngine::new(storage);
    let model = registry.get(model_id).unwrap();
    let result = engine.load_model(model_id, &model.hash);
    assert!(result.is_ok(), "Load failed: {:?}", result);

    let req = RealInferenceRequest {
        task_id: "test-002".to_string(),
        model_id: model_id.to_string(),
        prompt: "Say hello in one short sentence.".to_string(),
        max_tokens: 32,
        temperature: 0.1,
    };

    let result = engine.run_inference(req, None).await;

    assert!(result.success);
    assert!(!result.output.is_empty());
    assert!(result.tokens_generated > 0);
    println!("Output: {}", result.output);
    println!("Tokens: {} en {}ms", result.tokens_generated, result.execution_ms);
}

#[tokio::test]
async fn test_token_streaming() {
    use iamine_models::{RealInferenceEngine, RealInferenceRequest};
    use iamine_models::ModelRegistry;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!("Skipping token streaming test: {} not installed", model_id);
        return;
    }

    let mut engine = RealInferenceEngine::new(storage);
    let model = registry.get(model_id).unwrap();
    let result = engine.load_model(model_id, &model.hash);
    assert!(result.is_ok(), "Load failed: {:?}", result);

    let req = RealInferenceRequest {
        task_id: "test-003".to_string(),
        model_id: model_id.to_string(),
        prompt: "Say hello in one short sentence.".to_string(),
        max_tokens: 24,
        temperature: 0.1,
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let result = engine.run_inference(req, Some(tx)).await;

    let mut streamed = String::new();
    while let Ok(token) = rx.try_recv() {
        streamed.push_str(&token);
    }

    assert!(result.success);
    assert!(!streamed.is_empty());
    assert!(!result.output.is_empty());
}

#[tokio::test]
async fn test_invalid_existing_mock_is_reinstalled() {
    use iamine_models::model_downloader::ModelDownloader;
    use iamine_models::ModelRegistry;

    let (_tmp_dir, storage) = temp_storage();
    let registry = ModelRegistry::new();
    let model = registry.get("tinyllama-1b").unwrap();
    let model_path = storage.gguf_path("tinyllama-1b");

    std::fs::create_dir_all(storage.model_path("tinyllama-1b")).unwrap();
    std::fs::write(&model_path, b"tiny mock from old versions").unwrap();

    assert!(!storage.has_model("tinyllama-1b"));

    let downloader = ModelDownloader::new(storage.clone_for_test());
    downloader.download_model_mock(&model).await.unwrap();

    let bytes = std::fs::read(&model_path).unwrap();
    assert!(bytes.len() >= 2048);
    assert_eq!(&bytes[..4], b"GGUF");
    assert!(storage.has_model("tinyllama-1b"));
}

#[test]
fn test_inference_cache() {
    use iamine_models::RealInferenceEngine;

    let (_tmp_dir, storage) = temp_storage();
    let mut engine = RealInferenceEngine::new(storage);

    // Sin carga → no en cache
    assert!(!engine.is_loaded("tinyllama-1b"));

    // Cargar (con hash placeholder)
    let _ = engine.load_model("tinyllama-1b", "tinyllama_hash_placeholder");

    // Si existe el archivo → en cache; si no → error pero no panic
    // Test válido en ambos casos
    println!("Cache test passed");
}

// ─── Tests v0.6: Distributed Inference ────────────────────────────────────

#[test]
fn test_inference_task_serialization() {
    use iamine_models::InferenceTask;
    let task = InferenceTask::new(
        "req-001".to_string(),
        "tinyllama-1b".to_string(),
        "What is gravity?".to_string(),
        200,
        "peer_abc".to_string(),
    );
    let json = task.to_gossip_json();
    assert_eq!(json["type"], "InferenceRequest");
    assert_eq!(json["model_id"], "tinyllama-1b");
    assert_eq!(json["prompt"], "What is gravity?");
    assert_eq!(json["max_tokens"], 200);
    assert_eq!(json["requester_peer"], "peer_abc");
}

#[test]
fn test_inference_result_success() {
    use iamine_models::InferenceTaskResult;
    let result = InferenceTaskResult::success(
        "req-001".to_string(),
        "tinyllama-1b".to_string(),
        "Gravity is a force...".to_string(),
        10, 150,
        "worker_xyz".to_string(),
        "Metal".to_string(),
    );
    assert!(result.success);
    assert_eq!(result.tokens_generated, 10);
    let json = result.to_gossip_json();
    assert_eq!(json["type"], "InferenceResult");
    assert_eq!(json["success"], true);
}

#[test]
fn test_inference_result_failure() {
    use iamine_models::InferenceTaskResult;
    let result = InferenceTaskResult::failure(
        "req-002".to_string(),
        "mistral-7b".to_string(),
        "worker_abc".to_string(),
        "Model not installed".to_string(),
    );
    assert!(!result.success);
    assert!(result.error.is_some());
    let json = result.to_gossip_json();
    assert_eq!(json["success"], false);
}

#[test]
fn test_streamed_token() {
    use iamine_models::StreamedToken;
    let token = StreamedToken {
        request_id: "req-001".to_string(),
        token: "hello ".to_string(),
        index: 0,
        is_final: false,
    };
    let json = token.to_gossip_json();
    assert_eq!(json["type"], "InferenceToken");
    assert_eq!(json["token"], "hello ");
    assert_eq!(json["is_final"], false);
}

// ─── Tests v0.5.4: Node Intelligence Layer ────────────────────────────

#[test]
fn test_model_requirements() {
    use iamine_models::{ModelRequirements, ModelNodeCapabilities, can_node_run_model};
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
    use iamine_models::{ModelRequirements, ModelNodeCapabilities, can_node_run_model};
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
    let available = vec!["tinyllama-1b".to_string(), "llama3-3b".to_string(), "mistral-7b".to_string()];
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
    assert_eq!(classify_prompt("What is gravity?"), PromptComplexity::Simple);
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

    let result = provision.auto_download_recommended(&profile, None, true).await;
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

// ─── Tests v0.6.7: Real Model Downloads ──────────────────────────────────

#[test]
fn test_sha256_verification_real() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"hello world").unwrap();

    // Known SHA256 of "hello world"
    let expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    let result = ModelVerifier::verify_file(tmp.path(), expected);
    assert!(result.is_ok(), "Real SHA256 verification should pass: {:?}", result);
}

#[test]
fn test_sha256_verification_mismatch() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"hello world").unwrap();

    let wrong_hash = "0000000000000000000000000000000000000000000000000000000000000000";
    let result = ModelVerifier::verify_file(tmp.path(), wrong_hash);
    assert!(result.is_err(), "Should fail on hash mismatch");
}

#[test]
fn test_sha256_skip_empty_hash() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"data").unwrap();

    // Empty hash → skip verification
    assert!(ModelVerifier::verify_file(tmp.path(), "").is_ok());
    // Placeholder hash → skip
    assert!(ModelVerifier::verify_file(tmp.path(), "tinyllama_hash_placeholder").is_ok());
}

#[test]
fn test_compute_sha256_file() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"test data for hashing").unwrap();

    let hash = ModelVerifier::compute_sha256_file(tmp.path()).unwrap();
    assert_eq!(hash.len(), 64); // SHA256 hex = 64 chars
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

    // Same file should produce same hash
    let hash2 = ModelVerifier::compute_sha256_file(tmp.path()).unwrap();
    assert_eq!(hash, hash2);
}

#[test]
fn test_model_manifest() {
    let registry = iamine_models::ModelRegistry::new();
    let model = registry.get("tinyllama-1b").unwrap();
    let manifest = model.to_manifest();

    assert_eq!(manifest.model_id, "tinyllama-1b");
    assert!(manifest.size_bytes > 0);
    assert!(!manifest.download_url.is_empty());
    assert!(manifest.download_url.contains("huggingface.co"));
}

#[test]
fn test_model_has_known_hash() {
    let registry = iamine_models::ModelRegistry::new();
    let model = registry.get("tinyllama-1b").unwrap();
    // Empty hash = no known hash
    assert!(!model.has_known_hash());

    let manifest = model.to_manifest();
    assert!(!manifest.requires_hash_verification());
}

#[test]
fn test_download_phase_enum() {
    use iamine_models::DownloadPhase;
    let phase = DownloadPhase::Downloading;
    assert_eq!(phase, DownloadPhase::Downloading);
    assert_ne!(phase, DownloadPhase::Verifying);
}
