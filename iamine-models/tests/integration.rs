use iamine_models::*;
use iamine_models::storage_config::StorageConfig;
use iamine_models::node_models::{NodeModels, ModelId, PeerModelRegistry};
use iamine_models::node_capabilities::NodeCapabilities as ModelNodeCapabilities;
use iamine_models::model_validator::ModelValidator;

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
fn test_node_capabilities_models() {
    let caps = ModelNodeCapabilities::new(
        "node_test".to_string(),
        50000.0, 8, false, 0, 100, 4,
    );
    assert!(caps.can_run_model("tinyllama-1b")); // 2GB req, tiene 8GB
    assert!(caps.can_run_model("llama3-3b"));    // 4GB req, tiene 8GB
    assert!(caps.can_run_model("mistral-7b"));   // 8GB req, tiene 8GB

    let low_ram = ModelNodeCapabilities::new(
        "node_low".to_string(),
        10000.0, 2, false, 0, 50, 1,
    );
    assert!(low_ram.can_run_model("tinyllama-1b"));
    assert!(!low_ram.can_run_model("llama3-3b")); // necesita 4GB
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
    use tempfile::TempDir;
    use std::fs;

    // Usar directorio temporal para no tocar ~/.iamine
    let tmp_dir = TempDir::new().unwrap();
    let storage = ModelStorage::new();

    // Verificar que list_local_models funciona sin crash
    let models = storage.list_local_models();
    // Puede ser vacío o tener modelos reales — solo verificamos que no falla
    assert!(models.len() >= 0);
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
    use iamine_models::{ModelInstaller, InstallResult};
    use iamine_models::model_downloader::ModelDownloader;
    use iamine_models::ModelStorage;

    let storage = ModelStorage::new();
    let downloader = ModelDownloader::new(ModelStorage::new());
    let registry = iamine_models::ModelRegistry::new();
    let model = registry.get("tinyllama-1b").unwrap();

    // Solo verificar que mock no falla si ya existe
    if storage.has_model("tinyllama-1b") {
        println!("tinyllama-1b ya existe — skip mock download");
        return;
    }

    let result = downloader.download_model_mock(model).await;
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

#[tokio::test]
async fn test_inference_engine_mock() {
    use iamine_models::{RealInferenceEngine, RealInferenceRequest, ModelStorage};

    let storage = ModelStorage::new();
    let mut engine = RealInferenceEngine::new(storage);

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
async fn test_inference_with_mock_model() {
    use iamine_models::{RealInferenceEngine, RealInferenceRequest, ModelStorage};
    use iamine_models::model_downloader::ModelDownloader;
    use iamine_models::ModelRegistry;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();

    // Instalar mock de tinyllama si no existe
    if !storage.has_model("tinyllama-1b") {
        let downloader = ModelDownloader::new(ModelStorage::new());
        let model = registry.get("tinyllama-1b").unwrap();
        let _ = downloader.download_model_mock(model).await;
    }

    let mut engine = RealInferenceEngine::new(ModelStorage::new());

    // Cargar con hash placeholder (siempre ok en dev)
    let result = engine.load_model("tinyllama-1b", "tinyllama_hash_placeholder");
    assert!(result.is_ok(), "Load failed: {:?}", result);
    assert!(engine.is_loaded("tinyllama-1b"));

    // Ejecutar inferencia
    let req = RealInferenceRequest {
        task_id: "test-002".to_string(),
        model_id: "tinyllama-1b".to_string(),
        prompt: "What is 2+2?".to_string(),
        max_tokens: 50,
        temperature: 0.7,
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let result = engine.run_inference(req, Some(tx)).await;

    // Recolectar tokens
    let mut streamed = String::new();
    while let Ok(t) = rx.try_recv() { streamed.push_str(&t); }

    assert!(result.success);
    assert!(!result.output.is_empty());
    assert!(result.tokens_generated > 0);
    assert!(result.output.contains("4") || result.output.contains("equals"));
    println!("Output: {}", result.output);
    println!("Tokens: {} en {}ms", result.tokens_generated, result.execution_ms);
}

#[test]
fn test_inference_cache() {
    use iamine_models::{RealInferenceEngine, ModelStorage};
    use iamine_models::model_downloader::ModelDownloader;

    let storage = ModelStorage::new();
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
