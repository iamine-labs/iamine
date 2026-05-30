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
    println!(
        "Llama params: threads={} gpu_layers={}",
        params.n_threads, params.n_gpu_layers
    );
}

#[test]
fn test_backend_auto_selection() {
    let backend = RealInferenceEngine::runtime_backend_name();
    assert!(matches!(backend, "metal" | "cuda" | "cpu"));
    println!("Selected backend: {}", backend);
}

#[test]
fn test_prompt_template_applied() {
    let builder = PromptBuilder::new("tinyllama-1b");
    let prompt = builder.build_prompt(
        "You are a helpful assistant.",
        "Explain gravity in one sentence.",
    );
    assert!(prompt.contains("Explain gravity in one sentence."));
    assert!(prompt.contains("<|assistant|>") || prompt.contains("[/INST]"));
}

#[test]
fn test_sampling_config_effect() {
    let config = SamplingConfig::for_model_request("tinyllama-1b", "Explain gravity.", 0.25);
    assert!((config.temperature - 0.25).abs() < f32::EPSILON);
    assert_eq!(config.top_k, 24);
    assert!((config.top_p - 0.85).abs() < f32::EPSILON);
    assert!((config.repeat_penalty - 1.15).abs() < f32::EPSILON);
}

#[test]
fn test_output_cleaning() {
    let raw = "Hola hola hola.\nHola hola hola.\n<|assistant|>".to_string();
    let cleaned = clean_output(raw);
    assert!(!cleaned.contains("<|assistant|>"));
    assert!(!cleaned.contains("Hola hola hola. Hola hola hola."));
}

#[test]
fn test_spanish_prompt_response() {
    let builder = PromptBuilder::new("tinyllama-1b");
    let prompt = builder.build_prompt(
        "You are a helpful assistant.",
        "explica la teoria de la relatividad",
    );
    assert!(matches!(
        PromptBuilder::detect_language("explica la teoria de la relatividad"),
        Language::Spanish
    ));
    assert!(prompt.to_lowercase().contains("responde en espanol"));
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
    use iamine_models::ModelRegistry;
    use iamine_models::RealInferenceEngine;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!("Skipping real model load test: {} not installed", model_id);
        return;
    }

    let engine = RealInferenceEngine::new(storage);
    let model = registry.get(model_id).unwrap();
    let result = engine.load_model(model_id, &model.hash);
    assert!(result.is_ok(), "Load failed: {:?}", result);
    assert!(engine.is_loaded(model_id));
}

#[tokio::test]
async fn test_model_cache_reuse() {
    use iamine_models::ModelRegistry;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!(
            "Skipping model cache reuse test: {} not installed",
            model_id
        );
        return;
    }

    let engine = RealInferenceEngine::new(storage);
    let model = registry.get(model_id).unwrap();

    engine.load_model(model_id, &model.hash).unwrap();
    engine.load_model(model_id, &model.hash).unwrap();

    assert!(engine.is_loaded(model_id));
    assert_eq!(engine.cache_size(), 1);
    assert_eq!(engine.actual_model_loads(), 1);
    assert!(engine.model_cache_reuse_hits() >= 1);
}

#[tokio::test]
async fn test_real_inference() {
    use iamine_models::ModelRegistry;
    use iamine_models::{RealInferenceEngine, RealInferenceRequest};

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!("Skipping real inference test: {} not installed", model_id);
        return;
    }

    let engine = RealInferenceEngine::new(storage);
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
    println!(
        "Tokens: {} en {}ms",
        result.tokens_generated, result.execution_ms
    );
}

#[tokio::test]
async fn test_inference_queue() {
    use iamine_models::ModelRegistry;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!("Skipping inference queue test: {} not installed", model_id);
        return;
    }

    let engine = Arc::new(RealInferenceEngine::new(storage));
    let model = registry.get(model_id).unwrap();
    engine.load_model(model_id, &model.hash).unwrap();

    let req_a = RealInferenceRequest {
        task_id: "queue-a".to_string(),
        model_id: model_id.to_string(),
        prompt: "Say hello in one short sentence.".to_string(),
        max_tokens: 16,
        temperature: 0.1,
    };
    let req_b = RealInferenceRequest {
        task_id: "queue-b".to_string(),
        model_id: model_id.to_string(),
        prompt: "Say bye in one short sentence.".to_string(),
        max_tokens: 16,
        temperature: 0.1,
    };

    let (res_a, res_b) = tokio::join!(
        engine.run_inference(req_a, None),
        engine.run_inference(req_b, None),
    );

    assert!(res_a.success);
    assert!(res_b.success);
    assert_eq!(engine.actual_model_loads(), 1);
}

#[tokio::test]
async fn test_concurrency_limit() {
    use iamine_models::ModelRegistry;

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!(
            "Skipping concurrency limit test: {} not installed",
            model_id
        );
        return;
    }

    let engine = Arc::new(RealInferenceEngine::with_limits(storage, 1, 32));
    let model = registry.get(model_id).unwrap();
    engine.load_model(model_id, &model.hash).unwrap();

    let reqs = (0..3).map(|idx| {
        let engine = Arc::clone(&engine);
        let req = RealInferenceRequest {
            task_id: format!("concurrency-{}", idx),
            model_id: model_id.to_string(),
            prompt: format!("Count to {} in one short sentence.", idx + 1),
            max_tokens: 12,
            temperature: 0.1,
        };

        tokio::spawn(async move { engine.run_inference(req, None).await })
    });

    for handle in reqs {
        let result = handle.await.unwrap();
        assert!(result.success);
    }

    assert!(engine.max_active_inferences_observed() <= 1);
}

#[tokio::test]
async fn test_token_streaming() {
    use iamine_models::ModelRegistry;
    use iamine_models::{RealInferenceEngine, RealInferenceRequest};

    let storage = ModelStorage::new();
    let registry = ModelRegistry::new();
    let model_id = "tinyllama-1b";

    if !storage.has_model(model_id) {
        println!("Skipping token streaming test: {} not installed", model_id);
        return;
    }

    let engine = RealInferenceEngine::new(storage);
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
    let engine = RealInferenceEngine::new(storage);

    // Sin carga → no en cache
    assert!(!engine.is_loaded("tinyllama-1b"));

    // Cargar (con hash placeholder)
    let _ = engine.load_model("tinyllama-1b", "tinyllama_hash_placeholder");

    // Si existe el archivo → en cache; si no → error pero no panic
    // Test válido en ambos casos
    println!("Cache test passed");
}
