use super::*;
use crate::backend_runtime::{
    build_mock_inference_result, CpuFeatureSupport, InferenceBackendKind,
};
use crate::startup_model_validation::refresh_models_for_advertising;

fn write_valid_gguf(storage: &ModelStorage, model_id: &str) {
    let path = storage.gguf_path(model_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create model dir");
    }
    let mut data = vec![0u8; 2048];
    data[..4].copy_from_slice(b"GGUF");
    std::fs::write(path, data).expect("write gguf");
}

fn backend_state_real_available() -> InferenceBackendState {
    InferenceBackendState {
        configured_backend: InferenceBackendKind::Real,
        skip_model_validation: false,
        cpu_features: CpuFeatureSupport {
            avx: true,
            avx2: true,
            fma: true,
            f16c: true,
            sse4_1: true,
            sse4_2: true,
        },
        missing_cpu_features: Vec::new(),
        real_backend_available: true,
    }
}

fn backend_state_real_unavailable() -> InferenceBackendState {
    InferenceBackendState {
        configured_backend: InferenceBackendKind::Real,
        skip_model_validation: false,
        cpu_features: CpuFeatureSupport {
            avx: false,
            avx2: false,
            fma: false,
            f16c: false,
            sse4_1: true,
            sse4_2: true,
        },
        missing_cpu_features: vec![
            "avx".to_string(),
            "avx2".to_string(),
            "fma".to_string(),
            "f16c".to_string(),
        ],
        real_backend_available: false,
    }
}

fn backend_state_mock() -> InferenceBackendState {
    InferenceBackendState {
        configured_backend: InferenceBackendKind::Mock,
        skip_model_validation: false,
        cpu_features: CpuFeatureSupport {
            avx: false,
            avx2: false,
            fma: false,
            f16c: false,
            sse4_1: true,
            sse4_2: true,
        },
        missing_cpu_features: vec![
            "avx".to_string(),
            "avx2".to_string(),
            "fma".to_string(),
            "f16c".to_string(),
        ],
        real_backend_available: false,
    }
}

#[test]
fn test_backend_cli_mock_selection() {
    let args = vec![
        "iamine-node".to_string(),
        "--worker".to_string(),
        "--inference-backend".to_string(),
        "mock".to_string(),
    ];
    let state = InferenceBackendState::from_args(&args);
    assert!(state.mock_enabled());
    assert!(state.is_backend_available());
    assert!(state.should_skip_startup_model_load());
}

#[test]
fn test_skip_model_validation_flag_enables_startup_skip() {
    let args = vec![
        "iamine-node".to_string(),
        "--worker".to_string(),
        "--skip-model-validation".to_string(),
    ];
    let state = InferenceBackendState::from_args(&args);
    assert!(state.skip_model_validation);
    assert!(state.should_skip_startup_model_load());
}

#[test]
fn test_unsupported_cpu_marks_real_backend_unavailable() {
    let state = backend_state_real_unavailable();
    assert!(!state.real_backend_available);
    assert!(!state.is_backend_available());
    assert!(!state.should_advertise_inference_capabilities());
}

#[test]
fn test_backend_unavailable_does_not_advertise_models() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let storage = ModelStorage::new_in(tmp_dir.path().join("models"));
    write_valid_gguf(&storage, "tinyllama-1b");

    let registry = ModelRegistry::new();
    let node_caps = ModelNodeCapabilities::detect("test-node");
    let advertised = refresh_models_for_advertising(
        &registry,
        &storage,
        &node_caps,
        &backend_state_real_unavailable(),
    );

    assert!(advertised.is_empty());
}

#[test]
fn test_mock_backend_advertises_catalog_without_local_model() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let storage = ModelStorage::new_in(tmp_dir.path().join("models"));

    let registry = ModelRegistry::new();
    let node_caps = ModelNodeCapabilities::detect("test-node");
    let advertised =
        refresh_models_for_advertising(&registry, &storage, &node_caps, &backend_state_mock());

    assert!(!advertised.is_empty());
    assert!(advertised.iter().any(|model| model == "tinyllama-1b"));
}

#[test]
fn test_capabilities_refresh_detects_model_install_change() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let storage = ModelStorage::new_in(tmp_dir.path().join("models"));
    let registry = ModelRegistry::new();
    let node_caps = ModelNodeCapabilities::detect("test-node");

    let before = refresh_models_for_advertising(
        &registry,
        &storage,
        &node_caps,
        &backend_state_real_available(),
    );
    assert!(before.is_empty());

    write_valid_gguf(&storage, "tinyllama-1b");
    let after = refresh_models_for_advertising(
        &registry,
        &storage,
        &node_caps,
        &backend_state_real_available(),
    );

    assert!(after.iter().any(|model| model == "tinyllama-1b"));
}

#[test]
fn test_mock_inference_result_is_deterministic_and_tagged() {
    let request = RealInferenceRequest {
        task_id: "req-1".to_string(),
        model_id: "tinyllama-1b".to_string(),
        prompt: "2+2".to_string(),
        max_tokens: 64,
        temperature: 0.7,
    };

    let first = build_mock_inference_result(&request, "task-123");
    let second = build_mock_inference_result(&request, "task-123");

    assert_eq!(first.output, second.output);
    assert_eq!(first.accelerator_used, "mock");
    assert!(first.output.contains("[MOCK:"));
}
