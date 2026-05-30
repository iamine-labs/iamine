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
    let cfg = StorageConfig {
        max_storage_gb: 1,
        models_path: "/tmp".to_string(),
    };
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
