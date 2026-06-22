#[test]
fn test_models_list_reports_download_and_license_gates() -> Result<(), Box<dyn std::error::Error>>
{
    let (_tmp_dir, storage) = temp_storage();
    let installer = ModelInstaller::with_storage(storage);
    let models = installer.list_models();

    let Some(tiny) = models.iter().find(|model| model.id == "tinyllama-1b") else {
        return Err("tinyllama-1b should be present in registry list".into());
    };

    assert_eq!(tiny.download_policy_status, "pending_checksum");
    assert_eq!(tiny.download_policy_reason, "checksum_missing");
    assert_eq!(tiny.registry_integrity_status, "pending_integrity");
    assert_eq!(tiny.registry_integrity_reason, "checksum_missing");
    assert_eq!(tiny.license_policy_status, "pending_metadata");
    assert_eq!(tiny.license_policy_reason, "license_missing");
    assert_eq!(tiny.license_acceptance_status, "not_required");
    assert_eq!(
        tiny.license_acceptance_reason,
        "license_acceptance_not_required"
    );
    assert_eq!(tiny.network_policy_status, "allowed");
    assert_eq!(tiny.network_policy_reason, "network_policy_allowed");
    Ok(())
}

#[test]
fn test_installed_missing_license_is_legacy_for_list_and_execution(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, storage) = temp_storage();
    std::fs::create_dir_all(storage.model_path("tinyllama-1b"))?;
    let mut bytes = vec![0u8; 2048];
    bytes[..4].copy_from_slice(b"GGUF");
    std::fs::write(storage.gguf_path("tinyllama-1b"), bytes)?;

    let installer = ModelInstaller::with_storage(storage.clone());
    let models = installer.list_models();
    let Some(tiny) = models.iter().find(|model| model.id == "tinyllama-1b") else {
        return Err("tinyllama-1b should be present in registry list".into());
    };

    assert!(tiny.installed);
    assert_eq!(tiny.license_policy_status, "pending_metadata");
    assert_eq!(tiny.license_policy_reason, "legacy_installed_model");
    assert_eq!(tiny.license_acceptance_status, "not_required");
    assert_eq!(
        tiny.license_acceptance_reason,
        "license_acceptance_not_required"
    );
    assert_eq!(tiny.network_policy_status, "allowed");
    assert_eq!(tiny.network_policy_reason, "network_policy_allowed");

    let registry = ModelRegistry::new();
    let Some(model) = registry.get("tinyllama-1b") else {
        return Err("tinyllama-1b should be present in registry".into());
    };
    let decision =
        ModelLicensePolicy.evaluate_descriptor(model, LicenseOperation::ExistingExecution, true);
    assert!(decision.permits_operation);
    assert_eq!(decision.reason, LicensePolicyReason::LegacyInstalledModel);
    Ok(())
}

#[tokio::test]
async fn test_installer_blocks_missing_integrity_before_artifact(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, storage) = temp_storage();
    let installer = ModelInstaller::with_storage(storage.clone());

    let result = installer.install("llama3-3b", "test-node", None).await;

    match result {
        InstallResult::DownloadFailed(error) => {
            assert!(error.contains("model registry integrity policy"));
            assert!(error.contains("checksum_missing"));
        }
        other => return Err(format!("expected integrity gate failure, got {other:?}").into()),
    }
    assert!(!storage.gguf_path("llama3-3b").exists());
    assert!(!storage.model_path("llama3-3b").exists());
    Ok(())
}

#[tokio::test]
async fn test_mock_download_blocks_missing_integrity_before_artifact(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, storage) = temp_storage();
    let downloader = ModelDownloader::new(storage.clone_for_test());
    let registry = ModelRegistry::new();
    let Some(model) = registry.get("tinyllama-1b") else {
        return Err("tinyllama-1b should be present in registry".into());
    };

    let result = downloader.download_model_mock(model).await;

    match result {
        Err(error) => {
            assert!(error.contains("model registry integrity policy"));
            assert!(error.contains("checksum_missing"));
        }
        Ok(_) => return Err("mock download should be blocked by missing integrity".into()),
    }
    assert!(!storage.gguf_path("tinyllama-1b").exists());
    Ok(())
}

#[tokio::test]
async fn test_mock_download_allows_explicit_allowed_license_fixture(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, storage) = temp_storage();
    let downloader = ModelDownloader::new(storage.clone_for_test());
    let registry = ModelRegistry::new();
    let Some(model) = registry.get("tinyllama-1b") else {
        return Err("tinyllama-1b should be present in registry".into());
    };
    let model = with_allowed_test_license(model);

    downloader.download_model_mock(&model).await?;

    assert!(storage.gguf_path("tinyllama-1b").exists());
    assert!(storage.has_model("tinyllama-1b"));
    Ok(())
}

#[test]
fn test_build_node_models_preserves_legacy_installed_model(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, storage) = temp_storage();
    std::fs::create_dir_all(storage.model_path("tinyllama-1b"))?;
    let mut bytes = vec![0u8; 2048];
    bytes[..4].copy_from_slice(b"GGUF");
    std::fs::write(storage.gguf_path("tinyllama-1b"), bytes)?;

    let installer = ModelInstaller::with_storage(storage);
    let node_models = installer.build_node_models("test-node");

    assert!(node_models
        .models
        .iter()
        .any(|model| model.id == "tinyllama-1b"));
    Ok(())
}
