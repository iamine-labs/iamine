use crate::{
    evaluate_model_registry_admission, DownloadProgress, LicenseOperation, ModelDescriptor,
    ModelDownloader, ModelRegistry, ModelStorage,
};

#[derive(Debug, Clone)]
pub struct AutoProvisionProfile {
    pub cpu_score: u64,
    pub ram_gb: u32,
    pub gpu_available: bool,
    pub storage_available_gb: u32,
}

pub struct ModelAutoProvision {
    registry: ModelRegistry,
    downloader: ModelDownloader,
}

impl ModelAutoProvision {
    pub fn new(registry: ModelRegistry, storage: ModelStorage) -> Self {
        Self {
            registry,
            downloader: ModelDownloader::new(storage),
        }
    }

    pub fn installed_models(&self) -> Vec<String> {
        self.downloader.storage.list_local_models()
    }

    pub fn recommend_for_empty_node(&self, profile: &AutoProvisionProfile) -> Vec<ModelDescriptor> {
        let mut models: Vec<ModelDescriptor> = self
            .registry
            .list()
            .into_iter()
            .filter(|model| self.is_compatible(profile, model))
            .cloned()
            .collect();

        models.sort_by_key(|m| (m.required_ram_gb, m.size_bytes));
        models
    }

    pub fn recommend_compatible_models(
        &self,
        profile: &AutoProvisionProfile,
    ) -> Vec<ModelDescriptor> {
        let installed = self.installed_models();
        self.recommend_for_empty_node(profile)
            .into_iter()
            .filter(|model| !installed.iter().any(|m| m == &model.id))
            .collect()
    }

    pub fn startup_recommendations(&self, profile: &AutoProvisionProfile) -> Vec<ModelDescriptor> {
        if self.installed_models().is_empty() {
            self.recommend_for_empty_node(profile)
        } else {
            Vec::new()
        }
    }

    pub async fn auto_download_recommended(
        &self,
        profile: &AutoProvisionProfile,
        progress_tx: Option<tokio::sync::mpsc::Sender<DownloadProgress>>,
        mock: bool,
    ) -> Result<Option<String>, String> {
        let Some(model) = self.authorized_recommended_model(profile)? else {
            return Ok(None);
        };

        if mock {
            self.downloader.download_model_mock(&model).await?;
        } else {
            self.downloader.download_model(&model, progress_tx).await?;
        }

        Ok(Some(model.id.clone()))
    }

    fn authorized_recommended_model(
        &self,
        profile: &AutoProvisionProfile,
    ) -> Result<Option<ModelDescriptor>, String> {
        let recommended = self.recommend_compatible_models(profile);
        let Some(model) = recommended.first() else {
            return Ok(None);
        };

        let installed = self.downloader.storage.has_model(&model.id);
        let operation = if installed {
            LicenseOperation::ExistingExecution
        } else {
            LicenseOperation::Download
        };
        let admission = evaluate_model_registry_admission(model, operation, installed);
        if let Some(error) = admission.first_blocking_error() {
            return Err(error);
        }

        Ok(Some(model.clone()))
    }

    fn is_compatible(&self, profile: &AutoProvisionProfile, model: &ModelDescriptor) -> bool {
        let storage_needed_gb = model.size_bytes.div_ceil(1_073_741_824) as u32;

        profile.ram_gb >= model.required_ram_gb
            && profile.storage_available_gb >= storage_needed_gb
            && profile.cpu_score >= cpu_threshold(&model.id)
    }
}

fn cpu_threshold(model_id: &str) -> u64 {
    match model_id {
        "tinyllama-1b" => 50_000,
        "llama3-3b" => 100_000,
        "mistral-7b" => 140_000,
        "neural-chat-7b" => 120_000,
        "orca-mini-7b" => 110_000,
        "zephyr-7b" => 130_000,
        _ => 25_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LicenseClass, LicenseMetadata};
    use tempfile::TempDir;

    #[derive(Default)]
    struct DownloadSpy {
        mock_invocations: usize,
        real_invocations: usize,
    }

    impl ModelAutoProvision {
        async fn auto_download_recommended_with_spy_for_test(
            &self,
            profile: &AutoProvisionProfile,
            mock: bool,
            spy: &mut DownloadSpy,
        ) -> Result<Option<String>, String> {
            let Some(model) = self.authorized_recommended_model(profile)? else {
                return Ok(None);
            };

            if mock {
                spy.mock_invocations += 1;
            } else {
                spy.real_invocations += 1;
            }

            Ok(Some(model.id))
        }
    }

    fn temp_storage() -> Result<(TempDir, ModelStorage), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let storage = ModelStorage::new_in(dir.path().to_path_buf());
        Ok((dir, storage))
    }

    fn profile() -> AutoProvisionProfile {
        AutoProvisionProfile {
            cpu_score: 200_000,
            ram_gb: 64,
            gpu_available: false,
            storage_available_gb: 64,
        }
    }

    fn test_model(id: &str, license: LicenseMetadata) -> ModelDescriptor {
        ModelDescriptor {
            id: id.to_string(),
            version: "1.0".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 1_048_576,
            required_ram_gb: 1,
            required_vram_gb: 0,
            shards: 1,
            hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            download_url: format!("https://huggingface.co/iamine/{id}/resolve/main/{id}.gguf"),
            quantization: "q4_k_m".to_string(),
            license,
        }
    }

    fn allowed_license() -> LicenseMetadata {
        LicenseMetadata {
            license_id: Some("MIT".to_string()),
            license_url: Some("https://opensource.org/license/mit".to_string()),
            policy_class: Some(LicenseClass::Allowed),
            requires_acceptance: false,
            revision: Some("test-fixture".to_string()),
        }
    }

    fn restricted_license() -> LicenseMetadata {
        LicenseMetadata {
            license_id: Some("restricted-test".to_string()),
            license_url: Some("https://example.com/restricted-license".to_string()),
            policy_class: Some(LicenseClass::Restricted),
            requires_acceptance: false,
            revision: Some("test-fixture".to_string()),
        }
    }

    fn requires_acceptance_license() -> LicenseMetadata {
        LicenseMetadata {
            license_id: Some("acceptance-test".to_string()),
            license_url: Some("https://example.com/acceptance-license".to_string()),
            policy_class: Some(LicenseClass::RequiresAcceptance),
            requires_acceptance: true,
            revision: Some("test-fixture".to_string()),
        }
    }

    fn blocked_download_policy_model() -> ModelDescriptor {
        let mut model = test_model("blocked-download-model", allowed_license());
        model.download_url = "https://example.com/not-trusted.gguf".to_string();
        model
    }

    async fn assert_rejected_before_download(
        model: ModelDescriptor,
        expected_error: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, storage) = temp_storage()?;
        let storage_probe = storage.clone();
        let model_id = model.id.clone();
        let provision =
            ModelAutoProvision::new(ModelRegistry::from_models_for_test(vec![model]), storage);

        for mock in [true, false] {
            let mut spy = DownloadSpy::default();
            let result = provision
                .auto_download_recommended_with_spy_for_test(&profile(), mock, &mut spy)
                .await;

            match result {
                Err(error) => assert!(
                    error.contains(expected_error),
                    "expected error to contain {expected_error}, got {error}"
                ),
                Ok(other) => return Err(format!("expected rejection, got {other:?}").into()),
            }

            assert_eq!(spy.mock_invocations, 0);
            assert_eq!(spy.real_invocations, 0);
            assert!(!storage_probe.has_model(&model_id));
            assert!(!storage_probe.model_path(&model_id).exists());
        }

        Ok(())
    }

    #[tokio::test]
    async fn auto_provision_missing_metadata_returns_error_before_download(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_rejected_before_download(
            test_model("missing-license-model", LicenseMetadata::missing()),
            "license_missing",
        )
        .await
    }

    #[tokio::test]
    async fn auto_provision_missing_integrity_returns_error_before_download(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut model = test_model("missing-integrity-model", allowed_license());
        model.hash = String::new();
        assert_rejected_before_download(model, "checksum_missing").await
    }

    #[tokio::test]
    async fn auto_provision_restricted_license_returns_error_before_download(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_rejected_before_download(
            test_model("restricted-license-model", restricted_license()),
            "license_blocked",
        )
        .await
    }

    #[tokio::test]
    async fn auto_provision_requires_acceptance_returns_error_before_download(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_rejected_before_download(
            test_model("requires-acceptance-model", requires_acceptance_license()),
            "license_acceptance_required",
        )
        .await
    }

    #[tokio::test]
    async fn auto_provision_download_policy_block_returns_error_before_download(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_rejected_before_download(blocked_download_policy_model(), "untrusted_source").await
    }

    #[tokio::test]
    async fn auto_provision_denied_candidate_does_not_fallback_to_alternative(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, storage) = temp_storage()?;
        let storage_probe = storage.clone();
        let denied = test_model("aaa-denied-candidate", restricted_license());
        let mut alternative = test_model("zzz-allowed-alternative", allowed_license());
        alternative.size_bytes = denied.size_bytes * 2;
        let provision = ModelAutoProvision::new(
            ModelRegistry::from_models_for_test(vec![denied.clone(), alternative.clone()]),
            storage,
        );
        let mut spy = DownloadSpy::default();

        let result = provision
            .auto_download_recommended_with_spy_for_test(&profile(), true, &mut spy)
            .await;

        match result {
            Err(error) => assert!(
                error.contains("license_blocked"),
                "expected restricted candidate rejection, got {error}"
            ),
            Ok(other) => return Err(format!("expected rejection, got {other:?}").into()),
        }

        assert_eq!(spy.mock_invocations, 0);
        assert_eq!(spy.real_invocations, 0);
        assert!(!storage_probe.has_model(&denied.id));
        assert!(!storage_probe.has_model(&alternative.id));
        assert!(!storage_probe.model_path(&denied.id).exists());
        assert!(!storage_probe.model_path(&alternative.id).exists());
        Ok(())
    }
}
