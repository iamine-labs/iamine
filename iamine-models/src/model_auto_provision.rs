use crate::{DownloadProgress, ModelDescriptor, ModelDownloader, ModelRegistry, ModelStorage};

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
        let mut models: Vec<ModelDescriptor> = self.registry
            .list()
            .into_iter()
            .filter(|model| self.is_compatible(profile, model))
            .cloned()
            .collect();

        models.sort_by_key(|m| (m.required_ram_gb, m.size_bytes));
        models
    }

    pub fn recommend_compatible_models(&self, profile: &AutoProvisionProfile) -> Vec<ModelDescriptor> {
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
        let recommended = self.recommend_compatible_models(profile);
        let Some(model) = recommended.first() else {
            return Ok(None);
        };

        if mock {
            self.downloader.download_model_mock(model).await?;
        } else {
            self.downloader.download_model(model, progress_tx).await?;
        }

        Ok(Some(model.id.clone()))
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
