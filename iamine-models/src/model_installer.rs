use crate::model_registry::ModelRegistry;
use crate::model_storage::ModelStorage;
use crate::node_models::NodeModels;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub size_gb: f64,
    pub required_ram_gb: u32,
    pub version: String,
}

impl ModelInfo {
    pub fn display(&self) {
        println!("  {} v{} ({:.1}GB, {}GB RAM)",
            self.id, self.version, self.size_gb, self.required_ram_gb);
    }
}

#[derive(Debug, Clone)]
pub enum InstallResult {
    Installed(String),
    AlreadyExists(String),
    InsufficientStorage { needed_gb: f64, available_gb: f64 },
    DownloadFailed(String),
    ValidationFailed(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModelStatus {
    NotInstalled,
    Installing,
    Installed,
    Failed,
}

pub struct ModelInstaller;

impl ModelInstaller {
    pub fn new() -> Self {
        Self
    }

    pub fn list_models(&self) -> Vec<ModelInfo> {
        let registry = ModelRegistry::new();
        registry.list().into_iter().map(|m| {
            let size_gb = m.size_gb();
            let required_ram_gb = m.required_ram_gb;
            let version = m.version.clone();
            let id = m.id.clone();
            ModelInfo { id, size_gb, required_ram_gb, version }
        }).collect()
    }

    pub async fn install(
        &self,
        model_id: &str,
        _source: &str,
        _progress_tx: Option<tokio::sync::mpsc::Sender<crate::DownloadProgress>>,
    ) -> InstallResult {
        let registry = ModelRegistry::new();
        let storage = ModelStorage::new();
        let config = crate::StorageConfig::load();

        let model = match registry.get(model_id) {
            Some(m) => m,
            None => return InstallResult::DownloadFailed(format!("Modelo {} no encontrado", model_id)),
        };

        if storage.has_model(model_id) {
            return InstallResult::AlreadyExists(model_id.to_string());
        }

        let used_bytes = storage.total_size_bytes();
        let available_bytes = (config.max_storage_gb as u64 * 1_073_741_824) - used_bytes;

        if model.size_bytes > available_bytes {
            let needed_gb = model.size_bytes as f64 / 1_073_741_824.0;
            let available_gb = available_bytes as f64 / 1_073_741_824.0;
            return InstallResult::InsufficientStorage { needed_gb, available_gb };
        }

        let downloader = crate::ModelDownloader::new(storage);
        match downloader.download_model_mock(&model).await {
            Ok(_) => InstallResult::Installed(model_id.to_string()),
            Err(e) => InstallResult::DownloadFailed(e),
        }
    }

    pub fn remove(&self, model_id: &str) -> Result<(), String> {
        let storage = ModelStorage::new();
        storage.delete_model(model_id)?;
        Ok(())
    }

    pub fn build_node_models(&self, node_id: &str) -> NodeModels {
        let mut nm = NodeModels::new(node_id.to_string());
        let storage = ModelStorage::new();
        nm.models = storage.list_local_models()
            .into_iter()
            .filter_map(|id| {
                let registry = ModelRegistry::new();
                registry.get(&id).map(|m| crate::node_models::ModelId {
                    id: m.id,
                    version: m.version,
                    sha256: m.hash,
                    size_bytes: m.size_bytes,
                })
            })
            .collect();
        nm
    }
}
