use crate::model_registry::{ModelDescriptor, ModelRegistry};
use crate::model_storage::ModelStorage;
use crate::model_downloader::{ModelDownloader, DownloadProgress};
use crate::model_validator::ModelValidator;
use crate::storage_config::StorageConfig;
use crate::node_models::{NodeModels, ModelId};

pub struct ModelInstaller {
    pub registry: ModelRegistry,
    pub storage: ModelStorage,
    pub downloader: ModelDownloader,
    pub validator: ModelValidator,
    pub storage_config: StorageConfig,
}

#[derive(Debug)]
pub enum InstallResult {
    Installed(String),
    AlreadyExists(String),
    InsufficientStorage { needed_gb: f64, available_gb: f64 },
    DownloadFailed(String),
    ValidationFailed(String),
}

impl ModelInstaller {
    pub fn new() -> Self {
        let storage = ModelStorage::new();
        Self {
            registry: ModelRegistry::new(),
            downloader: ModelDownloader::new(ModelStorage::new()),
            validator: ModelValidator::new(),
            storage_config: StorageConfig::load(),
            storage,
        }
    }

    /// Instalar modelo completo: check → download → verify → register
    pub async fn install(
        &self,
        model_id: &str,
        node_id: &str,
        progress_tx: Option<tokio::sync::mpsc::Sender<DownloadProgress>>,
    ) -> InstallResult {
        // 1️⃣ Verificar que existe en registry
        let model = match self.registry.get(model_id) {
            Some(m) => m.clone(),
            None => return InstallResult::DownloadFailed(format!("Modelo '{}' no en registry", model_id)),
        };

        // 2️⃣ Ya instalado?
        if self.storage.has_model(model_id) {
            return InstallResult::AlreadyExists(model_id.to_string());
        }

        // 3️⃣ Verificar espacio
        let used = self.storage.total_size_bytes();
        if !self.storage_config.has_space_for(model.size_bytes, used) {
            let needed = model.size_bytes as f64 / 1_073_741_824.0;
            let max = self.storage_config.max_storage_gb as f64;
            let used_gb = used as f64 / 1_073_741_824.0;
            return InstallResult::InsufficientStorage {
                needed_gb: needed,
                available_gb: max - used_gb,
            };
        }

        println!("📦 Instalando {} ({:.1} GB)...", model_id, model.size_gb());

        // 4️⃣ Descargar
        if let Err(e) = self.downloader.download_model(&model, progress_tx).await {
            return InstallResult::DownloadFailed(e);
        }

        // 5️⃣ Verificar SHA256 + firma
        let gguf_path = self.storage.gguf_path(model_id);
        let validation = self.validator.validate(model_id, &gguf_path, &model.hash, None);
        if !validation.is_valid() {
            // Limpiar archivo corrupto
            let _ = self.storage.delete_model(model_id);
            return InstallResult::ValidationFailed(
                validation.error.unwrap_or_else(|| "Hash inválido".to_string())
            );
        }

        println!("✅ {} instalado correctamente", model_id);
        InstallResult::Installed(model_id.to_string())
    }

    /// Desinstalar modelo
    pub fn remove(&self, model_id: &str) -> Result<(), String> {
        if !self.storage.has_model(model_id) {
            return Err(format!("Modelo '{}' no está instalado", model_id));
        }
        self.storage.delete_model(model_id)?;
        println!("🗑️  Modelo {} eliminado", model_id);
        Ok(())
    }

    /// Listar modelos: registry + estado local
    pub fn list_models(&self) -> Vec<ModelStatus> {
        self.registry.list().iter().map(|m| {
            let installed = self.storage.has_model(&m.id);
            let size_on_disk = if installed {
                Some(self.storage.total_size_bytes())
            } else { None };
            ModelStatus {
                id: m.id.clone(),
                version: m.version.clone(),
                architecture: m.architecture.clone(),
                required_ram_gb: m.required_ram_gb,
                size_gb: m.size_gb(),
                installed,
                size_on_disk_mb: size_on_disk.map(|s| s / 1_048_576),
            }
        }).collect()
    }

    /// Generar NodeModels para broadcast P2P
    pub fn build_node_models(&self, node_id: &str) -> NodeModels {
        let mut nm = NodeModels::new(node_id.to_string());
        for m in self.registry.list() {
            if self.storage.has_model(&m.id) {
                nm.models.push(ModelId {
                    id: m.id.clone(),
                    version: m.version.clone(),
                    sha256: m.hash.clone(),
                    size_bytes: m.size_bytes,
                });
            }
        }
        nm
    }
}

#[derive(Debug, Clone)]
pub struct ModelStatus {
    pub id: String,
    pub version: String,
    pub architecture: String,
    pub required_ram_gb: u32,
    pub size_gb: f64,
    pub installed: bool,
    pub size_on_disk_mb: Option<u64>,
}

impl ModelStatus {
    pub fn display(&self) {
        let status = if self.installed { "✅" } else { "⬜" };
        let disk = self.size_on_disk_mb
            .map(|s| format!(" ({} MB en disco)", s))
            .unwrap_or_default();
        println!("  {} {} v{} | {:.1} GB | {}GB RAM{}",
            status, self.id, self.version,
            self.size_gb, self.required_ram_gb, disk);
    }
}
