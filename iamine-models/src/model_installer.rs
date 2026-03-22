use crate::model_downloader::{DownloadProgress, ModelDownloader};
use crate::model_registry::ModelRegistry;
use crate::model_storage::ModelStorage;
use crate::model_validator::ModelValidator;
use crate::node_models::{ModelId, NodeModels};
use crate::storage_config::StorageConfig;

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
        Self::with_storage(ModelStorage::new())
    }

    pub fn with_storage(storage: ModelStorage) -> Self {
        Self {
            registry: ModelRegistry::new(),
            downloader: ModelDownloader::new(storage.clone()),
            validator: ModelValidator::new(),
            storage_config: StorageConfig::load(),
            storage,
        }
    }

    /// Install model: check → download → verify → register
    pub async fn install(
        &self,
        model_id: &str,
        _node_id: &str,
        progress_tx: Option<tokio::sync::mpsc::Sender<DownloadProgress>>,
    ) -> InstallResult {
        // 1️⃣ Check registry
        let model = match self.registry.get(model_id) {
            Some(m) => m.clone(),
            None => {
                return InstallResult::DownloadFailed(format!(
                    "Model '{}' not in registry",
                    model_id
                ))
            }
        };

        // 2️⃣ Already installed?
        if self.storage.has_model(model_id) {
            return InstallResult::AlreadyExists(model_id.to_string());
        }

        // 3️⃣ Check storage space
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

        println!(
            "📦 Installing {} ({:.1} GB, {})...",
            model_id,
            model.size_gb(),
            model.quantization
        );
        println!("   URL: {}", model.download_url);

        // 4️⃣ Download (real HTTP streaming)
        if let Err(e) = self.downloader.download_model(&model, progress_tx).await {
            return InstallResult::DownloadFailed(e);
        }

        // 5️⃣ Verify GGUF header (basic sanity check)
        let gguf_path = self.storage.gguf_path(model_id);
        if gguf_path.exists() {
            let file_size = std::fs::metadata(&gguf_path).map(|m| m.len()).unwrap_or(0);
            println!("   📊 Final size: {:.1} MB", file_size as f64 / 1_048_576.0);

            // Check GGUF magic bytes (optional, non-blocking)
            if let Ok(mut f) = std::fs::File::open(&gguf_path) {
                let mut magic = [0u8; 4];
                if std::io::Read::read_exact(&mut f, &mut magic).is_ok() {
                    if &magic == b"GGUF" {
                        println!("   ✅ GGUF header verified");
                    } else {
                        println!("   ⚠️  File doesn't start with GGUF magic (may still work)");
                    }
                }
            }
        }

        println!("✅ {} installed successfully", model_id);
        InstallResult::Installed(model_id.to_string())
    }

    /// Desinstalar modelo
    pub fn remove(&self, model_id: &str) -> Result<(), String> {
        if !self.storage.model_path(model_id).exists() {
            return Err(format!("Modelo '{}' no está instalado", model_id));
        }
        self.storage.delete_model(model_id)?;
        println!("🗑️  Modelo {} eliminado", model_id);
        Ok(())
    }

    /// Listar modelos: registry + estado local
    pub fn list_models(&self) -> Vec<ModelStatus> {
        self.registry
            .list()
            .iter()
            .map(|m| {
                let installed = self.storage.has_model(&m.id);
                let size_on_disk = if installed {
                    Some(self.storage.model_size_bytes(&m.id))
                } else {
                    None
                };
                ModelStatus {
                    id: m.id.clone(),
                    version: m.version.clone(),
                    architecture: m.architecture.clone(),
                    required_ram_gb: m.required_ram_gb,
                    size_gb: m.size_gb(),
                    installed,
                    size_on_disk_mb: size_on_disk.map(|s| s / 1_048_576),
                }
            })
            .collect()
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
        let disk = self
            .size_on_disk_mb
            .map(|s| format!(" ({} MB en disco)", s))
            .unwrap_or_default();
        println!(
            "  {} {} v{} | {:.1} GB | {}GB RAM{}",
            status, self.id, self.version, self.size_gb, self.required_ram_gb, disk
        );
    }
}

#[cfg(test)]
mod tests {
    use super::ModelInstaller;
    use crate::ModelStorage;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_models_dir() -> std::path::PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-model-remove-{}", suffix))
    }

    #[test]
    fn test_models_remove() {
        let path = temp_models_dir();
        let storage = ModelStorage::new_in(path.clone());
        let installer = ModelInstaller::with_storage(storage.clone());
        let model_id = "llama3-3b";
        let model_dir = storage.model_path(model_id);
        fs::create_dir_all(&model_dir).unwrap();
        fs::write(storage.gguf_path(model_id), b"GGUFdemo").unwrap();

        assert!(storage.model_path(model_id).exists());
        installer.remove(model_id).unwrap();
        assert!(!storage.model_path(model_id).exists());
        assert!(!installer
            .list_models()
            .iter()
            .any(|model| model.id == model_id && model.installed));

        let _ = fs::remove_dir_all(path);
    }
}
