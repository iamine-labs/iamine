use crate::license_acceptance::{LicenseAcceptanceRecord, LicenseAcceptanceStore};
use crate::license_policy::LicenseOperation;
use crate::model_downloader::{DownloadProgress, ModelDownloader};
use crate::model_registry::ModelRegistry;
use crate::model_registry_admission::evaluate_model_registry_admission_with_license_acceptance_store;
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
    pub license_acceptance_store: LicenseAcceptanceStore,
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
        Self::with_storage_and_license_acceptance_store(storage, LicenseAcceptanceStore::new())
    }

    pub fn with_storage_and_license_acceptance_store(
        storage: ModelStorage,
        license_acceptance_store: LicenseAcceptanceStore,
    ) -> Self {
        Self {
            registry: ModelRegistry::new(),
            downloader: ModelDownloader::with_license_acceptance_store(
                storage.clone(),
                license_acceptance_store.clone(),
            ),
            validator: ModelValidator::new(),
            storage_config: StorageConfig::load(),
            license_acceptance_store,
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

        // 3️⃣ Admission gates before storage writes or network access.
        let admission = evaluate_model_registry_admission_with_license_acceptance_store(
            &model,
            LicenseOperation::Install,
            false,
            &self.license_acceptance_store,
        );
        if let Some(error) = admission.first_blocking_error() {
            return InstallResult::DownloadFailed(error);
        }

        // 4️⃣ Check storage space
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

        // 5️⃣ Download (real HTTP streaming)
        if let Err(e) = self.downloader.download_model(&model, progress_tx).await {
            return InstallResult::DownloadFailed(e);
        }

        // 6️⃣ Verify GGUF header (basic sanity check)
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
                let admission = evaluate_model_registry_admission_with_license_acceptance_store(
                    m,
                    LicenseOperation::List,
                    installed,
                    &self.license_acceptance_store,
                );
                ModelStatus {
                    id: m.id.clone(),
                    version: m.version.clone(),
                    architecture: m.architecture.clone(),
                    required_ram_gb: m.required_ram_gb,
                    size_gb: m.size_gb(),
                    installed,
                    size_on_disk_mb: size_on_disk.map(|s| s / 1_048_576),
                    download_policy_status: admission.download.status.as_str().to_string(),
                    download_policy_reason: admission.download.policy_reason(),
                    registry_integrity_status: admission
                        .registry_integrity
                        .status
                        .as_str()
                        .to_string(),
                    registry_integrity_reason: admission.registry_integrity.policy_reason(),
                    license_policy_status: admission.license.status.as_str().to_string(),
                    license_policy_reason: admission.license.reason.as_str().to_string(),
                    license_acceptance_status: admission
                        .license_acceptance
                        .status
                        .as_str()
                        .to_string(),
                    license_acceptance_reason: admission
                        .license_acceptance
                        .reason
                        .as_str()
                        .to_string(),
                }
            })
            .collect()
    }

    pub fn accept_license(&self, model_id: &str) -> Result<LicenseAcceptanceRecord, String> {
        let model = self
            .registry
            .get(model_id)
            .ok_or_else(|| format!("Model '{}' not in registry", model_id))?;
        self.license_acceptance_store.accept_descriptor(model)
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
    pub download_policy_status: String,
    pub download_policy_reason: String,
    pub registry_integrity_status: String,
    pub registry_integrity_reason: String,
    pub license_policy_status: String,
    pub license_policy_reason: String,
    pub license_acceptance_status: String,
    pub license_acceptance_reason: String,
}

impl ModelStatus {
    pub fn display(&self) {
        let status = if self.installed { "✅" } else { "⬜" };
        let disk = self
            .size_on_disk_mb
            .map(|s| format!(" ({} MB en disco)", s))
            .unwrap_or_default();
        println!(
            "  {} {} v{} | {:.1} GB | {}GB RAM{} | download_policy={} download_reason={} | registry_integrity={} registry_reason={} | license_policy={} license_reason={} | license_acceptance={} acceptance_reason={}",
            status,
            self.id,
            self.version,
            self.size_gb,
            self.required_ram_gb,
            disk,
            self.download_policy_status,
            self.download_policy_reason,
            self.registry_integrity_status,
            self.registry_integrity_reason,
            self.license_policy_status,
            self.license_policy_reason,
            self.license_acceptance_status,
            self.license_acceptance_reason
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
