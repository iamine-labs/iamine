use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub max_storage_gb: u64,
    pub models_path: String,
}

impl StorageConfig {
    pub fn default() -> Self {
        Self {
            max_storage_gb: 50,
            models_path: Self::default_path().to_string_lossy().to_string(),
        }
    }

    pub fn load() -> Self {
        let path = Self::config_path();
        if path.exists() {
            if let Ok(data) = fs::read_to_string(&path) {
                if let Ok(cfg) = serde_json::from_str(&data) {
                    return cfg;
                }
            }
        }
        let cfg = Self::default();
        cfg.save();
        cfg
    }

    pub fn save(&self) {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(path, serde_json::to_string_pretty(self).unwrap_or_default());
    }

    pub fn set_max_storage(&mut self, gb: u64) {
        self.max_storage_gb = gb;
        self.save();
        println!("💾 Storage limit actualizado: {} GB", gb);
    }

    /// Verificar si hay espacio para un modelo
    pub fn has_space_for(&self, model_size_bytes: u64, current_usage_bytes: u64) -> bool {
        let max_bytes = self.max_storage_gb * 1_073_741_824;
        current_usage_bytes + model_size_bytes <= max_bytes
    }

    fn default_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".iamine")
            .join("models")
    }

    fn config_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".iamine")
            .join("storage_config.json")
    }
}
