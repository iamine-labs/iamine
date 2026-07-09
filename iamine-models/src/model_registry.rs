use crate::license_policy::LicenseMetadata;
use crate::network_policy::NetworkPolicyMetadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub id: String,
    pub version: String,
    pub architecture: String,
    pub size_bytes: u64,
    pub required_ram_gb: u32,
    pub required_vram_gb: u32,
    pub shards: u32,
    pub hash: String, // SHA256 — empty string = skip verification
    pub download_url: String,
    pub quantization: String,
    #[serde(default)]
    pub license: LicenseMetadata,
    #[serde(default)]
    pub network_policy: NetworkPolicyMetadata,
}

impl ModelDescriptor {
    pub fn size_gb(&self) -> f64 {
        self.size_bytes as f64 / 1_073_741_824.0
    }

    pub fn can_run_on_cpu(&self) -> bool {
        self.required_vram_gb == 0
    }

    pub fn has_known_hash(&self) -> bool {
        !self.hash.is_empty() && !self.hash.ends_with("_placeholder")
    }

    pub fn to_manifest(&self) -> ModelManifest {
        ModelManifest {
            model_id: self.id.clone(),
            size_bytes: self.size_bytes,
            sha256: self.hash.clone(),
            ram_required_gb: self.required_ram_gb,
            download_url: self.download_url.clone(),
        }
    }
}

/// Lightweight manifest for download/verify operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelManifest {
    pub model_id: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub ram_required_gb: u32,
    pub download_url: String,
}

impl ModelManifest {
    pub fn requires_hash_verification(&self) -> bool {
        !self.sha256.is_empty() && !self.sha256.ends_with("_placeholder")
    }
}

pub struct ModelRegistry {
    models: HashMap<String, ModelDescriptor>,
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            models: HashMap::new(),
        };
        registry.register_defaults();
        registry
    }

    pub fn from_models(models: Vec<ModelDescriptor>) -> Self {
        Self {
            models: models
                .into_iter()
                .map(|model| (model.id.clone(), model))
                .collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_models_for_test(models: Vec<ModelDescriptor>) -> Self {
        Self::from_models(models)
    }

    fn register_defaults(&mut self) {
        for model in crate::beta_registry::beta_model_descriptors() {
            self.models.insert(model.id.clone(), model);
        }
    }

    pub fn get(&self, model_id: &str) -> Option<&ModelDescriptor> {
        self.models.get(model_id)
    }

    pub fn list(&self) -> Vec<&ModelDescriptor> {
        self.models.values().collect()
    }

    /// Verifica si el hardware puede ejecutar el modelo
    pub fn can_run(
        &self,
        model_id: &str,
        available_ram_gb: u64,
        gpu_available: bool,
    ) -> Result<(), String> {
        let model = self
            .get(model_id)
            .ok_or_else(|| format!("Modelo '{}' no encontrado en registry", model_id))?;

        if available_ram_gb < model.required_ram_gb as u64 {
            return Err(format!(
                "RAM insuficiente: necesita {}GB, disponible {}GB",
                model.required_ram_gb, available_ram_gb
            ));
        }

        if model.required_vram_gb > 0 && !gpu_available {
            return Err(format!(
                "GPU requerida para '{}' ({}GB VRAM)",
                model_id, model.required_vram_gb
            ));
        }

        Ok(())
    }
}
