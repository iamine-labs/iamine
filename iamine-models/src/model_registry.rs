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

impl ModelRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            models: HashMap::new(),
        };
        registry.register_defaults();
        registry
    }

    fn register_defaults(&mut self) {
        // TinyLlama 1.1B — Q4_K_M (~669 MB)
        self.models.insert("tinyllama-1b".to_string(), ModelDescriptor {
            id: "tinyllama-1b".to_string(),
            version: "1.1".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 669_000_000,
            required_ram_gb: 2,
            required_vram_gb: 0,
            shards: 1,
            hash: String::new(), // Will be computed on first download and cached
            download_url: "https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
        });

        // Llama 3.2 3B — Q4_K_M (~2.02 GB)
        self.models.insert("llama3-3b".to_string(), ModelDescriptor {
            id: "llama3-3b".to_string(),
            version: "3.2".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 2_019_000_000,
            required_ram_gb: 4,
            required_vram_gb: 0,
            shards: 1,
            hash: String::new(),
            download_url: "https://huggingface.co/bartowski/Llama-3.2-3B-Instruct-GGUF/resolve/main/Llama-3.2-3B-Instruct-Q4_K_M.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
        });

        // Mistral 7B — Q4_K_M (~4.37 GB)
        self.models.insert("mistral-7b".to_string(), ModelDescriptor {
            id: "mistral-7b".to_string(),
            version: "0.3".to_string(),
            architecture: "mistral".to_string(),
            size_bytes: 4_370_000_000,
            required_ram_gb: 8,
            required_vram_gb: 0,
            shards: 1,
            hash: String::new(),
            download_url: "https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.2-GGUF/resolve/main/mistral-7b-instruct-v0.2.Q4_K_M.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
        });
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
