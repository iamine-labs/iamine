use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub id: String,
    pub version: String,
    pub architecture: String,   // "llama", "mistral", "tinyllama"
    pub size_bytes: u64,
    pub required_ram_gb: u32,
    pub required_vram_gb: u32,  // 0 = CPU only
    pub shards: u32,
    pub hash: String,           // SHA256 del modelo completo
    pub download_url: String,
    pub quantization: String,   // "q4_k_m", "q8_0", "f16"
}

impl ModelDescriptor {
    pub fn size_gb(&self) -> f64 {
        self.size_bytes as f64 / 1_073_741_824.0
    }

    pub fn can_run_on_cpu(&self) -> bool {
        self.required_vram_gb == 0
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
        // TinyLlama 1.1B — funciona en cualquier CPU con 2GB RAM
        self.models.insert("tinyllama-1b".to_string(), ModelDescriptor {
            id: "tinyllama-1b".to_string(),
            version: "1.1".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 637_000_000,       // ~637 MB (q4_k_m)
            required_ram_gb: 2,
            required_vram_gb: 0,
            shards: 1,
            hash: "tinyllama_hash_placeholder".to_string(),
            download_url: "https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
        });

        // Llama 3 3B — necesita 4GB RAM
        self.models.insert("llama3-3b".to_string(), ModelDescriptor {
            id: "llama3-3b".to_string(),
            version: "3.0".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 1_800_000_000,     // ~1.8 GB (q4_k_m)
            required_ram_gb: 4,
            required_vram_gb: 0,
            shards: 2,
            hash: "llama3_3b_hash_placeholder".to_string(),
            download_url: "https://huggingface.co/bartowski/Llama-3.2-3B-Instruct-GGUF/resolve/main/Llama-3.2-3B-Instruct-Q4_K_M.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
        });

        // Mistral 7B — necesita 8GB RAM
        self.models.insert("mistral-7b".to_string(), ModelDescriptor {
            id: "mistral-7b".to_string(),
            version: "0.3".to_string(),
            architecture: "mistral".to_string(),
            size_bytes: 4_100_000_000,     // ~4.1 GB (q4_k_m)
            required_ram_gb: 8,
            required_vram_gb: 0,
            shards: 4,
            hash: "mistral_7b_hash_placeholder".to_string(),
            download_url: "https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.3-GGUF/resolve/main/mistral-7b-instruct-v0.3.Q4_K_M.gguf".to_string(),
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
        let model = self.get(model_id)
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
