use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub id: String,
    pub version: String,
    pub size_bytes: u64,
    pub required_ram_gb: u32,
    pub download_url: String,
    pub hash: String,
    pub architecture: String,
}

impl ModelDescriptor {
    pub fn size_gb(&self) -> f64 {
        self.size_bytes as f64 / 1_073_741_824.0
    }

    pub fn display(&self) {
        println!("  {} v{} ({:.1}GB, {}GB RAM)",
            self.id, self.version, self.size_gb(), self.required_ram_gb);
    }
}

pub struct ModelRegistry {
    models: std::collections::HashMap<String, ModelDescriptor>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            models: std::collections::HashMap::new(),
        };
        registry.init_default_models();
        registry
    }

    fn init_default_models(&mut self) {
        self.models.insert("tinyllama-1b".to_string(), ModelDescriptor {
            id: "tinyllama-1b".to_string(),
            version: "1.0".to_string(),
            size_bytes: 637_000_000,
            required_ram_gb: 2,
            download_url: "https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf".to_string(),
            hash: "tinyllama_hash_placeholder".to_string(),
            architecture: "llama".to_string(),
        });

        self.models.insert("llama3-3b".to_string(), ModelDescriptor {
            id: "llama3-3b".to_string(),
            version: "1.0".to_string(),
            size_bytes: 2_700_000_000,
            required_ram_gb: 4,
            download_url: "https://huggingface.co/TheBloke/Llama-2-3B-GGUF/resolve/main/llama-2-3b.Q4_K_M.gguf".to_string(),
            hash: "llama3_hash_placeholder".to_string(),
            architecture: "llama".to_string(),
        });

        self.models.insert("mistral-7b".to_string(), ModelDescriptor {
            id: "mistral-7b".to_string(),
            version: "1.0".to_string(),
            size_bytes: 4_100_000_000,
            required_ram_gb: 8,
            download_url: "https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.1-GGUF/resolve/main/mistral-7b-instruct-v0.1.Q4_K_M.gguf".to_string(),
            hash: "mistral_hash_placeholder".to_string(),
            architecture: "mistral".to_string(),
        });

        self.models.insert("neural-chat-7b".to_string(), ModelDescriptor {
            id: "neural-chat-7b".to_string(),
            version: "1.0".to_string(),
            size_bytes: 4_000_000_000,
            required_ram_gb: 8,
            download_url: "https://huggingface.co/Intel/neural-chat-7b-v3-1/resolve/main/model.gguf".to_string(),
            hash: "neural_chat_hash_placeholder".to_string(),
            architecture: "llama".to_string(),
        });

        self.models.insert("orca-mini-7b".to_string(), ModelDescriptor {
            id: "orca-mini-7b".to_string(),
            version: "1.0".to_string(),
            size_bytes: 3_500_000_000,
            required_ram_gb: 7,
            download_url: "https://huggingface.co/psmathur/orca_mini_v3_7b/resolve/main/model.gguf".to_string(),
            hash: "orca_mini_hash_placeholder".to_string(),
            architecture: "llama".to_string(),
        });

        self.models.insert("zephyr-7b".to_string(), ModelDescriptor {
            id: "zephyr-7b".to_string(),
            version: "1.0".to_string(),
            size_bytes: 4_200_000_000,
            required_ram_gb: 9,
            download_url: "https://huggingface.co/HuggingFaceH4/zephyr-7b-beta/resolve/main/model.gguf".to_string(),
            hash: "zephyr_hash_placeholder".to_string(),
            architecture: "llama".to_string(),
        });
    }

    pub fn get(&self, id: &str) -> Option<ModelDescriptor> {
        self.models.get(id).cloned()
    }

    pub fn list(&self) -> Vec<ModelDescriptor> {
        vec![
            self.get("tinyllama-1b").unwrap().clone(),
            self.get("llama3-3b").unwrap().clone(),
            self.get("mistral-7b").unwrap().clone(),
            self.get("neural-chat-7b").unwrap().clone(),
            self.get("orca-mini-7b").unwrap().clone(),
            self.get("zephyr-7b").unwrap().clone(),
        ]
    }

    pub fn can_run(&self, model_id: &str, ram_gb: u32, _gpu: bool) -> Result<(), String> {
        let model = self.get(model_id)
            .ok_or_else(|| format!("Modelo {} no encontrado", model_id))?;

        if ram_gb < model.required_ram_gb {
            return Err(format!(
                "RAM insuficiente: necesita {}GB, disponible {}GB",
                model.required_ram_gb, ram_gb
            ));
        }

        Ok(())
    }
}
