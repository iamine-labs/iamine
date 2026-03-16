use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub node_id: String,
    pub cpu_score: f64,
    pub ram_gb: u64,
    pub gpu_available: bool,
    pub gpu_vram_gb: u64,
    pub disk_available_gb: u64,
    pub inference_slots: usize,
    pub supported_models: Vec<String>,  // model_ids que puede ejecutar
}

impl NodeCapabilities {
    pub fn new(
        node_id: String,
        cpu_score: f64,
        ram_gb: u64,
        gpu_available: bool,
        gpu_vram_gb: u64,
        disk_available_gb: u64,
        inference_slots: usize,
    ) -> Self {
        let supported_models = Self::compute_supported_models(ram_gb, gpu_available);
        Self {
            node_id,
            cpu_score,
            ram_gb,
            gpu_available,
            gpu_vram_gb,
            disk_available_gb,
            inference_slots,
            supported_models,
        }
    }

    fn compute_supported_models(ram_gb: u64, _gpu: bool) -> Vec<String> {
        let mut models = vec![];
        if ram_gb >= 2 { models.push("tinyllama-1b".to_string()); }
        if ram_gb >= 4 { models.push("llama3-3b".to_string()); }
        if ram_gb >= 8 { models.push("mistral-7b".to_string()); }
        models
    }

    pub fn can_run_model(&self, model_id: &str) -> bool {
        self.supported_models.iter().any(|m| m == model_id)
    }

    /// Serializar para broadcast gossipsub
    pub fn to_gossip_payload(&self) -> Vec<u8> {
        let msg = serde_json::json!({
            "type": "NodeCapabilities",
            "node_id": self.node_id,
            "cpu_score": self.cpu_score,
            "ram_gb": self.ram_gb,
            "gpu_available": self.gpu_available,
            "gpu_vram_gb": self.gpu_vram_gb,
            "disk_available_gb": self.disk_available_gb,
            "inference_slots": self.inference_slots,
            "supported_models": self.supported_models,
        });
        serde_json::to_vec(&msg).unwrap_or_default()
    }

    pub fn display(&self) {
        println!("⚙️  Node Capabilities:");
        println!("   CPU Score:       {:.0}", self.cpu_score);
        println!("   RAM:             {} GB", self.ram_gb);
        println!("   GPU:             {} ({} GB VRAM)", self.gpu_available, self.gpu_vram_gb);
        println!("   Disk available:  {} GB", self.disk_available_gb);
        println!("   Inference slots: {}", self.inference_slots);
        println!("   Models supported: {:?}", self.supported_models);
    }
}
