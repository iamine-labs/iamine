use serde::{Deserialize, Serialize};
use crate::node_capabilities::NodeCapabilities;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRequirements {
    pub model_id: String,
    pub min_ram_gb: u32,
    pub min_storage_gb: u32,
    pub requires_gpu: bool,
    pub recommended_gpu_layers: Option<u32>,
}

impl ModelRequirements {
    pub fn for_model(model_id: &str) -> Option<Self> {
        match model_id {
            "tinyllama-1b" => Some(Self {
                model_id: "tinyllama-1b".to_string(),
                min_ram_gb: 2,
                min_storage_gb: 1,
                requires_gpu: false,
                recommended_gpu_layers: Some(32),
            }),
            "llama3-3b" => Some(Self {
                model_id: "llama3-3b".to_string(),
                min_ram_gb: 4,
                min_storage_gb: 2,
                requires_gpu: false,
                recommended_gpu_layers: Some(64),
            }),
            "mistral-7b" => Some(Self {
                model_id: "mistral-7b".to_string(),
                min_ram_gb: 8,
                min_storage_gb: 4,
                requires_gpu: false,
                recommended_gpu_layers: Some(99),
            }),
            _ => None,
        }
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::for_model("tinyllama-1b").unwrap(),
            Self::for_model("llama3-3b").unwrap(),
            Self::for_model("mistral-7b").unwrap(),
        ]
    }
}

/// Validar si un nodo puede ejecutar un modelo
pub fn can_node_run_model(cap: &NodeCapabilities, req: &ModelRequirements) -> bool {
    if cap.ram_gb < req.min_ram_gb {
        return false;
    }
    if cap.storage_available_gb < req.min_storage_gb {
        return false;
    }
    if req.requires_gpu && cap.gpu_type.is_none() {
        return false;
    }
    true
}

/// Validar todos los modelos que un nodo puede ejecutar
pub fn runnable_models(cap: &NodeCapabilities) -> Vec<String> {
    ModelRequirements::all()
        .iter()
        .filter(|req| can_node_run_model(cap, req))
        .map(|req| req.model_id.clone())
        .collect()
}
