#[derive(Debug, Clone)]
pub struct ModelHardwareRequirements {
    pub model_id: String,
    pub ram_required_gb: u32,
    pub gpu_required: bool,
    pub disk_required_gb: u32,
}

impl ModelHardwareRequirements {
    pub fn for_model(model_id: &str) -> Option<Self> {
        match model_id {
            "tinyllama-1b" => Some(Self {
                model_id: model_id.to_string(),
                ram_required_gb: 2,
                gpu_required: false,
                disk_required_gb: 1,
            }),
            "llama3-3b" => Some(Self {
                model_id: model_id.to_string(),
                ram_required_gb: 4,
                gpu_required: false,
                disk_required_gb: 2,
            }),
            "mistral-7b" => Some(Self {
                model_id: model_id.to_string(),
                ram_required_gb: 8,
                gpu_required: false,
                disk_required_gb: 5,
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeHardwareProfile {
    pub ram_gb: u32,
    pub gpu_available: bool,
    pub storage_available_gb: u32,
}

pub fn is_node_compatible_with_model(
    node: &NodeHardwareProfile,
    model: &ModelHardwareRequirements,
) -> bool {
    if node.ram_gb < model.ram_required_gb {
        return false;
    }
    if model.gpu_required && !node.gpu_available {
        return false;
    }
    if node.storage_available_gb < model.disk_required_gb {
        return false;
    }
    true
}
