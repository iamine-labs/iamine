use iamine_models::{
    evaluate_model_requirements_compatibility, ModelCompatibilityProfile, ModelRequirements,
};

#[derive(Debug, Clone)]
pub struct ModelHardwareRequirements {
    pub model_id: String,
    pub ram_required_gb: u32,
    pub gpu_required: bool,
    pub disk_required_gb: u32,
}

impl ModelHardwareRequirements {
    pub fn for_model(model_id: &str) -> Option<Self> {
        ModelRequirements::for_model(model_id).map(Self::from_model_requirements)
    }

    fn from_model_requirements(requirements: ModelRequirements) -> Self {
        Self {
            model_id: requirements.model_id,
            ram_required_gb: requirements.min_ram_gb,
            gpu_required: requirements.requires_gpu,
            disk_required_gb: requirements.min_storage_gb,
        }
    }

    fn to_model_requirements(&self) -> ModelRequirements {
        ModelRequirements {
            model_id: self.model_id.clone(),
            min_ram_gb: self.ram_required_gb,
            min_storage_gb: self.disk_required_gb,
            requires_gpu: self.gpu_required,
            recommended_gpu_layers: None,
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
    let profile = ModelCompatibilityProfile {
        ram_gb: Some(node.ram_gb),
        storage_available_gb: Some(node.storage_available_gb),
        gpu_available: Some(node.gpu_available),
        cpu_features: Vec::new(),
        accelerator: None,
    };

    evaluate_model_requirements_compatibility(&model.to_model_requirements(), &profile)
        .is_compatible()
}
