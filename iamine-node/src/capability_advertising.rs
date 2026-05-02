use iamine_models::{
    can_node_run_model, ModelNodeCapabilities, ModelRegistry, ModelRequirements, ModelStorage,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityRejectionReason {
    MissingRegistryDescriptor,
    MissingLocalModelFile,
    HardwareRequirements,
}

impl CapabilityRejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingRegistryDescriptor => "missing_registry_descriptor",
            Self::MissingLocalModelFile => "missing_or_invalid_local_model_file",
            Self::HardwareRequirements => "hardware_requirements",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityValidationResult {
    pub model_id: String,
    pub accepted: bool,
    pub rejection: Option<CapabilityRejectionReason>,
}

pub struct CapabilityAdvertisementValidator<'a> {
    registry: &'a ModelRegistry,
    storage: &'a ModelStorage,
    node_caps: &'a ModelNodeCapabilities,
}

impl<'a> CapabilityAdvertisementValidator<'a> {
    pub fn new(
        registry: &'a ModelRegistry,
        storage: &'a ModelStorage,
        node_caps: &'a ModelNodeCapabilities,
    ) -> Self {
        Self {
            registry,
            storage,
            node_caps,
        }
    }

    pub fn validate_local_models(&self) -> Vec<CapabilityValidationResult> {
        self.storage
            .list_local_models()
            .into_iter()
            .map(|model_id| self.validate_model(&model_id))
            .collect()
    }

    pub fn validate_model(&self, model_id: &str) -> CapabilityValidationResult {
        if !self.storage.has_model(model_id) {
            return CapabilityValidationResult {
                model_id: model_id.to_string(),
                accepted: false,
                rejection: Some(CapabilityRejectionReason::MissingLocalModelFile),
            };
        }

        if self.registry.get(model_id).is_none() {
            return CapabilityValidationResult {
                model_id: model_id.to_string(),
                accepted: false,
                rejection: Some(CapabilityRejectionReason::MissingRegistryDescriptor),
            };
        }

        if let Some(requirements) = ModelRequirements::for_model(model_id) {
            if !can_node_run_model(self.node_caps, &requirements) {
                return CapabilityValidationResult {
                    model_id: model_id.to_string(),
                    accepted: false,
                    rejection: Some(CapabilityRejectionReason::HardwareRequirements),
                };
            }
        }

        CapabilityValidationResult {
            model_id: model_id.to_string(),
            accepted: true,
            rejection: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CapabilityAdvertisementValidator, CapabilityRejectionReason};
    use iamine_models::{ModelNodeCapabilities, ModelRegistry, ModelStorage};

    fn write_valid_gguf(storage: &ModelStorage, model_id: &str) {
        let path = storage.gguf_path(model_id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut data = vec![0u8; 2048];
        data[..4].copy_from_slice(b"GGUF");
        std::fs::write(path, data).unwrap();
    }

    #[test]
    fn test_capability_advertising_does_not_require_runtime_model_load() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = ModelStorage::new_in(tmp_dir.path().join("models"));
        write_valid_gguf(&storage, "tinyllama-1b");

        let registry = ModelRegistry::new();
        let caps = ModelNodeCapabilities::detect("test-node");
        let validator = CapabilityAdvertisementValidator::new(&registry, &storage, &caps);
        let result = validator.validate_model("tinyllama-1b");

        assert!(result.accepted);
        assert!(result.rejection.is_none());
    }

    #[test]
    fn test_capability_rejects_missing_registry_descriptor() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = ModelStorage::new_in(tmp_dir.path().join("models"));
        write_valid_gguf(&storage, "model-not-in-registry");

        let registry = ModelRegistry::new();
        let caps = ModelNodeCapabilities::detect("test-node");
        let validator = CapabilityAdvertisementValidator::new(&registry, &storage, &caps);
        let result = validator.validate_model("model-not-in-registry");

        assert!(!result.accepted);
        assert_eq!(
            result.rejection,
            Some(CapabilityRejectionReason::MissingRegistryDescriptor)
        );
    }
}
