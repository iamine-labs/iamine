use crate::model_requirements::ModelRequirements;
use crate::node_capabilities::NodeCapabilities;
use iamine_hardware::{AcceleratorKind, NodeHardwareProfile};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompatibilityProfile {
    pub ram_gb: Option<u32>,
    pub storage_available_gb: Option<u32>,
    pub gpu_available: Option<bool>,
    pub cpu_features: Vec<String>,
    pub accelerator: Option<String>,
}

impl ModelCompatibilityProfile {
    pub fn from_node_capabilities(capabilities: &NodeCapabilities) -> Self {
        Self {
            ram_gb: Some(capabilities.ram_gb),
            storage_available_gb: Some(capabilities.storage_available_gb),
            gpu_available: Some(
                capabilities.gpu_type.is_some()
                    || accelerator_name_indicates_gpu(&capabilities.accelerator),
            ),
            cpu_features: capabilities.cpu_features.clone(),
            accelerator: Some(capabilities.accelerator.clone()),
        }
    }

    pub fn from_hardware_profile(profile: &NodeHardwareProfile) -> Self {
        let static_profile = &profile.static_profile;
        Self {
            ram_gb: Some(static_profile.memory.total_gb.min(u32::MAX as u64) as u32),
            storage_available_gb: static_profile
                .storage
                .available_bytes
                .map(bytes_to_gib_floor),
            gpu_available: Some(has_gpu_accelerator(profile)),
            cpu_features: static_profile.cpu.features.features.clone(),
            accelerator: Some(format!(
                "{:?}",
                static_profile.effective.effective_accelerator
            )),
        }
    }
}

impl From<&NodeCapabilities> for ModelCompatibilityProfile {
    fn from(capabilities: &NodeCapabilities) -> Self {
        Self::from_node_capabilities(capabilities)
    }
}

impl From<&NodeHardwareProfile> for ModelCompatibilityProfile {
    fn from(profile: &NodeHardwareProfile) -> Self {
        Self::from_hardware_profile(profile)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCompatibilityStatus {
    Compatible,
    Incompatible,
    UnknownModel,
    UnknownHardware,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCompatibilityReason {
    UnknownModel { model_id: String },
    MissingHardwareField { field: String },
    InsufficientRam { required_gb: u32, available_gb: u32 },
    InsufficientStorage { required_gb: u32, available_gb: u32 },
    GpuRequired { accelerator: Option<String> },
    MissingCpuFeature { feature: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompatibilityDecision {
    pub model_id: String,
    pub status: ModelCompatibilityStatus,
    pub reasons: Vec<ModelCompatibilityReason>,
}

impl ModelCompatibilityDecision {
    pub fn is_compatible(&self) -> bool {
        self.status == ModelCompatibilityStatus::Compatible
    }
}

pub fn evaluate_model_compatibility(
    model_id: &str,
    hardware: &ModelCompatibilityProfile,
) -> ModelCompatibilityDecision {
    let Some(requirements) = ModelRequirements::for_model(model_id) else {
        return ModelCompatibilityDecision {
            model_id: model_id.to_string(),
            status: ModelCompatibilityStatus::UnknownModel,
            reasons: vec![ModelCompatibilityReason::UnknownModel {
                model_id: model_id.to_string(),
            }],
        };
    };

    evaluate_model_requirements_compatibility(&requirements, hardware)
}

pub fn evaluate_node_model_compatibility(
    model_id: &str,
    capabilities: &NodeCapabilities,
) -> ModelCompatibilityDecision {
    evaluate_model_compatibility(
        model_id,
        &ModelCompatibilityProfile::from_node_capabilities(capabilities),
    )
}

pub fn evaluate_model_requirements_compatibility(
    requirements: &ModelRequirements,
    hardware: &ModelCompatibilityProfile,
) -> ModelCompatibilityDecision {
    let mut reasons = Vec::new();

    match hardware.ram_gb {
        Some(available_gb) if available_gb < requirements.min_ram_gb => {
            reasons.push(ModelCompatibilityReason::InsufficientRam {
                required_gb: requirements.min_ram_gb,
                available_gb,
            });
        }
        Some(_) => {}
        None => reasons.push(ModelCompatibilityReason::MissingHardwareField {
            field: "ram_gb".to_string(),
        }),
    }

    match hardware.storage_available_gb {
        Some(available_gb) if available_gb < requirements.min_storage_gb => {
            reasons.push(ModelCompatibilityReason::InsufficientStorage {
                required_gb: requirements.min_storage_gb,
                available_gb,
            });
        }
        Some(_) => {}
        None => reasons.push(ModelCompatibilityReason::MissingHardwareField {
            field: "storage_available_gb".to_string(),
        }),
    }

    if requirements.requires_gpu {
        match hardware.gpu_available {
            Some(true) => {}
            Some(false) => reasons.push(ModelCompatibilityReason::GpuRequired {
                accelerator: hardware.accelerator.clone(),
            }),
            None => reasons.push(ModelCompatibilityReason::MissingHardwareField {
                field: "gpu_available".to_string(),
            }),
        }
    }

    let status = if reasons.is_empty() {
        ModelCompatibilityStatus::Compatible
    } else if reasons.iter().all(|reason| {
        matches!(
            reason,
            ModelCompatibilityReason::MissingHardwareField { .. }
        )
    }) {
        ModelCompatibilityStatus::UnknownHardware
    } else {
        ModelCompatibilityStatus::Incompatible
    };

    ModelCompatibilityDecision {
        model_id: requirements.model_id.clone(),
        status,
        reasons,
    }
}

fn has_gpu_accelerator(profile: &NodeHardwareProfile) -> bool {
    is_gpu_accelerator_kind(profile.static_profile.effective.effective_accelerator)
        || profile
            .static_profile
            .accelerators
            .iter()
            .any(|accelerator| is_gpu_accelerator_kind(accelerator.kind))
}

fn is_gpu_accelerator_kind(kind: AcceleratorKind) -> bool {
    matches!(
        kind,
        AcceleratorKind::Metal
            | AcceleratorKind::Cuda
            | AcceleratorKind::Rocm
            | AcceleratorKind::Vulkan
    )
}

fn accelerator_name_indicates_gpu(accelerator: &str) -> bool {
    let normalized = accelerator.to_ascii_lowercase();
    ["metal", "cuda", "rocm", "vulkan", "gpu"]
        .iter()
        .any(|needle| normalized.contains(needle))
}

fn bytes_to_gib_floor(bytes: u64) -> u32 {
    (bytes / 1_073_741_824).min(u32::MAX as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use iamine_hardware::{
        build_node_hardware_profile, AcceleratorKind, AcceleratorStaticProfile, CpuFeatureProfile,
        CpuStaticProfile, DetectionConfidence, HardwareCollectionMode, HardwareProfileParts,
        MemoryStaticProfile, StorageStaticProfile,
    };

    fn profile(
        ram_gb: Option<u32>,
        storage_gb: Option<u32>,
        gpu_available: Option<bool>,
    ) -> ModelCompatibilityProfile {
        ModelCompatibilityProfile {
            ram_gb,
            storage_available_gb: storage_gb,
            gpu_available,
            cpu_features: vec!["AVX2".to_string(), "FMA".to_string()],
            accelerator: Some(
                if gpu_available == Some(true) {
                    "CUDA"
                } else {
                    "CPU"
                }
                .to_string(),
            ),
        }
    }

    fn hardware_profile_with_accelerator(kind: AcceleratorKind) -> NodeHardwareProfile {
        build_node_hardware_profile(HardwareProfileParts {
            mode: HardwareCollectionMode::StaticOnly,
            cpu: CpuStaticProfile {
                architecture: "x86_64".to_string(),
                vendor: None,
                brand: None,
                physical_cores: Some(4),
                logical_cores: 8,
                recommended_threads: 4,
                features: CpuFeatureProfile {
                    avx2: true,
                    avx512f: false,
                    fma: true,
                    neon: false,
                    features: vec!["AVX2".to_string(), "FMA".to_string()],
                },
                confidence: DetectionConfidence::High,
            },
            memory: MemoryStaticProfile {
                total_bytes: 16 * 1_073_741_824,
                available_bytes: Some(8 * 1_073_741_824),
                total_gb: 16,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            },
            accelerators: vec![AcceleratorStaticProfile {
                kind,
                name: format!("{kind:?}"),
                vendor: None,
                memory_bytes: None,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            }],
            storage: StorageStaticProfile {
                available_bytes: Some(20 * 1_073_741_824),
                confidence: DetectionConfidence::High,
            },
            dynamic_profile: None,
            warnings: Vec::new(),
            generated_at_unix_ms: 42,
        })
    }

    #[test]
    fn compatible_model_returns_positive_decision() {
        let decision =
            evaluate_model_compatibility("mistral-7b", &profile(Some(16), Some(20), Some(false)));

        assert!(decision.is_compatible());
        assert_eq!(decision.status, ModelCompatibilityStatus::Compatible);
        assert!(decision.reasons.is_empty());
    }

    #[test]
    fn insufficient_ram_is_structured_reason() {
        let decision =
            evaluate_model_compatibility("mistral-7b", &profile(Some(4), Some(20), Some(false)));

        assert!(!decision.is_compatible());
        assert_eq!(decision.status, ModelCompatibilityStatus::Incompatible);
        assert!(decision
            .reasons
            .contains(&ModelCompatibilityReason::InsufficientRam {
                required_gb: 8,
                available_gb: 4,
            }));
    }

    #[test]
    fn insufficient_storage_is_structured_reason() {
        let decision =
            evaluate_model_compatibility("mistral-7b", &profile(Some(16), Some(2), Some(false)));

        assert!(!decision.is_compatible());
        assert!(decision
            .reasons
            .contains(&ModelCompatibilityReason::InsufficientStorage {
                required_gb: 5,
                available_gb: 2,
            }));
    }

    #[test]
    fn gpu_requirement_is_enforced_when_declared() {
        let requirements = ModelRequirements {
            model_id: "gpu-model".to_string(),
            min_ram_gb: 4,
            min_storage_gb: 2,
            requires_gpu: true,
            recommended_gpu_layers: None,
        };

        let decision = evaluate_model_requirements_compatibility(
            &requirements,
            &profile(Some(16), Some(20), Some(false)),
        );

        assert!(!decision.is_compatible());
        assert!(decision
            .reasons
            .contains(&ModelCompatibilityReason::GpuRequired {
                accelerator: Some("CPU".to_string()),
            }));
    }

    #[test]
    fn unknown_model_is_not_assumed_compatible() {
        let decision = evaluate_model_compatibility(
            "unknown-model",
            &profile(Some(16), Some(20), Some(false)),
        );

        assert!(!decision.is_compatible());
        assert_eq!(decision.status, ModelCompatibilityStatus::UnknownModel);
    }

    #[test]
    fn missing_required_hardware_blocks_as_unknown_hardware() {
        let decision =
            evaluate_model_compatibility("tinyllama-1b", &profile(None, Some(20), Some(false)));

        assert!(!decision.is_compatible());
        assert_eq!(decision.status, ModelCompatibilityStatus::UnknownHardware);
        assert!(decision
            .reasons
            .contains(&ModelCompatibilityReason::MissingHardwareField {
                field: "ram_gb".to_string(),
            }));
    }

    #[test]
    fn hardware_profiler_profile_can_be_normalized_for_compatibility() {
        let hardware_profile = hardware_profile_with_accelerator(AcceleratorKind::Cpu);

        let normalized = ModelCompatibilityProfile::from_hardware_profile(&hardware_profile);
        let decision = evaluate_model_compatibility("mistral-7b", &normalized);

        assert_eq!(normalized.ram_gb, Some(16));
        assert_eq!(normalized.storage_available_gb, Some(20));
        assert_eq!(normalized.gpu_available, Some(false));
        assert!(decision.is_compatible());
    }

    #[test]
    fn hardware_profiler_npu_does_not_satisfy_gpu_requirement() {
        let hardware_profile = hardware_profile_with_accelerator(AcceleratorKind::Npu);
        let normalized = ModelCompatibilityProfile::from_hardware_profile(&hardware_profile);

        assert_eq!(normalized.gpu_available, Some(false));
    }

    #[test]
    fn node_capabilities_preserve_legacy_accelerator_when_gpu_type_is_absent() {
        let capabilities = NodeCapabilities {
            node_id: "node".to_string(),
            cpu_cores: 8,
            ram_gb: 32,
            gpu_type: None,
            npu_type: None,
            storage_available_gb: 50,
            worker_slots: 8,
            supported_models: Vec::new(),
            cpu_features: Vec::new(),
            accelerator: "Metal".to_string(),
        };

        let normalized = ModelCompatibilityProfile::from_node_capabilities(&capabilities);

        assert_eq!(normalized.gpu_available, Some(true));
    }

    #[test]
    fn node_capabilities_npu_accelerator_does_not_satisfy_gpu_requirement() {
        let capabilities = NodeCapabilities {
            node_id: "node".to_string(),
            cpu_cores: 8,
            ram_gb: 32,
            gpu_type: None,
            npu_type: Some("NPU".to_string()),
            storage_available_gb: 50,
            worker_slots: 8,
            supported_models: Vec::new(),
            cpu_features: Vec::new(),
            accelerator: "NPU".to_string(),
        };

        let normalized = ModelCompatibilityProfile::from_node_capabilities(&capabilities);

        assert_eq!(normalized.gpu_available, Some(false));
    }
}
