use iamine_hardware::{AcceleratorKind, NodeHardwareProfile};
use iamine_models::{ModelNodeCapabilities, ModelStorage};

pub(crate) const LOCAL_DIAGNOSTIC_NODE_ID: &str = "local";

pub(crate) fn capabilities_from_hardware_profile(
    node_id: &str,
    hardware_profile: Option<&NodeHardwareProfile>,
    storage: &ModelStorage,
) -> ModelNodeCapabilities {
    let supported_models = storage.list_local_models();

    let Some(profile) = hardware_profile else {
        return ModelNodeCapabilities {
            node_id: node_id.to_string(),
            cpu_cores: 1,
            ram_gb: 2,
            gpu_type: None,
            npu_type: None,
            storage_available_gb: 0,
            worker_slots: 1,
            supported_models,
            cpu_features: Vec::new(),
            accelerator: "Unknown".to_string(),
        };
    };

    let effective = &profile.static_profile.effective;
    let accelerator = accelerator_label(effective.effective_accelerator).to_string();
    let gpu_type = if effective.effective_accelerator == AcceleratorKind::Cpu
        || effective.effective_accelerator == AcceleratorKind::Unknown
    {
        None
    } else {
        Some(accelerator.clone())
    };

    ModelNodeCapabilities {
        node_id: node_id.to_string(),
        cpu_cores: profile.static_profile.cpu.logical_cores.max(1),
        ram_gb: u32_from_u64(profile.static_profile.memory.total_gb.max(2)),
        gpu_type,
        npu_type: None,
        storage_available_gb: match profile.static_profile.storage.available_bytes {
            Some(bytes) => u32_from_u64(bytes_to_gb(bytes)),
            None => 0,
        },
        worker_slots: effective.effective_worker_slots.max(1),
        supported_models,
        cpu_features: profile.static_profile.cpu.features.features.clone(),
        accelerator,
    }
}

fn accelerator_label(accelerator: AcceleratorKind) -> &'static str {
    match accelerator {
        AcceleratorKind::Cpu => "CPU",
        AcceleratorKind::Metal => "Metal",
        AcceleratorKind::Cuda => "CUDA",
        AcceleratorKind::Rocm => "ROCm",
        AcceleratorKind::Vulkan => "Vulkan",
        AcceleratorKind::Npu => "NPU",
        AcceleratorKind::Unknown => "Unknown",
    }
}

fn u32_from_u64(value: u64) -> u32 {
    value.min(u32::MAX as u64) as u32
}

fn bytes_to_gb(bytes: u64) -> u64 {
    bytes / 1_073_741_824
}

#[cfg(test)]
mod tests {
    use super::*;
    use iamine_hardware::{
        build_node_hardware_profile, AcceleratorStaticProfile, CpuFeatureProfile, CpuStaticProfile,
        DetectionConfidence, HardwareCollectionMode, HardwareProfileParts, MemoryStaticProfile,
        StorageStaticProfile,
    };
    use tempfile::TempDir;

    fn profile_with_accelerator(accelerator: AcceleratorKind) -> NodeHardwareProfile {
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
                kind: accelerator,
                name: accelerator_label(accelerator).to_string(),
                vendor: None,
                memory_bytes: None,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            }],
            storage: StorageStaticProfile {
                available_bytes: Some(90 * 1_073_741_824),
                confidence: DetectionConfidence::High,
            },
            dynamic_profile: None,
            warnings: Vec::new(),
            generated_at_unix_ms: 42,
        })
    }

    #[test]
    fn capability_snapshot_maps_static_hardware_without_noisy_detector(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage_dir = TempDir::new()?;
        let storage = ModelStorage::new_in(storage_dir.path().to_path_buf());
        let profile = profile_with_accelerator(AcceleratorKind::Metal);

        let capabilities =
            capabilities_from_hardware_profile("local-test", Some(&profile), &storage);

        assert_eq!(capabilities.node_id, "local-test");
        assert_eq!(capabilities.cpu_cores, 8);
        assert_eq!(capabilities.ram_gb, 16);
        assert_eq!(capabilities.accelerator, "Metal");
        assert_eq!(capabilities.gpu_type.as_deref(), Some("Metal"));
        assert_eq!(capabilities.storage_available_gb, 90);
        Ok(())
    }

    #[test]
    fn capability_snapshot_keeps_cpu_accelerator_out_of_gpu_type(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage_dir = TempDir::new()?;
        let storage = ModelStorage::new_in(storage_dir.path().to_path_buf());
        let profile = profile_with_accelerator(AcceleratorKind::Cpu);

        let capabilities =
            capabilities_from_hardware_profile("local-test", Some(&profile), &storage);

        assert_eq!(capabilities.accelerator, "CPU");
        assert_eq!(capabilities.gpu_type, None);
        Ok(())
    }
}
