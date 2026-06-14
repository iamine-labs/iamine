use crate::persistence::default_profile_path;
use crate::platform::detect_static;
use crate::runtime::{collect_quick_dynamic_profile, DynamicProfileOptions};
use crate::schema::{
    AcceleratorKind, DetectionConfidence, EffectiveHardwareProfile, HardwareCollectionMode,
    HardwareDynamicProfile, HardwareProfileWarning, HardwareStaticProfile, NetworkStaticProfile,
    NodeHardwareProfile, HARDWARE_PROFILE_SCHEMA_VERSION,
};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareProfilerConfig {
    pub mode: HardwareCollectionMode,
    pub dynamic_options: DynamicProfileOptions,
}

impl Default for HardwareProfilerConfig {
    fn default() -> Self {
        Self {
            mode: HardwareCollectionMode::StaticOnly,
            dynamic_options: DynamicProfileOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardwareProfileParts {
    pub mode: HardwareCollectionMode,
    pub cpu: crate::schema::CpuStaticProfile,
    pub memory: crate::schema::MemoryStaticProfile,
    pub accelerators: Vec<crate::schema::AcceleratorStaticProfile>,
    pub storage: crate::schema::StorageStaticProfile,
    pub dynamic_profile: Option<HardwareDynamicProfile>,
    pub warnings: Vec<HardwareProfileWarning>,
    pub generated_at_unix_ms: u64,
}

pub fn inspect_hardware(config: HardwareProfilerConfig) -> Result<NodeHardwareProfile, String> {
    inspect_hardware_with_profile_dir(config, profile_dir_from_default_path())
}

pub fn inspect_hardware_with_profile_dir(
    config: HardwareProfilerConfig,
    profile_dir: impl AsRef<Path>,
) -> Result<NodeHardwareProfile, String> {
    let profile_dir = profile_dir.as_ref();
    let static_detection = detect_static(profile_dir);
    let dynamic_profile = match config.mode {
        HardwareCollectionMode::StaticOnly => None,
        HardwareCollectionMode::QuickDynamic => {
            Some(collect_quick_dynamic_profile(config.dynamic_options)?)
        }
    };

    Ok(build_node_hardware_profile(HardwareProfileParts {
        mode: config.mode,
        cpu: static_detection.cpu,
        memory: static_detection.memory,
        accelerators: static_detection.accelerators,
        storage: static_detection.storage,
        dynamic_profile,
        warnings: static_detection.warnings,
        generated_at_unix_ms: now_ms(),
    }))
}

pub fn build_node_hardware_profile(mut parts: HardwareProfileParts) -> NodeHardwareProfile {
    let effective_accelerator = parts
        .accelerators
        .iter()
        .find(|accelerator| accelerator.kind != AcceleratorKind::Cpu)
        .map(|accelerator| accelerator.kind)
        .unwrap_or(AcceleratorKind::Cpu);

    if parts.accelerators.is_empty() {
        parts.warnings.push(HardwareProfileWarning::new(
            "accelerator_list_empty",
            "no accelerator entries were produced",
        ));
    }

    let static_profile = HardwareStaticProfile {
        os_family: std::env::consts::OS.to_string(),
        os_arch: std::env::consts::ARCH.to_string(),
        effective: EffectiveHardwareProfile {
            effective_cpu_threads: parts.cpu.recommended_threads.max(1),
            effective_worker_slots: parts.cpu.logical_cores.max(1),
            effective_ram_bytes: parts
                .memory
                .available_bytes
                .unwrap_or(parts.memory.total_bytes),
            effective_accelerator,
            inference_acceleration_available: effective_accelerator != AcceleratorKind::Cpu,
        },
        cpu: parts.cpu,
        memory: parts.memory,
        accelerators: parts.accelerators,
        storage: parts.storage,
        network: NetworkStaticProfile {
            probe: "not_collected_privacy_preserving".to_string(),
            confidence: DetectionConfidence::Unknown,
        },
        warnings: parts.warnings.clone(),
    };

    NodeHardwareProfile {
        schema_version: HARDWARE_PROFILE_SCHEMA_VERSION.to_string(),
        profile_id: format!("hw-{}-{}", parts.generated_at_unix_ms, std::process::id()),
        generated_at_unix_ms: parts.generated_at_unix_ms,
        collection_mode: parts.mode,
        static_profile,
        dynamic_profile: parts.dynamic_profile,
        warnings: parts.warnings,
    }
}

fn profile_dir_from_default_path() -> std::path::PathBuf {
    default_profile_path()
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        AcceleratorStaticProfile, CpuFeatureProfile, CpuStaticProfile, DetectionConfidence,
        MemoryStaticProfile, StorageStaticProfile,
    };

    fn test_cpu() -> CpuStaticProfile {
        CpuStaticProfile {
            architecture: "x86_64".to_string(),
            vendor: Some("GenuineIntel".to_string()),
            brand: Some("Test CPU".to_string()),
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
        }
    }

    fn test_memory() -> MemoryStaticProfile {
        MemoryStaticProfile {
            total_bytes: 16 * 1_073_741_824,
            available_bytes: Some(8 * 1_073_741_824),
            total_gb: 16,
            unified_memory: false,
            confidence: DetectionConfidence::High,
        }
    }

    #[test]
    fn profile_schema_static_roundtrip_is_parseable() -> Result<(), serde_json::Error> {
        let profile = build_node_hardware_profile(HardwareProfileParts {
            mode: HardwareCollectionMode::StaticOnly,
            cpu: test_cpu(),
            memory: test_memory(),
            accelerators: vec![AcceleratorStaticProfile {
                kind: AcceleratorKind::Cpu,
                name: "CPU".to_string(),
                vendor: None,
                memory_bytes: None,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            }],
            storage: StorageStaticProfile {
                available_bytes: Some(10),
                confidence: DetectionConfidence::Medium,
            },
            dynamic_profile: None,
            warnings: Vec::new(),
            generated_at_unix_ms: 42,
        });

        let json = serde_json::to_string(&profile)?;
        let decoded: NodeHardwareProfile = serde_json::from_str(&json)?;

        assert_eq!(decoded.schema_version, HARDWARE_PROFILE_SCHEMA_VERSION);
        assert!(decoded.validate_schema().is_ok());
        assert!(decoded.static_profile.cpu.features.avx2);
        assert_eq!(
            decoded.static_profile.network.probe,
            "not_collected_privacy_preserving"
        );
        Ok(())
    }

    #[test]
    fn profile_schema_rejects_incompatible_version() {
        let mut profile = build_node_hardware_profile(HardwareProfileParts {
            mode: HardwareCollectionMode::StaticOnly,
            cpu: test_cpu(),
            memory: test_memory(),
            accelerators: Vec::new(),
            storage: StorageStaticProfile {
                available_bytes: None,
                confidence: DetectionConfidence::Unknown,
            },
            dynamic_profile: None,
            warnings: Vec::new(),
            generated_at_unix_ms: 42,
        });
        profile.schema_version = "2.0.0".to_string();

        assert!(profile
            .validate_schema()
            .unwrap_err()
            .contains("unsupported"));
    }

    #[test]
    fn profile_json_does_not_contain_common_sensitive_keys() -> Result<(), serde_json::Error> {
        let profile = build_node_hardware_profile(HardwareProfileParts {
            mode: HardwareCollectionMode::StaticOnly,
            cpu: test_cpu(),
            memory: test_memory(),
            accelerators: Vec::new(),
            storage: StorageStaticProfile {
                available_bytes: None,
                confidence: DetectionConfidence::Unknown,
            },
            dynamic_profile: None,
            warnings: Vec::new(),
            generated_at_unix_ms: 42,
        });

        let json = serde_json::to_string(&profile)?;
        for blocked in [
            "hostname",
            "username",
            "home_dir",
            "mac_address",
            "ip_address",
        ] {
            assert!(!json.contains(blocked), "found sensitive key {blocked}");
        }
        Ok(())
    }
}
