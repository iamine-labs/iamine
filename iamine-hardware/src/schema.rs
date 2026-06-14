use serde::{Deserialize, Serialize};

pub const HARDWARE_PROFILE_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareCollectionMode {
    StaticOnly,
    QuickDynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DetectionConfidence {
    High,
    Medium,
    Low,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorKind {
    Cpu,
    Metal,
    Cuda,
    Rocm,
    Vulkan,
    Npu,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareProfileWarning {
    pub code: String,
    pub message: String,
}

impl HardwareProfileWarning {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuFeatureProfile {
    pub avx2: bool,
    pub avx512f: bool,
    pub fma: bool,
    pub neon: bool,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuStaticProfile {
    pub architecture: String,
    pub vendor: Option<String>,
    pub brand: Option<String>,
    pub physical_cores: Option<u32>,
    pub logical_cores: u32,
    pub recommended_threads: u32,
    pub features: CpuFeatureProfile,
    pub confidence: DetectionConfidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryStaticProfile {
    pub total_bytes: u64,
    pub available_bytes: Option<u64>,
    pub total_gb: u64,
    pub unified_memory: bool,
    pub confidence: DetectionConfidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceleratorStaticProfile {
    pub kind: AcceleratorKind,
    pub name: String,
    pub vendor: Option<String>,
    pub memory_bytes: Option<u64>,
    pub unified_memory: bool,
    pub confidence: DetectionConfidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageStaticProfile {
    pub available_bytes: Option<u64>,
    pub confidence: DetectionConfidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkStaticProfile {
    pub probe: String,
    pub confidence: DetectionConfidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveHardwareProfile {
    pub effective_cpu_threads: u32,
    pub effective_worker_slots: u32,
    pub effective_ram_bytes: u64,
    pub effective_accelerator: AcceleratorKind,
    pub inference_acceleration_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareStaticProfile {
    pub os_family: String,
    pub os_arch: String,
    pub cpu: CpuStaticProfile,
    pub memory: MemoryStaticProfile,
    pub accelerators: Vec<AcceleratorStaticProfile>,
    pub storage: StorageStaticProfile,
    pub network: NetworkStaticProfile,
    pub effective: EffectiveHardwareProfile,
    pub warnings: Vec<HardwareProfileWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuDynamicProfile {
    pub score_ops_per_sec: u64,
    pub sample_duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDynamicProfile {
    pub available_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageDynamicProfile {
    pub write_mb_per_sec: Option<u64>,
    pub sample_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareDynamicProfile {
    pub mode: HardwareCollectionMode,
    pub duration_ms: u64,
    pub cpu: CpuDynamicProfile,
    pub memory: MemoryDynamicProfile,
    pub storage: StorageDynamicProfile,
    pub warnings: Vec<HardwareProfileWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHardwareProfile {
    pub schema_version: String,
    pub profile_id: String,
    pub generated_at_unix_ms: u64,
    pub collection_mode: HardwareCollectionMode,
    pub static_profile: HardwareStaticProfile,
    pub dynamic_profile: Option<HardwareDynamicProfile>,
    pub warnings: Vec<HardwareProfileWarning>,
}

impl NodeHardwareProfile {
    pub fn validate_schema(&self) -> std::result::Result<(), String> {
        if self.schema_version == HARDWARE_PROFILE_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(format!(
                "unsupported hardware profile schema_version: {}",
                self.schema_version
            ))
        }
    }
}
