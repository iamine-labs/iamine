pub mod error;
pub mod persistence;
pub mod profile;
pub mod runtime;
pub mod schema;

mod platform;

pub use error::{HardwareProfileError, Result};
pub use persistence::{
    default_profile_path, HardwareProfileStore, ProfileLock, IAMINE_HARDWARE_PROFILE_PATH,
};
pub use profile::{
    build_node_hardware_profile, inspect_hardware, inspect_hardware_with_profile_dir,
    HardwareProfileParts, HardwareProfilerConfig,
};
pub use runtime::{collect_quick_dynamic_profile, DynamicProfileOptions};
pub use schema::{
    AcceleratorKind, AcceleratorStaticProfile, CpuDynamicProfile, CpuFeatureProfile,
    CpuStaticProfile, DetectionConfidence, EffectiveHardwareProfile, HardwareCollectionMode,
    HardwareDynamicProfile, HardwareProfileWarning, HardwareStaticProfile, MemoryDynamicProfile,
    MemoryStaticProfile, NetworkStaticProfile, NodeHardwareProfile, StorageDynamicProfile,
    StorageStaticProfile, HARDWARE_PROFILE_SCHEMA_VERSION,
};
