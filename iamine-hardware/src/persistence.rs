use crate::error::{HardwareProfileError, Result};
use crate::profile::{inspect_hardware, HardwareProfilerConfig};
use crate::schema::NodeHardwareProfile;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const IAMINE_HARDWARE_PROFILE_PATH: &str = "IAMINE_HARDWARE_PROFILE_PATH";

pub fn default_profile_path() -> PathBuf {
    if let Some(path) = std::env::var_os(IAMINE_HARDWARE_PROFILE_PATH) {
        return PathBuf::from(path);
    }

    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".iamine")
        .join("hardware")
        .join("profile.json")
}

#[derive(Debug, Clone)]
pub struct HardwareProfileStore {
    path: PathBuf,
}

impl HardwareProfileStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<NodeHardwareProfile> {
        let bytes = fs::read(&self.path)?;
        let profile: NodeHardwareProfile = serde_json::from_slice(&bytes)?;
        profile
            .validate_schema()
            .map_err(HardwareProfileError::Validation)?;
        Ok(profile)
    }

    pub fn save(&self, profile: &NodeHardwareProfile) -> Result<()> {
        profile
            .validate_schema()
            .map_err(HardwareProfileError::Validation)?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| HardwareProfileError::Io("profile path has no parent".to_string()))?;
        fs::create_dir_all(parent).map_err(|error| {
            HardwareProfileError::Io(format!(
                "creating hardware profile directory {}: {}",
                parent.display(),
                error
            ))
        })?;
        set_dir_permissions(parent).map_err(|error| {
            HardwareProfileError::Io(format!(
                "setting hardware profile directory permissions {}: {}",
                parent.display(),
                error
            ))
        })?;
        let _lock = ProfileLock::acquire(&self.lock_path())?;
        let temp_path = parent.join(format!(
            ".profile.json.tmp.{}.{}",
            std::process::id(),
            now_nanos()
        ));
        let data = serde_json::to_vec_pretty(profile)?;

        {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .map_err(|error| {
                    HardwareProfileError::Io(format!(
                        "creating temp hardware profile {}: {}",
                        temp_path.display(),
                        error
                    ))
                })?;
            file.write_all(&data).map_err(|error| {
                HardwareProfileError::Io(format!(
                    "writing temp hardware profile {}: {}",
                    temp_path.display(),
                    error
                ))
            })?;
            file.sync_all().map_err(|error| {
                HardwareProfileError::Io(format!(
                    "syncing temp hardware profile {}: {}",
                    temp_path.display(),
                    error
                ))
            })?;
        }
        set_file_permissions(&temp_path).map_err(|error| {
            HardwareProfileError::Io(format!(
                "setting temp hardware profile permissions {}: {}",
                temp_path.display(),
                error
            ))
        })?;
        fs::rename(&temp_path, &self.path).map_err(|error| {
            HardwareProfileError::Io(format!(
                "renaming temp hardware profile {} to {}: {}",
                temp_path.display(),
                self.path.display(),
                error
            ))
        })?;
        set_file_permissions(&self.path).map_err(|error| {
            HardwareProfileError::Io(format!(
                "setting hardware profile permissions {}: {}",
                self.path.display(),
                error
            ))
        })?;
        Ok(())
    }

    pub fn refresh(&self, config: HardwareProfilerConfig) -> Result<NodeHardwareProfile> {
        let profile = inspect_hardware(config).map_err(HardwareProfileError::Validation)?;
        self.save(&profile)?;
        Ok(profile)
    }

    pub fn is_stale(&self, max_age: Duration) -> Result<bool> {
        let profile = self.load()?;
        let now = now_ms();
        Ok(now.saturating_sub(profile.generated_at_unix_ms) > max_age.as_millis() as u64)
    }

    fn lock_path(&self) -> PathBuf {
        self.path.with_extension("json.lock")
    }
}

impl Default for HardwareProfileStore {
    fn default() -> Self {
        Self {
            path: default_profile_path(),
        }
    }
}

pub struct ProfileLock {
    path: PathBuf,
}

impl ProfileLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                file.write_all(b"locked")?;
                Ok(Self {
                    path: path.to_path_buf(),
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(HardwareProfileError::LockAlreadyHeld(path.to_path_buf()))
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for ProfileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(unix)]
fn set_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{build_node_hardware_profile, HardwareProfileParts};
    use crate::schema::{
        AcceleratorKind, AcceleratorStaticProfile, CpuFeatureProfile, CpuStaticProfile,
        DetectionConfidence, HardwareCollectionMode, MemoryStaticProfile, StorageStaticProfile,
    };

    fn test_profile(generated_at_unix_ms: u64) -> NodeHardwareProfile {
        build_node_hardware_profile(HardwareProfileParts {
            mode: HardwareCollectionMode::StaticOnly,
            cpu: CpuStaticProfile {
                architecture: "x86_64".to_string(),
                vendor: None,
                brand: None,
                physical_cores: Some(2),
                logical_cores: 4,
                recommended_threads: 2,
                features: CpuFeatureProfile {
                    avx2: false,
                    avx512f: false,
                    fma: false,
                    neon: false,
                    features: Vec::new(),
                },
                confidence: DetectionConfidence::High,
            },
            memory: MemoryStaticProfile {
                total_bytes: 8 * 1_073_741_824,
                available_bytes: Some(4 * 1_073_741_824),
                total_gb: 8,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            },
            accelerators: vec![AcceleratorStaticProfile {
                kind: AcceleratorKind::Cpu,
                name: "CPU".to_string(),
                vendor: None,
                memory_bytes: None,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            }],
            storage: StorageStaticProfile {
                available_bytes: Some(1),
                confidence: DetectionConfidence::Medium,
            },
            dynamic_profile: None,
            warnings: Vec::new(),
            generated_at_unix_ms,
        })
    }

    #[test]
    fn profile_store_roundtrip_validates_schema() -> Result<()> {
        let dir =
            tempfile::tempdir().map_err(|error| HardwareProfileError::Io(error.to_string()))?;
        let store = HardwareProfileStore::new(dir.path().join("profile.json"));
        let profile = test_profile(now_ms());

        store.save(&profile)?;
        let loaded = store.load()?;

        assert_eq!(loaded.schema_version, profile.schema_version);
        assert_eq!(loaded.static_profile.cpu.logical_cores, 4);
        Ok(())
    }

    #[test]
    fn default_profile_path_respects_env_override() -> Result<()> {
        let dir =
            tempfile::tempdir().map_err(|error| HardwareProfileError::Io(error.to_string()))?;
        let path = dir.path().join("custom-profile.json");
        std::env::set_var(IAMINE_HARDWARE_PROFILE_PATH, &path);
        let observed = default_profile_path();
        std::env::remove_var(IAMINE_HARDWARE_PROFILE_PATH);

        assert_eq!(observed, path);
        Ok(())
    }

    #[test]
    fn profile_store_rejects_incompatible_schema_on_save() -> Result<()> {
        let dir =
            tempfile::tempdir().map_err(|error| HardwareProfileError::Io(error.to_string()))?;
        let store = HardwareProfileStore::new(dir.path().join("profile.json"));
        let mut profile = test_profile(now_ms());
        profile.schema_version = "9.0.0".to_string();

        assert!(matches!(
            store.save(&profile),
            Err(HardwareProfileError::Validation(_))
        ));
        Ok(())
    }

    #[test]
    fn profile_store_reports_stale_profiles() -> Result<()> {
        let dir =
            tempfile::tempdir().map_err(|error| HardwareProfileError::Io(error.to_string()))?;
        let store = HardwareProfileStore::new(dir.path().join("profile.json"));
        store.save(&test_profile(1))?;

        assert!(store.is_stale(Duration::from_millis(1))?);
        Ok(())
    }

    #[test]
    fn profile_lock_rejects_second_writer() -> Result<()> {
        let dir =
            tempfile::tempdir().map_err(|error| HardwareProfileError::Io(error.to_string()))?;
        let lock_path = dir.path().join("profile.json.lock");
        let _lock = ProfileLock::acquire(&lock_path)?;

        assert!(matches!(
            ProfileLock::acquire(&lock_path),
            Err(HardwareProfileError::LockAlreadyHeld(_))
        ));
        Ok(())
    }
}
