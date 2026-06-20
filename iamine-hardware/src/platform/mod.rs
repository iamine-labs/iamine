#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

use crate::schema::{
    AcceleratorKind, AcceleratorStaticProfile, CpuFeatureProfile, CpuStaticProfile,
    DetectionConfidence, HardwareProfileWarning, MemoryStaticProfile, StorageStaticProfile,
};
use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone)]
pub(crate) struct StaticDetection {
    pub(crate) cpu: CpuStaticProfile,
    pub(crate) memory: MemoryStaticProfile,
    pub(crate) accelerators: Vec<AcceleratorStaticProfile>,
    pub(crate) storage: StorageStaticProfile,
    pub(crate) warnings: Vec<HardwareProfileWarning>,
}

pub(crate) fn detect_static(profile_dir: &Path) -> StaticDetection {
    let mut warnings = Vec::new();
    let cpu = detect_cpu(&mut warnings);
    let memory = detect_memory(&mut warnings);
    let accelerators = detect_accelerators(&memory, &mut warnings);
    let storage = detect_storage(profile_dir, &mut warnings);

    StaticDetection {
        cpu,
        memory,
        accelerators,
        storage,
        warnings,
    }
}

pub(crate) fn detect_available_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        return linux::detect_available_memory_bytes();
    }
    #[cfg(target_os = "macos")]
    {
        return macos::detect_available_memory_bytes();
    }
    #[allow(unreachable_code)]
    None
}

fn detect_cpu(warnings: &mut Vec<HardwareProfileWarning>) -> CpuStaticProfile {
    let logical_cores = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get() as u32)
        .unwrap_or(1);

    let mut detected = platform_cpu_profile(logical_cores);
    detected.features = detect_cpu_features();

    if detected.logical_cores == 0 {
        warnings.push(HardwareProfileWarning::new(
            "cpu_logical_cores_unknown",
            "logical CPU cores could not be detected; using one core",
        ));
        detected.logical_cores = 1;
    }

    detected.recommended_threads = (detected.logical_cores / 2).max(1);
    detected
}

fn platform_cpu_profile(logical_cores: u32) -> CpuStaticProfile {
    #[cfg(target_os = "linux")]
    {
        return linux::detect_cpu_profile(logical_cores);
    }
    #[cfg(target_os = "macos")]
    {
        return macos::detect_cpu_profile(logical_cores);
    }
    #[allow(unreachable_code)]
    CpuStaticProfile {
        architecture: std::env::consts::ARCH.to_string(),
        vendor: None,
        brand: None,
        physical_cores: None,
        logical_cores,
        recommended_threads: (logical_cores / 2).max(1),
        features: CpuFeatureProfile {
            avx2: false,
            avx512f: false,
            fma: false,
            neon: false,
            features: Vec::new(),
        },
        confidence: DetectionConfidence::Low,
    }
}

fn detect_cpu_features() -> CpuFeatureProfile {
    let mut features = Vec::new();
    let mut profile = CpuFeatureProfile {
        avx2: false,
        avx512f: false,
        fma: false,
        neon: false,
        features: Vec::new(),
    };

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            profile.avx2 = true;
            features.push("AVX2".to_string());
        }
        if std::is_x86_feature_detected!("avx512f") {
            profile.avx512f = true;
            features.push("AVX512F".to_string());
        }
        if std::is_x86_feature_detected!("fma") {
            profile.fma = true;
            features.push("FMA".to_string());
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        profile.neon = true;
        features.push("NEON".to_string());
        features.push("ARM64".to_string());
    }

    profile.features = features;
    profile
}

fn detect_memory(warnings: &mut Vec<HardwareProfileWarning>) -> MemoryStaticProfile {
    #[cfg(target_os = "linux")]
    {
        return linux::detect_memory_profile(warnings);
    }
    #[cfg(target_os = "macos")]
    {
        return macos::detect_memory_profile(warnings);
    }
    #[allow(unreachable_code)]
    {
        warnings.push(HardwareProfileWarning::new(
            "memory_detection_unavailable",
            "memory detection is not implemented for this platform",
        ));
        MemoryStaticProfile {
            total_bytes: 0,
            available_bytes: None,
            total_gb: 0,
            unified_memory: false,
            confidence: DetectionConfidence::Unknown,
        }
    }
}

fn detect_accelerators(
    memory: &MemoryStaticProfile,
    warnings: &mut Vec<HardwareProfileWarning>,
) -> Vec<AcceleratorStaticProfile> {
    #[cfg(target_os = "macos")]
    {
        macos::detect_accelerators(memory, warnings)
    }
    #[cfg(target_os = "linux")]
    {
        let _ = memory;
        linux::detect_accelerators(warnings)
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = memory;
        let _ = warnings;
        vec![cpu_accelerator()]
    }
}

fn detect_storage(
    profile_dir: &Path,
    warnings: &mut Vec<HardwareProfileWarning>,
) -> StorageStaticProfile {
    let probe_path = existing_storage_probe_path(profile_dir);
    let output = Command::new("df").arg("-k").arg(probe_path).output();
    match output {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let available_bytes = stdout
                .lines()
                .nth(1)
                .and_then(|line| line.split_whitespace().nth(3))
                .and_then(|kilobytes| kilobytes.parse::<u64>().ok())
                .map(|kilobytes| kilobytes * 1024);
            StorageStaticProfile {
                available_bytes,
                confidence: if available_bytes.is_some() {
                    DetectionConfidence::Medium
                } else {
                    DetectionConfidence::Low
                },
            }
        }
        _ => {
            warnings.push(HardwareProfileWarning::new(
                "storage_detection_unavailable",
                "storage free-space detection could not run",
            ));
            StorageStaticProfile {
                available_bytes: None,
                confidence: DetectionConfidence::Unknown,
            }
        }
    }
}

fn existing_storage_probe_path(path: &Path) -> &Path {
    let mut current = path;
    while !current.exists() {
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent;
    }
    current
}

pub(crate) fn cpu_accelerator() -> AcceleratorStaticProfile {
    AcceleratorStaticProfile {
        kind: AcceleratorKind::Cpu,
        name: "CPU".to_string(),
        vendor: None,
        memory_bytes: None,
        unified_memory: false,
        confidence: DetectionConfidence::High,
    }
}

pub(crate) fn run_command(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_probe_uses_existing_parent_for_missing_profile_dir() -> Result<(), std::io::Error> {
        let temp = tempfile::tempdir()?;
        let missing = temp.path().join("missing").join("hardware");

        assert_eq!(existing_storage_probe_path(&missing), temp.path());
        Ok(())
    }
}
