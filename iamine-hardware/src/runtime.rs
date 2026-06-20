use crate::platform::detect_available_memory_bytes;
use crate::schema::{
    CpuDynamicProfile, HardwareCollectionMode, HardwareDynamicProfile, HardwareProfileWarning,
    MemoryDynamicProfile, StorageDynamicProfile,
};
use std::io::Write;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_CPU_SAMPLE_MS: u64 = 250;
const STORAGE_SAMPLE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamicProfileOptions {
    pub max_duration_ms: u64,
}

impl Default for DynamicProfileOptions {
    fn default() -> Self {
        Self {
            max_duration_ms: 30_000,
        }
    }
}

pub fn collect_quick_dynamic_profile(
    options: DynamicProfileOptions,
) -> Result<HardwareDynamicProfile, String> {
    if options.max_duration_ms == 0 {
        return Err("dynamic profile max_duration_ms must be greater than zero".to_string());
    }

    let started = Instant::now();
    let mut warnings = Vec::new();
    let cpu = sample_cpu(options.max_duration_ms.min(DEFAULT_CPU_SAMPLE_MS));
    let memory = MemoryDynamicProfile {
        available_bytes: detect_available_memory_bytes(),
    };
    let storage = sample_storage(&mut warnings);
    let duration_ms = started.elapsed().as_millis() as u64;

    if duration_ms > options.max_duration_ms {
        warnings.push(HardwareProfileWarning::new(
            "dynamic_profile_duration_exceeded",
            "quick dynamic profile exceeded the requested duration budget",
        ));
    }

    Ok(HardwareDynamicProfile {
        mode: HardwareCollectionMode::QuickDynamic,
        duration_ms,
        cpu,
        memory,
        storage,
        warnings,
    })
}

fn sample_cpu(sample_ms: u64) -> CpuDynamicProfile {
    let started = Instant::now();
    let duration = Duration::from_millis(sample_ms.max(1));
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut ops = 0u64;

    while started.elapsed() < duration {
        state = state.rotate_left(7) ^ 0xa076_1d64_78bd_642f;
        state = state.wrapping_mul(0xe703_7ed1_a0b4_28db);
        ops = ops.saturating_add(1);
    }

    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    CpuDynamicProfile {
        score_ops_per_sec: ops.saturating_mul(1000) / elapsed_ms,
        sample_duration_ms: elapsed_ms,
    }
}

fn sample_storage(warnings: &mut Vec<HardwareProfileWarning>) -> StorageDynamicProfile {
    let path = std::env::temp_dir().join(format!(
        "iamine-hardware-profile-{}-{}.tmp",
        std::process::id(),
        now_nanos()
    ));
    let data = vec![0x5au8; STORAGE_SAMPLE_BYTES];
    let started = Instant::now();
    let result = std::fs::File::create(&path)
        .and_then(|mut file| file.write_all(&data).and_then(|_| file.sync_all()));
    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    let _ = std::fs::remove_file(&path);

    match result {
        Ok(()) => StorageDynamicProfile {
            write_mb_per_sec: Some(1000 / elapsed_ms),
            sample_bytes: STORAGE_SAMPLE_BYTES as u64,
        },
        Err(error) => {
            warnings.push(HardwareProfileWarning::new(
                "dynamic_storage_sample_failed",
                format!("storage dynamic sample failed: {error}"),
            ));
            StorageDynamicProfile {
                write_mb_per_sec: None,
                sample_bytes: STORAGE_SAMPLE_BYTES as u64,
            }
        }
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_profile_rejects_zero_timeout() {
        let error = collect_quick_dynamic_profile(DynamicProfileOptions { max_duration_ms: 0 })
            .unwrap_err();

        assert!(error.contains("greater than zero"));
    }

    #[test]
    fn dynamic_profile_is_bounded_and_populates_cpu_sample() -> Result<(), String> {
        let profile = collect_quick_dynamic_profile(DynamicProfileOptions {
            max_duration_ms: 2_000,
        })?;

        assert_eq!(profile.mode, HardwareCollectionMode::QuickDynamic);
        assert!(profile.cpu.score_ops_per_sec > 0);
        assert!(profile.duration_ms < 30_000);
        Ok(())
    }
}
