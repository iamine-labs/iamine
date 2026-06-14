use crate::platform::{cpu_accelerator, run_command};
use crate::schema::{
    AcceleratorKind, AcceleratorStaticProfile, CpuFeatureProfile, CpuStaticProfile,
    DetectionConfidence, HardwareProfileWarning, MemoryStaticProfile,
};

pub(crate) fn detect_cpu_profile(logical_cores: u32) -> CpuStaticProfile {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let vendor = find_cpuinfo_value(&cpuinfo, "vendor_id");
    let brand = find_cpuinfo_value(&cpuinfo, "model name");
    let physical_cores =
        find_cpuinfo_value(&cpuinfo, "cpu cores").and_then(|value| value.parse().ok());

    CpuStaticProfile {
        architecture: std::env::consts::ARCH.to_string(),
        vendor,
        brand,
        physical_cores,
        logical_cores,
        recommended_threads: (logical_cores / 2).max(1),
        features: CpuFeatureProfile {
            avx2: false,
            avx512f: false,
            fma: false,
            neon: false,
            features: Vec::new(),
        },
        confidence: DetectionConfidence::Medium,
    }
}

pub(crate) fn detect_memory_profile(
    warnings: &mut Vec<HardwareProfileWarning>,
) -> MemoryStaticProfile {
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let total_bytes = find_meminfo_kb(&meminfo, "MemTotal:")
        .map(|kb| kb * 1024)
        .unwrap_or(0);
    let available_bytes = find_meminfo_kb(&meminfo, "MemAvailable:").map(|kb| kb * 1024);

    if total_bytes == 0 {
        warnings.push(HardwareProfileWarning::new(
            "memory_total_unavailable",
            "MemTotal was not available from /proc/meminfo",
        ));
    }

    MemoryStaticProfile {
        total_bytes,
        available_bytes,
        total_gb: bytes_to_gb_floor(total_bytes),
        unified_memory: false,
        confidence: if total_bytes > 0 {
            DetectionConfidence::High
        } else {
            DetectionConfidence::Unknown
        },
    }
}

pub(crate) fn detect_available_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    find_meminfo_kb(&meminfo, "MemAvailable:").map(|kb| kb * 1024)
}

pub(crate) fn detect_accelerators(
    warnings: &mut Vec<HardwareProfileWarning>,
) -> Vec<AcceleratorStaticProfile> {
    let mut accelerators = Vec::new();

    if let Some(stdout) = run_command(
        "nvidia-smi",
        &["--query-gpu=name,memory.total", "--format=csv,noheader"],
    ) {
        for line in stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            let mut parts = line.splitn(2, ',');
            let name = parts.next().unwrap_or("NVIDIA GPU").trim().to_string();
            let memory_bytes = parts
                .next()
                .and_then(|raw| raw.trim().strip_suffix(" MiB").or(Some(raw.trim())))
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(|mib| mib * 1024 * 1024);
            accelerators.push(AcceleratorStaticProfile {
                kind: AcceleratorKind::Cuda,
                name,
                vendor: Some("NVIDIA".to_string()),
                memory_bytes,
                unified_memory: false,
                confidence: DetectionConfidence::Medium,
            });
        }
    }

    if accelerators.is_empty() && run_command("rocm-smi", &["--showproductname"]).is_some() {
        accelerators.push(AcceleratorStaticProfile {
            kind: AcceleratorKind::Rocm,
            name: "AMD ROCm GPU".to_string(),
            vendor: Some("AMD".to_string()),
            memory_bytes: None,
            unified_memory: false,
            confidence: DetectionConfidence::Low,
        });
    }

    if accelerators.is_empty() {
        warnings.push(HardwareProfileWarning::new(
            "accelerator_gpu_not_detected",
            "no CUDA or ROCm accelerator was detected",
        ));
        accelerators.push(cpu_accelerator());
    }

    accelerators
}

fn find_cpuinfo_value(cpuinfo: &str, key: &str) -> Option<String> {
    cpuinfo.lines().find_map(|line| {
        let (left, right) = line.split_once(':')?;
        (left.trim() == key).then(|| right.trim().to_string())
    })
}

fn find_meminfo_kb(meminfo: &str, key: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        if !line.starts_with(key) {
            return None;
        }
        line.split_whitespace().nth(1)?.parse::<u64>().ok()
    })
}

fn bytes_to_gb_floor(bytes: u64) -> u64 {
    bytes / 1_073_741_824
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_meminfo_parser_reads_total_and_available() {
        let input = "MemTotal:       16384000 kB\nMemAvailable:    8192000 kB\n";

        assert_eq!(find_meminfo_kb(input, "MemTotal:"), Some(16_384_000));
        assert_eq!(find_meminfo_kb(input, "MemAvailable:"), Some(8_192_000));
    }
}
