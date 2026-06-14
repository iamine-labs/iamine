use crate::platform::{cpu_accelerator, run_command};
use crate::schema::{
    AcceleratorKind, AcceleratorStaticProfile, CpuFeatureProfile, CpuStaticProfile,
    DetectionConfidence, HardwareProfileWarning, MemoryStaticProfile,
};

pub(crate) fn detect_cpu_profile(logical_cores: u32) -> CpuStaticProfile {
    let hardware_overview = run_command("system_profiler", &["SPHardwareDataType"]);
    CpuStaticProfile {
        architecture: std::env::consts::ARCH.to_string(),
        vendor: Some("Apple".to_string()).filter(|_| cfg!(target_arch = "aarch64")),
        brand: sysctl_value("machdep.cpu.brand_string")
            .or_else(|| parse_hardware_value(hardware_overview.as_deref().unwrap_or(""), "Chip"))
            .or_else(|| sysctl_value("hw.model")),
        physical_cores: sysctl_value("hw.physicalcpu")
            .and_then(|value| value.parse().ok())
            .or_else(|| parse_total_cores(hardware_overview.as_deref().unwrap_or(""))),
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
    let total_bytes = sysctl_value("hw.memsize")
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| {
            run_command("system_profiler", &["SPHardwareDataType"])
                .as_deref()
                .and_then(parse_memory_bytes)
        })
        .unwrap_or(0);

    if total_bytes == 0 {
        warnings.push(HardwareProfileWarning::new(
            "memory_total_unavailable",
            "hw.memsize was not available from sysctl",
        ));
    }

    MemoryStaticProfile {
        total_bytes,
        available_bytes: detect_available_memory_bytes(),
        total_gb: total_bytes / 1_073_741_824,
        unified_memory: cfg!(target_arch = "aarch64"),
        confidence: if total_bytes > 0 {
            DetectionConfidence::High
        } else {
            DetectionConfidence::Unknown
        },
    }
}

pub(crate) fn detect_available_memory_bytes() -> Option<u64> {
    let page_size = sysctl_value("hw.pagesize")?.parse::<u64>().ok()?;
    let vm_stat = run_command("vm_stat", &[])?;
    let free_pages = vm_stat.lines().find_map(|line| {
        if !line.trim_start().starts_with("Pages free:") {
            return None;
        }
        line.split_whitespace()
            .last()
            .map(|value| value.trim_end_matches('.'))
            .and_then(|value| value.parse::<u64>().ok())
    })?;
    Some(free_pages * page_size)
}

pub(crate) fn detect_accelerators(
    memory: &MemoryStaticProfile,
    warnings: &mut Vec<HardwareProfileWarning>,
) -> Vec<AcceleratorStaticProfile> {
    let Some(stdout) = run_command("system_profiler", &["SPDisplaysDataType"]) else {
        warnings.push(HardwareProfileWarning::new(
            "accelerator_detection_unavailable",
            "system_profiler SPDisplaysDataType did not complete",
        ));
        return vec![cpu_accelerator()];
    };

    let name = parse_display_chipset(&stdout).unwrap_or_else(|| "Apple Metal".to_string());
    let has_metal = stdout.contains("Metal") || name.contains("Apple");

    if has_metal {
        vec![AcceleratorStaticProfile {
            kind: AcceleratorKind::Metal,
            name,
            vendor: Some("Apple".to_string()),
            memory_bytes: parse_vram_bytes(&stdout).or(Some(memory.total_bytes)),
            unified_memory: true,
            confidence: DetectionConfidence::Medium,
        }]
    } else {
        warnings.push(HardwareProfileWarning::new(
            "accelerator_gpu_not_detected",
            "no Metal accelerator was detected",
        ));
        vec![cpu_accelerator()]
    }
}

fn sysctl_value(name: &str) -> Option<String> {
    run_command("sysctl", &["-n", name]).map(|value| value.trim().to_string())
}

fn parse_display_chipset(output: &str) -> Option<String> {
    output.lines().find_map(|line| {
        let trimmed = line.trim();
        let (_, value) = trimmed.split_once("Chipset Model:")?;
        Some(value.trim().to_string())
    })
}

fn parse_hardware_value(output: &str, key: &str) -> Option<String> {
    output.lines().find_map(|line| {
        let trimmed = line.trim();
        let (left, right) = trimmed.split_once(':')?;
        (left == key).then(|| right.trim().to_string())
    })
}

fn parse_memory_bytes(output: &str) -> Option<u64> {
    let raw = parse_hardware_value(output, "Memory")?;
    let mut parts = raw.split_whitespace();
    let amount = parts.next()?.parse::<u64>().ok()?;
    let unit = parts.next().unwrap_or("GB").to_ascii_lowercase();
    if unit.starts_with("gb") || unit.starts_with("gib") {
        Some(amount * 1_073_741_824)
    } else if unit.starts_with("mb") || unit.starts_with("mib") {
        Some(amount * 1024 * 1024)
    } else {
        None
    }
}

fn parse_total_cores(output: &str) -> Option<u32> {
    let raw = parse_hardware_value(output, "Total Number of Cores")?;
    raw.split_whitespace().next()?.parse::<u32>().ok()
}

fn parse_vram_bytes(output: &str) -> Option<u64> {
    output.lines().find_map(|line| {
        let trimmed = line.trim();
        if !trimmed.starts_with("VRAM") {
            return None;
        }
        let digits = trimmed
            .split_whitespace()
            .find_map(|token| token.replace(',', "").parse::<u64>().ok())?;
        if trimmed.contains("MB") || trimmed.contains("MiB") {
            Some(digits * 1024 * 1024)
        } else if trimmed.contains("GB") || trimmed.contains("GiB") {
            Some(digits * 1_073_741_824)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macos_display_parser_extracts_chipset_without_paths() {
        let output =
            "Graphics/Displays:\n    Chipset Model: Apple M3\n    Metal Support: Metal 3\n";

        assert_eq!(parse_display_chipset(output), Some("Apple M3".to_string()));
    }

    #[test]
    fn macos_vram_parser_handles_gb() {
        let output = "VRAM (Total): 8 GB\n";

        assert_eq!(parse_vram_bytes(output), Some(8 * 1_073_741_824));
    }

    #[test]
    fn macos_hardware_parser_extracts_allowed_fields_only() {
        let output =
            "Chip: Apple M1\nMemory: 8 GB\nSerial Number (system): SECRET\nHardware UUID: SECRET\n";

        assert_eq!(
            parse_hardware_value(output, "Chip"),
            Some("Apple M1".to_string())
        );
        assert_eq!(parse_memory_bytes(output), Some(8 * 1_073_741_824));
        assert_eq!(parse_total_cores(output), None);
    }

    #[test]
    fn macos_hardware_parser_extracts_total_cores() {
        let output = "Total Number of Cores: 8 (4 Performance and 4 Efficiency)\n";

        assert_eq!(parse_total_cores(output), Some(8));
    }
}
