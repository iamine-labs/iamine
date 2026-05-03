use crate::backend_runtime::InferenceBackendState;
use iamine_models::{HardwareAcceleration, ModelStorage, StorageConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub cpu_cores: usize,
    pub ram_gb: u64,
    pub gpu_available: bool,
    pub disk_available_gb: u64,
    pub supported_tasks: Vec<String>,
    pub avg_latency_ms: f64,
    pub inference_backend: String,
    pub real_inference_available: bool,
    pub mock_inference_enabled: bool,
}

impl WorkerCapabilities {
    pub fn detect(backend_state: &InferenceBackendState) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        let hw = HardwareAcceleration::detect();
        let storage = ModelStorage::new();
        let cfg = StorageConfig::load();
        let used_gb = storage.total_size_bytes() / 1_073_741_824;
        let disk_available_gb = cfg.max_storage_gb.saturating_sub(used_gb);

        let mut supported_tasks = vec![
            "reverse_string".to_string(),
            "compute_hash".to_string(),
            "validate_challenge".to_string(),
        ];
        if backend_state.should_advertise_inference_capabilities() {
            supported_tasks.push("inference".to_string());
        }

        Self {
            cpu_cores: cores,
            ram_gb: sysinfo_ram_gb().max(2),
            gpu_available: !matches!(hw.accelerator, iamine_models::AcceleratorType::CPU),
            disk_available_gb,
            supported_tasks,
            avg_latency_ms: 0.0,
            inference_backend: backend_state.configured_backend.as_str().to_string(),
            real_inference_available: backend_state.real_backend_available,
            mock_inference_enabled: backend_state.mock_enabled(),
        }
    }

    /// Verifica si este worker puede ejecutar el tipo de tarea
    pub fn supports(&self, task_type: &str) -> bool {
        self.supported_tasks.iter().any(|t| t == task_type)
    }
}

fn sysinfo_ram_gb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("MemTotal:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    return kb / 1_048_576;
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("hw.memsize")
            .output()
        {
            let s = String::from_utf8_lossy(&out.stdout);
            if let Ok(bytes) = s.trim().parse::<u64>() {
                return bytes / 1_073_741_824;
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
        // Prefer PowerShell CIM (works on Win11; avoids extra crates)
        if let Ok(out) = std::process::Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory",
            ])
            .output()
        {
            let s = String::from_utf8_lossy(&out.stdout);
            if let Ok(bytes) = s.trim().parse::<u64>() {
                return bytes / 1_073_741_824;
            }
        }
    }
    8
}
