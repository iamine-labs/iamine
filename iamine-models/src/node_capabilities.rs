use crate::hardware_acceleration::HardwareAcceleration;
use crate::model_storage::ModelStorage;
use crate::storage_config::StorageConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub node_id: String,
    pub cpu_cores: u32,
    pub ram_gb: u32,
    pub gpu_type: Option<String>,
    pub npu_type: Option<String>,
    pub storage_available_gb: u32,
    pub worker_slots: u32,
    pub supported_models: Vec<String>,
    pub cpu_features: Vec<String>,
    pub accelerator: String,
}

impl NodeCapabilities {
    /// Detectar automáticamente las capacidades del nodo
    pub fn detect(node_id: &str) -> Self {
        let hw = HardwareAcceleration::detect();
        let storage = ModelStorage::new();
        let config = StorageConfig::load();

        let local_models = storage.list_local_models();
        let used_bytes = storage.total_size_bytes();
        let used_gb = (used_bytes / 1_073_741_824) as u32;
        let max_gb = config.max_storage_gb as u32;
        let available_gb = max_gb.saturating_sub(used_gb);

        let gpu_type = match hw.accelerator {
            crate::hardware_acceleration::AcceleratorType::Metal => Some("Metal".to_string()),
            crate::hardware_acceleration::AcceleratorType::CUDA => Some("CUDA".to_string()),
            crate::hardware_acceleration::AcceleratorType::ROCm => Some("ROCm".to_string()),
            _ => None,
        };

        let worker_slots = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(4);

        Self {
            node_id: node_id.to_string(),
            cpu_cores: hw.cpu_cores as u32,
            ram_gb: (sysinfo_ram_gb()).max(2),
            gpu_type,
            npu_type: None,
            storage_available_gb: available_gb,
            worker_slots,
            supported_models: local_models,
            cpu_features: hw.cpu_features.clone(),
            accelerator: format!("{:?}", hw.accelerator),
        }
    }

    /// Serializar para gossipsub
    pub fn to_gossip_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "NodeCapabilities",
            "node_id": self.node_id,
            "cpu_cores": self.cpu_cores,
            "ram_gb": self.ram_gb,
            "gpu_type": self.gpu_type,
            "npu_type": self.npu_type,
            "storage_available_gb": self.storage_available_gb,
            "worker_slots": self.worker_slots,
            "supported_models": self.supported_models,
            "cpu_features": self.cpu_features,
            "accelerator": self.accelerator,
        })
    }

    pub fn display(&self) {
        println!("🖥️  Node Capabilities:");
        println!(
            "   Node ID:     {}",
            &self.node_id[..12.min(self.node_id.len())]
        );
        println!(
            "   CPU:         {} cores [{}]",
            self.cpu_cores,
            self.cpu_features.join(", ")
        );
        println!("   RAM:         {} GB", self.ram_gb);
        println!(
            "   GPU:         {}",
            self.gpu_type.as_deref().unwrap_or("ninguna")
        );
        println!("   Accelerator: {}", self.accelerator);
        println!(
            "   Storage:     {} GB disponibles",
            self.storage_available_gb
        );
        println!("   Slots:       {}", self.worker_slots);
        println!(
            "   Modelos:     {}",
            if self.supported_models.is_empty() {
                "(ninguno)".to_string()
            } else {
                self.supported_models.join(", ")
            }
        );
    }

    pub fn has_model(&self, model_id: &str) -> bool {
        self.supported_models.iter().any(|m| m == model_id)
    }
}

/// Detectar RAM del sistema (heurística rápida sin dependencias extra)
fn sysinfo_ram_gb() -> u32 {
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
                    return (kb / 1_048_576) as u32;
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
                return (bytes / 1_073_741_824) as u32;
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
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
                return (bytes / 1_073_741_824) as u32;
            }
        }
    }
    8 // fallback
}
