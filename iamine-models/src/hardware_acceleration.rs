use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AcceleratorType {
    CPU,
    Metal, // Apple Silicon
    CUDA,  // NVIDIA
    ROCm,  // AMD
    Vulkan,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareAcceleration {
    pub accelerator: AcceleratorType,
    pub device_name: String,
    pub vram_gb: u64,
    pub cpu_cores: usize,
    pub cpu_features: Vec<String>, // AVX2, AVX512, etc.
    pub recommended_threads: usize,
}

impl HardwareAcceleration {
    pub fn detect() -> Self {
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let recommended_threads = (cpu_cores / 2).max(1);

        // Detectar CPU features
        let cpu_features = Self::detect_cpu_features();

        // Detectar GPU
        let (accelerator, device_name, vram_gb) = Self::detect_gpu();

        let hw = Self {
            accelerator,
            device_name,
            vram_gb,
            cpu_cores,
            cpu_features,
            recommended_threads,
        };

        hw.display();
        hw
    }

    fn detect_gpu() -> (AcceleratorType, String, u64) {
        // 1️⃣ Apple Metal
        #[cfg(target_os = "macos")]
        {
            if let Ok(out) = Command::new("system_profiler")
                .arg("SPDisplaysDataType")
                .output()
            {
                let s = String::from_utf8_lossy(&out.stdout);
                if s.contains("Metal") || s.contains("Apple M") {
                    let vram = Self::parse_vram_macos(&s);
                    return (AcceleratorType::Metal, "Apple Metal".to_string(), vram);
                }
            }
        }

        // 2️⃣ NVIDIA CUDA
        if let Ok(out) = Command::new("nvidia-smi")
            .args(["--query-gpu=name,memory.total", "--format=csv,noheader"])
            .output()
        {
            if out.status.success() {
                let s = String::from_utf8_lossy(&out.stdout);
                let parts: Vec<&str> = s.trim().splitn(2, ',').collect();
                let name = parts.first().unwrap_or(&"NVIDIA GPU").trim().to_string();
                let vram = parts
                    .get(1)
                    .and_then(|v| v.trim().replace(" MiB", "").parse::<u64>().ok())
                    .map(|mb| mb / 1024)
                    .unwrap_or(0);
                return (AcceleratorType::CUDA, name, vram);
            }
        }

        // 3️⃣ AMD ROCm
        if let Ok(out) = Command::new("rocm-smi").arg("--showproductname").output() {
            if out.status.success() {
                return (AcceleratorType::ROCm, "AMD ROCm GPU".to_string(), 0);
            }
        }

        (AcceleratorType::CPU, "CPU only".to_string(), 0)
    }

    fn detect_cpu_features() -> Vec<String> {
        let mut features = vec![];

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                features.push("AVX2".to_string());
            }
            if is_x86_feature_detected!("avx512f") {
                features.push("AVX512".to_string());
            }
            if is_x86_feature_detected!("fma") {
                features.push("FMA".to_string());
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            features.push("NEON".to_string());
            features.push("ARM64".to_string());
        }

        features
    }

    #[cfg(target_os = "macos")]
    fn parse_vram_macos(s: &str) -> u64 {
        // Apple unified memory — usar RAM disponible / 2 como VRAM efectiva
        for line in s.lines() {
            if line.contains("VRAM") || line.contains("vram") {
                if let Some(num) = line.split_whitespace().find(|w| w.parse::<u64>().is_ok()) {
                    return num.parse::<u64>().unwrap_or(0) / 1024;
                }
            }
        }
        0 // Unified memory — se reporta como 0 VRAM dedicada
    }

    pub fn display(&self) {
        println!("🔧 Hardware Acceleration:");
        println!("   Accelerator: {:?}", self.accelerator);
        println!("   Device:      {}", self.device_name);
        println!("   CPU Cores:   {}", self.cpu_cores);
        println!("   Threads:     {}", self.recommended_threads);
        if !self.cpu_features.is_empty() {
            println!("   CPU Feats:   {}", self.cpu_features.join(", "));
        }
        if self.vram_gb > 0 {
            println!("   VRAM:        {} GB", self.vram_gb);
        }
    }

    pub fn supports_metal(&self) -> bool {
        self.accelerator == AcceleratorType::Metal
    }

    pub fn supports_cuda(&self) -> bool {
        self.accelerator == AcceleratorType::CUDA
    }

    /// Parámetros óptimos para llama.cpp
    pub fn llama_params(&self) -> LlamaParams {
        let n_gpu_layers = match &self.accelerator {
            AcceleratorType::Metal => 99, // Todas las capas en GPU Metal
            AcceleratorType::CUDA => 99,
            AcceleratorType::ROCm => 32,
            _ => 0, // Solo CPU
        };

        LlamaParams {
            n_threads: self.recommended_threads as i32,
            n_gpu_layers,
            use_mmap: true,
            use_mlock: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LlamaParams {
    pub n_threads: i32,
    pub n_gpu_layers: i32,
    pub use_mmap: bool,
    pub use_mlock: bool,
}
