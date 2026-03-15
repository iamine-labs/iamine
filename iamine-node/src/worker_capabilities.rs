use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub cpu_cores: usize,
    pub ram_gb: u64,
    pub gpu_available: bool,
    pub supported_tasks: Vec<String>,
    pub avg_latency_ms: f64,
}

impl WorkerCapabilities {
    pub fn detect() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        Self {
            cpu_cores: cores,
            ram_gb: 8, // default — sysinfo opcional
            gpu_available: false,
            supported_tasks: vec![
                "reverse_string".to_string(),
                "compute_hash".to_string(),
                "validate_challenge".to_string(),
            ],
            avg_latency_ms: 0.0,
        }
    }

    /// Verifica si este worker puede ejecutar el tipo de tarea
    pub fn supports(&self, task_type: &str) -> bool {
        self.supported_tasks.iter().any(|t| t == task_type)
    }
}
