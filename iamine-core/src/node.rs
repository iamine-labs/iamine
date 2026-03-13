use serde::{Deserialize, Serialize};
use crate::task::TaskType;

/// Capacidades de un nodo worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub cores: usize,
    pub ram_gb: u64,
    pub supported_tasks: Vec<TaskType>,
    pub max_concurrent: usize,
}

impl NodeCapabilities {
    pub fn detect() -> Self {
        // Detectar hardware real
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        Self {
            cores,
            ram_gb: 8, // TODO: detectar real con sysinfo
            supported_tasks: vec![
                TaskType::ReverseString,
                TaskType::ComputeHash,
                TaskType::ValidateChallenge,
            ],
            max_concurrent: cores, // 1 tarea por core
        }
    }

    pub fn can_handle(&self, task_type: &TaskType) -> bool {
        self.supported_tasks.contains(task_type)
            && task_type.required_cores() <= self.cores
    }
}

/// Reputación de un nodo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReputation {
    pub peer_id: String,
    pub solana_pubkey: Option<String>,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub tasks_timed_out: u64,
    pub reputation_score: u32, // 0-100
    pub total_earned_lamports: u64,
    pub avg_execution_ms: f64,
}

impl NodeReputation {
    pub fn new(peer_id: String) -> Self {
        Self {
            peer_id,
            solana_pubkey: None,
            tasks_completed: 0,
            tasks_failed: 0,
            tasks_timed_out: 0,
            reputation_score: 100,
            total_earned_lamports: 0,
            avg_execution_ms: 0.0,
        }
    }

    pub fn task_success(&mut self, reward: u64, execution_ms: u64) {
        self.tasks_completed += 1;
        self.total_earned_lamports += reward;
        self.reputation_score = std::cmp::min(100, self.reputation_score + 1);

        // Media móvil de tiempo de ejecución
        let n = self.tasks_completed as f64;
        self.avg_execution_ms = (self.avg_execution_ms * (n - 1.0) + execution_ms as f64) / n;
    }

    pub fn task_failed(&mut self) {
        self.tasks_failed += 1;
        self.reputation_score = self.reputation_score.saturating_sub(5);
    }

    pub fn task_timed_out(&mut self) {
        self.tasks_timed_out += 1;
        self.reputation_score = self.reputation_score.saturating_sub(3);
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.tasks_completed + self.tasks_failed + self.tasks_timed_out;
        if total == 0 { return 1.0; }
        self.tasks_completed as f64 / total as f64
    }

    pub fn is_reliable(&self) -> bool {
        self.reputation_score >= 50 && self.success_rate() >= 0.8
    }
}
