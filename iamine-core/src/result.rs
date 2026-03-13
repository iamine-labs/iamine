use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Resultado de una tarea ejecutada por un worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: String,
    pub worker_id: String,
    pub worker_pubkey: Option<String>,
    pub output: String,
    pub output_hash: [u8; 32],
    pub success: bool,
    pub error: Option<String>,
    pub execution_ms: u64,
    pub attempt: u32,
}

impl TaskResult {
    pub fn success(task_id: String, worker_id: String, output: String, execution_ms: u64) -> Self {
        let output_hash = Self::hash(&output);
        Self {
            task_id,
            worker_id,
            worker_pubkey: None,
            output,
            output_hash,
            success: true,
            error: None,
            execution_ms,
            attempt: 0,
        }
    }

    pub fn failure(task_id: String, worker_id: String, reason: String) -> Self {
        Self {
            task_id,
            worker_id,
            worker_pubkey: None,
            output: String::new(),
            output_hash: [0u8; 32],
            success: false,
            error: Some(reason),
            execution_ms: 0,
            attempt: 0,
        }
    }

    pub fn with_pubkey(mut self, pubkey: String) -> Self {
        self.worker_pubkey = Some(pubkey);
        self
    }

    pub fn verify(&self, expected_hash: &[u8; 32]) -> bool {
        &self.output_hash == expected_hash
    }

    fn hash(data: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        hasher.finalize().into()
    }
}
