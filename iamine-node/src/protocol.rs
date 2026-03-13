#[allow(dead_code)]
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IaMineMessage {
    // Nodo A → Nodo B: "Ejecuta esta tarea"
    TaskAssign {
        task_id: String,
        task_type: TaskType,
        data: String,
        reward: u64,
    },
    // Nodo B → Nodo A: "Aquí está el resultado"
    TaskResult {
        task_id: String,
        result: String,
        success: bool,
    },
    // Nodo A → Nodo B: "¿Estás vivo?"
    Ping,
    // Nodo B → Nodo A: "Sí, estoy aquí"
    Pong,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    ReverseString,
    ComputeHash,
    ValidateChallenge { expected_hash: String },
}

#[allow(dead_code)]
impl TaskType {
    pub fn execute(&self, data: &str) -> Result<String, String> {
        match self {
            TaskType::ReverseString => {
                Ok(data.chars().rev().collect())
            }
            TaskType::ComputeHash => {
                use sha2::{Sha256, Digest};
                let mut hasher = Sha256::new();
                hasher.update(data.as_bytes());
                let result = hasher.finalize();
                Ok(format!("{:x}", result))
            }
            TaskType::ValidateChallenge { expected_hash } => {
                use sha2::{Sha256, Digest};
                let mut hasher = Sha256::new();
                hasher.update(data.as_bytes());
                let result = hasher.finalize();
                let computed = format!("{:x}", result);
                
                if computed == *expected_hash {
                    Ok(computed)
                } else {
                    Err(format!("Hash mismatch: {} != {}", computed, expected_hash))
                }
            }
        }
    }
}
