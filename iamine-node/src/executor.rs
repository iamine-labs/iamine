use crate::task_protocol::TaskResponse;
use sha2::{Digest, Sha256};

#[allow(dead_code)]
pub struct TaskExecutor;

#[allow(dead_code)]
impl TaskExecutor {
    pub fn execute_task(task_id: String, task_type: String, data: String) -> TaskResponse {
        let result = match task_type.as_str() {
            "reverse_string" => Ok(data.chars().rev().collect::<String>()),
            "compute_hash" => {
                let mut hasher = Sha256::new();
                hasher.update(data.as_bytes());
                Ok(format!("{:x}", hasher.finalize()))
            }
            "validate_challenge" => {
                // data format: "input:expected_hash"
                let parts: Vec<&str> = data.splitn(2, ':').collect();
                if parts.len() == 2 {
                    let mut hasher = Sha256::new();
                    hasher.update(parts[0].as_bytes());
                    let computed = format!("{:x}", hasher.finalize());
                    if computed == parts[1] {
                        Ok(computed)
                    } else {
                        Err(format!("Hash mismatch: {} != {}", computed, parts[1]))
                    }
                } else {
                    Err("Invalid data format".to_string())
                }
            }
            _ => Err(format!("Unknown task type: {}", task_type)),
        };

        match result {
            Ok(output) => {
                println!("✅ Tarea {} completada: {}", task_id, output);
                TaskResponse::legacy(task_id, output, true)
            }
            Err(e) => {
                println!("❌ Error en tarea {}: {}", task_id, e);
                TaskResponse::legacy(task_id, e, false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TaskExecutor;

    #[test]
    fn test_task_execution() {
        let response = TaskExecutor::execute_task(
            "task-1".to_string(),
            "reverse_string".to_string(),
            "iamine".to_string(),
        );
        assert!(response.success);
        assert_eq!(response.result, "enimai");
    }
}
