use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub prompt: String,
    pub model: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: String,
    pub output: String,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskMessage {
    Submit(Task),
    Result(TaskResult),
}

impl Task {
    pub fn new(id: impl Into<String>, prompt: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            prompt: prompt.into(),
            model: model.into(),
            created_at: now_ms(),
        }
    }
}

impl TaskResult {
    pub fn success(task_id: impl Into<String>, output: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            output: output.into(),
            success: true,
        }
    }

    pub fn failure(task_id: impl Into<String>, output: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            output: output.into(),
            success: false,
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::{Task, TaskMessage, TaskResult};

    #[test]
    fn test_task_creation() {
        let task = Task::new("task-1", "What is 2+2?", "llama3-3b");
        assert_eq!(task.id, "task-1");
        assert_eq!(task.prompt, "What is 2+2?");
        assert_eq!(task.model, "llama3-3b");
        assert!(task.created_at > 0);
    }

    #[test]
    fn test_task_message_roundtrip() {
        let task = Task::new("task-2", "Explain gravity", "llama3-3b");
        let encoded = serde_json::to_string(&TaskMessage::Submit(task.clone())).unwrap();
        let decoded: TaskMessage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, TaskMessage::Submit(task));
    }

    #[test]
    fn test_task_result_constructors() {
        let ok = TaskResult::success("task-3", "4");
        let err = TaskResult::failure("task-3", "model unavailable");
        assert!(ok.success);
        assert!(!err.success);
    }
}
