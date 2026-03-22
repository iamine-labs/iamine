use iamine_network::{DistributedTask, DistributedTaskResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequest {
    pub task_id: String,
    pub task_type: String,
    pub data: String,
    pub distributed_task: Option<DistributedTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResponse {
    pub task_id: String,
    pub result: String,
    pub success: bool,
    pub distributed_result: Option<DistributedTaskResult>,
}

impl TaskRequest {
    pub fn legacy(task_id: String, task_type: String, data: String) -> Self {
        Self {
            task_id,
            task_type,
            data,
            distributed_task: None,
        }
    }

    pub fn distributed(task: DistributedTask) -> Self {
        Self {
            task_id: task.id.clone(),
            task_type: "distributed_inference".to_string(),
            data: task.prompt.clone(),
            distributed_task: Some(task),
        }
    }
}

impl TaskResponse {
    pub fn legacy(task_id: String, result: String, success: bool) -> Self {
        Self {
            task_id,
            result,
            success,
            distributed_result: None,
        }
    }

    pub fn distributed(result: DistributedTaskResult) -> Self {
        Self {
            task_id: result.task_id.clone(),
            result: result.output.clone(),
            success: result.success,
            distributed_result: Some(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskRequest, TaskResponse};
    use iamine_network::{DistributedTask, DistributedTaskResult};

    #[test]
    fn test_task_send_receive() {
        let task = DistributedTask::new("task-1", "2+2", "llama3-3b");
        let request = TaskRequest::distributed(task.clone());
        let encoded = serde_json::to_string(&request).unwrap();
        let decoded: TaskRequest = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.distributed_task, Some(task));
    }

    #[test]
    fn test_task_result_return() {
        let result = DistributedTaskResult::success("task-1", "4");
        let response = TaskResponse::distributed(result.clone());
        let encoded = serde_json::to_string(&response).unwrap();
        let decoded: TaskResponse = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.distributed_result, Some(result));
        assert!(decoded.success);
    }
}
