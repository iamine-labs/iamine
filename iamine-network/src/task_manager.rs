use crate::task::{Task, TaskResult};
use crate::task_state::TaskStatus;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskClaim {
    Started,
    InProgress,
    Finished(TaskResult),
}

#[derive(Clone, Default)]
pub struct TaskManager {
    tasks: Arc<RwLock<HashMap<String, Task>>>,
    statuses: Arc<RwLock<HashMap<String, TaskStatus>>>,
    results: Arc<RwLock<HashMap<String, TaskResult>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_task(&self, task: Task) {
        let task_id = task.id.clone();
        self.tasks.write().await.insert(task_id.clone(), task);
        self.statuses
            .write()
            .await
            .insert(task_id, TaskStatus::Pending);
    }

    pub async fn claim_task(&self, task: Task) -> TaskClaim {
        let task_id = task.id.clone();

        if let Some(status) = self.statuses.read().await.get(&task_id).cloned() {
            return match status {
                TaskStatus::Pending | TaskStatus::Running => TaskClaim::InProgress,
                TaskStatus::Completed | TaskStatus::Failed => {
                    if let Some(result) = self.results.read().await.get(&task_id).cloned() {
                        TaskClaim::Finished(result)
                    } else {
                        TaskClaim::InProgress
                    }
                }
            };
        }

        self.tasks.write().await.insert(task_id.clone(), task);
        self.statuses
            .write()
            .await
            .insert(task_id, TaskStatus::Running);
        TaskClaim::Started
    }

    pub async fn mark_running(&self, task_id: &str) {
        self.statuses
            .write()
            .await
            .insert(task_id.to_string(), TaskStatus::Running);
    }

    pub async fn complete(&self, result: TaskResult) {
        let task_id = result.task_id.clone();
        self.results.write().await.insert(task_id.clone(), result);
        self.statuses
            .write()
            .await
            .insert(task_id, TaskStatus::Completed);
    }

    pub async fn fail(&self, task_id: &str, output: impl Into<String>) {
        let task_id = task_id.to_string();
        self.results.write().await.insert(
            task_id.clone(),
            TaskResult::failure(task_id.clone(), output.into()),
        );
        self.statuses
            .write()
            .await
            .insert(task_id, TaskStatus::Failed);
    }

    pub async fn status(&self, task_id: &str) -> Option<TaskStatus> {
        self.statuses.read().await.get(task_id).cloned()
    }

    pub async fn result(&self, task_id: &str) -> Option<TaskResult> {
        self.results.read().await.get(task_id).cloned()
    }

    pub async fn active_tasks(&self) -> usize {
        self.statuses
            .read()
            .await
            .values()
            .filter(|status| matches!(status, TaskStatus::Pending | TaskStatus::Running))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskClaim, TaskManager};
    use crate::task::{Task, TaskResult};
    use crate::task_state::TaskStatus;

    #[tokio::test]
    async fn test_task_lifecycle() {
        let manager = TaskManager::new();
        let task = Task::new("task-1", "2+2", "llama3-3b");
        manager.register_task(task.clone()).await;
        assert_eq!(manager.status(&task.id).await, Some(TaskStatus::Pending));

        manager.mark_running(&task.id).await;
        assert_eq!(manager.status(&task.id).await, Some(TaskStatus::Running));

        manager
            .complete(TaskResult::success(task.id.clone(), "4"))
            .await;
        assert_eq!(manager.status(&task.id).await, Some(TaskStatus::Completed));
        assert_eq!(
            manager.result(&task.id).await,
            Some(TaskResult::success(task.id, "4"))
        );
    }

    #[tokio::test]
    async fn test_task_failure() {
        let manager = TaskManager::new();
        let task = Task::new("task-2", "Explain gravity", "llama3-3b");
        manager.register_task(task.clone()).await;
        manager.fail(&task.id, "worker unavailable").await;
        assert_eq!(manager.status(&task.id).await, Some(TaskStatus::Failed));
    }

    #[tokio::test]
    async fn test_no_duplicate_task_execution() {
        let manager = TaskManager::new();
        let task = Task::new("task-3", "Explain gravity", "llama3-3b");

        assert_eq!(manager.claim_task(task.clone()).await, TaskClaim::Started);
        assert_eq!(manager.claim_task(task).await, TaskClaim::InProgress);
    }
}
