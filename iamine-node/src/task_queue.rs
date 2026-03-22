use iamine_core::{NodeReputation, TaskResult};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

use crate::executor::TaskExecutor;

#[derive(Debug, Clone)]
pub struct QueuedTask {
    pub task_id: String,
    pub task_type: String,
    pub data: String,
    pub attempt: u32,
    pub max_retries: u32,
    pub timeout: Duration,
    pub enqueued_at: Instant,
}

impl QueuedTask {
    pub fn new(task_id: String, task_type: String, data: String) -> Self {
        let timeout = match task_type.as_str() {
            "reverse_string" => Duration::from_secs(5),
            "compute_hash" => Duration::from_secs(10),
            _ => Duration::from_secs(30),
        };
        Self {
            task_id,
            task_type,
            data,
            attempt: 0,
            max_retries: 3,
            timeout,
            enqueued_at: Instant::now(),
        }
    }

    pub fn retry(mut self) -> Self {
        self.attempt += 1;
        self.enqueued_at = Instant::now();
        self
    }

    pub fn can_retry(&self) -> bool {
        self.attempt < self.max_retries
    }
}

#[derive(Debug, Clone)]
pub struct TaskOutcome {
    pub task_id: String,
    pub result: Option<TaskResult>,
    pub attempts: u32,
    pub status: OutcomeStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutcomeStatus {
    Success,
    Failed,
    TimedOut,
    MaxRetriesExceeded,
}

/// Task queue con retries, timeouts y reputación
pub struct TaskQueue {
    tx: mpsc::Sender<QueuedTask>,
    _outcome_tx: mpsc::Sender<TaskOutcome>,
    pub outcome_rx: Arc<Mutex<mpsc::Receiver<TaskOutcome>>>,
    reputation: Arc<Mutex<NodeReputation>>,
}

impl TaskQueue {
    pub fn new(worker_id: String) -> Self {
        let (tx, rx) = mpsc::channel::<QueuedTask>(256);
        let (outcome_tx, outcome_rx) = mpsc::channel::<TaskOutcome>(256);
        let reputation = Arc::new(Mutex::new(NodeReputation::new(worker_id)));

        let rep = Arc::clone(&reputation);
        let otx = outcome_tx.clone();
        tokio::spawn(Self::process_loop(rx, otx, rep));

        Self {
            tx,
            _outcome_tx: outcome_tx,
            outcome_rx: Arc::new(Mutex::new(outcome_rx)),
            reputation,
        }
    }

    /// Encolar tarea
    pub async fn push(
        &self,
        task_id: String,
        task_type: String,
        data: String,
    ) -> Result<(), String> {
        let task = QueuedTask::new(task_id, task_type, data);
        self.tx.send(task).await.map_err(|e| e.to_string())
    }

    /// Reputación actual
    pub async fn reputation(&self) -> NodeReputation {
        self.reputation.lock().await.clone()
    }

    /// Loop principal de procesamiento
    async fn process_loop(
        mut rx: mpsc::Receiver<QueuedTask>,
        outcome_tx: mpsc::Sender<TaskOutcome>,
        reputation: Arc<Mutex<NodeReputation>>,
    ) {
        let (internal_tx, mut internal_rx) = mpsc::channel::<QueuedTask>(256);

        let itx = internal_tx.clone();
        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                let _ = itx.send(task).await;
            }
        });

        while let Some(task) = internal_rx.recv().await {
            let attempt = task.attempt; // ← quitar task_id variable
            let timeout = task.timeout;
            let otx = outcome_tx.clone();
            let rep = Arc::clone(&reputation);
            let retry_tx = internal_tx.clone();

            tokio::spawn(async move {
                println!(
                    "🔄 [Queue] [{}/{}] {} → '{}'",
                    attempt + 1,
                    task.max_retries + 1,
                    task.task_id,
                    task.task_type
                );

                let t_id = task.task_id.clone();
                let t_type = task.task_type.clone();
                let t_data = task.data.clone();

                // Ejecutar con timeout
                let exec_result = tokio::time::timeout(
                    timeout,
                    tokio::task::spawn_blocking(move || {
                        TaskExecutor::execute_task(t_id, t_type, t_data)
                    }),
                )
                .await;

                match exec_result {
                    // ✅ Completada a tiempo
                    Ok(Ok(response)) if response.success => {
                        println!(
                            "✅ [Queue] [{}] completada (intento {}): {}",
                            task.task_id,
                            attempt + 1,
                            response.result
                        );

                        let elapsed = task.enqueued_at.elapsed().as_millis() as u64;
                        let result = TaskResult::success(
                            task.task_id.clone(),
                            "queue_worker".to_string(),
                            response.result,
                            elapsed,
                        );

                        rep.lock().await.task_success(0, elapsed);

                        let _ = otx
                            .send(TaskOutcome {
                                task_id: task.task_id,
                                result: Some(result),
                                attempts: attempt + 1,
                                status: OutcomeStatus::Success,
                            })
                            .await;
                    }

                    // ❌ Falló — retry si puede
                    Ok(Ok(response)) => {
                        println!(
                            "❌ [Queue] [{}] falló (intento {}): {}",
                            task.task_id,
                            attempt + 1,
                            response.result
                        );

                        if task.can_retry() {
                            println!(
                                "🔁 [Queue] [{}] reintentando ({}/{})",
                                task.task_id,
                                attempt + 1,
                                task.max_retries
                            );
                            let _ = retry_tx.send(task.retry()).await;
                        } else {
                            println!("💀 [Queue] [{}] max reintentos alcanzados", task.task_id);
                            rep.lock().await.task_failed();
                            let _ = otx
                                .send(TaskOutcome {
                                    task_id: task.task_id,
                                    result: None,
                                    attempts: attempt + 1,
                                    status: OutcomeStatus::MaxRetriesExceeded,
                                })
                                .await;
                        }
                    }

                    // ⏱️ Timeout
                    Err(_) => {
                        println!(
                            "⏱️ [Queue] [{}] timeout después de {}s (intento {})",
                            task.task_id,
                            timeout.as_secs(),
                            attempt + 1
                        );

                        if task.can_retry() {
                            println!("🔁 [Queue] [{}] reintentando tras timeout", task.task_id);
                            let _ = retry_tx.send(task.retry()).await;
                        } else {
                            rep.lock().await.task_timed_out();
                            let _ = otx
                                .send(TaskOutcome {
                                    task_id: task.task_id,
                                    result: None,
                                    attempts: attempt + 1,
                                    status: OutcomeStatus::TimedOut,
                                })
                                .await;
                        }
                    }

                    // 💥 Panic en spawn_blocking
                    Ok(Err(e)) => {
                        eprintln!("💥 [Queue] Panic en [{}]: {:?}", task.task_id, e);
                        rep.lock().await.task_failed();
                        let _ = otx
                            .send(TaskOutcome {
                                task_id: task.task_id,
                                result: None,
                                attempts: attempt + 1,
                                status: OutcomeStatus::Failed,
                            })
                            .await;
                    }
                }
            });
        }
    }
}
