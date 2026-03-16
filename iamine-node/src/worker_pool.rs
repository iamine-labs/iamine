use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Semaphore};
use iamine_core::TaskResult;  // ← quitar TaskType

use crate::executor::TaskExecutor;

/// Mensaje interno del pool
#[derive(Debug)]
pub struct PoolTask {
    pub task_id: String,
    pub task_type: String,
    pub data: String,
    pub result_tx: tokio::sync::oneshot::Sender<TaskResult>,
}

/// Worker pool — ejecuta N tareas en paralelo
pub struct WorkerPool {
    /// Limita concurrencia a N slots (1 por core)
    semaphore: Arc<Semaphore>,
    /// Canal para enviar tareas al pool
    task_tx: mpsc::Sender<PoolTask>,
    pub max_concurrent: usize,
}

impl WorkerPool {
    pub fn new(max_concurrent: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let (task_tx, task_rx) = mpsc::channel::<PoolTask>(max_concurrent * 4);

        let sem = Arc::clone(&semaphore);
        tokio::spawn(Self::run_loop(task_rx, sem));

        println!("⚡ WorkerPool iniciado: {} slots paralelos", max_concurrent);

        Self { semaphore, task_tx, max_concurrent }
    }

    /// Detectar cores y crear pool automáticamente
    pub fn auto() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get()).unwrap_or(2);
        Self::with_slots(cores)
    }

    pub fn with_slots(slots: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(slots));
        let (task_tx, task_rx) = mpsc::channel::<PoolTask>(slots * 4);

        let sem = Arc::clone(&semaphore);
        tokio::spawn(Self::run_loop(task_rx, sem));

        println!("⚡ WorkerPool iniciado: {} slots paralelos", slots);

        Self { semaphore, task_tx, max_concurrent: slots }
    }

    /// Enviar tarea al pool — retorna resultado via oneshot
    pub async fn submit(&self, task_id: String, task_type: String, data: String)
        -> Result<TaskResult, String>
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        self.task_tx.send(PoolTask { task_id, task_type, data, result_tx })
            .await
            .map_err(|e| format!("Pool channel cerrado: {}", e))?;

        result_rx.await.map_err(|e| format!("Worker falló: {}", e))
    }

    /// Slots disponibles
    pub fn available_slots(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Slots ocupados
    pub fn active_tasks(&self) -> usize {
        self.max_concurrent - self.available_slots()
    }

    /// Loop interno que procesa tareas con concurrencia controlada
    async fn run_loop(mut rx: mpsc::Receiver<PoolTask>, semaphore: Arc<Semaphore>) {
        while let Some(pool_task) = rx.recv().await {
            let sem = Arc::clone(&semaphore);

            tokio::spawn(async move {
                // Adquirir slot — bloquea si pool lleno
                let _permit = sem.acquire().await.expect("Semaphore cerrado");

                let start = Instant::now();
                println!("⚙️  [Pool] Ejecutando [{}] {} → '{}'",
                    pool_task.task_id, pool_task.task_type, pool_task.data);

                // Ejecutar en blocking thread para no bloquear tokio runtime
                let task_id = pool_task.task_id.clone();
                let task_type = pool_task.task_type.clone();
                let data = pool_task.data.clone();

                let result = tokio::task::spawn_blocking(move || {
                    TaskExecutor::execute_task(task_id, task_type, data)
                }).await;

                let elapsed = start.elapsed().as_millis() as u64;

                match result {
                    Ok(response) => {  // ← quitar mut
                        let task_result = if response.success {
                            println!("✅ [Pool] [{}] completada en {}ms: {}",
                                pool_task.task_id, elapsed, response.result);
                            TaskResult::success(
                                pool_task.task_id.clone(),
                                "local_worker".to_string(),
                                response.result,
                                elapsed,
                            )
                        } else {
                            println!("❌ [Pool] [{}] falló: {}", pool_task.task_id, response.result);
                            TaskResult::failure(
                                pool_task.task_id.clone(),
                                "local_worker".to_string(),
                                response.result,
                            )
                        };

                        let _ = pool_task.result_tx.send(task_result);
                    }
                    Err(e) => {
                        eprintln!("💥 [Pool] Panic en [{}]: {:?}", pool_task.task_id, e);
                        let _ = pool_task.result_tx.send(
                            TaskResult::failure(
                                pool_task.task_id,
                                "local_worker".to_string(),
                                format!("Task panicked: {:?}", e),
                            )
                        );
                    }
                }
                // _permit se libera aquí automáticamente
            });
        }
    }
}
