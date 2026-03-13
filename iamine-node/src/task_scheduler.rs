use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use iamine_core::node::NodeCapabilities;

/// Bid de un worker para ejecutar una tarea
#[derive(Debug, Clone)]
pub struct WorkerBid {
    pub worker_id: String,
    pub reputation_score: u32,
    pub available_slots: usize,
    pub estimated_ms: u64,
    pub received_at: Instant,
}

/// Estado de una tarea en el scheduler
#[derive(Debug, Clone, PartialEq)]
pub enum SchedulerTaskStatus {
    /// Esperando bids de workers
    CollectingBids { deadline: Instant },
    /// Asignada a un worker
    Assigned { worker_id: String, assigned_at: Instant },
    /// Completada
    Completed { worker_id: String },
    /// Falló — todos los workers rechazaron
    Failed,
}

#[derive(Debug, Clone)]
pub struct SchedulerTask {
    pub task_id: String,
    pub task_type: String,
    pub bids: Vec<WorkerBid>,
    pub status: SchedulerTaskStatus,
}

/// Scheduler — recibe bids y asigna tareas al mejor worker
pub struct TaskScheduler {
    /// Tareas pendientes de asignación
    tasks: Arc<Mutex<HashMap<String, SchedulerTask>>>,
    /// Workers conocidos y sus capacidades
    workers: Arc<Mutex<HashMap<String, NodeCapabilities>>>,
    /// Tiempo máximo para recolectar bids
    bid_window: Duration,
}

impl TaskScheduler {
    pub fn new() -> Self {
        let scheduler = Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            bid_window: Duration::from_millis(1500), // ← aumentar a 1.5s
        };

        // Limpiar tareas expiradas periódicamente
        let tasks_ref = Arc::clone(&scheduler.tasks);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                Self::cleanup_expired(&tasks_ref).await;
            }
        });

        scheduler
    }

    /// Registrar nueva tarea — empieza a recolectar bids
    pub async fn register_task(&self, task_id: String, task_type: String) {
        let deadline = Instant::now() + self.bid_window;
        let mut tasks = self.tasks.lock().await;
        tasks.insert(task_id.clone(), SchedulerTask {
            task_id,
            task_type,
            bids: Vec::new(),
            status: SchedulerTaskStatus::CollectingBids { deadline },
        });
    }

    /// Recibir bid de un worker
    pub async fn receive_bid(
        &self,
        task_id: &str,
        worker_id: String,
        reputation_score: u32,
        available_slots: usize,
        estimated_ms: u64,
    ) -> Option<String> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks.get_mut(task_id)?;

        match &task.status {
            SchedulerTaskStatus::CollectingBids { deadline } => {
                let deadline = *deadline;
                task.bids.push(WorkerBid {
                    worker_id,
                    reputation_score,
                    available_slots,
                    estimated_ms,
                    received_at: Instant::now(),
                });

                let bid_count = task.bids.len();
                println!("📊 [Scheduler] {} bid(s) recibidos para tarea {}", bid_count, task_id);

                // Asignar si: expiró el deadline O tenemos al menos 2 bids
                if Instant::now() >= deadline || bid_count >= 2 {
                    println!("⏱️ [Scheduler] Cerrando bids para {} ({} bids)", task_id, bid_count);
                    return self.select_winner_internal(task);
                }
                None
            }
            SchedulerTaskStatus::Assigned { worker_id, .. } => {
                println!("⚠️ [Scheduler] Tarea {} ya asignada a {}", task_id, &worker_id[..8.min(worker_id.len())]);
                None
            }
            _ => None,
        }
    }

    /// Seleccionar ganador manualmente (cuando expira bid_window)
    pub async fn try_assign(&self, task_id: &str) -> Option<String> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks.get_mut(task_id)?;

        match &task.status {
            SchedulerTaskStatus::CollectingBids { deadline } => {
                if Instant::now() >= *deadline || !task.bids.is_empty() {
                    self.select_winner_internal(task)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Marcar tarea como completada
    pub async fn mark_completed(&self, task_id: &str, worker_id: &str) {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = SchedulerTaskStatus::Completed {
                worker_id: worker_id.to_string(),
            };
        }
    }

    /// Registrar worker con sus capacidades
    pub async fn register_worker(&self, worker_id: String, capabilities: NodeCapabilities) {
        println!("📋 Scheduler: worker registrado {} ({} cores)", worker_id, capabilities.cores);
        self.workers.lock().await.insert(worker_id, capabilities);
    }

    /// Workers conocidos
    pub async fn known_workers(&self) -> Vec<String> {
        self.workers.lock().await.keys().cloned().collect()
    }

    /// Stats del scheduler
    pub async fn stats(&self) -> (usize, usize, usize) {
        let tasks = self.tasks.lock().await;
        let collecting = tasks.values()
            .filter(|t| matches!(t.status, SchedulerTaskStatus::CollectingBids { .. }))
            .count();
        let assigned = tasks.values()
            .filter(|t| matches!(t.status, SchedulerTaskStatus::Assigned { .. }))
            .count();
        let completed = tasks.values()
            .filter(|t| matches!(t.status, SchedulerTaskStatus::Completed { .. }))
            .count();
        (collecting, assigned, completed)
    }

    /// Seleccionar mejor worker según: reputación > slots disponibles > velocidad estimada
    fn select_winner_internal(&self, task: &mut SchedulerTask) -> Option<String> {
        if task.bids.is_empty() {
            task.status = SchedulerTaskStatus::Failed;
            println!("💀 [Scheduler] Tarea {} sin bids — fallida", task.task_id);
            return None;
        }

        // Scoring: reputación (peso 60%) + slots (20%) + velocidad (20%)
        let winner = task.bids.iter().max_by_key(|bid| {
            let rep_score = bid.reputation_score as u64 * 60;
            let slot_score = (bid.available_slots.min(8) as u64) * 20;
            let speed_score = if bid.estimated_ms == 0 { 20u64 }
                              else { (1000 / bid.estimated_ms.max(1)).min(20) };
            rep_score + slot_score + speed_score
        })?;

        let winner_id = winner.worker_id.clone();

        println!("🏆 [Scheduler] Tarea {} asignada a {} (rep:{} slots:{})",
            task.task_id, winner_id,
            winner.reputation_score, winner.available_slots);

        task.status = SchedulerTaskStatus::Assigned {
            worker_id: winner_id.clone(),
            assigned_at: Instant::now(),
        };

        Some(winner_id)
    }

    async fn cleanup_expired(tasks: &Arc<Mutex<HashMap<String, SchedulerTask>>>) {
        let mut tasks = tasks.lock().await;
        let expired_timeout = Duration::from_secs(120);
        tasks.retain(|_, task| {
            match &task.status {
                SchedulerTaskStatus::Assigned { assigned_at, .. } => {
                    assigned_at.elapsed() < expired_timeout
                }
                SchedulerTaskStatus::Completed { .. } | SchedulerTaskStatus::Failed => false,
                _ => true,
            }
        });
    }
}
