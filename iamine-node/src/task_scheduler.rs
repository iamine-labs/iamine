use crate::router_scheduler::{
    SchedulerCandidate, SchedulerDecision, SchedulerRejectedCandidate, SchedulerTaskRequest,
    SelectionReason,
};
use crate::scheduler_policy::{classify_scheduler_candidate, SchedulerCandidateClassification};
use iamine_core::node::NodeCapabilities;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Bid de un worker para ejecutar una tarea
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WorkerBid {
    pub worker_id: String,
    pub reputation_score: u32,
    pub available_slots: usize,
    pub estimated_ms: u64,
    pub received_at: Instant,
}

/// Estado de una tarea en el scheduler
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum SchedulerTaskStatus {
    /// Esperando bids de workers
    CollectingBids { deadline: Instant },
    /// Asignada a un worker
    Assigned {
        worker_id: String,
        assigned_at: Instant,
    },
    /// Completada
    Completed { worker_id: String },
    /// Falló — todos los workers rechazaron
    Failed,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SchedulerTask {
    pub task_id: String,
    pub task_type: String,
    pub bids: Vec<WorkerBid>,
    capability_candidates: Vec<SchedulerCandidate>,
    rejected_candidates: Vec<SchedulerRejectedCandidate>,
    capability_filter_applied: bool,
    pub status: SchedulerTaskStatus,
}

/// Scheduler — recibe bids y asigna tareas al mejor worker
#[allow(dead_code)]
pub struct TaskScheduler {
    /// Tareas pendientes de asignación
    tasks: Arc<Mutex<HashMap<String, SchedulerTask>>>,
    /// Workers conocidos y sus capacidades
    workers: Arc<Mutex<HashMap<String, NodeCapabilities>>>,
    /// Tiempo máximo para recolectar bids
    bid_window: Duration,
}

#[allow(dead_code)]
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
        tasks.insert(
            task_id.clone(),
            SchedulerTask {
                task_id,
                task_type,
                bids: Vec::new(),
                capability_candidates: Vec::new(),
                rejected_candidates: Vec::new(),
                capability_filter_applied: false,
                status: SchedulerTaskStatus::CollectingBids { deadline },
            },
        );
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
        Self::receive_bid_for_task(
            task,
            worker_id,
            reputation_score,
            available_slots,
            estimated_ms,
        )
    }

    pub async fn receive_capability_filtered_bid(
        &self,
        task_id: &str,
        candidate: SchedulerCandidate,
        reputation_score: u32,
        available_slots: usize,
        estimated_ms: u64,
    ) -> Option<String> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks.get_mut(task_id)?;
        task.capability_filter_applied = true;
        Self::upsert_capability_candidate(task, candidate.clone());

        let request = SchedulerTaskRequest::new(&task.task_id, &task.task_type);
        match classify_scheduler_candidate(&request, &candidate) {
            SchedulerCandidateClassification::Accepted => {
                task.rejected_candidates
                    .retain(|rejected| rejected.peer_id != candidate.peer_id);
                Self::receive_bid_for_task(
                    task,
                    candidate.peer_id,
                    reputation_score,
                    available_slots,
                    estimated_ms,
                )
            }
            SchedulerCandidateClassification::Rejected(reasons) => {
                Self::upsert_rejected_candidate(
                    task,
                    SchedulerRejectedCandidate::from_candidate(&candidate, reasons),
                );
                None
            }
        }
    }

    fn receive_bid_for_task(
        task: &mut SchedulerTask,
        worker_id: String,
        reputation_score: u32,
        available_slots: usize,
        estimated_ms: u64,
    ) -> Option<String> {
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
                println!(
                    "📊 [Scheduler] {} bid(s) recibidos para tarea {}",
                    bid_count, task.task_id
                );

                // Asignar si: expiró el deadline O tenemos al menos 2 bids
                if Instant::now() >= deadline || bid_count >= 2 {
                    println!(
                        "⏱️ [Scheduler] Cerrando bids para {} ({} bids)",
                        task.task_id, bid_count
                    );
                    return Self::select_winner_internal(task);
                }
                None
            }
            SchedulerTaskStatus::Assigned { worker_id, .. } => {
                println!(
                    "⚠️ [Scheduler] Tarea {} ya asignada a {}",
                    task.task_id,
                    &worker_id[..8.min(worker_id.len())]
                );
                None
            }
            _ => None,
        }
    }

    pub async fn decision_for_assignment(
        &self,
        task_id: &str,
        task_type: &str,
        selected_worker_peer_id: &str,
        fallback_candidate_ids: Vec<String>,
        selection_reason: SelectionReason,
        decision_timestamp_ms: u64,
    ) -> SchedulerDecision {
        let tasks = self.tasks.lock().await;
        let Some(task) = tasks.get(task_id) else {
            return SchedulerDecision::from_current_broadcast_policy(
                task_id,
                task_type,
                selected_worker_peer_id,
                fallback_candidate_ids,
                selection_reason,
                decision_timestamp_ms,
            );
        };
        if !task.capability_filter_applied {
            return SchedulerDecision::from_current_broadcast_policy(
                task_id,
                task_type,
                selected_worker_peer_id,
                fallback_candidate_ids,
                selection_reason,
                decision_timestamp_ms,
            );
        }

        SchedulerDecision::from_capability_filtered_broadcast_policy(
            &SchedulerTaskRequest::new(task_id, task_type),
            selected_worker_peer_id,
            task.capability_candidates.clone(),
            task.rejected_candidates.clone(),
            selection_reason,
            decision_timestamp_ms,
        )
    }

    pub async fn no_compatible_worker_decision(
        &self,
        task_id: &str,
        decision_timestamp_ms: u64,
    ) -> Option<SchedulerDecision> {
        let tasks = self.tasks.lock().await;
        let task = tasks.get(task_id)?;
        if !matches!(task.status, SchedulerTaskStatus::Failed)
            || !task.capability_filter_applied
            || task.rejected_candidates.is_empty()
        {
            return None;
        }

        Some(SchedulerDecision::no_compatible_worker(
            &SchedulerTaskRequest::new(&task.task_id, &task.task_type),
            task.capability_candidates.clone(),
            task.rejected_candidates.clone(),
            decision_timestamp_ms,
        ))
    }

    fn upsert_capability_candidate(task: &mut SchedulerTask, candidate: SchedulerCandidate) {
        if let Some(existing) = task
            .capability_candidates
            .iter_mut()
            .find(|existing| existing.peer_id == candidate.peer_id)
        {
            *existing = candidate;
        } else {
            task.capability_candidates.push(candidate);
        }
    }

    fn upsert_rejected_candidate(
        task: &mut SchedulerTask,
        rejected_candidate: SchedulerRejectedCandidate,
    ) {
        if let Some(existing) = task
            .rejected_candidates
            .iter_mut()
            .find(|existing| existing.peer_id == rejected_candidate.peer_id)
        {
            *existing = rejected_candidate;
        } else {
            task.rejected_candidates.push(rejected_candidate);
        }
    }

    /// Seleccionar ganador manualmente (cuando expira bid_window)
    pub async fn try_assign(&self, task_id: &str) -> Option<String> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks.get_mut(task_id)?;

        match &task.status {
            SchedulerTaskStatus::CollectingBids { deadline } => {
                if Instant::now() >= *deadline || !task.bids.is_empty() {
                    Self::select_winner_internal(task)
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
        println!(
            "📋 Scheduler: worker registrado {} ({} cores)",
            worker_id, capabilities.cores
        );
        self.workers.lock().await.insert(worker_id, capabilities);
    }

    /// Workers conocidos
    pub async fn known_workers(&self) -> Vec<String> {
        self.workers.lock().await.keys().cloned().collect()
    }

    /// Stats del scheduler
    pub async fn stats(&self) -> (usize, usize, usize) {
        let tasks = self.tasks.lock().await;
        let collecting = tasks
            .values()
            .filter(|t| matches!(t.status, SchedulerTaskStatus::CollectingBids { .. }))
            .count();
        let assigned = tasks
            .values()
            .filter(|t| matches!(t.status, SchedulerTaskStatus::Assigned { .. }))
            .count();
        let completed = tasks
            .values()
            .filter(|t| matches!(t.status, SchedulerTaskStatus::Completed { .. }))
            .count();
        (collecting, assigned, completed)
    }

    /// Seleccionar mejor worker según: reputación > slots disponibles > velocidad estimada
    fn select_winner_internal(task: &mut SchedulerTask) -> Option<String> {
        if task.bids.is_empty() {
            task.status = SchedulerTaskStatus::Failed;
            println!("💀 [Scheduler] Tarea {} sin bids — fallida", task.task_id);
            return None;
        }

        // Scoring: reputación (peso 60%) + slots (20%) + velocidad (20%)
        let winner = task.bids.iter().max_by_key(|bid| {
            let rep_score = bid.reputation_score as u64 * 60;
            let slot_score = (bid.available_slots.min(8) as u64) * 20;
            let speed_score = if bid.estimated_ms == 0 {
                20u64
            } else {
                (1000 / bid.estimated_ms.max(1)).min(20)
            };
            rep_score + slot_score + speed_score
        })?;

        let winner_id = winner.worker_id.clone();

        println!(
            "🏆 [Scheduler] Tarea {} asignada a {} (rep:{} slots:{})",
            task.task_id, winner_id, winner.reputation_score, winner.available_slots
        );

        task.status = SchedulerTaskStatus::Assigned {
            worker_id: winner_id.clone(),
            assigned_at: Instant::now(),
        };

        Some(winner_id)
    }

    async fn cleanup_expired(tasks: &Arc<Mutex<HashMap<String, SchedulerTask>>>) {
        let mut tasks = tasks.lock().await;
        let expired_timeout = Duration::from_secs(120);
        tasks.retain(|_, task| match &task.status {
            SchedulerTaskStatus::Assigned { assigned_at, .. } => {
                assigned_at.elapsed() < expired_timeout
            }
            SchedulerTaskStatus::Completed { .. } | SchedulerTaskStatus::Failed => false,
            _ => true,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_health::ClusterHealth;
    use crate::cluster_readiness::ClusterReadinessReason;
    use crate::cluster_registry::{
        ClusterBackend, ClusterExecutionMode, ClusterMetricsStatus, ClusterRole,
    };
    use crate::router_scheduler::SchedulerRejectionReason;

    fn mock_candidate(peer_id: &str) -> SchedulerCandidate {
        SchedulerCandidate {
            peer_id: peer_id.to_string(),
            hostname: peer_id.to_string(),
            role: ClusterRole::Worker,
            health: ClusterHealth::Healthy,
            ready_for_tasks: true,
            readiness_reason: ClusterReadinessReason::MockSimpleTasksReady,
            backend: ClusterBackend::Mock,
            execution_mode: ClusterExecutionMode::Mock,
            real_inference_available: Some(false),
            supported_task_types: vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string(),
            ],
            models_in_storage: Vec::new(),
            models_in_registry: Vec::new(),
            executable_models: Vec::new(),
            metadata_only_models: Vec::new(),
            unavailable_models: Vec::new(),
            capability_snapshot_available: true,
            latency_ms: Some(10),
            metrics_status: ClusterMetricsStatus::Fallback,
            last_seen_ms: Some(100),
        }
    }

    #[tokio::test]
    async fn scheduler_filtered_bid_preserves_mock_simple_selection() {
        let scheduler = TaskScheduler::new();
        scheduler
            .register_task("task-simple".to_string(), "reverse_string".to_string())
            .await;

        assert_eq!(
            scheduler
                .receive_capability_filtered_bid(
                    "task-simple",
                    mock_candidate("peer-mock"),
                    90,
                    1,
                    10,
                )
                .await,
            None
        );
        assert_eq!(
            scheduler.try_assign("task-simple").await.as_deref(),
            Some("peer-mock")
        );
        let decision = scheduler
            .decision_for_assignment(
                "task-simple",
                "reverse_string",
                "peer-mock",
                Vec::new(),
                SelectionReason::BroadcastBidSelected,
                200,
            )
            .await;

        assert!(decision.capability_filter_applied);
        assert_eq!(decision.compatible_candidates_count, 1);
        assert!(decision.rejected_candidates.is_empty());
    }

    #[tokio::test]
    async fn scheduler_filtered_bid_rejects_mock_real_inference() -> Result<(), String> {
        let scheduler = TaskScheduler::new();
        scheduler
            .register_task("task-infer".to_string(), "inference".to_string())
            .await;
        let mut candidate = mock_candidate("peer-mock");
        candidate.supported_task_types.push("inference".to_string());

        assert_eq!(
            scheduler
                .receive_capability_filtered_bid("task-infer", candidate, 90, 1, 10)
                .await,
            None
        );
        let tasks = scheduler.tasks.lock().await;
        let task = tasks
            .get("task-infer")
            .ok_or_else(|| "registered task should remain visible".to_string())?;

        assert!(task.bids.is_empty());
        assert_eq!(task.rejected_candidates.len(), 1);
        assert!(task.rejected_candidates[0]
            .reasons
            .contains(&SchedulerRejectionReason::BackendIncompatible));
        assert!(task.rejected_candidates[0]
            .reasons
            .contains(&SchedulerRejectionReason::RealInferenceUnavailable));
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_no_compatible_worker_decision_is_controlled() -> Result<(), String> {
        let scheduler = TaskScheduler::new();
        scheduler
            .register_task("task-no-compatible".to_string(), "inference".to_string())
            .await;
        let mut candidate = mock_candidate("peer-mock");
        candidate.supported_task_types.push("inference".to_string());
        assert_eq!(
            scheduler
                .receive_capability_filtered_bid("task-no-compatible", candidate, 90, 1, 10)
                .await,
            None
        );
        {
            let mut tasks = scheduler.tasks.lock().await;
            if let Some(task) = tasks.get_mut("task-no-compatible") {
                task.status = SchedulerTaskStatus::Failed;
            }
        }

        let decision = scheduler
            .no_compatible_worker_decision("task-no-compatible", 200)
            .await;
        let decision = decision.ok_or_else(|| {
            "failed filtered task should expose a controlled decision".to_string()
        })?;

        assert_eq!(decision.selected_worker_peer_id, None);
        assert_eq!(decision.compatible_candidates_count, 0);
        assert!(decision.capability_filter_applied);
        assert_eq!(decision.rejected_candidate_ids(), vec!["peer-mock"]);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_filtered_bid_records_rejected_candidates_and_compatible_count() {
        let scheduler = TaskScheduler::new();
        scheduler
            .register_task("task-mixed".to_string(), "reverse_string".to_string())
            .await;
        let mut rejected = mock_candidate("peer-not-ready");
        rejected.ready_for_tasks = false;
        rejected.readiness_reason = ClusterReadinessReason::StartupIncomplete;

        assert_eq!(
            scheduler
                .receive_capability_filtered_bid("task-mixed", rejected, 99, 1, 10)
                .await,
            None
        );
        assert_eq!(
            scheduler
                .receive_capability_filtered_bid(
                    "task-mixed",
                    mock_candidate("peer-ready"),
                    90,
                    1,
                    10,
                )
                .await,
            None
        );
        assert_eq!(
            scheduler.try_assign("task-mixed").await.as_deref(),
            Some("peer-ready")
        );
        let decision = scheduler
            .decision_for_assignment(
                "task-mixed",
                "reverse_string",
                "peer-ready",
                Vec::new(),
                SelectionReason::BroadcastBidSelected,
                200,
            )
            .await;

        assert_eq!(decision.compatible_candidates_count, 1);
        assert_eq!(decision.rejected_candidate_ids(), vec!["peer-not-ready"]);
        assert_eq!(
            decision.rejected_reason_strings(),
            vec!["not_ready_for_tasks"]
        );
    }
}
