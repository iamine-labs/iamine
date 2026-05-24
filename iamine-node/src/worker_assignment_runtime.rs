use crate::broadcast_protocol::BroadcastResultToPublish;
use crate::broadcast_worker::{
    emit_broadcast_result_prepare_event, emit_worker_task_assign_received_event,
    should_execute_task_assignment,
};
use crate::log_observability_event;
use crate::metrics::NodeMetrics;
use crate::task_events::{
    emit_task_lifecycle_completed, emit_task_lifecycle_failed, emit_task_lifecycle_finalized,
    emit_task_lifecycle_started,
};
use crate::task_lifecycle::TaskLifecycleErrorCode;
use crate::task_queue::{OutcomeStatus, TaskQueue};
use iamine_network::LogLevel;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerTaskAssignment {
    pub(crate) assigned_worker: String,
    pub(crate) task_id: String,
    pub(crate) task_type: String,
    pub(crate) data: String,
    pub(crate) origin_peer: String,
    pub(crate) deadline_ms: u64,
}

impl WorkerTaskAssignment {
    pub(crate) fn from_value(value: &Value) -> Self {
        Self {
            assigned_worker: value["assigned_worker"].as_str().unwrap_or("").to_string(),
            task_id: value["task_id"].as_str().unwrap_or("").to_string(),
            task_type: value["task_type"].as_str().unwrap_or("").to_string(),
            data: value["data"].as_str().unwrap_or("").to_string(),
            origin_peer: value["origin_peer"].as_str().unwrap_or("").to_string(),
            deadline_ms: value["deadline_ms"].as_u64().unwrap_or(30_000),
        }
    }
}

pub(crate) fn worker_assignment_will_execute(
    assignment: &WorkerTaskAssignment,
    local_peer_id: &str,
    executed_broadcast_assignments: &HashSet<String>,
) -> bool {
    should_execute_task_assignment(
        &assignment.assigned_worker,
        local_peer_id,
        executed_broadcast_assignments.contains(&assignment.task_id),
    )
}

pub(crate) fn handle_worker_task_assignment(
    assignment: WorkerTaskAssignment,
    local_peer_id: String,
    executed_broadcast_assignments: &mut HashSet<String>,
    queue: Arc<TaskQueue>,
    metrics: Arc<RwLock<NodeMetrics>>,
    result_tx: mpsc::Sender<BroadcastResultToPublish>,
) {
    let will_execute =
        worker_assignment_will_execute(&assignment, &local_peer_id, executed_broadcast_assignments);
    emit_worker_task_assign_received_event(
        &assignment.task_id,
        &assignment.assigned_worker,
        &local_peer_id,
        will_execute,
    );

    if will_execute {
        executed_broadcast_assignments.insert(assignment.task_id.clone());
        println!(
            "🎯 [Worker] ¡Asignado! task_id={} (deadline: {}ms)",
            assignment.task_id, assignment.deadline_ms
        );
        emit_task_lifecycle_started(&assignment.task_id, &assignment.task_type, &local_peer_id);

        tokio::spawn(async move {
            execute_assigned_broadcast_task(assignment, local_peer_id, queue, metrics, result_tx)
                .await;
        });
    } else if assignment.assigned_worker == local_peer_id {
        println!(
            "⏭️  [Worker] TaskAssign duplicado ignorado: task_id={}",
            assignment.task_id
        );
    } else {
        println!(
            "⏭️  [Worker] Tarea {} → otro worker ganó",
            assignment.task_id
        );
    }
}

async fn execute_assigned_broadcast_task(
    assignment: WorkerTaskAssignment,
    worker_id: String,
    queue: Arc<TaskQueue>,
    metrics: Arc<RwLock<NodeMetrics>>,
    result_tx: mpsc::Sender<BroadcastResultToPublish>,
) {
    let task_id = assignment.task_id.clone();
    let task_type = assignment.task_type.clone();
    let origin_peer = assignment.origin_peer.clone();
    let exec = async {
        let _ = queue
            .push(
                assignment.task_id.clone(),
                assignment.task_type.clone(),
                assignment.data.clone(),
            )
            .await;
        queue.outcome_rx.lock().await.recv().await
    };

    match tokio::time::timeout(Duration::from_millis(assignment.deadline_ms), exec).await {
        Ok(Some(outcome)) => {
            let success = matches!(outcome.status, OutcomeStatus::Success);
            {
                let mut m = metrics.write().await;
                if success {
                    m.task_success(0);
                } else {
                    m.task_failed();
                }
            }
            log_observability_event(
                LogLevel::Info,
                "task_completed",
                &outcome.task_id,
                Some(&outcome.task_id),
                None,
                None,
                {
                    let mut fields = Map::new();
                    fields.insert("success".to_string(), success.into());
                    fields.insert("worker_peer_id".to_string(), worker_id.clone().into());
                    fields.insert("attempts".to_string(), outcome.attempts.into());
                    fields.insert("source".to_string(), "broadcast_task_assign".into());
                    fields
                },
            );
            println!(
                "✅ [Worker] Tarea {} completada (success={})",
                outcome.task_id, success
            );
            let rep = queue.reputation().await;
            println!("⭐ Reputación: {}/100", rep.reputation_score);

            let output = outcome
                .result
                .as_ref()
                .map(|result| result.output.clone())
                .unwrap_or_default();
            let elapsed_ms = outcome
                .result
                .as_ref()
                .map(|result| result.execution_ms)
                .unwrap_or_default();
            let error = if success {
                None
            } else {
                Some("broadcast task execution failed".to_string())
            };
            if success {
                emit_task_lifecycle_completed(
                    &outcome.task_id,
                    &task_type,
                    &worker_id,
                    &output,
                    elapsed_ms,
                );
                emit_task_lifecycle_finalized(
                    &outcome.task_id,
                    &task_type,
                    &worker_id,
                    "success",
                    &output,
                );
            } else {
                emit_task_lifecycle_failed(
                    &outcome.task_id,
                    &task_type,
                    TaskLifecycleErrorCode::UnknownError,
                    "broadcast task execution failed",
                );
                emit_task_lifecycle_finalized(
                    &outcome.task_id,
                    &task_type,
                    &worker_id,
                    "failed",
                    &output,
                );
            }
            let broadcast_result = BroadcastResultToPublish {
                task_id: outcome.task_id.clone(),
                task_type,
                worker_peer_id: worker_id,
                origin_peer: origin_peer.clone(),
                success,
                output,
                elapsed_ms,
                error,
                attempts: outcome.attempts,
                source: "broadcast_task_assign",
            };
            emit_broadcast_result_prepare_event(&broadcast_result);
            let _ = result_tx.send(broadcast_result).await;
            println!(
                "📤 [Worker] Resultado listo → origin {}",
                &origin_peer[..origin_peer.len().min(8)]
            );
        }
        Ok(None) => eprintln!("❌ [Worker] Canal cerrado"),
        Err(_) => {
            eprintln!("⏱️ [Worker] Deadline expirado {}ms", assignment.deadline_ms);
            metrics.write().await.task_timed_out();
            emit_task_lifecycle_failed(
                &task_id,
                &task_type,
                TaskLifecycleErrorCode::TaskTimeout,
                "broadcast task deadline expired",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment_for(assigned_worker: &str) -> WorkerTaskAssignment {
        WorkerTaskAssignment {
            assigned_worker: assigned_worker.to_string(),
            task_id: "task-1".to_string(),
            task_type: "reverse_string".to_string(),
            data: "abc".to_string(),
            origin_peer: "controller".to_string(),
            deadline_ms: 30_000,
        }
    }

    #[test]
    fn worker_assignment_parses_required_fields() {
        let value = serde_json::json!({
            "assigned_worker": "worker-a",
            "task_id": "task-1",
            "task_type": "reverse_string",
            "data": "abc",
            "origin_peer": "controller",
            "deadline_ms": 42
        });

        let assignment = WorkerTaskAssignment::from_value(&value);

        assert_eq!(assignment.assigned_worker, "worker-a");
        assert_eq!(assignment.task_id, "task-1");
        assert_eq!(assignment.task_type, "reverse_string");
        assert_eq!(assignment.data, "abc");
        assert_eq!(assignment.origin_peer, "controller");
        assert_eq!(assignment.deadline_ms, 42);
    }

    #[test]
    fn worker_assignment_will_execute_only_for_assigned_worker() {
        let executed = HashSet::new();

        assert!(worker_assignment_will_execute(
            &assignment_for("worker-a"),
            "worker-a",
            &executed
        ));
        assert!(!worker_assignment_will_execute(
            &assignment_for("worker-a"),
            "worker-b",
            &executed
        ));
    }

    #[test]
    fn worker_assignment_non_winner_does_not_execute() {
        let mut executed = HashSet::new();
        executed.insert("task-1".to_string());

        assert!(!worker_assignment_will_execute(
            &assignment_for("worker-a"),
            "worker-a",
            &executed
        ));
    }
}
