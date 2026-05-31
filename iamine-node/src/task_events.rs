use crate::broadcast_protocol::broadcast_payload_preview;
use crate::log_observability_event;
use crate::router_scheduler::SchedulerDecision;
use crate::task_lifecycle::{
    now_ms, TaskLifecycleErrorCode, TaskLifecycleEvent, TaskLifecycleStatus, TaskSelectionReason,
};
use crate::task_trace::record_task_lifecycle_event;
use iamine_network::LogLevel;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub(crate) fn emit_task_lifecycle_created(
    task_id: &str,
    task_type: &str,
    input: &str,
    controller_peer_id: &str,
) {
    let event = task_lifecycle_created_event(task_id, task_type, input);
    emit_task_lifecycle_event(event, Some(controller_peer_id));
}

pub(crate) fn emit_task_lifecycle_assigned(
    task_id: &str,
    task_type: &str,
    assigned_worker_peer_id: &str,
    candidate_workers: Vec<String>,
    selection_reason: TaskSelectionReason,
) {
    let event = TaskLifecycleEvent::new(
        "task_lifecycle_assigned",
        task_id,
        TaskLifecycleStatus::Assigned,
        now_ms(),
    )
    .with_task_type(task_type)
    .with_assignment(assigned_worker_peer_id, candidate_workers, selection_reason);
    emit_task_lifecycle_event(event, Some(assigned_worker_peer_id));
}

pub(crate) fn emit_task_lifecycle_scheduler_decision(decision: &SchedulerDecision) {
    let selected_worker_peer_id = decision.selected_worker_peer_id.as_deref();
    let mut event = TaskLifecycleEvent::new(
        "task_lifecycle_scheduler_decision",
        &decision.task_id,
        if selected_worker_peer_id.is_some() {
            TaskLifecycleStatus::Assigned
        } else {
            TaskLifecycleStatus::Failed
        },
        now_ms(),
    )
    .with_task_type(&decision.task_type)
    .with_scheduler_metadata(
        decision.rejected_candidate_ids(),
        decision.rejected_reason_strings(),
    )
    .with_scheduler_capability_metadata(
        decision.compatible_candidates_count,
        decision.capability_filter_applied,
    );
    if let Some(selected_worker_peer_id) = selected_worker_peer_id {
        event = event.with_assignment(
            selected_worker_peer_id,
            decision.candidate_worker_ids(),
            decision.selection_reason.to_task_selection_reason(),
        );
    } else {
        event.candidate_workers = decision.candidate_worker_ids();
    }
    emit_task_lifecycle_event(event, selected_worker_peer_id);
}

pub(crate) fn emit_task_lifecycle_started(task_id: &str, task_type: &str, worker_peer_id: &str) {
    let event = TaskLifecycleEvent::new(
        "task_lifecycle_started",
        task_id,
        TaskLifecycleStatus::Running,
        now_ms(),
    )
    .with_task_type(task_type)
    .with_worker(worker_peer_id);
    emit_task_lifecycle_event(event, Some(worker_peer_id));
}

pub(crate) fn emit_task_lifecycle_result_received(
    task_id: &str,
    task_type: &str,
    worker_peer_id: &str,
    success: bool,
    output: &str,
    latency_ms: u64,
) {
    let status = if success {
        TaskLifecycleStatus::Completed
    } else {
        TaskLifecycleStatus::Failed
    };
    let event =
        TaskLifecycleEvent::new("task_lifecycle_result_received", task_id, status, now_ms())
            .with_task_type(task_type)
            .with_worker(worker_peer_id)
            .with_result(
                success,
                Some(broadcast_payload_preview(output)),
                Some(latency_ms),
                None,
            );
    emit_task_lifecycle_event(event, Some(worker_peer_id));
}

pub(crate) fn emit_task_lifecycle_completed(
    task_id: &str,
    task_type: &str,
    worker_peer_id: &str,
    output: &str,
    latency_ms: u64,
) {
    let event = TaskLifecycleEvent::new(
        "task_lifecycle_completed",
        task_id,
        TaskLifecycleStatus::Completed,
        now_ms(),
    )
    .with_task_type(task_type)
    .with_worker(worker_peer_id)
    .with_result(
        true,
        Some(broadcast_payload_preview(output)),
        Some(latency_ms),
        Some("success".to_string()),
    );
    emit_task_lifecycle_event(event, Some(worker_peer_id));
}

pub(crate) fn emit_task_lifecycle_failed(
    task_id: &str,
    task_type: &str,
    error_code: TaskLifecycleErrorCode,
    error_message: &str,
) {
    let event = TaskLifecycleEvent::new(
        "task_lifecycle_failed",
        task_id,
        TaskLifecycleStatus::Failed,
        now_ms(),
    )
    .with_task_type(task_type)
    .with_error(error_code, error_message);
    emit_task_lifecycle_event(event, None);
}

pub(crate) fn emit_task_lifecycle_retrying(
    task_id: &str,
    task_type: &str,
    retry_count: u32,
    fallback_reason: Option<String>,
) {
    let event = TaskLifecycleEvent::new(
        "task_lifecycle_retrying",
        task_id,
        TaskLifecycleStatus::Retrying,
        now_ms(),
    )
    .with_task_type(task_type)
    .with_retry(retry_count, fallback_reason);
    emit_task_lifecycle_event(event, None);
}

#[allow(dead_code)]
pub(crate) fn emit_task_lifecycle_cancelled(task_id: &str, task_type: &str, reason: &str) {
    let event = TaskLifecycleEvent::new(
        "task_lifecycle_cancelled",
        task_id,
        TaskLifecycleStatus::Cancelled,
        now_ms(),
    )
    .with_task_type(task_type)
    .with_error(TaskLifecycleErrorCode::UnknownError, reason);
    emit_task_lifecycle_event(event, None);
}

pub(crate) fn emit_task_lifecycle_finalized(
    task_id: &str,
    task_type: &str,
    worker_peer_id: &str,
    final_outcome: &str,
    output: &str,
) {
    let event = TaskLifecycleEvent::new(
        "task_lifecycle_finalized",
        task_id,
        if final_outcome == "success" {
            TaskLifecycleStatus::Completed
        } else {
            TaskLifecycleStatus::Failed
        },
        now_ms(),
    )
    .with_task_type(task_type)
    .with_worker(worker_peer_id)
    .with_result(
        final_outcome == "success",
        Some(broadcast_payload_preview(output)),
        None,
        Some(final_outcome.to_string()),
    );
    emit_task_lifecycle_event(event, Some(worker_peer_id));
}

pub(crate) fn task_lifecycle_created_event(
    task_id: &str,
    task_type: &str,
    input: &str,
) -> TaskLifecycleEvent {
    TaskLifecycleEvent::new(
        "task_lifecycle_created",
        task_id,
        TaskLifecycleStatus::Pending,
        now_ms(),
    )
    .with_task_type(task_type)
    .with_input(
        Some(broadcast_payload_preview(input)),
        Some(stable_input_hash(input)),
    )
}

fn emit_task_lifecycle_event(event: TaskLifecycleEvent, peer_id: Option<&str>) {
    let mut fields = task_lifecycle_event_fields(&event);
    if let Some(peer_id) = peer_id {
        fields.insert("peer_id".to_string(), peer_id.into());
    }
    log_observability_event(
        LogLevel::Info,
        &event.event,
        &event.task_id,
        Some(&event.task_id),
        event.model_id.as_deref(),
        event.error_code.map(|code| code.as_str()),
        fields,
    );
    let _ = record_task_lifecycle_event(event);
}

fn task_lifecycle_event_fields(event: &TaskLifecycleEvent) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("status".to_string(), event.status.as_str().into());
    fields.insert("timestamp_ms".to_string(), event.timestamp_ms.into());
    insert_option(&mut fields, "task_type", event.task_type.as_deref());
    insert_option(
        &mut fields,
        "worker_peer_id",
        event.worker_peer_id.as_deref(),
    );
    insert_option(
        &mut fields,
        "assigned_worker_peer_id",
        event.assigned_worker_peer_id.as_deref(),
    );
    insert_option(
        &mut fields,
        "selected_worker_peer_id",
        event.selected_worker_peer_id.as_deref(),
    );
    insert_option(&mut fields, "model_id", event.model_id.as_deref());
    insert_option(&mut fields, "model_hash", event.model_hash.as_deref());
    insert_option(&mut fields, "input_summary", event.input_summary.as_deref());
    insert_option(&mut fields, "input_hash", event.input_hash.as_deref());
    insert_option(
        &mut fields,
        "fallback_reason",
        event.fallback_reason.as_deref(),
    );
    insert_option(&mut fields, "error_message", event.error_message.as_deref());
    insert_option(
        &mut fields,
        "result_location",
        event.result_location.as_deref(),
    );
    insert_option(
        &mut fields,
        "result_summary",
        event.result_summary.as_deref(),
    );
    insert_option(&mut fields, "final_outcome", event.final_outcome.as_deref());
    fields.insert("retry_count".to_string(), event.retry_count.into());
    fields.insert("fallback_used".to_string(), event.fallback_used.into());
    if let Some(latency_ms) = event.latency_ms {
        fields.insert("latency_ms".to_string(), latency_ms.into());
    }
    if let Some(selection_reason) = event.selection_reason {
        fields.insert(
            "selection_reason".to_string(),
            selection_reason.as_str().into(),
        );
    }
    if let Some(error_code) = event.error_code {
        fields.insert("error_code".to_string(), error_code.as_str().into());
    }
    fields.insert(
        "candidate_workers_count".to_string(),
        (event.candidate_workers.len() as u64).into(),
    );
    fields.insert(
        "candidate_workers".to_string(),
        Value::Array(
            event
                .candidate_workers
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    fields.insert(
        "rejected_candidates_count".to_string(),
        (event.rejected_candidates.len() as u64).into(),
    );
    fields.insert(
        "rejected_candidates".to_string(),
        Value::Array(
            event
                .rejected_candidates
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    fields.insert(
        "rejected_reasons".to_string(),
        Value::Array(
            event
                .rejected_reasons
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    fields.insert(
        "compatible_candidates_count".to_string(),
        (event.compatible_candidates_count as u64).into(),
    );
    fields.insert(
        "capability_filter_applied".to_string(),
        event.capability_filter_applied.into(),
    );
    fields.insert(
        "required_capabilities".to_string(),
        Value::Array(
            event
                .required_capabilities
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    fields.insert(
        "declared_capabilities".to_string(),
        Value::Array(
            event
                .declared_capabilities
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    fields
}

fn insert_option(fields: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        fields.insert(key.to_string(), value.into());
    }
}

fn stable_input_hash(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_lifecycle_events_preserve_event_names() {
        let created = task_lifecycle_created_event("task-created", "reverse_string", "abc");

        assert_eq!(created.event, "task_lifecycle_created");
        assert_eq!(created.status, TaskLifecycleStatus::Pending);
        assert_eq!(created.task_type.as_deref(), Some("reverse_string"));
    }

    #[test]
    fn broadcast_creates_task_lifecycle_record() {
        let event = task_lifecycle_created_event("task-offer", "reverse_string", "payload");

        let fields = task_lifecycle_event_fields(&event);

        assert_eq!(
            fields.get("task_type").and_then(|value| value.as_str()),
            Some("reverse_string")
        );
        assert_eq!(
            fields.get("status").and_then(|value| value.as_str()),
            Some("pending")
        );
        assert!(fields
            .get("input_hash")
            .and_then(|value| value.as_str())
            .is_some());
    }

    #[test]
    fn broadcast_assigns_task_lifecycle_worker() {
        let event = TaskLifecycleEvent::new(
            "task_lifecycle_assigned",
            "task-assign",
            TaskLifecycleStatus::Assigned,
            10,
        )
        .with_assignment(
            "worker-a",
            vec!["worker-a".to_string(), "worker-b".to_string()],
            TaskSelectionReason::CurrentBroadcastPolicy,
        );

        let fields = task_lifecycle_event_fields(&event);

        assert_eq!(
            fields
                .get("assigned_worker_peer_id")
                .and_then(|value| value.as_str()),
            Some("worker-a")
        );
        assert_eq!(
            fields
                .get("candidate_workers_count")
                .and_then(|value| value.as_u64()),
            Some(2)
        );
    }

    #[test]
    fn task_lifecycle_records_scheduler_decision_metadata() {
        let event = TaskLifecycleEvent::new(
            "task_lifecycle_scheduler_decision",
            "task-scheduler",
            TaskLifecycleStatus::Assigned,
            10,
        )
        .with_assignment(
            "worker-a",
            vec!["worker-a".to_string(), "worker-b".to_string()],
            TaskSelectionReason::ReadyWorkerSupportsTask,
        )
        .with_scheduler_metadata(
            vec!["worker-b".to_string()],
            vec!["not_ready_for_tasks".to_string()],
        );

        let fields = task_lifecycle_event_fields(&event);

        assert_eq!(
            fields
                .get("selection_reason")
                .and_then(|value| value.as_str()),
            Some("ready_worker_supports_task")
        );
        assert_eq!(
            fields
                .get("rejected_candidates_count")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
    }

    #[test]
    fn broadcast_result_updates_lifecycle_completed() {
        let event = TaskLifecycleEvent::new(
            "task_lifecycle_result_received",
            "task-result",
            TaskLifecycleStatus::Completed,
            10,
        )
        .with_worker("worker-a")
        .with_result(
            true,
            Some("ok".to_string()),
            Some(42),
            Some("success".to_string()),
        );

        let fields = task_lifecycle_event_fields(&event);

        assert_eq!(
            fields.get("status").and_then(|value| value.as_str()),
            Some("completed")
        );
        assert_eq!(
            fields.get("final_outcome").and_then(|value| value.as_str()),
            Some("success")
        );
    }
}
