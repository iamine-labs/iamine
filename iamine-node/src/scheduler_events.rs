use crate::log_observability_event;
use crate::router_scheduler::{SchedulerDecision, SchedulerRejectedCandidate};
use iamine_network::LogLevel;
use serde_json::{Map, Value};

pub(crate) fn emit_scheduler_decision_events(decision: &SchedulerDecision) {
    emit_scheduler_candidates_built(decision);
    for rejected in &decision.rejected_candidates {
        emit_scheduler_candidate_rejected(decision, rejected);
    }
    if decision.selected_worker_peer_id.is_some() {
        emit_scheduler_worker_selected(decision);
    } else {
        emit_scheduler_no_compatible_worker(decision);
    }
    emit_scheduler_decision_recorded(decision);
}

fn emit_scheduler_candidates_built(decision: &SchedulerDecision) {
    log_observability_event(
        LogLevel::Info,
        "scheduler_candidates_built",
        &decision.task_id,
        Some(&decision.task_id),
        decision.required_model_id.as_deref(),
        None,
        base_fields(decision),
    );
}

fn emit_scheduler_candidate_rejected(
    decision: &SchedulerDecision,
    rejected: &SchedulerRejectedCandidate,
) {
    let mut fields = base_fields(decision);
    fields.insert(
        "rejected_peer_id".to_string(),
        rejected.peer_id.clone().into(),
    );
    fields.insert(
        "rejected_hostname".to_string(),
        rejected.hostname.clone().into(),
    );
    fields.insert(
        "rejection_reasons".to_string(),
        Value::Array(
            rejected
                .reasons
                .iter()
                .map(|reason| Value::String(reason.as_str().to_string()))
                .collect(),
        ),
    );
    log_observability_event(
        LogLevel::Info,
        "scheduler_candidate_rejected",
        &decision.task_id,
        Some(&decision.task_id),
        decision.required_model_id.as_deref(),
        None,
        fields,
    );
}

fn emit_scheduler_worker_selected(decision: &SchedulerDecision) {
    log_observability_event(
        LogLevel::Info,
        "scheduler_worker_selected",
        &decision.task_id,
        Some(&decision.task_id),
        decision.required_model_id.as_deref(),
        None,
        base_fields(decision),
    );
}

fn emit_scheduler_no_compatible_worker(decision: &SchedulerDecision) {
    log_observability_event(
        LogLevel::Warn,
        "scheduler_no_compatible_worker",
        &decision.task_id,
        Some(&decision.task_id),
        decision.required_model_id.as_deref(),
        Some("no_workers_available"),
        base_fields(decision),
    );
}

fn emit_scheduler_decision_recorded(decision: &SchedulerDecision) {
    log_observability_event(
        LogLevel::Info,
        "scheduler_decision_recorded",
        &decision.task_id,
        Some(&decision.task_id),
        decision.required_model_id.as_deref(),
        None,
        base_fields(decision),
    );
}

fn base_fields(decision: &SchedulerDecision) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("task_type".to_string(), decision.task_type.clone().into());
    fields.insert(
        "selection_reason".to_string(),
        decision.selection_reason.as_str().into(),
    );
    fields.insert(
        "candidate_workers_count".to_string(),
        (decision.candidate_workers.len() as u64).into(),
    );
    fields.insert(
        "compatible_candidates_count".to_string(),
        (decision.compatible_candidates_count as u64).into(),
    );
    fields.insert(
        "capability_filter_applied".to_string(),
        decision.capability_filter_applied.into(),
    );
    fields.insert(
        "required_task_type".to_string(),
        decision.task_type.clone().into(),
    );
    fields.insert(
        "rejected_candidates_count".to_string(),
        (decision.rejected_candidates.len() as u64).into(),
    );
    fields.insert(
        "candidate_workers".to_string(),
        Value::Array(
            decision
                .candidate_workers
                .iter()
                .map(|candidate| Value::String(candidate.peer_id.clone()))
                .collect(),
        ),
    );
    fields.insert(
        "rejected_candidates".to_string(),
        Value::Array(
            decision
                .rejected_candidates
                .iter()
                .map(|candidate| Value::String(candidate.peer_id.clone()))
                .collect(),
        ),
    );
    fields.insert(
        "rejected_reasons".to_string(),
        Value::Array(
            decision
                .rejected_reasons
                .iter()
                .map(|reason| Value::String(reason.as_str().to_string()))
                .collect(),
        ),
    );
    fields.insert("retry_count".to_string(), decision.retry_count.into());
    fields.insert("fallback_used".to_string(), decision.fallback_used.into());
    fields.insert(
        "timestamp_ms".to_string(),
        decision.decision_timestamp_ms.into(),
    );
    fields.insert(
        "required_capabilities".to_string(),
        Value::Array(
            decision
                .required_capabilities
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    if let Some(model_id) = decision.required_model_id.as_deref() {
        fields.insert("required_model_id".to_string(), model_id.into());
        fields.insert("required_model".to_string(), model_id.into());
    }
    if let Some(peer_id) = decision.selected_worker_peer_id.as_deref() {
        fields.insert("selected_worker_peer_id".to_string(), peer_id.into());
    }
    if let Some(hostname) = decision.selected_worker_hostname.as_deref() {
        fields.insert("selected_worker_hostname".to_string(), hostname.into());
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router_scheduler::{SchedulerDecision, SelectionReason};

    #[test]
    fn scheduler_event_fields_include_required_metadata() {
        let decision = SchedulerDecision::from_current_broadcast_policy(
            "task-1",
            "reverse_string",
            "worker-a",
            vec!["worker-a".to_string(), "worker-b".to_string()],
            SelectionReason::CurrentBroadcastPolicy,
            42,
        );

        let fields = base_fields(&decision);

        assert_eq!(
            fields
                .get("selected_worker_peer_id")
                .and_then(Value::as_str),
            Some("worker-a")
        );
        assert_eq!(
            fields
                .get("candidate_workers_count")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            fields.get("selection_reason").and_then(Value::as_str),
            Some("current_broadcast_policy")
        );
        assert_eq!(
            fields
                .get("compatible_candidates_count")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            fields
                .get("capability_filter_applied")
                .and_then(Value::as_bool),
            Some(false)
        );
    }
}
