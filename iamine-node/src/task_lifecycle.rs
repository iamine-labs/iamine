use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskLifecycleStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

impl TaskLifecycleStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Assigned => "assigned",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Retrying => "retrying",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }

        matches!(
            (self, next),
            (Self::Pending, Self::Assigned)
                | (Self::Pending, Self::Running)
                | (Self::Pending, Self::Cancelled)
                | (Self::Pending, Self::Failed)
                | (Self::Assigned, Self::Running)
                | (Self::Assigned, Self::Completed)
                | (Self::Assigned, Self::Failed)
                | (Self::Assigned, Self::Cancelled)
                | (Self::Running, Self::Completed)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Cancelled)
                | (Self::Failed, Self::Retrying)
                | (Self::Retrying, Self::Assigned)
        )
    }
}

impl fmt::Display for TaskLifecycleStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskSelectionReason {
    BroadcastBidSelected,
    FirstValidBid,
    HighestCapacityBid,
    LowestLatencyBid,
    ManualAssignment,
    FallbackLocal,
    CurrentBroadcastPolicy,
    Unknown,
}

impl TaskSelectionReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::BroadcastBidSelected => "broadcast_bid_selected",
            Self::FirstValidBid => "first_valid_bid",
            Self::HighestCapacityBid => "highest_capacity_bid",
            Self::LowestLatencyBid => "lowest_latency_bid",
            Self::ManualAssignment => "manual_assignment",
            Self::FallbackLocal => "fallback_local",
            Self::CurrentBroadcastPolicy => "current_broadcast_policy",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskLifecycleErrorCode {
    TaskTimeout,
    WorkerUnavailable,
    WorkerRejected,
    ResultTimeout,
    WrongWorkerResult,
    DuplicateResult,
    ModelUnavailable,
    BackendUnavailable,
    InsufficientCapabilities,
    NoWorkersAvailable,
    UnknownError,
}

impl TaskLifecycleErrorCode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TaskTimeout => "task_timeout",
            Self::WorkerUnavailable => "worker_unavailable",
            Self::WorkerRejected => "worker_rejected",
            Self::ResultTimeout => "result_timeout",
            Self::WrongWorkerResult => "wrong_worker_result",
            Self::DuplicateResult => "duplicate_result",
            Self::ModelUnavailable => "model_unavailable",
            Self::BackendUnavailable => "backend_unavailable",
            Self::InsufficientCapabilities => "insufficient_capabilities",
            Self::NoWorkersAvailable => "no_workers_available",
            Self::UnknownError => "unknown_error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskLifecycleTransitionError {
    pub(crate) task_id: String,
    pub(crate) from: TaskLifecycleStatus,
    pub(crate) to: TaskLifecycleStatus,
}

impl fmt::Display for TaskLifecycleTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid task lifecycle transition task_id={} from={} to={}",
            self.task_id, self.from, self.to
        )
    }
}

impl std::error::Error for TaskLifecycleTransitionError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TaskLifecycleRecord {
    pub(crate) task_id: String,
    pub(crate) task_type: Option<String>,
    pub(crate) input_summary: Option<String>,
    pub(crate) input_hash: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) assigned_node: Option<String>,
    pub(crate) selected_worker: Option<String>,
    pub(crate) candidate_workers: Vec<String>,
    pub(crate) selection_reason: Option<TaskSelectionReason>,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) declared_capabilities: Vec<String>,
    pub(crate) model_id: Option<String>,
    pub(crate) model_hash: Option<String>,
    pub(crate) status: TaskLifecycleStatus,
    pub(crate) started_at_ms: Option<u64>,
    pub(crate) finished_at_ms: Option<u64>,
    pub(crate) latency_ms: Option<u64>,
    pub(crate) retry_count: u32,
    pub(crate) fallback_used: bool,
    pub(crate) fallback_reason: Option<String>,
    pub(crate) error_code: Option<TaskLifecycleErrorCode>,
    pub(crate) error_message: Option<String>,
    pub(crate) result_location: Option<String>,
    pub(crate) result_summary: Option<String>,
    pub(crate) final_outcome: Option<String>,
}

impl TaskLifecycleRecord {
    pub(crate) fn new(
        task_id: impl Into<String>,
        task_type: impl Into<String>,
        input_summary: Option<String>,
        input_hash: Option<String>,
        created_at_ms: u64,
    ) -> Self {
        let task_type = task_type.into();
        Self {
            task_id: task_id.into(),
            task_type: non_empty(task_type),
            input_summary,
            input_hash,
            created_at_ms,
            assigned_node: None,
            selected_worker: None,
            candidate_workers: Vec::new(),
            selection_reason: None,
            required_capabilities: Vec::new(),
            declared_capabilities: Vec::new(),
            model_id: None,
            model_hash: None,
            status: TaskLifecycleStatus::Pending,
            started_at_ms: None,
            finished_at_ms: None,
            latency_ms: None,
            retry_count: 0,
            fallback_used: false,
            fallback_reason: None,
            error_code: None,
            error_message: None,
            result_location: None,
            result_summary: None,
            final_outcome: None,
        }
    }

    pub(crate) fn seed_from_event(event: &TaskLifecycleEvent) -> Self {
        Self::new(
            event.task_id.clone(),
            event.task_type.clone().unwrap_or_default(),
            event.input_summary.clone(),
            event.input_hash.clone(),
            event.timestamp_ms,
        )
    }

    pub(crate) fn transition_to(
        &mut self,
        next: TaskLifecycleStatus,
        now_ms: u64,
    ) -> Result<(), TaskLifecycleTransitionError> {
        if !self.status.can_transition_to(next) {
            return Err(TaskLifecycleTransitionError {
                task_id: self.task_id.clone(),
                from: self.status,
                to: next,
            });
        }

        self.status = next;
        match next {
            TaskLifecycleStatus::Running => {
                self.started_at_ms.get_or_insert(now_ms);
            }
            TaskLifecycleStatus::Completed
            | TaskLifecycleStatus::Failed
            | TaskLifecycleStatus::Cancelled => {
                self.finished_at_ms.get_or_insert(now_ms);
                self.refresh_latency();
            }
            TaskLifecycleStatus::Retrying => {
                self.retry_count = self.retry_count.saturating_add(1);
            }
            TaskLifecycleStatus::Pending | TaskLifecycleStatus::Assigned => {}
        }
        Ok(())
    }

    pub(crate) fn apply_event(
        &mut self,
        event: &TaskLifecycleEvent,
    ) -> Result<(), TaskLifecycleTransitionError> {
        self.transition_to(event.status, event.timestamp_ms)?;
        merge_option(&mut self.task_type, event.task_type.clone());
        merge_option(&mut self.input_summary, event.input_summary.clone());
        merge_option(&mut self.input_hash, event.input_hash.clone());
        merge_option(
            &mut self.assigned_node,
            event.assigned_worker_peer_id.clone(),
        );
        merge_option(
            &mut self.selected_worker,
            event.selected_worker_peer_id.clone(),
        );
        merge_option(&mut self.model_id, event.model_id.clone());
        merge_option(&mut self.model_hash, event.model_hash.clone());
        merge_option(&mut self.fallback_reason, event.fallback_reason.clone());
        merge_option(&mut self.error_message, event.error_message.clone());
        merge_option(&mut self.result_location, event.result_location.clone());
        merge_option(&mut self.result_summary, event.result_summary.clone());
        merge_option(&mut self.final_outcome, event.final_outcome.clone());

        if !event.candidate_workers.is_empty() {
            self.candidate_workers = event.candidate_workers.clone();
        }
        if let Some(selection_reason) = event.selection_reason {
            self.selection_reason = Some(selection_reason);
        }
        if !event.required_capabilities.is_empty() {
            self.required_capabilities = event.required_capabilities.clone();
        }
        if !event.declared_capabilities.is_empty() {
            self.declared_capabilities = event.declared_capabilities.clone();
        }
        if event.retry_count > self.retry_count {
            self.retry_count = event.retry_count;
        }
        if event.fallback_used {
            self.fallback_used = true;
        }
        if let Some(error_code) = event.error_code {
            self.error_code = Some(error_code);
        }
        if let Some(latency_ms) = event.latency_ms {
            self.latency_ms = Some(latency_ms);
        }
        if matches!(
            event.status,
            TaskLifecycleStatus::Completed
                | TaskLifecycleStatus::Failed
                | TaskLifecycleStatus::Cancelled
        ) {
            self.finished_at_ms = Some(event.timestamp_ms);
            self.refresh_latency();
        }
        Ok(())
    }

    fn refresh_latency(&mut self) {
        if self.latency_ms.is_none() {
            if let Some(finished_at_ms) = self.finished_at_ms {
                self.latency_ms = Some(finished_at_ms.saturating_sub(self.created_at_ms));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TaskLifecycleEvent {
    pub(crate) event: String,
    pub(crate) task_id: String,
    pub(crate) task_type: Option<String>,
    pub(crate) status: TaskLifecycleStatus,
    pub(crate) timestamp_ms: u64,
    pub(crate) worker_peer_id: Option<String>,
    pub(crate) assigned_worker_peer_id: Option<String>,
    pub(crate) selected_worker_peer_id: Option<String>,
    pub(crate) candidate_workers: Vec<String>,
    pub(crate) selection_reason: Option<TaskSelectionReason>,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) declared_capabilities: Vec<String>,
    pub(crate) model_id: Option<String>,
    pub(crate) model_hash: Option<String>,
    pub(crate) input_summary: Option<String>,
    pub(crate) input_hash: Option<String>,
    pub(crate) retry_count: u32,
    pub(crate) fallback_used: bool,
    pub(crate) fallback_reason: Option<String>,
    pub(crate) latency_ms: Option<u64>,
    pub(crate) error_code: Option<TaskLifecycleErrorCode>,
    pub(crate) error_message: Option<String>,
    pub(crate) result_location: Option<String>,
    pub(crate) result_summary: Option<String>,
    pub(crate) final_outcome: Option<String>,
}

impl TaskLifecycleEvent {
    pub(crate) fn new(
        event: impl Into<String>,
        task_id: impl Into<String>,
        status: TaskLifecycleStatus,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            event: event.into(),
            task_id: task_id.into(),
            task_type: None,
            status,
            timestamp_ms,
            worker_peer_id: None,
            assigned_worker_peer_id: None,
            selected_worker_peer_id: None,
            candidate_workers: Vec::new(),
            selection_reason: None,
            required_capabilities: Vec::new(),
            declared_capabilities: Vec::new(),
            model_id: None,
            model_hash: None,
            input_summary: None,
            input_hash: None,
            retry_count: 0,
            fallback_used: false,
            fallback_reason: None,
            latency_ms: None,
            error_code: None,
            error_message: None,
            result_location: None,
            result_summary: None,
            final_outcome: None,
        }
    }

    pub(crate) fn with_task_type(mut self, task_type: impl Into<String>) -> Self {
        self.task_type = non_empty(task_type.into());
        self
    }

    pub(crate) fn with_input(
        mut self,
        input_summary: Option<String>,
        input_hash: Option<String>,
    ) -> Self {
        self.input_summary = input_summary;
        self.input_hash = input_hash;
        self
    }

    pub(crate) fn with_worker(mut self, worker_peer_id: impl Into<String>) -> Self {
        self.worker_peer_id = non_empty(worker_peer_id.into());
        self
    }

    pub(crate) fn with_assignment(
        mut self,
        assigned_worker_peer_id: impl Into<String>,
        candidate_workers: Vec<String>,
        selection_reason: TaskSelectionReason,
    ) -> Self {
        let assigned = assigned_worker_peer_id.into();
        self.assigned_worker_peer_id = non_empty(assigned.clone());
        self.selected_worker_peer_id = non_empty(assigned);
        self.candidate_workers = candidate_workers;
        self.selection_reason = Some(selection_reason);
        self
    }

    pub(crate) fn with_result(
        mut self,
        success: bool,
        result_summary: Option<String>,
        latency_ms: Option<u64>,
        final_outcome: Option<String>,
    ) -> Self {
        self.result_summary = result_summary;
        self.latency_ms = latency_ms;
        if let Some(final_outcome) = final_outcome {
            self.final_outcome = Some(final_outcome);
        } else if success {
            self.final_outcome = Some("success".to_string());
        }
        self
    }

    pub(crate) fn with_error(
        mut self,
        error_code: TaskLifecycleErrorCode,
        error_message: impl Into<String>,
    ) -> Self {
        self.error_code = Some(error_code);
        self.error_message = non_empty(error_message.into());
        self
    }

    pub(crate) fn with_retry(mut self, retry_count: u32, fallback_reason: Option<String>) -> Self {
        self.retry_count = retry_count;
        self.fallback_used = fallback_reason.is_some();
        self.fallback_reason = fallback_reason;
        self
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn merge_option<T>(target: &mut Option<T>, value: Option<T>) {
    if value.is_some() {
        *target = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> TaskLifecycleRecord {
        TaskLifecycleRecord::new(
            "task-1",
            "reverse_string",
            Some("hello".to_string()),
            Some("hash".to_string()),
            100,
        )
    }

    #[test]
    fn task_lifecycle_starts_pending() {
        let record = record();

        assert_eq!(record.status, TaskLifecycleStatus::Pending);
        assert_eq!(record.task_type.as_deref(), Some("reverse_string"));
    }

    #[test]
    fn task_lifecycle_pending_to_assigned() {
        let mut record = record();

        record
            .transition_to(TaskLifecycleStatus::Assigned, 120)
            .expect("pending -> assigned should be valid");

        assert_eq!(record.status, TaskLifecycleStatus::Assigned);
    }

    #[test]
    fn task_lifecycle_assigned_to_running() {
        let mut record = record();
        record
            .transition_to(TaskLifecycleStatus::Assigned, 120)
            .unwrap();

        record
            .transition_to(TaskLifecycleStatus::Running, 140)
            .expect("assigned -> running should be valid");

        assert_eq!(record.status, TaskLifecycleStatus::Running);
        assert_eq!(record.started_at_ms, Some(140));
    }

    #[test]
    fn task_lifecycle_running_to_completed() {
        let mut record = record();
        record
            .transition_to(TaskLifecycleStatus::Assigned, 120)
            .unwrap();
        record
            .transition_to(TaskLifecycleStatus::Running, 140)
            .unwrap();

        record
            .transition_to(TaskLifecycleStatus::Completed, 190)
            .expect("running -> completed should be valid");

        assert_eq!(record.status, TaskLifecycleStatus::Completed);
        assert_eq!(record.finished_at_ms, Some(190));
        assert_eq!(record.latency_ms, Some(90));
    }

    #[test]
    fn task_lifecycle_running_to_failed() {
        let mut record = record();
        record
            .transition_to(TaskLifecycleStatus::Assigned, 120)
            .unwrap();
        record
            .transition_to(TaskLifecycleStatus::Running, 140)
            .unwrap();

        record
            .transition_to(TaskLifecycleStatus::Failed, 190)
            .expect("running -> failed should be valid");

        assert_eq!(record.status, TaskLifecycleStatus::Failed);
    }

    #[test]
    fn task_lifecycle_failed_to_retrying() {
        let mut record = record();
        record
            .transition_to(TaskLifecycleStatus::Failed, 140)
            .unwrap();

        record
            .transition_to(TaskLifecycleStatus::Retrying, 150)
            .expect("failed -> retrying should be valid");

        assert_eq!(record.status, TaskLifecycleStatus::Retrying);
        assert_eq!(record.retry_count, 1);
    }

    #[test]
    fn task_lifecycle_invalid_transition_is_controlled() {
        let mut record = record();

        let error = record
            .transition_to(TaskLifecycleStatus::Completed, 150)
            .expect_err("pending -> completed should be invalid");

        assert_eq!(error.from, TaskLifecycleStatus::Pending);
        assert_eq!(error.to, TaskLifecycleStatus::Completed);
        assert_eq!(record.status, TaskLifecycleStatus::Pending);
    }

    #[test]
    fn task_lifecycle_event_applies_assignment_fields() {
        let event = TaskLifecycleEvent::new(
            "task_lifecycle_assigned",
            "task-1",
            TaskLifecycleStatus::Assigned,
            125,
        )
        .with_task_type("reverse_string")
        .with_assignment(
            "worker-a",
            vec!["worker-a".to_string(), "worker-b".to_string()],
            TaskSelectionReason::CurrentBroadcastPolicy,
        );
        let mut record = record();

        record.apply_event(&event).unwrap();

        assert_eq!(record.status, TaskLifecycleStatus::Assigned);
        assert_eq!(record.assigned_node.as_deref(), Some("worker-a"));
        assert_eq!(
            record.selection_reason,
            Some(TaskSelectionReason::CurrentBroadcastPolicy)
        );
        assert_eq!(record.candidate_workers.len(), 2);
    }
}
