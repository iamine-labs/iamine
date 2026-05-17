#![allow(dead_code)]

use crate::cluster_health::{ClusterHealth, ClusterHealthThresholds};
use crate::cluster_readiness::ClusterReadinessReason;
use crate::cluster_registry::{
    ClusterBackend, ClusterExecutionMode, ClusterMetricsStatus, ClusterNode, ClusterRole,
};
use crate::scheduler_policy::{
    classify_scheduler_candidate, compare_scheduler_candidates, SchedulerCandidateClassification,
};
use crate::task_lifecycle::TaskSelectionReason;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchedulerTaskRequest {
    pub(crate) task_id: String,
    pub(crate) task_type: String,
    pub(crate) required_model_id: Option<String>,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) retry_count: u32,
    pub(crate) fallback_used: bool,
}

impl SchedulerTaskRequest {
    pub(crate) fn new(task_id: impl Into<String>, task_type: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            task_type: task_type.into(),
            required_model_id: None,
            required_capabilities: Vec::new(),
            retry_count: 0,
            fallback_used: false,
        }
    }

    pub(crate) fn with_required_model(mut self, model_id: impl Into<String>) -> Self {
        let model_id = model_id.into();
        if !model_id.trim().is_empty() {
            self.required_model_id = Some(model_id);
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchedulerCandidate {
    pub(crate) peer_id: String,
    pub(crate) hostname: String,
    pub(crate) role: ClusterRole,
    pub(crate) health: ClusterHealth,
    pub(crate) ready_for_tasks: bool,
    pub(crate) readiness_reason: ClusterReadinessReason,
    pub(crate) backend: ClusterBackend,
    pub(crate) execution_mode: ClusterExecutionMode,
    pub(crate) real_inference_available: Option<bool>,
    pub(crate) supported_task_types: Vec<String>,
    pub(crate) models_in_storage: Vec<String>,
    pub(crate) models_in_registry: Vec<String>,
    pub(crate) executable_models: Vec<String>,
    pub(crate) latency_ms: Option<u32>,
    pub(crate) metrics_status: ClusterMetricsStatus,
    pub(crate) last_seen_ms: Option<u64>,
}

impl SchedulerCandidate {
    #[allow(dead_code)]
    pub(crate) fn from_cluster_node(
        node: &ClusterNode,
        now_ms: u64,
        thresholds: ClusterHealthThresholds,
    ) -> Self {
        let readiness = node.readiness_at(now_ms, thresholds);
        Self {
            peer_id: node.peer_id.clone(),
            hostname: node.hostname.clone(),
            role: node.role,
            health: node.health_at(now_ms, thresholds),
            ready_for_tasks: readiness.ready_for_tasks,
            readiness_reason: readiness.readiness_reason,
            backend: node.backend,
            execution_mode: node.execution_mode,
            real_inference_available: node.capabilities.real_inference_available,
            supported_task_types: node.capabilities.supported_task_types.clone(),
            models_in_storage: node.capabilities.models_in_storage.clone(),
            models_in_registry: node.capabilities.models_in_registry.clone(),
            executable_models: node.capabilities.executable_models.clone(),
            latency_ms: node.latency_ms,
            metrics_status: node.metrics_status,
            last_seen_ms: node.last_seen_ms,
        }
    }

    pub(crate) fn broadcast_peer(peer_id: impl Into<String>) -> Self {
        let peer_id = peer_id.into();
        Self {
            hostname: peer_id.clone(),
            peer_id,
            role: ClusterRole::Worker,
            health: ClusterHealth::Unknown,
            ready_for_tasks: false,
            readiness_reason: ClusterReadinessReason::Unknown,
            backend: ClusterBackend::Unknown,
            execution_mode: ClusterExecutionMode::Unknown,
            real_inference_available: None,
            supported_task_types: Vec::new(),
            models_in_storage: Vec::new(),
            models_in_registry: Vec::new(),
            executable_models: Vec::new(),
            latency_ms: None,
            metrics_status: ClusterMetricsStatus::Unknown,
            last_seen_ms: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SchedulerRejectionReason {
    NotReadyForTasks,
    HealthOffline,
    HealthStale,
    UnsupportedTaskType,
    BackendIncompatible,
    RealInferenceUnavailable,
    ModelNotExecutable,
    MissingCapabilities,
    MetricsUnavailable,
    Unknown,
}

impl SchedulerRejectionReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotReadyForTasks => "not_ready_for_tasks",
            Self::HealthOffline => "health_offline",
            Self::HealthStale => "health_stale",
            Self::UnsupportedTaskType => "unsupported_task_type",
            Self::BackendIncompatible => "backend_incompatible",
            Self::RealInferenceUnavailable => "real_inference_unavailable",
            Self::ModelNotExecutable => "model_not_executable",
            Self::MissingCapabilities => "missing_capabilities",
            Self::MetricsUnavailable => "metrics_unavailable",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelectionReason {
    ReadyWorkerSupportsTask,
    ReadyWorkerSupportsModel,
    LowestLatencyReadyWorker,
    CurrentBroadcastPolicy,
    BroadcastBidSelected,
    OnlyCompatibleWorker,
    FallbackLocal,
    Unknown,
}

impl SelectionReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ReadyWorkerSupportsTask => "ready_worker_supports_task",
            Self::ReadyWorkerSupportsModel => "ready_worker_supports_model",
            Self::LowestLatencyReadyWorker => "lowest_latency_ready_worker",
            Self::CurrentBroadcastPolicy => "current_broadcast_policy",
            Self::BroadcastBidSelected => "broadcast_bid_selected",
            Self::OnlyCompatibleWorker => "only_compatible_worker",
            Self::FallbackLocal => "fallback_local",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn to_task_selection_reason(self) -> TaskSelectionReason {
        match self {
            Self::ReadyWorkerSupportsTask => TaskSelectionReason::ReadyWorkerSupportsTask,
            Self::ReadyWorkerSupportsModel => TaskSelectionReason::ReadyWorkerSupportsModel,
            Self::LowestLatencyReadyWorker => TaskSelectionReason::LowestLatencyReadyWorker,
            Self::CurrentBroadcastPolicy => TaskSelectionReason::CurrentBroadcastPolicy,
            Self::BroadcastBidSelected => TaskSelectionReason::BroadcastBidSelected,
            Self::OnlyCompatibleWorker => TaskSelectionReason::OnlyCompatibleWorker,
            Self::FallbackLocal => TaskSelectionReason::FallbackLocal,
            Self::Unknown => TaskSelectionReason::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchedulerRejectedCandidate {
    pub(crate) peer_id: String,
    pub(crate) hostname: String,
    pub(crate) reasons: Vec<SchedulerRejectionReason>,
}

impl SchedulerRejectedCandidate {
    fn from_candidate(
        candidate: &SchedulerCandidate,
        reasons: Vec<SchedulerRejectionReason>,
    ) -> Self {
        Self {
            peer_id: candidate.peer_id.clone(),
            hostname: candidate.hostname.clone(),
            reasons,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchedulerDecision {
    pub(crate) task_id: String,
    pub(crate) task_type: String,
    pub(crate) required_model_id: Option<String>,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) selected_worker_peer_id: Option<String>,
    pub(crate) selected_worker_hostname: Option<String>,
    pub(crate) selection_reason: SelectionReason,
    pub(crate) candidate_workers: Vec<SchedulerCandidate>,
    pub(crate) rejected_candidates: Vec<SchedulerRejectedCandidate>,
    pub(crate) rejected_reasons: Vec<SchedulerRejectionReason>,
    pub(crate) fallback_used: bool,
    pub(crate) retry_count: u32,
    pub(crate) decision_timestamp_ms: u64,
}

impl SchedulerDecision {
    pub(crate) fn no_compatible_worker(
        request: &SchedulerTaskRequest,
        candidate_workers: Vec<SchedulerCandidate>,
        rejected_candidates: Vec<SchedulerRejectedCandidate>,
        decision_timestamp_ms: u64,
    ) -> Self {
        let rejected_reasons = unique_rejection_reasons(&rejected_candidates);
        Self {
            task_id: request.task_id.clone(),
            task_type: request.task_type.clone(),
            required_model_id: request.required_model_id.clone(),
            required_capabilities: request.required_capabilities.clone(),
            selected_worker_peer_id: None,
            selected_worker_hostname: None,
            selection_reason: SelectionReason::Unknown,
            candidate_workers,
            rejected_candidates,
            rejected_reasons,
            fallback_used: request.fallback_used,
            retry_count: request.retry_count,
            decision_timestamp_ms,
        }
    }

    pub(crate) fn from_current_broadcast_policy(
        task_id: &str,
        task_type: &str,
        selected_worker_peer_id: &str,
        candidate_worker_ids: Vec<String>,
        selection_reason: SelectionReason,
        decision_timestamp_ms: u64,
    ) -> Self {
        let mut candidate_workers: Vec<_> = candidate_worker_ids
            .into_iter()
            .map(SchedulerCandidate::broadcast_peer)
            .collect();
        if !candidate_workers
            .iter()
            .any(|candidate| candidate.peer_id == selected_worker_peer_id)
        {
            candidate_workers.push(SchedulerCandidate::broadcast_peer(selected_worker_peer_id));
        }
        candidate_workers.sort_by(|left, right| left.peer_id.cmp(&right.peer_id));
        Self {
            task_id: task_id.to_string(),
            task_type: task_type.to_string(),
            required_model_id: None,
            required_capabilities: Vec::new(),
            selected_worker_peer_id: Some(selected_worker_peer_id.to_string()),
            selected_worker_hostname: Some(selected_worker_peer_id.to_string()),
            selection_reason,
            candidate_workers,
            rejected_candidates: Vec::new(),
            rejected_reasons: Vec::new(),
            fallback_used: false,
            retry_count: 0,
            decision_timestamp_ms,
        }
    }

    pub(crate) fn candidate_worker_ids(&self) -> Vec<String> {
        self.candidate_workers
            .iter()
            .map(|candidate| candidate.peer_id.clone())
            .collect()
    }

    pub(crate) fn rejected_candidate_ids(&self) -> Vec<String> {
        self.rejected_candidates
            .iter()
            .map(|candidate| candidate.peer_id.clone())
            .collect()
    }

    pub(crate) fn rejected_reason_strings(&self) -> Vec<String> {
        self.rejected_reasons
            .iter()
            .map(|reason| reason.as_str().to_string())
            .collect()
    }
}

#[allow(dead_code)]
pub(crate) fn build_candidates(
    nodes: &[ClusterNode],
    now_ms: u64,
    thresholds: ClusterHealthThresholds,
) -> Vec<SchedulerCandidate> {
    let mut candidates: Vec<_> = nodes
        .iter()
        .map(|node| SchedulerCandidate::from_cluster_node(node, now_ms, thresholds))
        .collect();
    candidates.sort_by(|left, right| left.peer_id.cmp(&right.peer_id));
    candidates
}

pub(crate) fn select_worker_for_task(
    request: &SchedulerTaskRequest,
    candidates: Vec<SchedulerCandidate>,
    decision_timestamp_ms: u64,
) -> SchedulerDecision {
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();

    for candidate in &candidates {
        match classify_scheduler_candidate(request, candidate) {
            SchedulerCandidateClassification::Accepted => accepted.push(candidate.clone()),
            SchedulerCandidateClassification::Rejected(reasons) => {
                rejected.push(SchedulerRejectedCandidate::from_candidate(
                    candidate, reasons,
                ));
            }
        }
    }

    accepted.sort_by(compare_scheduler_candidates);
    let Some(selected) = accepted.first() else {
        return SchedulerDecision::no_compatible_worker(
            request,
            candidates,
            rejected,
            decision_timestamp_ms,
        );
    };

    let selection_reason = selection_reason_for(request, &accepted);
    SchedulerDecision {
        task_id: request.task_id.clone(),
        task_type: request.task_type.clone(),
        required_model_id: request.required_model_id.clone(),
        required_capabilities: request.required_capabilities.clone(),
        selected_worker_peer_id: Some(selected.peer_id.clone()),
        selected_worker_hostname: Some(selected.hostname.clone()),
        selection_reason,
        candidate_workers: candidates,
        rejected_reasons: unique_rejection_reasons(&rejected),
        rejected_candidates: rejected,
        fallback_used: request.fallback_used,
        retry_count: request.retry_count,
        decision_timestamp_ms,
    }
}

fn selection_reason_for(
    request: &SchedulerTaskRequest,
    accepted: &[SchedulerCandidate],
) -> SelectionReason {
    if accepted.len() == 1 {
        return SelectionReason::OnlyCompatibleWorker;
    }
    if request.required_model_id.is_some() {
        return SelectionReason::ReadyWorkerSupportsModel;
    }
    if accepted
        .first()
        .and_then(|candidate| candidate.latency_ms)
        .is_some()
    {
        return SelectionReason::LowestLatencyReadyWorker;
    }
    SelectionReason::ReadyWorkerSupportsTask
}

fn unique_rejection_reasons(
    rejected_candidates: &[SchedulerRejectedCandidate],
) -> Vec<SchedulerRejectionReason> {
    let mut reasons = Vec::new();
    for rejected in rejected_candidates {
        for reason in &rejected.reasons {
            if !reasons.contains(reason) {
                reasons.push(*reason);
            }
        }
    }
    reasons
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(peer_id: &str) -> SchedulerCandidate {
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
            supported_task_types: vec!["reverse_string".to_string(), "test".to_string()],
            models_in_storage: Vec::new(),
            models_in_registry: Vec::new(),
            executable_models: Vec::new(),
            latency_ms: Some(20),
            metrics_status: ClusterMetricsStatus::Fallback,
            last_seen_ms: Some(100),
        }
    }

    fn request() -> SchedulerTaskRequest {
        SchedulerTaskRequest::new("task-1", "reverse_string")
    }

    #[test]
    fn scheduler_selects_real_worker_for_model_task() {
        let mut mock = candidate("peer-mock");
        mock.models_in_storage = vec!["tinyllama-1b".to_string()];
        mock.models_in_registry = vec!["tinyllama-1b".to_string()];
        let mut real = candidate("peer-real");
        real.backend = ClusterBackend::Cpu;
        real.execution_mode = ClusterExecutionMode::Real;
        real.real_inference_available = Some(true);
        real.readiness_reason = ClusterReadinessReason::RealBackendReady;
        real.supported_task_types = vec!["inference".to_string()];
        real.executable_models = vec!["tinyllama-1b".to_string()];
        let request =
            SchedulerTaskRequest::new("task-1", "inference").with_required_model("tinyllama-1b");

        let decision = select_worker_for_task(&request, vec![mock, real], 200);

        assert_eq!(
            decision.selected_worker_peer_id.as_deref(),
            Some("peer-real")
        );
        assert_eq!(
            decision.rejected_reasons,
            vec![
                SchedulerRejectionReason::BackendIncompatible,
                SchedulerRejectionReason::RealInferenceUnavailable,
                SchedulerRejectionReason::ModelNotExecutable,
            ]
        );
    }

    #[test]
    fn scheduler_records_selection_reason() {
        let mut fast = candidate("peer-a");
        fast.latency_ms = Some(5);
        let mut slow = candidate("peer-b");
        slow.latency_ms = Some(50);

        let decision = select_worker_for_task(&request(), vec![slow, fast], 200);

        assert_eq!(decision.selected_worker_peer_id.as_deref(), Some("peer-a"));
        assert_eq!(
            decision.selection_reason,
            SelectionReason::LowestLatencyReadyWorker
        );
    }

    #[test]
    fn scheduler_records_rejected_candidates() {
        let mut rejected = candidate("peer-offline");
        rejected.health = ClusterHealth::Offline;
        rejected.ready_for_tasks = false;
        let decision =
            select_worker_for_task(&request(), vec![candidate("peer-ok"), rejected], 200);

        assert_eq!(decision.rejected_candidates.len(), 1);
        assert_eq!(
            decision.rejected_candidates[0].reasons,
            vec![
                SchedulerRejectionReason::HealthOffline,
                SchedulerRejectionReason::NotReadyForTasks
            ]
        );
    }

    #[test]
    fn scheduler_decision_wraps_current_broadcast_policy() {
        let decision = SchedulerDecision::from_current_broadcast_policy(
            "task-1",
            "reverse_string",
            "worker-b",
            vec!["worker-a".to_string(), "worker-b".to_string()],
            SelectionReason::BroadcastBidSelected,
            300,
        );

        assert_eq!(
            decision.selected_worker_peer_id.as_deref(),
            Some("worker-b")
        );
        assert_eq!(decision.candidate_worker_ids().len(), 2);
        assert_eq!(
            decision.selection_reason.to_task_selection_reason(),
            TaskSelectionReason::BroadcastBidSelected
        );
    }
}
