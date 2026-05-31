#![allow(dead_code)]

use crate::cluster_health::ClusterHealth;
use crate::cluster_registry::ClusterMetricsStatus;
use crate::router_scheduler::{SchedulerCandidate, SchedulerRejectionReason, SchedulerTaskRequest};
use crate::scheduler_capability_matching::{
    scheduler_capability_rejection_reasons, SchedulerCapabilityRejectReason,
    SchedulerTaskRequirements, SchedulerWorkerCapabilityView,
};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchedulerCandidateClassification {
    Accepted,
    Rejected(Vec<SchedulerRejectionReason>),
}

pub(crate) fn classify_scheduler_candidate(
    request: &SchedulerTaskRequest,
    candidate: &SchedulerCandidate,
) -> SchedulerCandidateClassification {
    let mut reasons = Vec::new();

    if candidate.capability_snapshot_available {
        match candidate.health {
            ClusterHealth::Offline => reasons.push(SchedulerRejectionReason::HealthOffline),
            ClusterHealth::Stale => reasons.push(SchedulerRejectionReason::HealthStale),
            ClusterHealth::Unknown => reasons.push(SchedulerRejectionReason::MissingCapabilities),
            ClusterHealth::Healthy | ClusterHealth::Degraded => {}
        }
    }

    reasons.extend(
        scheduler_capability_rejection_reasons(
            &SchedulerTaskRequirements {
                required_task_type: &request.task_type,
                required_model_id: request.required_model_id.as_deref(),
            },
            &SchedulerWorkerCapabilityView {
                capability_snapshot_available: candidate.capability_snapshot_available,
                ready_for_tasks: candidate.ready_for_tasks,
                readiness_reason: candidate.readiness_reason,
                backend: candidate.backend,
                execution_mode: candidate.execution_mode,
                real_inference_available: candidate.real_inference_available,
                supported_task_types: &candidate.supported_task_types,
                executable_models: &candidate.executable_models,
                metadata_only_models: &candidate.metadata_only_models,
                unavailable_models: &candidate.unavailable_models,
            },
        )
        .into_iter()
        .map(map_capability_rejection_reason),
    );

    // Metrics fallback is intentionally non-blocking. A hard unavailable metrics state is recorded
    // only as a reason when the node is already missing readiness/capability evidence.
    if matches!(candidate.metrics_status, ClusterMetricsStatus::Unavailable)
        && !candidate.ready_for_tasks
    {
        reasons.push(SchedulerRejectionReason::MetricsUnavailable);
    }

    if reasons.is_empty() {
        SchedulerCandidateClassification::Accepted
    } else {
        reasons.dedup();
        SchedulerCandidateClassification::Rejected(reasons)
    }
}

pub(crate) fn compare_scheduler_candidates(
    left: &SchedulerCandidate,
    right: &SchedulerCandidate,
) -> Ordering {
    health_rank(left.health)
        .cmp(&health_rank(right.health))
        .then_with(|| ready_rank(left.ready_for_tasks).cmp(&ready_rank(right.ready_for_tasks)))
        .then_with(|| latency_rank(left.latency_ms).cmp(&latency_rank(right.latency_ms)))
        .then_with(|| left.peer_id.cmp(&right.peer_id))
}

fn map_capability_rejection_reason(
    reason: SchedulerCapabilityRejectReason,
) -> SchedulerRejectionReason {
    match reason {
        SchedulerCapabilityRejectReason::NotReadyForTasks => {
            SchedulerRejectionReason::NotReadyForTasks
        }
        SchedulerCapabilityRejectReason::UnsupportedTaskType => {
            SchedulerRejectionReason::UnsupportedTaskType
        }
        SchedulerCapabilityRejectReason::BackendIncompatible => {
            SchedulerRejectionReason::BackendIncompatible
        }
        SchedulerCapabilityRejectReason::RealInferenceUnavailable => {
            SchedulerRejectionReason::RealInferenceUnavailable
        }
        SchedulerCapabilityRejectReason::ModelMetadataOnly => {
            SchedulerRejectionReason::ModelMetadataOnly
        }
        SchedulerCapabilityRejectReason::ModelMissingFromStorage => {
            SchedulerRejectionReason::ModelMissingFromStorage
        }
        SchedulerCapabilityRejectReason::ModelNotExecutable => {
            SchedulerRejectionReason::ModelNotExecutable
        }
        SchedulerCapabilityRejectReason::MissingCapabilities => {
            SchedulerRejectionReason::MissingCapabilities
        }
    }
}

fn health_rank(health: ClusterHealth) -> u8 {
    match health {
        ClusterHealth::Healthy => 0,
        ClusterHealth::Degraded => 1,
        ClusterHealth::Unknown => 2,
        ClusterHealth::Stale => 3,
        ClusterHealth::Offline => 4,
    }
}

fn ready_rank(ready: bool) -> u8 {
    if ready {
        0
    } else {
        1
    }
}

fn latency_rank(latency_ms: Option<u32>) -> u32 {
    latency_ms.unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_readiness::ClusterReadinessReason;
    use crate::cluster_registry::{
        ClusterBackend, ClusterExecutionMode, ClusterMetricsStatus, ClusterRole,
    };

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
            supported_task_types: vec!["reverse_string".to_string(), "test".to_string()],
            models_in_storage: vec!["tinyllama-1b".to_string()],
            models_in_registry: vec!["tinyllama-1b".to_string()],
            executable_models: Vec::new(),
            metadata_only_models: Vec::new(),
            unavailable_models: Vec::new(),
            capability_snapshot_available: true,
            latency_ms: Some(10),
            metrics_status: ClusterMetricsStatus::Fallback,
            last_seen_ms: Some(100),
        }
    }

    fn real_candidate(peer_id: &str) -> SchedulerCandidate {
        SchedulerCandidate {
            backend: ClusterBackend::Cpu,
            execution_mode: ClusterExecutionMode::Real,
            real_inference_available: Some(true),
            readiness_reason: ClusterReadinessReason::RealBackendReady,
            supported_task_types: vec!["inference".to_string(), "reverse_string".to_string()],
            executable_models: vec!["tinyllama-1b".to_string()],
            ..mock_candidate(peer_id)
        }
    }

    #[test]
    fn scheduler_rejects_not_ready_worker() {
        let mut candidate = mock_candidate("peer-a");
        candidate.ready_for_tasks = false;
        let request = SchedulerTaskRequest::new("task-1", "reverse_string");

        let SchedulerCandidateClassification::Rejected(reasons) =
            classify_scheduler_candidate(&request, &candidate)
        else {
            panic!("candidate should be rejected");
        };

        assert!(reasons.contains(&SchedulerRejectionReason::NotReadyForTasks));
    }

    #[test]
    fn scheduler_rejects_offline_worker() {
        let mut candidate = mock_candidate("peer-a");
        candidate.health = ClusterHealth::Offline;
        let request = SchedulerTaskRequest::new("task-1", "reverse_string");

        let SchedulerCandidateClassification::Rejected(reasons) =
            classify_scheduler_candidate(&request, &candidate)
        else {
            panic!("candidate should be rejected");
        };

        assert!(reasons.contains(&SchedulerRejectionReason::HealthOffline));
    }

    #[test]
    fn scheduler_rejects_stale_worker() {
        let mut candidate = mock_candidate("peer-a");
        candidate.health = ClusterHealth::Stale;
        let request = SchedulerTaskRequest::new("task-1", "reverse_string");

        let SchedulerCandidateClassification::Rejected(reasons) =
            classify_scheduler_candidate(&request, &candidate)
        else {
            panic!("candidate should be rejected");
        };

        assert!(reasons.contains(&SchedulerRejectionReason::HealthStale));
    }

    #[test]
    fn scheduler_allows_mock_for_simple_task() {
        let request = SchedulerTaskRequest::new("task-1", "reverse_string");

        assert_eq!(
            classify_scheduler_candidate(&request, &mock_candidate("peer-a")),
            SchedulerCandidateClassification::Accepted
        );
    }

    #[test]
    fn scheduler_rejects_unsupported_task_type() {
        let request = SchedulerTaskRequest::new("task-1", "unknown_task");

        let SchedulerCandidateClassification::Rejected(reasons) =
            classify_scheduler_candidate(&request, &mock_candidate("peer-a"))
        else {
            panic!("candidate should be rejected");
        };

        assert!(reasons.contains(&SchedulerRejectionReason::UnsupportedTaskType));
    }

    #[test]
    fn scheduler_rejects_mock_for_real_inference() {
        let request =
            SchedulerTaskRequest::new("task-1", "inference").with_required_model("tinyllama-1b");

        let SchedulerCandidateClassification::Rejected(reasons) =
            classify_scheduler_candidate(&request, &mock_candidate("peer-a"))
        else {
            panic!("candidate should be rejected");
        };

        assert!(reasons.contains(&SchedulerRejectionReason::BackendIncompatible));
        assert!(reasons.contains(&SchedulerRejectionReason::RealInferenceUnavailable));
    }

    #[test]
    fn scheduler_requires_executable_model_for_inference() {
        let mut candidate = real_candidate("peer-a");
        candidate.executable_models.clear();
        let request =
            SchedulerTaskRequest::new("task-1", "inference").with_required_model("tinyllama-1b");

        let SchedulerCandidateClassification::Rejected(reasons) =
            classify_scheduler_candidate(&request, &candidate)
        else {
            panic!("candidate should be rejected");
        };

        assert!(reasons.contains(&SchedulerRejectionReason::ModelNotExecutable));
    }

    #[test]
    fn scheduler_metrics_fallback_not_blocking() {
        let request = SchedulerTaskRequest::new("task-1", "reverse_string");
        let mut candidate = mock_candidate("peer-a");
        candidate.metrics_status = ClusterMetricsStatus::Fallback;

        assert_eq!(
            classify_scheduler_candidate(&request, &candidate),
            SchedulerCandidateClassification::Accepted
        );
    }

    #[test]
    fn scheduler_deterministic_tie_breaker() {
        let mut a = mock_candidate("peer-a");
        a.latency_ms = Some(10);
        let mut b = mock_candidate("peer-b");
        b.latency_ms = Some(10);

        assert_eq!(compare_scheduler_candidates(&a, &b), Ordering::Less);
        assert_eq!(compare_scheduler_candidates(&b, &a), Ordering::Greater);
    }
}
