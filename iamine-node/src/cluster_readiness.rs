use crate::cluster_health::ClusterHealth;
use crate::cluster_registry::{
    ClusterBackend, ClusterExecutionMode, ClusterMetricsStatus, ClusterRole,
};
use serde::{Deserialize, Serialize};

const SIMPLE_TASK_TYPES: &[&str] = &["reverse_string", "test", "echo"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterReadinessReason {
    WorkerReady,
    WorkerPubsubReady,
    MockSimpleTasksReady,
    RealBackendReady,
    MetricsUnavailableNonBlocking,
    MissingCapabilities,
    StaleNode,
    OfflineNode,
    RealBackendUnavailable,
    NoSupportedTaskTypes,
    NoExecutableModels,
    StartupIncomplete,
    Unknown,
}

impl ClusterReadinessReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WorkerReady => "worker_ready",
            Self::WorkerPubsubReady => "worker_pubsub_ready",
            Self::MockSimpleTasksReady => "mock_simple_tasks_ready",
            Self::RealBackendReady => "real_backend_ready",
            Self::MetricsUnavailableNonBlocking => "metrics_unavailable_non_blocking",
            Self::MissingCapabilities => "missing_capabilities",
            Self::StaleNode => "stale_node",
            Self::OfflineNode => "offline_node",
            Self::RealBackendUnavailable => "real_backend_unavailable",
            Self::NoSupportedTaskTypes => "no_supported_task_types",
            Self::NoExecutableModels => "no_executable_models",
            Self::StartupIncomplete => "startup_incomplete",
            Self::Unknown => "unknown",
        }
    }
}

impl Default for ClusterReadinessReason {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterReadiness {
    pub(crate) ready_for_tasks: bool,
    pub(crate) readiness_reason: ClusterReadinessReason,
}

impl ClusterReadiness {
    pub(crate) fn not_ready(readiness_reason: ClusterReadinessReason) -> Self {
        Self {
            ready_for_tasks: false,
            readiness_reason,
        }
    }

    pub(crate) fn ready(readiness_reason: ClusterReadinessReason) -> Self {
        Self {
            ready_for_tasks: true,
            readiness_reason,
        }
    }
}

impl Default for ClusterReadiness {
    fn default() -> Self {
        Self::not_ready(ClusterReadinessReason::Unknown)
    }
}

pub(crate) struct ClusterReadinessInput<'a> {
    pub(crate) role: ClusterRole,
    pub(crate) execution_mode: ClusterExecutionMode,
    pub(crate) backend: ClusterBackend,
    pub(crate) health: ClusterHealth,
    pub(crate) metrics_status: ClusterMetricsStatus,
    pub(crate) real_inference_available: Option<bool>,
    pub(crate) supported_task_types: &'a [String],
    pub(crate) executable_models: &'a [String],
}

pub(crate) fn derive_cluster_readiness(input: &ClusterReadinessInput<'_>) -> ClusterReadiness {
    match input.health {
        ClusterHealth::Offline => {
            return ClusterReadiness::not_ready(ClusterReadinessReason::OfflineNode);
        }
        ClusterHealth::Stale => {
            return ClusterReadiness::not_ready(ClusterReadinessReason::StaleNode);
        }
        ClusterHealth::Unknown => {
            return ClusterReadiness::not_ready(ClusterReadinessReason::Unknown);
        }
        ClusterHealth::Healthy | ClusterHealth::Degraded => {}
    }

    if !matches!(input.role, ClusterRole::Worker) {
        return ClusterReadiness::not_ready(ClusterReadinessReason::StartupIncomplete);
    }

    let has_simple_task = input
        .supported_task_types
        .iter()
        .any(|task| SIMPLE_TASK_TYPES.contains(&task.as_str()));
    let mock_mode = matches!(input.backend, ClusterBackend::Mock)
        || matches!(input.execution_mode, ClusterExecutionMode::Mock);
    let real_mode = matches!(
        input.backend,
        ClusterBackend::Cpu | ClusterBackend::Metal | ClusterBackend::Cuda | ClusterBackend::Real
    ) || matches!(input.execution_mode, ClusterExecutionMode::Real);

    if mock_mode {
        return if has_simple_task {
            ClusterReadiness::ready(ClusterReadinessReason::MockSimpleTasksReady)
        } else {
            ClusterReadiness::not_ready(ClusterReadinessReason::NoSupportedTaskTypes)
        };
    }

    if real_mode {
        if input.real_inference_available != Some(true) {
            return if matches!(input.health, ClusterHealth::Degraded) && has_simple_task {
                ClusterReadiness::ready(ClusterReadinessReason::WorkerReady)
            } else {
                ClusterReadiness::not_ready(ClusterReadinessReason::RealBackendUnavailable)
            };
        }
        return if input.executable_models.is_empty() {
            ClusterReadiness::not_ready(ClusterReadinessReason::NoExecutableModels)
        } else {
            ClusterReadiness::ready(ClusterReadinessReason::RealBackendReady)
        };
    }

    if input.supported_task_types.is_empty() {
        return ClusterReadiness::not_ready(ClusterReadinessReason::MissingCapabilities);
    }

    if matches!(input.metrics_status, ClusterMetricsStatus::Fallback) && has_simple_task {
        return ClusterReadiness::ready(ClusterReadinessReason::MetricsUnavailableNonBlocking);
    }

    ClusterReadiness::not_ready(ClusterReadinessReason::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        backend: ClusterBackend,
        execution_mode: ClusterExecutionMode,
        health: ClusterHealth,
        real_inference_available: Option<bool>,
        supported_task_types: &[&str],
        executable_models: &[&str],
        metrics_status: ClusterMetricsStatus,
    ) -> ClusterReadinessInput<'static> {
        ClusterReadinessInput {
            role: ClusterRole::Worker,
            execution_mode,
            backend,
            health,
            metrics_status,
            real_inference_available,
            supported_task_types: Box::leak(Box::new(
                supported_task_types
                    .iter()
                    .map(|task| task.to_string())
                    .collect::<Vec<_>>(),
            )),
            executable_models: Box::leak(Box::new(
                executable_models
                    .iter()
                    .map(|model| model.to_string())
                    .collect::<Vec<_>>(),
            )),
        }
    }

    #[test]
    fn cluster_mock_worker_ready_for_simple_tasks() {
        let readiness = derive_cluster_readiness(&input(
            ClusterBackend::Mock,
            ClusterExecutionMode::Mock,
            ClusterHealth::Healthy,
            Some(false),
            &["reverse_string", "test", "echo"],
            &[],
            ClusterMetricsStatus::Fallback,
        ));

        assert!(readiness.ready_for_tasks);
        assert_eq!(
            readiness.readiness_reason,
            ClusterReadinessReason::MockSimpleTasksReady
        );
    }

    #[test]
    fn cluster_mock_worker_not_ready_for_real_llm() {
        let readiness = derive_cluster_readiness(&input(
            ClusterBackend::Mock,
            ClusterExecutionMode::Mock,
            ClusterHealth::Healthy,
            Some(false),
            &[],
            &[],
            ClusterMetricsStatus::Fallback,
        ));

        assert!(!readiness.ready_for_tasks);
        assert_ne!(
            readiness.readiness_reason,
            ClusterReadinessReason::RealBackendReady
        );
    }

    #[test]
    fn cluster_real_cpu_worker_ready_for_inference() {
        let readiness = derive_cluster_readiness(&input(
            ClusterBackend::Cpu,
            ClusterExecutionMode::Real,
            ClusterHealth::Healthy,
            Some(true),
            &["inference"],
            &["tinyllama-1b"],
            ClusterMetricsStatus::Available,
        ));

        assert!(readiness.ready_for_tasks);
        assert_eq!(
            readiness.readiness_reason,
            ClusterReadinessReason::RealBackendReady
        );
    }

    #[test]
    fn cluster_metrics_fallback_does_not_block_readiness() {
        let readiness = derive_cluster_readiness(&input(
            ClusterBackend::Mock,
            ClusterExecutionMode::Mock,
            ClusterHealth::Healthy,
            Some(false),
            &["reverse_string"],
            &[],
            ClusterMetricsStatus::Fallback,
        ));

        assert!(readiness.ready_for_tasks);
    }

    #[test]
    fn cluster_offline_node_not_ready() {
        let readiness = derive_cluster_readiness(&input(
            ClusterBackend::Cpu,
            ClusterExecutionMode::Real,
            ClusterHealth::Offline,
            Some(true),
            &["inference"],
            &["tinyllama-1b"],
            ClusterMetricsStatus::Available,
        ));

        assert!(!readiness.ready_for_tasks);
        assert_eq!(
            readiness.readiness_reason,
            ClusterReadinessReason::OfflineNode
        );
    }

    #[test]
    fn cluster_stale_node_not_ready_by_default() {
        let readiness = derive_cluster_readiness(&input(
            ClusterBackend::Mock,
            ClusterExecutionMode::Mock,
            ClusterHealth::Stale,
            Some(false),
            &["reverse_string"],
            &[],
            ClusterMetricsStatus::Fallback,
        ));

        assert!(!readiness.ready_for_tasks);
        assert_eq!(
            readiness.readiness_reason,
            ClusterReadinessReason::StaleNode
        );
    }
}
