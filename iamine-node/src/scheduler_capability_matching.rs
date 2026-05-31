use crate::cluster_readiness::ClusterReadinessReason;
use crate::cluster_registry::{ClusterBackend, ClusterExecutionMode};
use crate::model_capability_consistency::{ModelCapabilityStatus, ModelExecutionStatus};

const SIMPLE_TASK_TYPES: &[&str] = &["reverse_string", "test", "echo"];
const INFERENCE_TASK_TYPES: &[&str] = &["inference", "infer", "llm", "generate"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchedulerCapabilityRejectReason {
    NotReadyForTasks,
    UnsupportedTaskType,
    BackendIncompatible,
    RealInferenceUnavailable,
    ModelMetadataOnly,
    ModelMissingFromStorage,
    ModelNotExecutable,
    MissingCapabilities,
}

pub(crate) struct SchedulerTaskRequirements<'a> {
    pub(crate) required_task_type: &'a str,
    pub(crate) required_model_id: Option<&'a str>,
}

impl SchedulerTaskRequirements<'_> {
    fn requires_real_inference(&self) -> bool {
        self.required_model_id.is_some() || is_inference_task(self.required_task_type)
    }
}

pub(crate) struct SchedulerWorkerCapabilityView<'a> {
    pub(crate) capability_snapshot_available: bool,
    pub(crate) ready_for_tasks: bool,
    pub(crate) readiness_reason: ClusterReadinessReason,
    pub(crate) backend: ClusterBackend,
    pub(crate) execution_mode: ClusterExecutionMode,
    pub(crate) real_inference_available: Option<bool>,
    pub(crate) supported_task_types: &'a [String],
    pub(crate) executable_models: &'a [String],
    pub(crate) metadata_only_models: &'a [String],
    pub(crate) unavailable_models: &'a [ModelCapabilityStatus],
}

pub(crate) fn scheduler_capability_rejection_reasons(
    requirements: &SchedulerTaskRequirements<'_>,
    worker: &SchedulerWorkerCapabilityView<'_>,
) -> Vec<SchedulerCapabilityRejectReason> {
    if !worker.capability_snapshot_available {
        return if is_simple_task(requirements.required_task_type)
            && requirements.required_model_id.is_none()
        {
            Vec::new()
        } else {
            vec![SchedulerCapabilityRejectReason::MissingCapabilities]
        };
    }

    let mut reasons = Vec::new();
    if !worker.ready_for_tasks || readiness_blocks_tasks(worker.readiness_reason) {
        reasons.push(SchedulerCapabilityRejectReason::NotReadyForTasks);
    }
    if !supports_task(worker, requirements.required_task_type) {
        reasons.push(SchedulerCapabilityRejectReason::UnsupportedTaskType);
    }

    if requirements.requires_real_inference() {
        if !is_real_backend(worker) {
            reasons.push(SchedulerCapabilityRejectReason::BackendIncompatible);
        }
        if worker.real_inference_available != Some(true) {
            reasons.push(SchedulerCapabilityRejectReason::RealInferenceUnavailable);
        }
    }

    if let Some(model_id) = requirements.required_model_id {
        if !worker
            .executable_models
            .iter()
            .any(|model| model == model_id)
        {
            reasons.push(model_rejection_reason(worker, model_id));
        }
    } else if requirements.requires_real_inference() && worker.executable_models.is_empty() {
        reasons.push(SchedulerCapabilityRejectReason::ModelNotExecutable);
    }

    reasons.dedup();
    reasons
}

pub(crate) fn is_simple_task(task_type: &str) -> bool {
    SIMPLE_TASK_TYPES.contains(&task_type)
}

pub(crate) fn is_inference_task(task_type: &str) -> bool {
    INFERENCE_TASK_TYPES.contains(&task_type)
}

fn supports_task(worker: &SchedulerWorkerCapabilityView<'_>, task_type: &str) -> bool {
    worker
        .supported_task_types
        .iter()
        .any(|supported| supported == task_type)
}

fn is_real_backend(worker: &SchedulerWorkerCapabilityView<'_>) -> bool {
    matches!(
        worker.backend,
        ClusterBackend::Cpu | ClusterBackend::Metal | ClusterBackend::Cuda | ClusterBackend::Real
    ) || matches!(worker.execution_mode, ClusterExecutionMode::Real)
}

fn readiness_blocks_tasks(readiness_reason: ClusterReadinessReason) -> bool {
    matches!(
        readiness_reason,
        ClusterReadinessReason::MissingCapabilities
            | ClusterReadinessReason::StaleNode
            | ClusterReadinessReason::OfflineNode
            | ClusterReadinessReason::RealBackendUnavailable
            | ClusterReadinessReason::NoSupportedTaskTypes
            | ClusterReadinessReason::NoExecutableModels
            | ClusterReadinessReason::StartupIncomplete
            | ClusterReadinessReason::Unknown
    )
}

fn model_rejection_reason(
    worker: &SchedulerWorkerCapabilityView<'_>,
    model_id: &str,
) -> SchedulerCapabilityRejectReason {
    if worker.unavailable_models.iter().any(|model| {
        model.model_id == model_id
            && matches!(
                model.execution_status,
                ModelExecutionStatus::MissingFromStorage
            )
    }) {
        SchedulerCapabilityRejectReason::ModelMissingFromStorage
    } else if worker
        .metadata_only_models
        .iter()
        .any(|model| model == model_id)
    {
        SchedulerCapabilityRejectReason::ModelMetadataOnly
    } else {
        SchedulerCapabilityRejectReason::ModelNotExecutable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model_status(
        model_id: &str,
        execution_status: ModelExecutionStatus,
    ) -> ModelCapabilityStatus {
        ModelCapabilityStatus {
            model_id: model_id.to_string(),
            known_by_registry: true,
            present_in_storage: !matches!(
                execution_status,
                ModelExecutionStatus::MissingFromStorage
            ),
            executable: matches!(execution_status, ModelExecutionStatus::Executable),
            execution_status,
            reason: execution_status.as_str().to_string(),
        }
    }

    fn mock_worker<'a>(
        supported_task_types: &'a [String],
        executable_models: &'a [String],
        metadata_only_models: &'a [String],
        unavailable_models: &'a [ModelCapabilityStatus],
    ) -> SchedulerWorkerCapabilityView<'a> {
        SchedulerWorkerCapabilityView {
            capability_snapshot_available: true,
            ready_for_tasks: true,
            readiness_reason: ClusterReadinessReason::MockSimpleTasksReady,
            backend: ClusterBackend::Mock,
            execution_mode: ClusterExecutionMode::Mock,
            real_inference_available: Some(false),
            supported_task_types,
            executable_models,
            metadata_only_models,
            unavailable_models,
        }
    }

    #[test]
    fn scheduler_allows_mock_simple_task() {
        let tasks = vec!["reverse_string".to_string(), "test".to_string()];
        let worker = mock_worker(&tasks, &[], &[], &[]);

        assert!(scheduler_capability_rejection_reasons(
            &SchedulerTaskRequirements {
                required_task_type: "reverse_string",
                required_model_id: None,
            },
            &worker,
        )
        .is_empty());
    }

    #[test]
    fn scheduler_rejects_mock_for_real_inference() {
        let tasks = vec!["inference".to_string()];
        let worker = mock_worker(&tasks, &[], &[], &[]);
        let reasons = scheduler_capability_rejection_reasons(
            &SchedulerTaskRequirements {
                required_task_type: "inference",
                required_model_id: Some("tinyllama-1b"),
            },
            &worker,
        );

        assert!(reasons.contains(&SchedulerCapabilityRejectReason::BackendIncompatible));
        assert!(reasons.contains(&SchedulerCapabilityRejectReason::RealInferenceUnavailable));
        assert!(reasons.contains(&SchedulerCapabilityRejectReason::ModelNotExecutable));
    }

    #[test]
    fn scheduler_rejects_metadata_only_model() {
        let tasks = vec!["inference".to_string()];
        let metadata_only = vec!["tinyllama-1b".to_string()];
        let unavailable = vec![model_status(
            "tinyllama-1b",
            ModelExecutionStatus::MetadataOnly,
        )];
        let worker = SchedulerWorkerCapabilityView {
            backend: ClusterBackend::Cpu,
            execution_mode: ClusterExecutionMode::Real,
            real_inference_available: Some(true),
            metadata_only_models: &metadata_only,
            unavailable_models: &unavailable,
            ..mock_worker(&tasks, &[], &metadata_only, &unavailable)
        };

        assert_eq!(
            scheduler_capability_rejection_reasons(
                &SchedulerTaskRequirements {
                    required_task_type: "inference",
                    required_model_id: Some("tinyllama-1b"),
                },
                &worker,
            ),
            vec![SchedulerCapabilityRejectReason::ModelMetadataOnly]
        );
    }

    #[test]
    fn scheduler_rejects_missing_storage_model() {
        let tasks = vec!["inference".to_string()];
        let metadata_only = vec!["llama3-3b".to_string()];
        let unavailable = vec![model_status(
            "llama3-3b",
            ModelExecutionStatus::MissingFromStorage,
        )];
        let worker = SchedulerWorkerCapabilityView {
            backend: ClusterBackend::Cpu,
            execution_mode: ClusterExecutionMode::Real,
            real_inference_available: Some(true),
            metadata_only_models: &metadata_only,
            unavailable_models: &unavailable,
            ..mock_worker(&tasks, &[], &metadata_only, &unavailable)
        };

        assert_eq!(
            scheduler_capability_rejection_reasons(
                &SchedulerTaskRequirements {
                    required_task_type: "inference",
                    required_model_id: Some("llama3-3b"),
                },
                &worker,
            ),
            vec![SchedulerCapabilityRejectReason::ModelMissingFromStorage]
        );
    }

    #[test]
    fn scheduler_rejects_not_ready_worker() {
        let tasks = vec!["reverse_string".to_string()];
        let worker = SchedulerWorkerCapabilityView {
            ready_for_tasks: false,
            readiness_reason: ClusterReadinessReason::StartupIncomplete,
            ..mock_worker(&tasks, &[], &[], &[])
        };

        assert_eq!(
            scheduler_capability_rejection_reasons(
                &SchedulerTaskRequirements {
                    required_task_type: "reverse_string",
                    required_model_id: None,
                },
                &worker,
            ),
            vec![SchedulerCapabilityRejectReason::NotReadyForTasks]
        );
    }

    #[test]
    fn scheduler_rejects_unsupported_task_type() {
        let tasks = vec!["reverse_string".to_string()];
        let worker = mock_worker(&tasks, &[], &[], &[]);

        assert_eq!(
            scheduler_capability_rejection_reasons(
                &SchedulerTaskRequirements {
                    required_task_type: "echo",
                    required_model_id: None,
                },
                &worker,
            ),
            vec![SchedulerCapabilityRejectReason::UnsupportedTaskType]
        );
    }

    #[test]
    fn scheduler_allows_real_worker_with_executable_model() {
        let tasks = vec!["inference".to_string()];
        let executable = vec!["tinyllama-1b".to_string()];
        let worker = SchedulerWorkerCapabilityView {
            backend: ClusterBackend::Cpu,
            execution_mode: ClusterExecutionMode::Real,
            real_inference_available: Some(true),
            executable_models: &executable,
            ..mock_worker(&tasks, &executable, &[], &[])
        };

        assert!(scheduler_capability_rejection_reasons(
            &SchedulerTaskRequirements {
                required_task_type: "inference",
                required_model_id: Some("tinyllama-1b"),
            },
            &worker,
        )
        .is_empty());
    }

    #[test]
    fn scheduler_allows_unverified_simple_broadcast_peer_for_backward_compatibility() {
        let worker = SchedulerWorkerCapabilityView {
            capability_snapshot_available: false,
            ready_for_tasks: false,
            readiness_reason: ClusterReadinessReason::Unknown,
            backend: ClusterBackend::Unknown,
            execution_mode: ClusterExecutionMode::Unknown,
            real_inference_available: None,
            supported_task_types: &[],
            executable_models: &[],
            metadata_only_models: &[],
            unavailable_models: &[],
        };

        assert!(scheduler_capability_rejection_reasons(
            &SchedulerTaskRequirements {
                required_task_type: "reverse_string",
                required_model_id: None,
            },
            &worker,
        )
        .is_empty());
        assert_eq!(
            scheduler_capability_rejection_reasons(
                &SchedulerTaskRequirements {
                    required_task_type: "inference",
                    required_model_id: None,
                },
                &worker,
            ),
            vec![SchedulerCapabilityRejectReason::MissingCapabilities]
        );
    }
}
