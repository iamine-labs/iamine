#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModelBackendAvailabilityStatus {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModelBackendAvailabilityReason {
    Available,
    MockBackend,
    ModelLoadSkipped,
    CpuFeatureIncompatible,
    LegacyCpuDaemonOnly,
    RealInferenceUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ModelBackendAvailabilityDecision {
    pub(crate) status: ModelBackendAvailabilityStatus,
    pub(crate) reason: ModelBackendAvailabilityReason,
}

impl ModelBackendAvailabilityDecision {
    pub(crate) fn available() -> Self {
        Self {
            status: ModelBackendAvailabilityStatus::Available,
            reason: ModelBackendAvailabilityReason::Available,
        }
    }

    pub(crate) fn unavailable(reason: ModelBackendAvailabilityReason) -> Self {
        Self {
            status: ModelBackendAvailabilityStatus::Unavailable,
            reason,
        }
    }

    pub(crate) fn permits_real_inference(self) -> bool {
        self.status == ModelBackendAvailabilityStatus::Available
            && matches!(
                self.reason,
                ModelBackendAvailabilityReason::Available
                    | ModelBackendAvailabilityReason::LegacyCpuDaemonOnly
            )
    }

    pub(crate) fn permits_local_backend_load(self) -> bool {
        self.status == ModelBackendAvailabilityStatus::Available
            && self.reason == ModelBackendAvailabilityReason::Available
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ModelBackendAvailabilityInput {
    pub(crate) backend_is_mock: bool,
    pub(crate) skip_model_load_on_startup: bool,
    pub(crate) cpu_feature_compatible: bool,
    pub(crate) legacy_cpu_daemon_only: bool,
    pub(crate) real_inference_available: bool,
}

pub(crate) fn evaluate_model_backend_availability(
    input: &ModelBackendAvailabilityInput,
) -> ModelBackendAvailabilityDecision {
    if input.backend_is_mock {
        return ModelBackendAvailabilityDecision::unavailable(
            ModelBackendAvailabilityReason::MockBackend,
        );
    }
    if input.skip_model_load_on_startup {
        return ModelBackendAvailabilityDecision::unavailable(
            ModelBackendAvailabilityReason::ModelLoadSkipped,
        );
    }
    if !input.cpu_feature_compatible {
        if input.legacy_cpu_daemon_only && input.real_inference_available {
            return ModelBackendAvailabilityDecision {
                status: ModelBackendAvailabilityStatus::Available,
                reason: ModelBackendAvailabilityReason::LegacyCpuDaemonOnly,
            };
        }
        return ModelBackendAvailabilityDecision::unavailable(
            ModelBackendAvailabilityReason::CpuFeatureIncompatible,
        );
    }
    if !input.real_inference_available {
        return ModelBackendAvailabilityDecision::unavailable(
            ModelBackendAvailabilityReason::RealInferenceUnavailable,
        );
    }
    ModelBackendAvailabilityDecision::available()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision(
        backend_is_mock: bool,
        skip_model_load_on_startup: bool,
        cpu_feature_compatible: bool,
        legacy_cpu_daemon_only: bool,
        real_inference_available: bool,
    ) -> ModelBackendAvailabilityDecision {
        evaluate_model_backend_availability(&ModelBackendAvailabilityInput {
            backend_is_mock,
            skip_model_load_on_startup,
            cpu_feature_compatible,
            legacy_cpu_daemon_only,
            real_inference_available,
        })
    }

    #[test]
    fn backend_availability_allows_real_backend_when_all_inputs_permit_it() {
        let decision = decision(false, false, true, false, true);

        assert_eq!(decision.status, ModelBackendAvailabilityStatus::Available);
        assert_eq!(decision.reason, ModelBackendAvailabilityReason::Available);
        assert!(decision.permits_real_inference());
        assert!(decision.permits_local_backend_load());
    }

    #[test]
    fn backend_availability_blocks_mock_backend_before_real_inference_signal() {
        let decision = decision(true, false, true, false, true);

        assert_eq!(
            decision,
            ModelBackendAvailabilityDecision::unavailable(
                ModelBackendAvailabilityReason::MockBackend
            )
        );
        assert!(!decision.permits_real_inference());
    }

    #[test]
    fn backend_availability_blocks_startup_skip_before_backend_load() {
        let decision = decision(false, true, true, false, true);

        assert_eq!(
            decision.reason,
            ModelBackendAvailabilityReason::ModelLoadSkipped
        );
        assert!(!decision.permits_real_inference());
    }

    #[test]
    fn backend_availability_blocks_cpu_feature_incompatible_real_backend() {
        let decision = decision(false, false, false, false, true);

        assert_eq!(
            decision.reason,
            ModelBackendAvailabilityReason::CpuFeatureIncompatible
        );
        assert!(!decision.permits_real_inference());
    }

    #[test]
    fn backend_availability_allows_legacy_cpu_daemon_only_without_local_load() {
        let decision = decision(false, false, false, true, true);

        assert_eq!(decision.status, ModelBackendAvailabilityStatus::Available);
        assert_eq!(
            decision.reason,
            ModelBackendAvailabilityReason::LegacyCpuDaemonOnly
        );
        assert!(decision.permits_real_inference());
        assert!(!decision.permits_local_backend_load());
    }

    #[test]
    fn backend_availability_blocks_contradictory_unavailable_runtime_signal() {
        let decision = decision(false, false, true, false, false);

        assert_eq!(
            decision.reason,
            ModelBackendAvailabilityReason::RealInferenceUnavailable
        );
        assert!(!decision.permits_real_inference());
    }
}
