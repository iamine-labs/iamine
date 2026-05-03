use super::*;
use crate::capability_advertising::{
    CapabilityAdvertisementValidator, CapabilityRejectionReason, CapabilityValidationResult,
};

pub(super) fn validate_models_for_advertising(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
    backend_state: &InferenceBackendState,
) -> Vec<String> {
    compute_models_for_advertising(registry, storage, node_caps, backend_state, true)
}

pub(super) fn refresh_models_for_advertising(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
    backend_state: &InferenceBackendState,
) -> Vec<String> {
    compute_models_for_advertising(registry, storage, node_caps, backend_state, false)
}

fn compute_models_for_advertising(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
    backend_state: &InferenceBackendState,
    emit_logs: bool,
) -> Vec<String> {
    if !backend_state.should_advertise_inference_capabilities() {
        if emit_logs {
            log_observability_event(
                LogLevel::Warn,
                "inference_backend_unavailable",
                "startup",
                None,
                None,
                Some(WORKER_INFERENCE_BACKEND_DISABLED_001),
                {
                    let mut fields = Map::new();
                    fields.insert(
                        "reason".to_string(),
                        "backend_unavailable_do_not_advertise_models".into(),
                    );
                    fields.insert("recoverable".to_string(), true.into());
                    fields
                },
            );
        }
        return Vec::new();
    }

    let validator = CapabilityAdvertisementValidator::new(registry, storage, node_caps);
    let mut accepted_local_models = validator
        .validate_local_models()
        .into_iter()
        .filter_map(|result| {
            if result.accepted {
                if emit_logs {
                    log_observability_event(
                        LogLevel::Info,
                        "model_validated",
                        "startup",
                        None,
                        Some(&result.model_id),
                        None,
                        {
                            let mut fields = Map::new();
                            fields.insert("validation_mode".to_string(), "metadata_only".into());
                            fields
                        },
                    );
                }
                return Some(result.model_id);
            }

            let CapabilityValidationResult {
                model_id,
                accepted: _,
                rejection,
            } = result;
            match rejection {
                Some(CapabilityRejectionReason::HardwareRequirements) => {
                    if emit_logs {
                        println!(
                            "[Health] Skipping advertisement for {}: hardware requirements not satisfied",
                            model_id
                        );
                        log_observability_event(
                            LogLevel::Warn,
                            "model_advertisement_rejected",
                            "startup",
                            None,
                            Some(&model_id),
                            Some(MODEL_UNSUPPORTED_HW_002),
                            {
                                let mut fields = Map::new();
                                fields.insert("reason".to_string(), "hardware_requirements".into());
                                fields
                            },
                        );
                    }
                }
                Some(reason) => {
                    if emit_logs {
                        println!(
                            "[Health] Skipping advertisement for {}: {}",
                            model_id,
                            reason.as_str()
                        );
                        log_observability_event(
                            LogLevel::Error,
                            "model_advertisement_rejected",
                            "startup",
                            None,
                            Some(&model_id),
                            Some(MODEL_LOAD_FAILED_001),
                            {
                                let mut fields = Map::new();
                                fields.insert("reason".to_string(), reason.as_str().into());
                                fields
                            },
                        );
                    }
                }
                None => {}
            }
            None
        })
        .collect::<Vec<_>>();

    if backend_state.mock_enabled() && accepted_local_models.is_empty() {
        accepted_local_models = registry
            .list()
            .into_iter()
            .map(|model| model.id.clone())
            .collect::<Vec<_>>();

        if emit_logs {
            log_observability_event(
                LogLevel::Info,
                "mock_inference_catalog_selected",
                "startup",
                None,
                None,
                None,
                {
                    let mut fields = Map::new();
                    fields.insert(
                        "models".to_string(),
                        serde_json::json!(accepted_local_models),
                    );
                    fields.insert(
                        "reason".to_string(),
                        "mock_backend_with_no_local_models".into(),
                    );
                    fields
                },
            );
        }
    }

    accepted_local_models
}
