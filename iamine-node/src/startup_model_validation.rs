use super::*;
use crate::capability_advertising::{
    CapabilityAdvertisementValidator, CapabilityRejectionReason, CapabilityValidationResult,
};

pub(super) fn validate_models_for_advertising(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
) -> Vec<String> {
    let validator = CapabilityAdvertisementValidator::new(registry, storage, node_caps);
    validator
        .validate_local_models()
        .into_iter()
        .filter_map(|result| {
            if result.accepted {
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
                return Some(result.model_id);
            }

            let CapabilityValidationResult {
                model_id,
                accepted: _,
                rejection,
            } = result;
            match rejection {
                Some(CapabilityRejectionReason::HardwareRequirements) => {
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
                Some(reason) => {
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
                None => {}
            }
            None
        })
        .collect()
}
