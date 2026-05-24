use crate::backend_policy::WorkerInferenceBackend;
use crate::env_config::{
    env_string, env_truthy, IAMINE_INFERENCE_BACKEND, IAMINE_SKIP_MODEL_LOAD_ON_STARTUP,
};

pub(crate) fn inference_backend_env() -> Option<String> {
    env_string(IAMINE_INFERENCE_BACKEND)
}

pub(crate) fn skip_model_load_env() -> Option<String> {
    env_string(IAMINE_SKIP_MODEL_LOAD_ON_STARTUP)
}

pub(crate) fn skip_model_load_enabled_from_value(value: Option<&str>) -> bool {
    value.map(env_truthy).unwrap_or(false)
}

pub(crate) fn worker_backend_from_env_value(value: Option<&str>) -> WorkerInferenceBackend {
    WorkerInferenceBackend::from_env_value(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_test_utils::with_env_var;
    use crate::env_config::{IAMINE_INFERENCE_BACKEND, IAMINE_SKIP_MODEL_LOAD_ON_STARTUP};

    #[test]
    fn backend_env_mock_preserved() {
        with_env_var(IAMINE_INFERENCE_BACKEND, Some("mock"), || {
            assert_eq!(inference_backend_env().as_deref(), Some("mock"));
            assert!(worker_backend_from_env_value(inference_backend_env().as_deref()).is_mock());
        });
    }

    #[test]
    fn skip_model_load_env_preserved() {
        with_env_var(IAMINE_SKIP_MODEL_LOAD_ON_STARTUP, Some("1"), || {
            assert_eq!(skip_model_load_env().as_deref(), Some("1"));
            assert!(skip_model_load_enabled_from_value(
                skip_model_load_env().as_deref()
            ));
        });
    }

    #[test]
    fn skip_model_load_invalid_value_stays_false() {
        assert!(!skip_model_load_enabled_from_value(Some("definitely")));
    }
}
