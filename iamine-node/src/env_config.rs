use std::ffi::OsString;
use std::path::PathBuf;

pub(crate) const IAMINE_CLUSTER_ID: &str = "IAMINE_CLUSTER_ID";
pub(crate) const IAMINE_CLUSTER_STATUS_WAIT_MS: &str = "IAMINE_CLUSTER_STATUS_WAIT_MS";
pub(crate) const IAMINE_INFERENCE_BACKEND: &str = "IAMINE_INFERENCE_BACKEND";
#[allow(dead_code)]
pub(crate) const IAMINE_LOG_FORMAT: &str = "IAMINE_LOG_FORMAT";
#[allow(dead_code)]
pub(crate) const IAMINE_LOG_PATH: &str = "IAMINE_LOG_PATH";
#[allow(dead_code)]
pub(crate) const IAMINE_NODE_LOG_PATH: &str = "IAMINE_NODE_LOG_PATH";
pub(crate) const IAMINE_SKIP_MODEL_LOAD_ON_STARTUP: &str = "IAMINE_SKIP_MODEL_LOAD_ON_STARTUP";
pub(crate) const IAMINE_TASK_LIFECYCLE_PATH: &str = "IAMINE_TASK_LIFECYCLE_PATH";
pub(crate) const HOSTNAME: &str = "HOSTNAME";
pub(crate) const COMPUTERNAME: &str = "COMPUTERNAME";

pub(crate) fn env_string(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

pub(crate) fn env_os(key: &str) -> Option<OsString> {
    std::env::var_os(key)
}

pub(crate) fn env_path(key: &str) -> Option<PathBuf> {
    env_os(key).map(PathBuf::from)
}

#[allow(dead_code)]
pub(crate) fn env_bool_or_default(key: &str, default: bool) -> bool {
    env_string(key)
        .as_deref()
        .map(env_truthy)
        .unwrap_or(default)
}

pub(crate) fn env_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

pub(crate) fn env_u64_clamped(key: &str, default: u64, min: u64, max: u64) -> u64 {
    env_string(key)
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(|value| value.clamp(min, max))
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_test_utils::with_env_var;

    #[test]
    fn env_config_bool_defaults_preserved() {
        with_env_var("IAMINE_TEST_BOOL_DEFAULT", None, || {
            assert!(!env_bool_or_default("IAMINE_TEST_BOOL_DEFAULT", false));
            assert!(env_bool_or_default("IAMINE_TEST_BOOL_DEFAULT", true));
        });
    }

    #[test]
    fn env_config_bool_true_false_parsing_preserved() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            with_env_var("IAMINE_TEST_BOOL_PARSE", Some(value), || {
                assert!(env_bool_or_default("IAMINE_TEST_BOOL_PARSE", false));
            });
        }
        for value in ["0", "false", "no", "off", ""] {
            with_env_var("IAMINE_TEST_BOOL_PARSE", Some(value), || {
                assert!(!env_bool_or_default("IAMINE_TEST_BOOL_PARSE", false));
            });
        }
    }

    #[test]
    fn env_config_invalid_bool_behavior_preserved() {
        with_env_var("IAMINE_TEST_BOOL_INVALID", Some("definitely"), || {
            assert!(!env_bool_or_default("IAMINE_TEST_BOOL_INVALID", false));
        });
    }

    #[test]
    fn env_config_u64_clamp_preserved() {
        with_env_var("IAMINE_TEST_U64", Some("10"), || {
            assert_eq!(env_u64_clamped("IAMINE_TEST_U64", 100, 20, 200), 20);
        });
        with_env_var("IAMINE_TEST_U64", Some("999"), || {
            assert_eq!(env_u64_clamped("IAMINE_TEST_U64", 100, 20, 200), 200);
        });
        with_env_var("IAMINE_TEST_U64", Some("not-a-number"), || {
            assert_eq!(env_u64_clamped("IAMINE_TEST_U64", 100, 20, 200), 100);
        });
    }
}
