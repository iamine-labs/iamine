use crate::env_config::{env_path, IAMINE_LOG_FORMAT, IAMINE_LOG_PATH, IAMINE_TASK_LIFECYCLE_PATH};
use std::path::PathBuf;

#[cfg(not(test))]
pub(crate) const DEFAULT_TASK_LIFECYCLE_TRACE_FILE: &str = "task_lifecycle_traces.json";
#[cfg(test)]
pub(crate) const TEST_TASK_LIFECYCLE_TRACE_FILE: &str = "iamine-task-lifecycle-tests.json";

pub(crate) fn default_task_lifecycle_trace_path() -> PathBuf {
    if let Some(path) = env_path(IAMINE_TASK_LIFECYCLE_PATH) {
        return path;
    }

    #[cfg(test)]
    {
        std::env::temp_dir().join(TEST_TASK_LIFECYCLE_TRACE_FILE)
    }

    #[cfg(not(test))]
    {
        default_iamine_home_dir().join(DEFAULT_TASK_LIFECYCLE_TRACE_FILE)
    }
}

#[cfg(not(test))]
fn default_iamine_home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".iamine")
}

#[allow(dead_code)]
pub(crate) fn configured_log_path() -> Option<PathBuf> {
    env_path(IAMINE_LOG_PATH)
}

#[allow(dead_code)]
pub(crate) fn configured_log_format() -> Option<String> {
    crate::env_config::env_string(IAMINE_LOG_FORMAT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_test_utils::with_env_var;

    #[test]
    fn task_lifecycle_path_env_preserved() {
        with_env_var(
            IAMINE_TASK_LIFECYCLE_PATH,
            Some("/tmp/iamine-config-test/tasks.json"),
            || {
                assert_eq!(
                    default_task_lifecycle_trace_path(),
                    PathBuf::from("/tmp/iamine-config-test/tasks.json")
                );
            },
        );
    }

    #[test]
    fn trace_store_default_path_preserved() {
        with_env_var(IAMINE_TASK_LIFECYCLE_PATH, None, || {
            assert_eq!(
                default_task_lifecycle_trace_path(),
                std::env::temp_dir().join(TEST_TASK_LIFECYCLE_TRACE_FILE)
            );
        });
    }

    #[test]
    fn log_path_env_preserved() {
        with_env_var(
            IAMINE_LOG_PATH,
            Some("/tmp/iamine-config-test/log.ndjson"),
            || {
                assert_eq!(
                    configured_log_path(),
                    Some(PathBuf::from("/tmp/iamine-config-test/log.ndjson"))
                );
            },
        );
    }

    #[test]
    fn log_format_env_preserved() {
        with_env_var(IAMINE_LOG_FORMAT, Some("ndjson"), || {
            assert_eq!(configured_log_format().as_deref(), Some("ndjson"));
        });
    }
}
