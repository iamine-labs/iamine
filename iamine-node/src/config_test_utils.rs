use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn with_env_var<T>(key: &str, value: Option<&str>, action: impl FnOnce() -> T) -> T {
    let _guard = ENV_LOCK.lock().expect("env test lock poisoned");
    let previous = std::env::var(key).ok();
    match value {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }

    let result = action();

    if let Some(previous) = previous {
        std::env::set_var(key, previous);
    } else {
        std::env::remove_var(key);
    }

    result
}
