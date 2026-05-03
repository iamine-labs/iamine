use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn uuid_simple() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}{}", timestamp.as_secs(), timestamp.subsec_nanos())
}
