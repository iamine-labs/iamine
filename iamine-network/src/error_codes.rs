pub const NET_TIMEOUT_001: &str = "NET_TIMEOUT_001";
pub const NET_PEER_DISCONNECTED_002: &str = "NET_PEER_DISCONNECTED_002";
pub const NETWORK_NO_PUBSUB_PEERS_001: &str = "NETWORK_NO_PUBSUB_PEERS_001";
pub const PUBSUB_TOPIC_NOT_READY_001: &str = "PUBSUB_TOPIC_NOT_READY_001";
pub const TASK_DISPATCH_UNCONFIRMED_001: &str = "TASK_DISPATCH_UNCONFIRMED_001";

pub const SCH_NO_NODE_001: &str = "SCH_NO_NODE_001";
pub const SCH_NODE_UNHEALTHY_002: &str = "SCH_NODE_UNHEALTHY_002";

pub const TASK_TIMEOUT_001: &str = "TASK_TIMEOUT_001";
pub const TASK_FAILED_002: &str = "TASK_FAILED_002";
pub const TASK_EMPTY_RESULT_003: &str = "TASK_EMPTY_RESULT_003";

pub const MODEL_LOAD_FAILED_001: &str = "MODEL_LOAD_FAILED_001";
pub const MODEL_UNSUPPORTED_HW_002: &str = "MODEL_UNSUPPORTED_HW_002";
pub const WORKER_STARTUP_OVERFLOW_001: &str = "WORKER_STARTUP_OVERFLOW_001";

pub const NODE_BLACKLISTED_001: &str = "NODE_BLACKLISTED_001";
pub const NODE_UNHEALTHY_002: &str = "NODE_UNHEALTHY_002";

pub fn is_standard_error_code(code: &str) -> bool {
    matches!(
        code,
        NET_TIMEOUT_001
            | NET_PEER_DISCONNECTED_002
            | NETWORK_NO_PUBSUB_PEERS_001
            | PUBSUB_TOPIC_NOT_READY_001
            | TASK_DISPATCH_UNCONFIRMED_001
            | SCH_NO_NODE_001
            | SCH_NODE_UNHEALTHY_002
            | TASK_TIMEOUT_001
            | TASK_FAILED_002
            | TASK_EMPTY_RESULT_003
            | MODEL_LOAD_FAILED_001
            | MODEL_UNSUPPORTED_HW_002
            | WORKER_STARTUP_OVERFLOW_001
            | NODE_BLACKLISTED_001
            | NODE_UNHEALTHY_002
    )
}

#[cfg(test)]
mod tests {
    use super::{
        is_standard_error_code, NETWORK_NO_PUBSUB_PEERS_001, PUBSUB_TOPIC_NOT_READY_001,
        TASK_DISPATCH_UNCONFIRMED_001, TASK_TIMEOUT_001, WORKER_STARTUP_OVERFLOW_001,
    };

    #[test]
    fn test_error_code_present() {
        assert!(is_standard_error_code(TASK_TIMEOUT_001));
        assert!(is_standard_error_code(WORKER_STARTUP_OVERFLOW_001));
        assert!(is_standard_error_code(NETWORK_NO_PUBSUB_PEERS_001));
        assert!(is_standard_error_code(PUBSUB_TOPIC_NOT_READY_001));
        assert!(is_standard_error_code(TASK_DISPATCH_UNCONFIRMED_001));
        assert!(!is_standard_error_code("BAD_000"));
    }
}
