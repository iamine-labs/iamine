pub(super) const TASK_TOPIC: &str = "iamine-tasks";
pub(super) const CAP_TOPIC: &str = "iamine-capabilities";
pub(super) const DIRECT_INF_TOPIC: &str = "iamine-direct-inference";
pub(super) const RESULTS_TOPIC: &str = "iamine-results";

pub(super) const INFER_TIMEOUT_MS: u64 = 5_000;
pub(super) const INFER_FALLBACK_AFTER_MS: u64 = 2_000;
pub(super) const MAX_DISTRIBUTED_RETRIES: u8 = 2;

pub(super) const MIN_ADAPTIVE_TIMEOUT_MS: u64 = 7_500;
pub(super) const MAX_ADAPTIVE_TIMEOUT_MS: u64 = 45_000;
pub(super) const WATCHDOG_EXTENSION_STEP_MS: u64 = 4_000;
pub(super) const WATCHDOG_STALL_FACTOR_NUM: u64 = 3;
pub(super) const WATCHDOG_STALL_FACTOR_DEN: u64 = 5;
pub(super) const WATCHDOG_MIN_STALL_MS: u64 = 6_000;
