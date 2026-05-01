use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const SUCCESS_RATE_THRESHOLD: f32 = 0.40;
const TIMEOUT_RATE_THRESHOLD: f32 = 0.50;
const TEMP_BAN_TIMEOUT_MS: u64 = 8_000;
const TEMP_BAN_FAILURE_MS: u64 = 15_000;
const TEMP_BAN_REPEATED_FAILURE_MS: u64 = 30_000;
const TIMEOUT_BLACKLIST_THRESHOLD: u32 = 2;
const FAILURE_BLACKLIST_THRESHOLD: u32 = 3;

pub fn health_policy_thresholds() -> (u32, u32) {
    (TIMEOUT_BLACKLIST_THRESHOLD, FAILURE_BLACKLIST_THRESHOLD)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeHealth {
    pub success_rate: f32,
    pub avg_latency_ms: f32,
    pub failure_count: u32,
    pub timeout_count: u32,
    pub last_success_timestamp: Option<u64>,
    #[serde(default)]
    pub total_runs: u32,
    #[serde(default)]
    pub blacklisted_until: Option<u64>,
}

impl Default for NodeHealth {
    fn default() -> Self {
        Self {
            success_rate: 1.0,
            avg_latency_ms: 0.0,
            failure_count: 0,
            timeout_count: 0,
            last_success_timestamp: None,
            total_runs: 0,
            blacklisted_until: None,
        }
    }
}

impl NodeHealth {
    pub fn record_success(&mut self, latency_ms: u64) {
        self.total_runs = self.total_runs.saturating_add(1);
        let n = self.total_runs as f32;
        self.success_rate = ((self.success_rate * (n - 1.0)) + 1.0) / n;
        self.avg_latency_ms = if self.avg_latency_ms == 0.0 {
            latency_ms as f32
        } else {
            ((self.avg_latency_ms * (n - 1.0)) + latency_ms as f32) / n
        };
        self.last_success_timestamp = Some(now_ms());
        self.blacklisted_until = None;
    }

    pub fn record_failure(&mut self) {
        self.total_runs = self.total_runs.saturating_add(1);
        let n = self.total_runs as f32;
        self.success_rate = ((self.success_rate * (n - 1.0)) + 0.0) / n;
        self.failure_count = self.failure_count.saturating_add(1);

        if self.failure_count < FAILURE_BLACKLIST_THRESHOLD {
            self.blacklisted_until = None;
            return;
        }

        let ban_ms = if self.failure_count > FAILURE_BLACKLIST_THRESHOLD {
            TEMP_BAN_REPEATED_FAILURE_MS
        } else {
            TEMP_BAN_FAILURE_MS
        };
        self.blacklisted_until = Some(now_ms().saturating_add(ban_ms));
    }

    pub fn record_timeout(&mut self) {
        self.total_runs = self.total_runs.saturating_add(1);
        let n = self.total_runs as f32;
        self.success_rate = ((self.success_rate * (n - 1.0)) + 0.0) / n;
        self.timeout_count = self.timeout_count.saturating_add(1);
        if self.timeout_count >= TIMEOUT_BLACKLIST_THRESHOLD {
            self.blacklisted_until = Some(now_ms().saturating_add(TEMP_BAN_TIMEOUT_MS));
        } else {
            self.blacklisted_until = None;
        }
    }

    pub fn timeout_rate(&self) -> f32 {
        if self.total_runs == 0 {
            0.0
        } else {
            self.timeout_count as f32 / self.total_runs as f32
        }
    }

    pub fn is_blacklisted(&self) -> bool {
        self.blacklisted_until
            .map(|deadline| deadline > now_ms())
            .unwrap_or(false)
    }

    pub fn is_schedulable(&self) -> bool {
        !self.is_blacklisted()
            && self.success_rate >= SUCCESS_RATE_THRESHOLD
            && self.timeout_rate() <= TIMEOUT_RATE_THRESHOLD
    }

    pub fn policy_state(&self) -> &'static str {
        if self.is_blacklisted() {
            "blacklisted"
        } else if self.is_schedulable() {
            "healthy"
        } else {
            "degraded"
        }
    }

    pub fn failure_penalty(&self) -> f64 {
        ((self.failure_count + self.timeout_count) as f64 / 10.0).min(1.0)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::NodeHealth;

    #[test]
    fn test_node_health_tracking() {
        let mut health = NodeHealth::default();
        health.record_success(100);
        health.record_success(200);
        health.record_failure();
        assert!(health.success_rate < 1.0);
        assert!(health.avg_latency_ms >= 100.0);
        assert_eq!(health.failure_count, 1);
        assert!(health.last_success_timestamp.is_some());
    }

    #[test]
    fn test_blacklist() {
        let mut health = NodeHealth::default();
        health.record_timeout();
        assert!(!health.is_blacklisted());
        assert!(!health.is_schedulable());
        health.record_timeout();
        assert!(health.is_blacklisted());
        assert!(!health.is_schedulable());
    }

    #[test]
    fn test_failure_blacklist_requires_threshold() {
        let mut health = NodeHealth::default();
        health.record_failure();
        assert!(!health.is_blacklisted());
        health.record_failure();
        assert!(!health.is_blacklisted());
        health.record_failure();
        assert!(health.is_blacklisted());
    }
}
