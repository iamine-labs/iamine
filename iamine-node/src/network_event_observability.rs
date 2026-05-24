use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct HumanLogThrottle {
    last_log_ms: HashMap<String, u64>,
    last_values: HashMap<String, String>,
}

impl HumanLogThrottle {
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    pub(crate) fn should_log(&mut self, key: &str, interval_ms: u64, value: Option<&str>) -> bool {
        let now_ms = Self::now_ms();
        let previous_ms = self.last_log_ms.get(key).copied().unwrap_or(0);
        let unchanged = value
            .and_then(|val| self.last_values.get(key).map(|previous| previous == val))
            .unwrap_or(false);

        if unchanged && now_ms.saturating_sub(previous_ms) < interval_ms {
            return false;
        }

        self.last_log_ms.insert(key.to_string(), now_ms);
        if let Some(value) = value {
            self.last_values.insert(key.to_string(), value.to_string());
        }
        true
    }
}

pub(crate) fn cluster_label_for_latency(latency_ms: f64) -> &'static str {
    if latency_ms < 10.0 {
        "LOCAL"
    } else if latency_ms < 50.0 {
        "NEARBY"
    } else {
        "REMOTE"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_log_throttle_deduplicates_repetitive_values() {
        let mut throttle = HumanLogThrottle::default();
        assert!(throttle.should_log("heartbeat:peer-a", 60_000, Some("slots=4 rep=100 peers=2")));
        assert!(!throttle.should_log("heartbeat:peer-a", 60_000, Some("slots=4 rep=100 peers=2")));
        assert!(throttle.should_log("heartbeat:peer-a", 60_000, Some("slots=3 rep=100 peers=2")));
    }

    #[test]
    fn latency_labels_are_stable() {
        assert_eq!(cluster_label_for_latency(5.0), "LOCAL");
        assert_eq!(cluster_label_for_latency(25.0), "NEARBY");
        assert_eq!(cluster_label_for_latency(120.0), "REMOTE");
    }
}
