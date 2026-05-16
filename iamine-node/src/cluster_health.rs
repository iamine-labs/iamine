use serde::{Deserialize, Serialize};

pub(crate) const DEFAULT_CLUSTER_STALE_AFTER_MS: u64 = 30_000;
pub(crate) const DEFAULT_CLUSTER_OFFLINE_AFTER_MS: u64 = 90_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterHealth {
    Healthy,
    Degraded,
    Stale,
    Offline,
    Unknown,
}

impl ClusterHealth {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Stale => "stale",
            Self::Offline => "offline",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClusterHealthThresholds {
    pub(crate) stale_after_ms: u64,
    pub(crate) offline_after_ms: u64,
}

impl Default for ClusterHealthThresholds {
    fn default() -> Self {
        Self {
            stale_after_ms: DEFAULT_CLUSTER_STALE_AFTER_MS,
            offline_after_ms: DEFAULT_CLUSTER_OFFLINE_AFTER_MS,
        }
    }
}

pub(crate) fn classify_cluster_health(
    now_ms: u64,
    last_seen_ms: Option<u64>,
    degraded: bool,
    thresholds: ClusterHealthThresholds,
) -> ClusterHealth {
    let Some(last_seen_ms) = last_seen_ms else {
        return ClusterHealth::Unknown;
    };
    let age_ms = now_ms.saturating_sub(last_seen_ms);
    if age_ms >= thresholds.offline_after_ms {
        ClusterHealth::Offline
    } else if age_ms >= thresholds.stale_after_ms {
        ClusterHealth::Stale
    } else if degraded {
        ClusterHealth::Degraded
    } else {
        ClusterHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_health_marks_stale_after_timeout() {
        let thresholds = ClusterHealthThresholds {
            stale_after_ms: 30_000,
            offline_after_ms: 90_000,
        };

        assert_eq!(
            classify_cluster_health(35_000, Some(0), false, thresholds),
            ClusterHealth::Stale
        );
    }

    #[test]
    fn cluster_health_marks_offline_after_timeout() {
        let thresholds = ClusterHealthThresholds {
            stale_after_ms: 30_000,
            offline_after_ms: 90_000,
        };

        assert_eq!(
            classify_cluster_health(95_000, Some(0), false, thresholds),
            ClusterHealth::Offline
        );
    }

    #[test]
    fn cluster_metrics_unavailable_does_not_mark_offline() {
        let thresholds = ClusterHealthThresholds::default();

        assert_eq!(
            classify_cluster_health(1_000, Some(900), false, thresholds),
            ClusterHealth::Healthy
        );
    }
}
