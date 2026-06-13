use serde::{Deserialize, Serialize};

use crate::cluster_stress_validation::{identity_duplicate_counts, StressTaskObservation};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStressMetrics {
    pub total_requests: usize,
    pub observed_requests: usize,
    pub not_run: usize,
    pub completed: usize,
    pub failed: usize,
    pub timed_out: usize,
    pub retried: u32,
    pub fallback_used: usize,
    pub duplicate_results: usize,
    pub duplicate_executions: usize,
    pub duplicate_request_ids: usize,
    pub duplicate_task_ids: usize,
    pub incompatible_assignments: usize,
    pub min_latency_ms: Option<u64>,
    pub max_latency_ms: Option<u64>,
    pub p50_latency_ms: Option<u64>,
    pub p95_latency_ms: Option<u64>,
    pub p99_latency_ms: Option<u64>,
}

impl ClusterStressMetrics {
    pub fn from_observations(
        total_requests: usize,
        observations: &[StressTaskObservation],
    ) -> Self {
        let latencies = observations
            .iter()
            .map(|observation| observation.latency_ms)
            .collect::<Vec<_>>();
        let percentiles = latency_percentiles(&latencies);
        let identity_duplicates = identity_duplicate_counts(observations);

        Self {
            total_requests,
            observed_requests: observations.len(),
            not_run: total_requests.saturating_sub(observations.len()),
            completed: observations
                .iter()
                .filter(|observation| observation.success)
                .count(),
            failed: observations
                .iter()
                .filter(|observation| !observation.success)
                .count(),
            timed_out: observations
                .iter()
                .filter(|observation| observation.timed_out)
                .count(),
            retried: observations
                .iter()
                .map(|observation| observation.retry_count)
                .sum(),
            fallback_used: observations
                .iter()
                .filter(|observation| observation.fallback_used)
                .count(),
            duplicate_results: observations
                .iter()
                .map(StressTaskObservation::duplicate_result_count)
                .sum(),
            duplicate_executions: observations
                .iter()
                .map(StressTaskObservation::duplicate_execution_count)
                .sum(),
            duplicate_request_ids: identity_duplicates.duplicate_request_ids,
            duplicate_task_ids: identity_duplicates.duplicate_task_ids,
            incompatible_assignments: observations
                .iter()
                .filter(|observation| observation.has_incompatible_assignment())
                .count(),
            min_latency_ms: latencies.iter().min().copied(),
            max_latency_ms: latencies.iter().max().copied(),
            p50_latency_ms: percentiles.p50,
            p95_latency_ms: percentiles.p95,
            p99_latency_ms: percentiles.p99,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LatencyPercentiles {
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
}

pub fn latency_percentiles(samples: &[u64]) -> LatencyPercentiles {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    LatencyPercentiles {
        p50: nearest_rank(&sorted, 50),
        p95: nearest_rank(&sorted, 95),
        p99: nearest_rank(&sorted, 99),
    }
}

fn nearest_rank(sorted_samples: &[u64], percentile: usize) -> Option<u64> {
    if sorted_samples.is_empty() {
        return None;
    }

    let rank = sorted_samples.len() * percentile;
    let index = rank.div_ceil(100).saturating_sub(1);
    sorted_samples.get(index).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_stress_validation::StressTaskObservation;

    #[test]
    fn stress_metrics_percentiles_are_stable() {
        let samples = (1..=100).collect::<Vec<_>>();
        let percentiles = latency_percentiles(&samples);

        assert_eq!(percentiles.p50, Some(50));
        assert_eq!(percentiles.p95, Some(95));
        assert_eq!(percentiles.p99, Some(99));
    }

    #[test]
    fn stress_metrics_empty_samples_are_controlled() {
        assert_eq!(latency_percentiles(&[]), LatencyPercentiles::default());
    }

    #[test]
    fn stress_summary_counts_success_failure_timeout() {
        let observations = vec![
            StressTaskObservation {
                request_id: "success".to_string(),
                success: true,
                latency_ms: 8,
                ..StressTaskObservation::default()
            },
            StressTaskObservation {
                request_id: "timeout".to_string(),
                timed_out: true,
                latency_ms: 20,
                ..StressTaskObservation::default()
            },
        ];

        let metrics = ClusterStressMetrics::from_observations(3, &observations);

        assert_eq!(metrics.total_requests, 3);
        assert_eq!(metrics.observed_requests, 2);
        assert_eq!(metrics.not_run, 1);
        assert_eq!(metrics.completed, 1);
        assert_eq!(metrics.failed, 1);
        assert_eq!(metrics.timed_out, 1);
        assert_eq!(metrics.min_latency_ms, Some(8));
        assert_eq!(metrics.max_latency_ms, Some(20));
    }

    #[test]
    fn stress_summary_counts_duplicate_task_id_correlations() {
        let observations = vec![
            StressTaskObservation {
                request_id: "request-001".to_string(),
                task_id: Some("task-collision".to_string()),
                ..StressTaskObservation::default()
            },
            StressTaskObservation {
                request_id: "request-002".to_string(),
                task_id: Some("task-collision".to_string()),
                ..StressTaskObservation::default()
            },
        ];

        let metrics = ClusterStressMetrics::from_observations(2, &observations);

        assert_eq!(metrics.duplicate_request_ids, 0);
        assert_eq!(metrics.duplicate_task_ids, 1);
    }
}
