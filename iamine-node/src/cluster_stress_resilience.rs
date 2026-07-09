use crate::cluster_stress::ClusterStressConfig;
use crate::cluster_stress_metrics::ClusterStressMetrics;
use crate::cluster_stress_validation::{
    StressTaskObservation, StressValidationFailure, StressValidationIssue,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterStressProfile {
    #[default]
    Standard,
    TestnetLoadResilience,
}

impl ClusterStressProfile {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "standard" => Ok(Self::Standard),
            "testnet-load-resilience" => Ok(Self::TestnetLoadResilience),
            _ => Err(format!("perfil cluster stress desconocido: {}", value)),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::TestnetLoadResilience => "testnet-load-resilience",
        }
    }
}

pub(crate) fn validate_resilience_profile(config: &ClusterStressConfig) -> Result<(), String> {
    if config.require_recovery_evidence
        && config.profile != ClusterStressProfile::TestnetLoadResilience
    {
        return Err(
            "--require-recovery-evidence requiere --profile testnet-load-resilience".to_string(),
        );
    }
    if config.profile == ClusterStressProfile::TestnetLoadResilience {
        if config.request_count == 1 {
            return Err(
                "--profile testnet-load-resilience requiere --requests 0 o al menos 2".to_string(),
            );
        }
        if config.request_count > 0 && config.concurrency < 2 {
            return Err(
                "--profile testnet-load-resilience requiere --concurrency al menos 2".to_string(),
            );
        }
        if config.stop_on_first_failure {
            return Err(
                "--profile testnet-load-resilience no debe usar --stop-on-first-failure"
                    .to_string(),
            );
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterStressResilienceFailure {
    RequestsMissing,
    ConcurrencyNotExercised,
    FailedRequests,
    TimedOutRequests,
    DuplicateResults,
    DuplicateExecutions,
    DuplicateIdentities,
    IncompatibleAssignments,
    LifecycleValidationFailures,
    RecoveryEvidenceMissing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterStressResilienceReport {
    pub(crate) profile: ClusterStressProfile,
    pub(crate) all_requests_accounted: bool,
    pub(crate) concurrency_exercised: bool,
    pub(crate) no_failed_requests: bool,
    pub(crate) no_timed_out_requests: bool,
    pub(crate) no_duplicate_results: bool,
    pub(crate) no_duplicate_executions: bool,
    pub(crate) no_duplicate_identities: bool,
    pub(crate) no_incompatible_assignments: bool,
    pub(crate) lifecycle_validated: bool,
    pub(crate) recovery_evidence_required: bool,
    pub(crate) recovery_evidence_observed: bool,
    pub(crate) blocking_failures: Vec<ClusterStressResilienceFailure>,
    pub(crate) passed: bool,
}

impl Default for ClusterStressResilienceReport {
    fn default() -> Self {
        Self {
            profile: ClusterStressProfile::Standard,
            all_requests_accounted: true,
            concurrency_exercised: true,
            no_failed_requests: true,
            no_timed_out_requests: true,
            no_duplicate_results: true,
            no_duplicate_executions: true,
            no_duplicate_identities: true,
            no_incompatible_assignments: true,
            lifecycle_validated: true,
            recovery_evidence_required: false,
            recovery_evidence_observed: false,
            blocking_failures: Vec::new(),
            passed: true,
        }
    }
}

pub(crate) fn evaluate_resilience(
    config: &ClusterStressConfig,
    metrics: &ClusterStressMetrics,
    observations: &[StressTaskObservation],
    validation_failures: &[StressValidationFailure],
) -> ClusterStressResilienceReport {
    let all_requests_accounted =
        metrics.observed_requests == metrics.total_requests && metrics.not_run == 0;
    let concurrency_exercised = match config.profile {
        ClusterStressProfile::Standard => true,
        ClusterStressProfile::TestnetLoadResilience => {
            metrics.total_requests == 0 || (metrics.total_requests >= 2 && config.concurrency >= 2)
        }
    };
    let no_failed_requests = metrics.failed == 0;
    let no_timed_out_requests = metrics.timed_out == 0;
    let no_duplicate_results = metrics.duplicate_results == 0;
    let no_duplicate_executions = metrics.duplicate_executions == 0;
    let no_duplicate_identities =
        metrics.duplicate_request_ids == 0 && metrics.duplicate_task_ids == 0;
    let no_incompatible_assignments = metrics.incompatible_assignments == 0;
    let lifecycle_validated = !validation_failures.iter().any(|failure| {
        matches!(
            failure.issue,
            StressValidationIssue::MissingLifecycleEvent(_)
        )
    });
    let recovery_evidence_observed =
        metrics.retried > 0 || metrics.fallback_used > 0 || observations.iter().any(has_recovery);

    let mut blocking_failures = Vec::new();
    if matches!(config.profile, ClusterStressProfile::TestnetLoadResilience) {
        push_if(
            &mut blocking_failures,
            !all_requests_accounted,
            ClusterStressResilienceFailure::RequestsMissing,
        );
        push_if(
            &mut blocking_failures,
            !concurrency_exercised,
            ClusterStressResilienceFailure::ConcurrencyNotExercised,
        );
        push_if(
            &mut blocking_failures,
            !no_failed_requests,
            ClusterStressResilienceFailure::FailedRequests,
        );
        push_if(
            &mut blocking_failures,
            !no_timed_out_requests,
            ClusterStressResilienceFailure::TimedOutRequests,
        );
        push_if(
            &mut blocking_failures,
            !no_duplicate_results,
            ClusterStressResilienceFailure::DuplicateResults,
        );
        push_if(
            &mut blocking_failures,
            !no_duplicate_executions,
            ClusterStressResilienceFailure::DuplicateExecutions,
        );
        push_if(
            &mut blocking_failures,
            !no_duplicate_identities,
            ClusterStressResilienceFailure::DuplicateIdentities,
        );
        push_if(
            &mut blocking_failures,
            !no_incompatible_assignments,
            ClusterStressResilienceFailure::IncompatibleAssignments,
        );
        push_if(
            &mut blocking_failures,
            !validation_failures.is_empty() || !lifecycle_validated,
            ClusterStressResilienceFailure::LifecycleValidationFailures,
        );
        push_if(
            &mut blocking_failures,
            config.require_recovery_evidence && !recovery_evidence_observed,
            ClusterStressResilienceFailure::RecoveryEvidenceMissing,
        );
    }

    ClusterStressResilienceReport {
        profile: config.profile,
        all_requests_accounted,
        concurrency_exercised,
        no_failed_requests,
        no_timed_out_requests,
        no_duplicate_results,
        no_duplicate_executions,
        no_duplicate_identities,
        no_incompatible_assignments,
        lifecycle_validated,
        recovery_evidence_required: config.require_recovery_evidence,
        recovery_evidence_observed,
        passed: blocking_failures.is_empty(),
        blocking_failures,
    }
}

fn has_recovery(observation: &StressTaskObservation) -> bool {
    observation.retry_count > 0 || observation.fallback_used
}

fn push_if(
    blocking_failures: &mut Vec<ClusterStressResilienceFailure>,
    condition: bool,
    failure: ClusterStressResilienceFailure,
) {
    if condition {
        blocking_failures.push(failure);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_stress_validation::{
        StressTaskObservation, StressValidationFailure, StressValidationIssue,
    };

    fn testnet_config() -> ClusterStressConfig {
        ClusterStressConfig {
            profile: ClusterStressProfile::TestnetLoadResilience,
            request_count: 2,
            concurrency: 2,
            ..ClusterStressConfig::default()
        }
    }

    #[test]
    fn validation_rejects_recovery_evidence_without_testnet_profile() {
        let config = ClusterStressConfig {
            require_recovery_evidence: true,
            ..ClusterStressConfig::default()
        };

        assert!(validate_resilience_profile(&config).is_err());
    }

    #[test]
    fn validation_rejects_single_request_testnet_profile() {
        let config = ClusterStressConfig {
            request_count: 1,
            concurrency: 1,
            ..testnet_config()
        };

        assert!(validate_resilience_profile(&config).is_err());
    }

    #[test]
    fn validation_rejects_testnet_load_profile_without_concurrency() {
        let config = ClusterStressConfig {
            concurrency: 1,
            ..testnet_config()
        };

        assert!(validate_resilience_profile(&config).is_err());
    }

    #[test]
    fn validation_rejects_stop_on_first_failure_for_testnet_profile() {
        let config = ClusterStressConfig {
            stop_on_first_failure: true,
            ..testnet_config()
        };

        assert!(validate_resilience_profile(&config).is_err());
    }

    fn successful_observations() -> Vec<StressTaskObservation> {
        vec![
            StressTaskObservation {
                request_id: "request-001".to_string(),
                success: true,
                lifecycle_events: vec!["task_lifecycle_finalized".to_string()],
                ..StressTaskObservation::default()
            },
            StressTaskObservation {
                request_id: "request-002".to_string(),
                success: true,
                lifecycle_events: vec!["task_lifecycle_finalized".to_string()],
                ..StressTaskObservation::default()
            },
        ]
    }

    #[test]
    fn standard_profile_does_not_add_resilience_blockers() {
        let config = ClusterStressConfig::default();
        let observations = vec![StressTaskObservation {
            request_id: "failed".to_string(),
            success: false,
            timed_out: true,
            ..StressTaskObservation::default()
        }];
        let metrics = ClusterStressMetrics::from_observations(1, &observations);

        let report = evaluate_resilience(&config, &metrics, &observations, &[]);

        assert!(report.passed);
        assert_eq!(report.profile, ClusterStressProfile::Standard);
    }

    #[test]
    fn testnet_profile_passes_when_bounded_load_is_clean() {
        let config = testnet_config();
        let observations = successful_observations();
        let metrics = ClusterStressMetrics::from_observations(2, &observations);

        let report = evaluate_resilience(&config, &metrics, &observations, &[]);

        assert!(report.passed);
        assert!(report.all_requests_accounted);
        assert!(report.concurrency_exercised);
    }

    #[test]
    fn testnet_profile_blocks_missing_requests_and_timeouts() {
        let config = testnet_config();
        let observations = vec![StressTaskObservation {
            request_id: "request-001".to_string(),
            success: false,
            timed_out: true,
            ..StressTaskObservation::default()
        }];
        let metrics = ClusterStressMetrics::from_observations(2, &observations);

        let report = evaluate_resilience(&config, &metrics, &observations, &[]);

        assert!(!report.passed);
        assert!(report
            .blocking_failures
            .contains(&ClusterStressResilienceFailure::RequestsMissing));
        assert!(report
            .blocking_failures
            .contains(&ClusterStressResilienceFailure::TimedOutRequests));
    }

    #[test]
    fn testnet_profile_can_require_recovery_evidence() {
        let config = ClusterStressConfig {
            require_recovery_evidence: true,
            ..testnet_config()
        };
        let mut observations = successful_observations();
        let metrics = ClusterStressMetrics::from_observations(2, &observations);

        let missing_report = evaluate_resilience(&config, &metrics, &observations, &[]);
        assert!(missing_report
            .blocking_failures
            .contains(&ClusterStressResilienceFailure::RecoveryEvidenceMissing));

        observations[0].retry_count = 1;
        let recovered_metrics = ClusterStressMetrics::from_observations(2, &observations);
        let recovered_report = evaluate_resilience(&config, &recovered_metrics, &observations, &[]);

        assert!(recovered_report.passed);
        assert!(recovered_report.recovery_evidence_observed);
    }

    #[test]
    fn testnet_profile_blocks_lifecycle_validation_failures() {
        let config = testnet_config();
        let observations = successful_observations();
        let metrics = ClusterStressMetrics::from_observations(2, &observations);
        let failures = vec![StressValidationFailure {
            request_id: "request-001".to_string(),
            issue: StressValidationIssue::MissingLifecycleEvent(
                "task_lifecycle_completed".to_string(),
            ),
        }];

        let report = evaluate_resilience(&config, &metrics, &observations, &failures);

        assert!(!report.passed);
        assert!(report
            .blocking_failures
            .contains(&ClusterStressResilienceFailure::LifecycleValidationFailures));
    }
}
