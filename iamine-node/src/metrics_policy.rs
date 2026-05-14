use crate::worker_startup_policy::StartupMathError;

pub(crate) const METRICS_WORKER_PORT_BASE: u16 = 9000;
pub(crate) const METRICS_HTTP_PORT_BASE: u16 = 9090;
pub(crate) const METRICS_FALLBACK_CONTINUE: &str = "continue_without_metrics_server";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsUnavailableReason {
    InvalidPortMath,
    PortBelowBase,
    #[allow(dead_code)]
    PortInUse,
    #[allow(dead_code)]
    DisabledByConfig,
    #[allow(dead_code)]
    Unknown,
}

impl MetricsUnavailableReason {
    #[cfg(test)]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InvalidPortMath => "invalid_port_math",
            Self::PortBelowBase => "worker_port_below_metrics_base",
            Self::PortInUse => "port_in_use",
            Self::DisabledByConfig => "disabled_by_config",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MetricsStartupDecision {
    StartMetrics {
        port: u16,
    },
    ContinueWithoutMetrics {
        reason: MetricsUnavailableReason,
        error: StartupMathError,
    },
    #[allow(dead_code)]
    Disabled {
        reason: MetricsUnavailableReason,
    },
}

impl MetricsStartupDecision {
    #[cfg(test)]
    pub(crate) fn can_continue_worker_startup(&self) -> bool {
        matches!(
            self,
            Self::StartMetrics { .. } | Self::ContinueWithoutMetrics { .. } | Self::Disabled { .. }
        )
    }

    #[cfg(test)]
    pub(crate) fn fallback_behavior(&self) -> &'static str {
        match self {
            Self::StartMetrics { .. } => "start_metrics_server",
            Self::ContinueWithoutMetrics { .. } | Self::Disabled { .. } => {
                METRICS_FALLBACK_CONTINUE
            }
        }
    }
}

fn checked_sub_u16(
    operation: &'static str,
    a: u16,
    b: u16,
    reason: &'static str,
) -> Result<u16, StartupMathError> {
    a.checked_sub(b)
        .ok_or_else(|| StartupMathError::new(operation, a as u64, b as u64, reason))
}

pub(crate) fn compute_metrics_port(worker_port: u16) -> Result<u16, StartupMathError> {
    let offset = checked_sub_u16(
        "worker_port_minus_base",
        worker_port,
        METRICS_WORKER_PORT_BASE,
        "worker_port_below_metrics_base",
    )?;

    METRICS_HTTP_PORT_BASE.checked_add(offset).ok_or_else(|| {
        StartupMathError::new(
            "metrics_port_plus_offset",
            METRICS_HTTP_PORT_BASE as u64,
            offset as u64,
            "metrics_port_out_of_range",
        )
    })
}

pub(crate) fn metrics_startup_decision(worker_port: u16) -> MetricsStartupDecision {
    match compute_metrics_port(worker_port) {
        Ok(port) => MetricsStartupDecision::StartMetrics { port },
        Err(error) => {
            let reason = if error.reason == "worker_port_below_metrics_base" {
                MetricsUnavailableReason::PortBelowBase
            } else {
                MetricsUnavailableReason::InvalidPortMath
            };
            MetricsStartupDecision::ContinueWithoutMetrics { reason, error }
        }
    }
}

#[cfg(test)]
pub(crate) fn metrics_bind_failure_decision(error: StartupMathError) -> MetricsStartupDecision {
    MetricsStartupDecision::ContinueWithoutMetrics {
        reason: MetricsUnavailableReason::PortInUse,
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AttemptClaimSource, AttemptLifecycleState, AttemptTimeoutPolicy, AttemptWatchdog};
    use iamine_network::{DistributedTaskMetrics, NodeHealth};
    use std::time::Duration;

    fn apply_retry_fallback_metrics(
        metrics: &mut DistributedTaskMetrics,
        retry_taken: bool,
        fallback_broadcast: bool,
    ) {
        if retry_taken {
            metrics.retry_recorded();
        }
        if fallback_broadcast {
            metrics.fallback_recorded();
        }
    }

    #[test]
    fn metrics_policy_valid_worker_port_derives_metrics_port() {
        assert_eq!(compute_metrics_port(9000), Ok(9090));
        assert_eq!(compute_metrics_port(9001), Ok(9091));
    }

    #[test]
    fn metrics_policy_port_below_base_keeps_existing_fallback() {
        match metrics_startup_decision(4101) {
            MetricsStartupDecision::ContinueWithoutMetrics { reason, error } => {
                assert_eq!(reason, MetricsUnavailableReason::PortBelowBase);
                assert_eq!(reason.as_str(), "worker_port_below_metrics_base");
                assert_eq!(error.operation, "worker_port_minus_base");
                assert_eq!(error.operand_a, 4101);
                assert_eq!(error.operand_b, METRICS_WORKER_PORT_BASE as u64);
                assert_eq!(error.reason, "worker_port_below_metrics_base");
            }
            decision => panic!("expected fallback decision, got {decision:?}"),
        }
    }

    #[test]
    fn metrics_policy_ts140_port_7002_keeps_existing_fallback() {
        match metrics_startup_decision(7002) {
            MetricsStartupDecision::ContinueWithoutMetrics { reason, error } => {
                assert_eq!(reason, MetricsUnavailableReason::PortBelowBase);
                assert_eq!(error.operand_a, 7002);
                assert_eq!(error.operand_b, METRICS_WORKER_PORT_BASE as u64);
                assert_eq!(error.reason, "worker_port_below_metrics_base");
            }
            decision => panic!("expected fallback decision, got {decision:?}"),
        }
    }

    #[test]
    fn metrics_policy_invalid_math_reports_reason() {
        let error = StartupMathError::new(
            "metrics_port_plus_offset",
            METRICS_HTTP_PORT_BASE as u64,
            u16::MAX as u64,
            "metrics_port_out_of_range",
        );
        let decision = metrics_bind_failure_decision(error.clone());

        match decision {
            MetricsStartupDecision::ContinueWithoutMetrics { reason, error } => {
                assert_eq!(reason, MetricsUnavailableReason::PortInUse);
                assert_eq!(error.reason, "metrics_port_out_of_range");
            }
            decision => panic!("expected fallback decision, got {decision:?}"),
        }
    }

    #[test]
    fn metrics_policy_continue_without_metrics_does_not_block_worker() {
        let decision = metrics_startup_decision(4103);

        assert!(decision.can_continue_worker_startup());
        assert_eq!(decision.fallback_behavior(), METRICS_FALLBACK_CONTINUE);
    }

    #[test]
    fn ts140_metrics_port_policy_documented() {
        let decision = metrics_startup_decision(7002);

        assert!(decision.can_continue_worker_startup());
        assert_eq!(decision.fallback_behavior(), METRICS_FALLBACK_CONTINUE);
    }

    #[test]
    fn test_metrics_increments_for_retry_and_fallback() {
        let mut metrics = DistributedTaskMetrics::default();
        apply_retry_fallback_metrics(&mut metrics, true, false);
        apply_retry_fallback_metrics(&mut metrics, false, true);

        assert_eq!(metrics.retries_count, 1);
        assert_eq!(metrics.fallback_count, 1);
        assert_eq!(metrics.failed_tasks, 0);
    }

    #[test]
    fn test_late_results_accounting_does_not_increment_failed_tasks() {
        let mut metrics = DistributedTaskMetrics::default();
        metrics.late_result_recorded();
        assert_eq!(metrics.late_results_count, 1);
        assert_eq!(metrics.failed_tasks, 0);
    }

    #[test]
    fn test_final_failure_increments_failed_tasks_once() {
        let mut metrics = DistributedTaskMetrics::default();
        metrics.task_failed();

        assert_eq!(metrics.failed_tasks, 1);
    }

    #[test]
    fn test_final_outcome_success_when_fallback_worker_completes_after_100s() {
        let mut metrics = DistributedTaskMetrics::default();
        apply_retry_fallback_metrics(&mut metrics, true, false);
        apply_retry_fallback_metrics(&mut metrics, false, true);

        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-long-fallback".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            AttemptTimeoutPolicy::from_model_and_node("mistral-7b", None),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);
        assert!(watchdog.record_progress("tokens_generated_count", Some(340)));
        watchdog.started_at = tokio::time::Instant::now() - Duration::from_secs(109);
        watchdog.last_progress_at = tokio::time::Instant::now();

        assert_ne!(watchdog.check(), crate::WatchdogCheck::TimedOut);
        assert_ne!(watchdog.check(), crate::WatchdogCheck::Stalled);
        assert!(watchdog.transition_state(AttemptLifecycleState::Completed));

        metrics.total_tasks = 1;
        assert_eq!(metrics.failed_tasks, 0);
        assert_eq!(watchdog.worker_peer_id, "TS140");
        assert_eq!(watchdog.state, AttemptLifecycleState::Completed);
    }

    #[test]
    fn test_retry_success_metrics_and_health_credit_do_not_mark_failed() {
        let mut metrics = DistributedTaskMetrics::default();
        apply_retry_fallback_metrics(&mut metrics, true, false);
        apply_retry_fallback_metrics(&mut metrics, false, true);

        let mut failed_worker_health = NodeHealth::default();
        failed_worker_health.record_timeout();
        let mut retry_worker_health = NodeHealth::default();
        retry_worker_health.record_success(250);

        assert_eq!(metrics.retries_count, 1);
        assert_eq!(metrics.fallback_count, 1);
        assert_eq!(metrics.failed_tasks, 0);
        assert_eq!(failed_worker_health.policy_state(), "degraded");
        assert_eq!(retry_worker_health.failure_count, 0);
        assert!(retry_worker_health.last_success_timestamp.is_some());
    }
}
