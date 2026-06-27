use crate::worker_startup_policy::StartupMathError;

pub(crate) const METRICS_WORKER_PORT_BASE: u16 = 9000;
pub(crate) const METRICS_HTTP_PORT_BASE: u16 = 9090;
pub(crate) const METRICS_LOW_WORKER_PORT_HTTP_OFFSET: u16 = 10_000;
pub(crate) const METRICS_FALLBACK_CONTINUE: &str = "continue_without_metrics_server";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsUnavailableReason {
    InvalidPortMath,
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
            Self::PortInUse => "port_in_use",
            Self::DisabledByConfig => "disabled_by_config",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsPortAllocationStrategy {
    LegacyWorkerBase,
    LowWorkerPortOffset,
}

impl MetricsPortAllocationStrategy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LegacyWorkerBase => "legacy_worker_base",
            Self::LowWorkerPortOffset => "low_worker_port_offset",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetricsPortAllocation {
    pub(crate) worker_port: u16,
    pub(crate) metrics_port: u16,
    pub(crate) strategy: MetricsPortAllocationStrategy,
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

pub(crate) fn compute_metrics_port(worker_port: u16) -> Result<u16, StartupMathError> {
    allocate_metrics_port(worker_port).map(|allocation| allocation.metrics_port)
}

pub(crate) fn allocate_metrics_port(
    worker_port: u16,
) -> Result<MetricsPortAllocation, StartupMathError> {
    if worker_port < METRICS_WORKER_PORT_BASE {
        let metrics_port = METRICS_LOW_WORKER_PORT_HTTP_OFFSET
            .checked_add(worker_port)
            .ok_or_else(|| {
                StartupMathError::new(
                    "low_worker_metrics_port_plus_offset",
                    METRICS_LOW_WORKER_PORT_HTTP_OFFSET as u64,
                    worker_port as u64,
                    "metrics_port_out_of_range",
                )
            })?;

        return Ok(MetricsPortAllocation {
            worker_port,
            metrics_port,
            strategy: MetricsPortAllocationStrategy::LowWorkerPortOffset,
        });
    }

    let offset = worker_port - METRICS_WORKER_PORT_BASE;

    let metrics_port = METRICS_HTTP_PORT_BASE.checked_add(offset).ok_or_else(|| {
        StartupMathError::new(
            "metrics_port_plus_offset",
            METRICS_HTTP_PORT_BASE as u64,
            offset as u64,
            "metrics_port_out_of_range",
        )
    })?;

    Ok(MetricsPortAllocation {
        worker_port,
        metrics_port,
        strategy: MetricsPortAllocationStrategy::LegacyWorkerBase,
    })
}

pub(crate) fn metrics_startup_decision(worker_port: u16) -> MetricsStartupDecision {
    match compute_metrics_port(worker_port) {
        Ok(port) => MetricsStartupDecision::StartMetrics { port },
        Err(error) => MetricsStartupDecision::ContinueWithoutMetrics {
            reason: MetricsUnavailableReason::InvalidPortMath,
            error,
        },
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
    fn metrics_policy_low_worker_port_derives_metrics_port() {
        let allocation = match allocate_metrics_port(4101) {
            Ok(allocation) => allocation,
            Err(error) => {
                assert_eq!(error.reason, "low_worker_port_allocation_should_not_fail");
                return;
            }
        };

        assert_eq!(allocation.worker_port, 4101);
        assert_eq!(allocation.metrics_port, 14101);
        assert_eq!(
            allocation.strategy,
            MetricsPortAllocationStrategy::LowWorkerPortOffset
        );
        assert_eq!(allocation.strategy.as_str(), "low_worker_port_offset");
        assert_eq!(compute_metrics_port(4101), Ok(14101));
    }

    #[test]
    fn metrics_policy_ts140_port_7002_gets_deterministic_metrics_endpoint() {
        assert_eq!(
            metrics_startup_decision(7002),
            MetricsStartupDecision::StartMetrics { port: 17002 }
        );
    }

    #[test]
    fn metrics_policy_proxmox_ports_get_distinct_metrics_endpoints() {
        let mut metrics_ports = Vec::new();

        for worker_port in [4101, 4102, 4103] {
            let decision = metrics_startup_decision(worker_port);

            assert!(decision.can_continue_worker_startup());
            assert_eq!(decision.fallback_behavior(), "start_metrics_server");
            let metrics_port = match decision {
                MetricsStartupDecision::StartMetrics { port } => port,
                MetricsStartupDecision::ContinueWithoutMetrics { .. }
                | MetricsStartupDecision::Disabled { .. } => 0,
            };
            assert_eq!(
                metrics_port,
                worker_port + METRICS_LOW_WORKER_PORT_HTTP_OFFSET
            );
            metrics_ports.push(metrics_port);
        }

        metrics_ports.sort_unstable();
        metrics_ports.dedup();
        assert_eq!(metrics_ports.len(), 3);
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
    fn metrics_policy_out_of_range_metrics_port_keeps_non_blocking_fallback() {
        let decision = metrics_startup_decision(u16::MAX);

        assert!(decision.can_continue_worker_startup());
        assert_eq!(decision.fallback_behavior(), METRICS_FALLBACK_CONTINUE);
        match decision {
            MetricsStartupDecision::ContinueWithoutMetrics { reason, error } => {
                assert_eq!(reason, MetricsUnavailableReason::InvalidPortMath);
                assert_eq!(reason.as_str(), "invalid_port_math");
                assert_eq!(error.operation, "metrics_port_plus_offset");
                assert_eq!(error.reason, "metrics_port_out_of_range");
            }
            MetricsStartupDecision::StartMetrics { .. }
            | MetricsStartupDecision::Disabled { .. } => {
                assert_eq!("fallback_decision", "metrics_start_or_disabled");
            }
        }
    }

    #[test]
    fn ts140_metrics_port_policy_documented() {
        let decision = metrics_startup_decision(7002);

        assert!(decision.can_continue_worker_startup());
        assert_eq!(decision.fallback_behavior(), "start_metrics_server");
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
