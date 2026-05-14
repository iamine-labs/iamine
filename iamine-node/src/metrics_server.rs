use crate::metrics::{start_metrics_server, NodeMetrics};
use crate::metrics_policy::{
    metrics_startup_decision, MetricsStartupDecision, METRICS_FALLBACK_CONTINUE,
};
use crate::resource_policy::ResourcePolicy;
use crate::worker_startup_policy::{emit_worker_startup_overflow_event, StartupMathError};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy)]
pub(crate) struct MetricsStartupContext<'a> {
    pub(crate) trace_id: &'a str,
    pub(crate) node_id: &'a str,
    pub(crate) peer_id: &'a str,
    pub(crate) worker_port: u16,
    pub(crate) resource_policy: &'a ResourcePolicy,
    pub(crate) worker_slots: usize,
    pub(crate) max_concurrent: usize,
    pub(crate) available_slots: usize,
}

pub(crate) fn start_worker_metrics_or_continue(
    metrics: Arc<RwLock<NodeMetrics>>,
    context: MetricsStartupContext<'_>,
) -> MetricsStartupDecision {
    let decision = metrics_startup_decision(context.worker_port);
    match &decision {
        MetricsStartupDecision::StartMetrics { port } => {
            spawn_metrics_server(metrics, *port);
        }
        MetricsStartupDecision::ContinueWithoutMetrics { error, .. } => {
            emit_metrics_fallback(context, error);
        }
        MetricsStartupDecision::Disabled { .. } => {}
    }
    decision
}

pub(crate) fn spawn_metrics_server(metrics: Arc<RwLock<NodeMetrics>>, port: u16) {
    println!("📊 Metrics en http://localhost:{}/metrics", port);
    tokio::spawn(async move {
        start_metrics_server(metrics, port).await;
    });
}

fn emit_metrics_fallback(context: MetricsStartupContext<'_>, error: &StartupMathError) {
    emit_worker_startup_overflow_event(
        context.trace_id,
        context.node_id,
        context.peer_id,
        context.worker_port,
        context.resource_policy,
        context.worker_slots,
        context.max_concurrent,
        context.available_slots,
        error,
        METRICS_FALLBACK_CONTINUE,
    );
    println!(
        "⚠️  [Startup] Cálculo de metrics port inválido: {}. Continuando en modo degradado (metrics deshabilitado).",
        error.describe()
    );
}

#[cfg(test)]
pub(crate) fn metrics_bind_failure_is_non_fatal(error: StartupMathError) -> MetricsStartupDecision {
    MetricsStartupDecision::ContinueWithoutMetrics {
        reason: crate::metrics_policy::MetricsUnavailableReason::PortInUse,
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics_policy::MetricsUnavailableReason;

    #[test]
    fn metrics_server_bind_failure_is_non_fatal() {
        let error = StartupMathError::new("bind_metrics_port", 9090, 0, "port_in_use");
        let decision = metrics_bind_failure_is_non_fatal(error);

        assert!(decision.can_continue_worker_startup());
        assert_eq!(decision.fallback_behavior(), METRICS_FALLBACK_CONTINUE);
        match decision {
            MetricsStartupDecision::ContinueWithoutMetrics { reason, error } => {
                assert_eq!(reason, MetricsUnavailableReason::PortInUse);
                assert_eq!(error.reason, "port_in_use");
            }
            decision => panic!("expected fallback decision, got {decision:?}"),
        }
    }
}
