use crate::result_observability::emit_final_outcome_success_event;
use crate::{log_observability_event, DistributedInferState};
use iamine_network::{
    distributed_task_metrics, record_distributed_task_failed, record_distributed_task_latency,
    record_task_latency, task_trace, DistributedTaskMetrics, LogLevel, TaskTrace,
};
use serde_json::Map;

pub(super) fn finalize_distributed_task_observability(
    trace_task_id: &str,
    started_at: Option<tokio::time::Instant>,
    failed: bool,
) -> (Option<TaskTrace>, DistributedTaskMetrics) {
    if let Some(started_at) = started_at {
        let latency_ms = started_at.elapsed().as_millis() as u64;
        let _ = record_task_latency(trace_task_id, latency_ms);
        let _ = record_distributed_task_latency(latency_ms);
    }

    if failed {
        let _ = record_distributed_task_failed();
    }

    let metrics = distributed_task_metrics();
    log_observability_event(
        LogLevel::Info,
        "metrics_snapshot",
        trace_task_id,
        Some(trace_task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("total_tasks".to_string(), metrics.total_tasks.into());
            fields.insert("failed_tasks".to_string(), metrics.failed_tasks.into());
            fields.insert("retries_count".to_string(), metrics.retries_count.into());
            fields.insert("fallback_count".to_string(), metrics.fallback_count.into());
            fields.insert(
                "late_results_count".to_string(),
                metrics.late_results_count.into(),
            );
            fields.insert("avg_latency_ms".to_string(), metrics.avg_latency_ms.into());
            fields.insert("failed".to_string(), failed.into());
            fields
        },
    );

    (task_trace(trace_task_id), metrics)
}

pub(super) fn print_human_final_task_summary(
    infer_state: &DistributedInferState,
    aggregate_metrics: &DistributedTaskMetrics,
    failed: bool,
) {
    println!("\n🧾 Task Summary");
    println!("task_id={}", infer_state.trace_task_id);
    println!(
        "counts: task_retry_count={} task_fallback_count={} global_retry_count={} global_fallback_count={} global_failed_tasks={}",
        infer_state.task_retry_count,
        infer_state.task_fallback_count,
        aggregate_metrics.retries_count,
        aggregate_metrics.fallback_count,
        aggregate_metrics.failed_tasks
    );
    println!(
        "final_outcome={}",
        if failed { "failed" } else { "success" }
    );
    println!("attempts:");
    for line in infer_state.attempt_summary_lines() {
        println!("{}", line);
    }
}

pub(super) fn emit_final_trace_summary_event(
    infer_state: &DistributedInferState,
    aggregate_metrics: &DistributedTaskMetrics,
    failed: bool,
) {
    let attempts = infer_state.attempt_summary_lines();
    if !failed {
        emit_final_outcome_success_event(
            &infer_state.trace_task_id,
            infer_state.current_model.as_deref(),
            &attempts,
        );
    }
    log_observability_event(
        LogLevel::Info,
        "final_trace_summary_constructed",
        &infer_state.trace_task_id,
        Some(&infer_state.trace_task_id),
        infer_state.current_model.as_deref(),
        None,
        {
            let mut fields = Map::new();
            fields.insert("failed".to_string(), failed.into());
            fields.insert(
                "task_retry_count".to_string(),
                infer_state.task_retry_count.into(),
            );
            fields.insert(
                "task_fallback_count".to_string(),
                infer_state.task_fallback_count.into(),
            );
            fields.insert(
                "global_retry_count".to_string(),
                aggregate_metrics.retries_count.into(),
            );
            fields.insert(
                "global_fallback_count".to_string(),
                aggregate_metrics.fallback_count.into(),
            );
            fields.insert(
                "global_failed_tasks".to_string(),
                aggregate_metrics.failed_tasks.into(),
            );
            fields.insert("attempts".to_string(), serde_json::json!(attempts));
            fields
        },
    );
}
