use super::*;

pub(super) fn print_model_karma_stats() {
    let registry = ModelRegistry::new();
    let mut stats = ranked_models();

    for descriptor in registry.list() {
        if !stats.iter().any(|entry| entry.model_id == descriptor.id) {
            stats.push(ModelKarma::new(descriptor.id.clone()));
        }
    }

    stats.sort_by(|left, right| {
        right
            .karma_score()
            .partial_cmp(&left.karma_score())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.model_id.cmp(&right.model_id))
    });

    println!("model | score | latency | success rate | semantic | runs");
    for entry in stats {
        println!(
            "{} | {:.3} | {:.3} | {:.1}% | {:.1}% | {}",
            entry.model_id,
            entry.karma_score(),
            entry.latency_score,
            entry.accuracy_score * 100.0,
            entry.semantic_success_rate * 100.0,
            entry.total_runs
        );
    }
}

pub(super) fn print_distributed_task_stats(metrics: &DistributedTaskMetrics) {
    println!("metric | value");
    println!("total_tasks | {}", metrics.total_tasks);
    println!("failed_tasks | {}", metrics.failed_tasks);
    println!("retries_count | {}", metrics.retries_count);
    println!("fallback_count | {}", metrics.fallback_count);
    println!("late_results_count | {}", metrics.late_results_count);
    println!("avg_latency_ms | {:.1}", metrics.avg_latency_ms);
}

pub(super) fn print_task_trace_entry(trace: &TaskTrace) {
    println!("task_id: {}", trace.task_id);
    println!(
        "node_history: {}",
        if trace.node_history.is_empty() {
            "-".to_string()
        } else {
            trace.node_history.join(" -> ")
        }
    );
    println!(
        "model_history: {}",
        if trace.model_history.is_empty() {
            "-".to_string()
        } else {
            trace.model_history.join(" -> ")
        }
    );
    println!("retries: {}", trace.retries);
    println!("fallbacks: {}", trace.fallbacks);
    println!("total_latency_ms: {}", trace.total_latency_ms);
}

pub(super) fn debug_task_log(
    flags: DebugFlags,
    peer_id: &str,
    cluster_id: Option<&str>,
    model_id: &str,
    task_id: &str,
    attempt_id: &str,
    message: &str,
) {
    if flags.tasks {
        println!(
            "[DebugTasks] peer_id={} cluster_id={} model_id={} task_id={} attempt_id={} {}",
            peer_id,
            cluster_id.unwrap_or("-"),
            model_id,
            task_id,
            attempt_id,
            message
        );
    }
}

pub(super) fn debug_scheduler_log(flags: DebugFlags, message: impl AsRef<str>) {
    if flags.scheduler {
        println!("[DebugScheduler] {}", message.as_ref());
    }
}

pub(super) fn debug_network_log(flags: DebugFlags, message: impl AsRef<str>) {
    if flags.network {
        println!("[DebugNetwork] {}", message.as_ref());
    }
}

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

pub(super) fn log_observability_event(
    level: LogLevel,
    event: &str,
    trace_id: &str,
    task_id: Option<&str>,
    model_id: Option<&str>,
    error_code: Option<&str>,
    fields: Map<String, Value>,
) {
    let mut entry = StructuredLogEntry::new(level, event, trace_id, "-");
    if let Some(task_id) = task_id {
        entry = entry.with_task_id(task_id.to_string());
    }
    if let Some(model_id) = model_id {
        entry = entry.with_model_id(model_id.to_string());
    }
    if let Some(error_code) = error_code {
        entry = entry.with_error_code(error_code.to_string());
    }
    entry.fields = fields;
    let _ = log_structured(entry);
}

pub(super) fn log_health_update(
    trace_id: &str,
    peer_id: &str,
    model_id: Option<&str>,
    health: &NodeHealth,
    error_code: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "health_update",
        trace_id,
        None,
        model_id,
        error_code,
        health_fields(peer_id, health),
    );

    if health.is_blacklisted() {
        log_observability_event(
            LogLevel::Warn,
            "node_blacklisted",
            trace_id,
            None,
            model_id,
            Some(NODE_BLACKLISTED_001),
            health_fields(peer_id, health),
        );
    } else if health.last_success_timestamp.is_some() {
        log_observability_event(
            LogLevel::Info,
            "node_recovered",
            trace_id,
            None,
            model_id,
            None,
            health_fields(peer_id, health),
        );
    }
}

pub(super) fn cluster_label_for_latency(latency_ms: f64) -> &'static str {
    if latency_ms < 10.0 {
        "LOCAL"
    } else if latency_ms < 50.0 {
        "NEARBY"
    } else {
        "REMOTE"
    }
}

fn health_fields(peer_id: &str, health: &NodeHealth) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("peer_id".to_string(), peer_id.into());
    fields.insert(
        "success_rate".to_string(),
        (health.success_rate as f64).into(),
    );
    fields.insert(
        "avg_latency_ms".to_string(),
        (health.avg_latency_ms as f64).into(),
    );
    fields.insert("failure_count".to_string(), health.failure_count.into());
    fields.insert("timeout_count".to_string(), health.timeout_count.into());
    fields.insert("blacklisted".to_string(), health.is_blacklisted().into());
    if let Some(last_success) = health.last_success_timestamp {
        fields.insert("last_success_timestamp".to_string(), last_success.into());
    }
    fields
}
