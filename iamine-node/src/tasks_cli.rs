use crate::task_trace::{
    global_task_lifecycle_trace_manager, render_task_trace_human, render_task_trace_json,
    TaskTraceStats,
};
use iamine_network::{all_task_traces, distributed_task_metrics, task_trace};
use serde_json::json;
use std::error::Error;

pub(crate) fn run_tasks_trace(task_id: &str, json_output: bool) -> Result<(), Box<dyn Error>> {
    let manager = global_task_lifecycle_trace_manager();
    let record = manager.get_record(task_id);
    let events = manager.events_for(task_id);

    if json_output {
        println!(
            "{}",
            render_task_trace_json(task_id, record.as_ref(), &events, manager.path())?
        );
        return Ok(());
    }

    if record.is_some() {
        println!(
            "{}",
            render_task_trace_human(task_id, record.as_ref(), &events, manager.path())
        );
        return Ok(());
    }

    if let Some(legacy) = task_trace(task_id) {
        println!("Task Trace:");
        println!("task_id: {}", legacy.task_id);
        println!("status: legacy_trace");
        println!(
            "node_history: {}",
            if legacy.node_history.is_empty() {
                "-".to_string()
            } else {
                legacy.node_history.join(" -> ")
            }
        );
        println!(
            "model_history: {}",
            if legacy.model_history.is_empty() {
                "-".to_string()
            } else {
                legacy.model_history.join(" -> ")
            }
        );
        println!("retry_count: {}", legacy.retries);
        println!("fallback_used: {}", legacy.fallbacks > 0);
        println!("latency_ms: {}", legacy.total_latency_ms);
        return Ok(());
    }

    println!(
        "{}",
        render_task_trace_human(task_id, None, &events, manager.path())
    );
    Ok(())
}

pub(crate) fn run_tasks_stats(json_output: bool) -> Result<(), Box<dyn Error>> {
    let manager = global_task_lifecycle_trace_manager();
    let lifecycle_stats = manager.stats();
    let legacy_metrics = distributed_task_metrics();
    let legacy_traces = all_task_traces();

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "task_lifecycle": lifecycle_stats,
                "legacy_metrics": {
                    "total_tasks": legacy_metrics.total_tasks,
                    "failed_tasks": legacy_metrics.failed_tasks,
                    "retries_count": legacy_metrics.retries_count,
                    "fallback_count": legacy_metrics.fallback_count,
                    "late_results_count": legacy_metrics.late_results_count,
                    "avg_latency_ms": legacy_metrics.avg_latency_ms,
                },
                "legacy_traces_count": legacy_traces.len(),
                "trace_path": manager.path(),
            }))?
        );
        return Ok(());
    }

    print_task_lifecycle_stats(&lifecycle_stats);
    println!("\nLegacy distributed metrics:");
    println!("metric | value");
    println!("total_tasks | {}", legacy_metrics.total_tasks);
    println!("failed_tasks | {}", legacy_metrics.failed_tasks);
    println!("retries_count | {}", legacy_metrics.retries_count);
    println!("fallback_count | {}", legacy_metrics.fallback_count);
    println!("late_results_count | {}", legacy_metrics.late_results_count);
    println!("avg_latency_ms | {:.1}", legacy_metrics.avg_latency_ms);
    println!("legacy_traces_count | {}", legacy_traces.len());
    println!("trace_path | {}", manager.path().display());
    Ok(())
}

fn print_task_lifecycle_stats(stats: &TaskTraceStats) {
    println!("Task Lifecycle Stats:");
    println!("metric | value");
    println!("total_tasks | {}", stats.total_tasks);
    println!("pending | {}", stats.pending);
    println!("assigned | {}", stats.assigned);
    println!("running | {}", stats.running);
    println!("completed | {}", stats.completed);
    println!("failed | {}", stats.failed);
    println!("retrying | {}", stats.retrying);
    println!("cancelled | {}", stats.cancelled);
    println!("retries_count | {}", stats.retries_count);
    println!("fallback_count | {}", stats.fallback_count);
}

#[cfg(test)]
mod tests {
    use crate::task_lifecycle::TaskLifecycleStatus;
    use crate::task_trace::TaskTraceStore;

    #[test]
    fn tasks_stats_handles_empty_lifecycle_store() {
        let store = TaskTraceStore::new();

        assert_eq!(store.stats().total_tasks, 0);
    }

    #[test]
    fn tasks_trace_cli_unknown_task_no_panic() {
        let rendered = crate::task_trace::render_task_trace_human(
            "missing",
            None,
            &[],
            std::path::Path::new("/tmp/trace.json"),
        );

        assert!(rendered.contains("status: not_found"));
        assert!(rendered.contains("No task trace found"));
    }

    #[test]
    fn tasks_trace_json_unknown_task_no_panic() {
        let rendered = crate::task_trace::render_task_trace_json(
            "missing",
            None,
            &[],
            std::path::Path::new("/tmp/trace.json"),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(value["status"], "not_found");
        assert_eq!(value["events"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn task_lifecycle_status_strings_are_stable() {
        assert_eq!(TaskLifecycleStatus::Pending.as_str(), "pending");
        assert_eq!(TaskLifecycleStatus::Assigned.as_str(), "assigned");
        assert_eq!(TaskLifecycleStatus::Running.as_str(), "running");
        assert_eq!(TaskLifecycleStatus::Completed.as_str(), "completed");
        assert_eq!(TaskLifecycleStatus::Failed.as_str(), "failed");
        assert_eq!(TaskLifecycleStatus::Retrying.as_str(), "retrying");
        assert_eq!(TaskLifecycleStatus::Cancelled.as_str(), "cancelled");
    }
}
