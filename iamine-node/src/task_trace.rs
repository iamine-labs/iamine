use crate::task_lifecycle::{
    TaskLifecycleEvent, TaskLifecycleRecord, TaskLifecycleStatus, TaskLifecycleTransitionError,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TaskTraceStore {
    records: HashMap<String, TaskLifecycleRecord>,
    events: HashMap<String, Vec<TaskLifecycleEvent>>,
}

impl TaskTraceStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn append_event(
        &mut self,
        event: TaskLifecycleEvent,
    ) -> Result<(), TaskLifecycleTransitionError> {
        let record = self
            .records
            .entry(event.task_id.clone())
            .or_insert_with(|| TaskLifecycleRecord::seed_from_event(&event));
        record.apply_event(&event)?;
        self.events
            .entry(event.task_id.clone())
            .or_default()
            .push(event);
        Ok(())
    }

    pub(crate) fn get_record(&self, task_id: &str) -> Option<&TaskLifecycleRecord> {
        self.records.get(task_id)
    }

    pub(crate) fn events_for(&self, task_id: &str) -> Vec<TaskLifecycleEvent> {
        self.events.get(task_id).cloned().unwrap_or_default()
    }

    pub(crate) fn stats(&self) -> TaskTraceStats {
        let mut stats = TaskTraceStats::default();
        for record in self.records.values() {
            stats.total_tasks = stats.total_tasks.saturating_add(1);
            match record.status {
                TaskLifecycleStatus::Pending => stats.pending = stats.pending.saturating_add(1),
                TaskLifecycleStatus::Assigned => stats.assigned = stats.assigned.saturating_add(1),
                TaskLifecycleStatus::Running => stats.running = stats.running.saturating_add(1),
                TaskLifecycleStatus::Completed => {
                    stats.completed = stats.completed.saturating_add(1)
                }
                TaskLifecycleStatus::Failed => stats.failed = stats.failed.saturating_add(1),
                TaskLifecycleStatus::Retrying => stats.retrying = stats.retrying.saturating_add(1),
                TaskLifecycleStatus::Cancelled => {
                    stats.cancelled = stats.cancelled.saturating_add(1)
                }
            }
            stats.retries_count = stats
                .retries_count
                .saturating_add(record.retry_count as u64);
            if record.fallback_used {
                stats.fallback_count = stats.fallback_count.saturating_add(1);
            }
        }
        stats
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TaskTraceStats {
    pub(crate) total_tasks: u64,
    pub(crate) pending: u64,
    pub(crate) assigned: u64,
    pub(crate) running: u64,
    pub(crate) completed: u64,
    pub(crate) failed: u64,
    pub(crate) retrying: u64,
    pub(crate) cancelled: u64,
    pub(crate) retries_count: u64,
    pub(crate) fallback_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskLifecycleTraceManager {
    path: PathBuf,
    store: Arc<RwLock<TaskTraceStore>>,
}

impl TaskLifecycleTraceManager {
    pub(crate) fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let store = load_task_trace_store_from_path(&path)?;
        Ok(Self {
            path,
            store: Arc::new(RwLock::new(store)),
        })
    }

    pub(crate) fn default() -> Self {
        Self::new(default_task_lifecycle_trace_path()).unwrap_or_else(|_| Self {
            path: default_task_lifecycle_trace_path(),
            store: Arc::new(RwLock::new(TaskTraceStore::new())),
        })
    }

    pub(crate) fn record_event(&self, event: TaskLifecycleEvent) -> io::Result<()> {
        let mut latest = load_task_trace_store_from_path(&self.path)?;
        latest
            .append_event(event)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        save_task_trace_store_to_path(&self.path, &latest)?;
        *self.store.write().expect("task lifecycle trace poisoned") = latest;
        Ok(())
    }

    pub(crate) fn get_record(&self, task_id: &str) -> Option<TaskLifecycleRecord> {
        self.store
            .read()
            .expect("task lifecycle trace poisoned")
            .get_record(task_id)
            .cloned()
    }

    pub(crate) fn events_for(&self, task_id: &str) -> Vec<TaskLifecycleEvent> {
        self.store
            .read()
            .expect("task lifecycle trace poisoned")
            .events_for(task_id)
    }

    pub(crate) fn stats(&self) -> TaskTraceStats {
        self.store
            .read()
            .expect("task lifecycle trace poisoned")
            .stats()
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

static GLOBAL_TASK_LIFECYCLE_TRACE_MANAGER: OnceLock<TaskLifecycleTraceManager> = OnceLock::new();

pub(crate) fn global_task_lifecycle_trace_manager() -> &'static TaskLifecycleTraceManager {
    GLOBAL_TASK_LIFECYCLE_TRACE_MANAGER.get_or_init(TaskLifecycleTraceManager::default)
}

pub(crate) fn record_task_lifecycle_event(event: TaskLifecycleEvent) -> Option<()> {
    global_task_lifecycle_trace_manager()
        .record_event(event)
        .ok()
}

pub(crate) fn default_task_lifecycle_trace_path() -> PathBuf {
    crate::path_config::default_task_lifecycle_trace_path()
}

pub(crate) fn load_task_trace_store_from_path(path: &Path) -> io::Result<TaskTraceStore> {
    if !path.exists() {
        return Ok(TaskTraceStore::new());
    }
    let data = fs::read(path)?;
    if data.is_empty() {
        return Ok(TaskTraceStore::new());
    }
    serde_json::from_slice(&data).map_err(io::Error::other)
}

fn save_task_trace_store_to_path(path: &Path, store: &TaskTraceStore) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let serialized = serde_json::to_vec_pretty(store).map_err(io::Error::other)?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_extension(format!("tmp-{}", suffix));
    fs::write(&tmp_path, serialized)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

pub(crate) fn render_task_trace_human(
    task_id: &str,
    record: Option<&TaskLifecycleRecord>,
    events: &[TaskLifecycleEvent],
    trace_path: &Path,
) -> String {
    let Some(record) = record else {
        return format!(
            "Task Trace:\ntask_id: {}\nstatus: not_found\n\nNo task trace found for this task_id.\nTrace persistence may require running with IAMINE_TASK_LIFECYCLE_PATH or daemon mode.\ntrace_path: {}\n",
            task_id,
            trace_path.display()
        );
    };

    let mut output = String::new();
    output.push_str("Task Trace:\n");
    output.push_str(&format!("task_id: {}\n", record.task_id));
    output.push_str(&format!(
        "task_type: {}\n",
        record.task_type.as_deref().unwrap_or("unknown")
    ));
    output.push_str(&format!("status: {}\n", record.status));
    output.push_str(&format!(
        "assigned_worker: {}\n",
        record.assigned_node.as_deref().unwrap_or("-")
    ));
    output.push_str(&format!(
        "selected_worker: {}\n",
        record.selected_worker.as_deref().unwrap_or("-")
    ));
    output.push_str(&format!(
        "selection_reason: {}\n",
        record
            .selection_reason
            .map(|reason| reason.as_str())
            .unwrap_or("unknown")
    ));
    output.push_str(&format!(
        "candidate_workers: {}\n",
        if record.candidate_workers.is_empty() {
            "-".to_string()
        } else {
            record.candidate_workers.join(",")
        }
    ));
    output.push_str(&format!(
        "rejected_candidates: {}\n",
        if record.rejected_candidates.is_empty() {
            "-".to_string()
        } else {
            record.rejected_candidates.join(",")
        }
    ));
    output.push_str(&format!(
        "rejected_reasons: {}\n",
        if record.rejected_reasons.is_empty() {
            "-".to_string()
        } else {
            record.rejected_reasons.join(",")
        }
    ));
    output.push_str(&format!(
        "compatible_candidates_count: {}\n",
        record.compatible_candidates_count
    ));
    output.push_str(&format!(
        "capability_filter_applied: {}\n",
        record.capability_filter_applied
    ));
    output.push_str(&format!("created_at_ms: {}\n", record.created_at_ms));
    output.push_str(&format!(
        "started_at_ms: {}\n",
        record
            .started_at_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    output.push_str(&format!(
        "finished_at_ms: {}\n",
        record
            .finished_at_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    output.push_str(&format!(
        "latency_ms: {}\n",
        record
            .latency_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    output.push_str(&format!("retry_count: {}\n", record.retry_count));
    output.push_str(&format!("fallback_used: {}\n", record.fallback_used));
    output.push_str(&format!(
        "fallback_reason: {}\n",
        record.fallback_reason.as_deref().unwrap_or("-")
    ));
    output.push_str(&format!(
        "error_code: {}\n",
        record.error_code.map(|code| code.as_str()).unwrap_or("-")
    ));
    output.push_str(&format!(
        "final_outcome: {}\n",
        record.final_outcome.as_deref().unwrap_or("-")
    ));
    output.push_str(&format!(
        "result_summary: {}\n",
        record.result_summary.as_deref().unwrap_or("-")
    ));
    output.push_str("\nEvents:\n");
    if events.is_empty() {
        output.push_str("(no lifecycle events recorded)\n");
    } else {
        for (index, event) in events.iter().enumerate() {
            output.push_str(&format!(
                "{}. {} status={} timestamp_ms={}\n",
                index + 1,
                event.event,
                event.status,
                event.timestamp_ms
            ));
        }
    }
    output
}

pub(crate) fn render_task_trace_json(
    task_id: &str,
    record: Option<&TaskLifecycleRecord>,
    events: &[TaskLifecycleEvent],
    trace_path: &Path,
) -> Result<String, serde_json::Error> {
    if let Some(record) = record {
        return serde_json::to_string_pretty(&json!({
            "task_id": task_id,
            "status": record.status,
            "record": record,
            "events": events,
            "trace_path": trace_path.display().to_string(),
        }));
    }

    serde_json::to_string_pretty(&json!({
        "task_id": task_id,
        "status": "not_found",
        "events": [],
        "message": "No task trace found for this task_id.",
        "trace_path": trace_path.display().to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_lifecycle::{TaskLifecycleEvent, TaskLifecycleStatus, TaskSelectionReason};

    fn temp_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-task-lifecycle-{}.json", suffix))
    }

    fn created(task_id: &str) -> TaskLifecycleEvent {
        TaskLifecycleEvent::new(
            "task_lifecycle_created",
            task_id,
            TaskLifecycleStatus::Pending,
            100,
        )
        .with_task_type("reverse_string")
    }

    #[test]
    fn task_trace_records_events_in_order() {
        let mut store = TaskTraceStore::new();
        store.append_event(created("task-1")).unwrap();
        store
            .append_event(
                TaskLifecycleEvent::new(
                    "task_lifecycle_assigned",
                    "task-1",
                    TaskLifecycleStatus::Assigned,
                    120,
                )
                .with_assignment(
                    "worker-a",
                    vec!["worker-a".to_string()],
                    TaskSelectionReason::CurrentBroadcastPolicy,
                ),
            )
            .unwrap();

        let events = store.events_for("task-1");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, "task_lifecycle_created");
        assert_eq!(events[1].event, "task_lifecycle_assigned");
        assert_eq!(
            store.get_record("task-1").unwrap().status,
            TaskLifecycleStatus::Assigned
        );
    }

    #[test]
    fn task_trace_not_found_is_controlled() {
        let store = TaskTraceStore::new();

        assert!(store.get_record("missing").is_none());
        assert!(store.events_for("missing").is_empty());
    }

    #[test]
    fn task_trace_render_human_contains_core_fields() {
        let mut store = TaskTraceStore::new();
        store.append_event(created("task-render")).unwrap();
        let record = store.get_record("task-render");
        let events = store.events_for("task-render");

        let rendered =
            render_task_trace_human("task-render", record, &events, Path::new("/tmp/trace.json"));

        assert!(rendered.contains("task_id: task-render"));
        assert!(rendered.contains("status: pending"));
        assert!(rendered.contains("Events:"));
    }

    #[test]
    fn tasks_trace_contains_scheduler_metadata() {
        let mut store = TaskTraceStore::new();
        store.append_event(created("task-scheduler")).unwrap();
        store
            .append_event(
                TaskLifecycleEvent::new(
                    "task_lifecycle_scheduler_decision",
                    "task-scheduler",
                    TaskLifecycleStatus::Assigned,
                    120,
                )
                .with_assignment(
                    "worker-a",
                    vec!["worker-a".to_string(), "worker-b".to_string()],
                    TaskSelectionReason::ReadyWorkerSupportsTask,
                )
                .with_scheduler_metadata(
                    vec!["worker-b".to_string()],
                    vec!["not_ready_for_tasks".to_string()],
                )
                .with_scheduler_capability_metadata(1, true),
            )
            .unwrap();
        let record = store.get_record("task-scheduler");
        let events = store.events_for("task-scheduler");

        let rendered = render_task_trace_human(
            "task-scheduler",
            record,
            &events,
            Path::new("/tmp/trace.json"),
        );

        assert!(rendered.contains("selected_worker: worker-a"));
        assert!(rendered.contains("selection_reason: ready_worker_supports_task"));
        assert!(rendered.contains("candidate_workers: worker-a,worker-b"));
        assert!(rendered.contains("rejected_reasons: not_ready_for_tasks"));
        assert!(rendered.contains("compatible_candidates_count: 1"));
        assert!(rendered.contains("capability_filter_applied: true"));
    }

    #[test]
    fn task_trace_json_contains_core_fields() {
        let mut store = TaskTraceStore::new();
        store.append_event(created("task-json")).unwrap();
        let record = store.get_record("task-json");
        let events = store.events_for("task-json");

        let rendered =
            render_task_trace_json("task-json", record, &events, Path::new("/tmp/trace.json"))
                .unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(value["task_id"], "task-json");
        assert_eq!(value["record"]["status"], "pending");
        assert_eq!(value["events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn task_trace_persistence_roundtrip() {
        let path = temp_path();
        let manager = TaskLifecycleTraceManager::new(&path).unwrap();
        manager.record_event(created("task-file")).unwrap();

        let reloaded = TaskLifecycleTraceManager::new(&path).unwrap();

        assert_eq!(
            reloaded.get_record("task-file").unwrap().status,
            TaskLifecycleStatus::Pending
        );
        let _ = fs::remove_file(path);
    }
}
