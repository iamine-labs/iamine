use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTrace {
    pub task_id: String,
    pub node_history: Vec<String>,
    pub model_history: Vec<String>,
    pub retries: u32,
    pub fallbacks: u32,
    pub total_latency_ms: u64,
}

impl TaskTrace {
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            node_history: Vec::new(),
            model_history: Vec::new(),
            retries: 0,
            fallbacks: 0,
            total_latency_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskTraceStore {
    traces: HashMap<String, TaskTrace>,
}

impl TaskTraceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_traces(traces: HashMap<String, TaskTrace>) -> Self {
        Self { traces }
    }

    pub fn get(&self, task_id: &str) -> Option<TaskTrace> {
        self.traces.get(task_id).cloned()
    }

    pub fn list(&self) -> Vec<TaskTrace> {
        let mut traces = self.traces.values().cloned().collect::<Vec<_>>();
        traces.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        traces
    }

    pub fn upsert_attempt(
        &mut self,
        task_id: &str,
        peer_id: &str,
        model_id: &str,
        is_retry: bool,
        is_fallback: bool,
    ) -> TaskTrace {
        let trace = self
            .traces
            .entry(task_id.to_string())
            .or_insert_with(|| TaskTrace::new(task_id));
        trace.node_history.push(peer_id.to_string());
        trace.model_history.push(model_id.to_string());
        if is_retry {
            trace.retries = trace.retries.saturating_add(1);
        }
        if is_fallback {
            trace.fallbacks = trace.fallbacks.saturating_add(1);
        }
        trace.clone()
    }

    pub fn set_latency(&mut self, task_id: &str, latency_ms: u64) -> TaskTrace {
        let trace = self
            .traces
            .entry(task_id.to_string())
            .or_insert_with(|| TaskTrace::new(task_id));
        trace.total_latency_ms = latency_ms;
        trace.clone()
    }

    pub fn traces(&self) -> &HashMap<String, TaskTrace> {
        &self.traces
    }
}

#[derive(Debug, Clone)]
pub struct TaskTraceManager {
    path: PathBuf,
    store: Arc<RwLock<TaskTraceStore>>,
}

impl TaskTraceManager {
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let store = TaskTraceStore::from_traces(load_traces_from_path(&path)?);
        Ok(Self {
            path,
            store: Arc::new(RwLock::new(store)),
        })
    }

    pub fn default() -> Self {
        Self::new(default_task_trace_path()).unwrap_or_else(|_| Self {
            path: default_task_trace_path(),
            store: Arc::new(RwLock::new(TaskTraceStore::new())),
        })
    }

    pub fn record_attempt(
        &self,
        task_id: &str,
        peer_id: &str,
        model_id: &str,
        is_retry: bool,
        is_fallback: bool,
    ) -> io::Result<TaskTrace> {
        let mut latest = TaskTraceStore::from_traces(load_traces_from_path(&self.path)?);
        let trace = latest.upsert_attempt(task_id, peer_id, model_id, is_retry, is_fallback);
        save_traces_to_path(&self.path, latest.traces())?;
        *self.store.write().expect("task trace manager poisoned") = latest;
        Ok(trace)
    }

    pub fn record_latency(&self, task_id: &str, latency_ms: u64) -> io::Result<TaskTrace> {
        let mut latest = TaskTraceStore::from_traces(load_traces_from_path(&self.path)?);
        let trace = latest.set_latency(task_id, latency_ms);
        save_traces_to_path(&self.path, latest.traces())?;
        *self.store.write().expect("task trace manager poisoned") = latest;
        Ok(trace)
    }

    pub fn get(&self, task_id: &str) -> Option<TaskTrace> {
        self.store
            .read()
            .expect("task trace manager poisoned")
            .get(task_id)
    }

    pub fn list(&self) -> Vec<TaskTrace> {
        self.store
            .read()
            .expect("task trace manager poisoned")
            .list()
    }
}

static GLOBAL_TASK_TRACE_MANAGER: OnceLock<TaskTraceManager> = OnceLock::new();

pub fn global_task_trace_manager() -> &'static TaskTraceManager {
    GLOBAL_TASK_TRACE_MANAGER.get_or_init(TaskTraceManager::default)
}

pub fn record_task_attempt(
    task_id: &str,
    peer_id: &str,
    model_id: &str,
    is_retry: bool,
    is_fallback: bool,
) -> Option<TaskTrace> {
    global_task_trace_manager()
        .record_attempt(task_id, peer_id, model_id, is_retry, is_fallback)
        .ok()
}

pub fn record_task_latency(task_id: &str, latency_ms: u64) -> Option<TaskTrace> {
    global_task_trace_manager()
        .record_latency(task_id, latency_ms)
        .ok()
}

pub fn task_trace(task_id: &str) -> Option<TaskTrace> {
    global_task_trace_manager().get(task_id)
}

pub fn all_task_traces() -> Vec<TaskTrace> {
    global_task_trace_manager().list()
}

pub fn default_task_trace_path() -> PathBuf {
    if let Some(path) = std::env::var_os("IAMINE_TASK_TRACE_PATH") {
        return PathBuf::from(path);
    }

    #[cfg(test)]
    {
        return std::env::temp_dir().join("iamine-task-traces-tests.json");
    }

    #[cfg(not(test))]
    {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".iamine")
            .join("task_traces.json");
    }
}

fn load_traces_from_path(path: &Path) -> io::Result<HashMap<String, TaskTrace>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let data = fs::read(path)?;
    if data.is_empty() {
        return Ok(HashMap::new());
    }
    serde_json::from_slice(&data).map_err(io::Error::other)
}

fn save_traces_to_path(path: &Path, traces: &HashMap<String, TaskTrace>) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let serialized = serde_json::to_vec_pretty(traces).map_err(io::Error::other)?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_extension(format!("tmp-{}", suffix));
    fs::write(&tmp_path, serialized)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TaskTraceManager, TaskTraceStore};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> std::path::PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-task-trace-{}.json", suffix))
    }

    #[test]
    fn test_task_trace() {
        let mut store = TaskTraceStore::new();
        let trace = store.upsert_attempt("task-1", "peer-a", "llama3-3b", false, false);
        assert_eq!(trace.node_history, vec!["peer-a".to_string()]);
        assert_eq!(trace.model_history, vec!["llama3-3b".to_string()]);
    }

    #[test]
    fn test_retry_trace() {
        let mut store = TaskTraceStore::new();
        store.upsert_attempt("task-1", "peer-a", "llama3-3b", false, false);
        let trace = store.upsert_attempt("task-1", "peer-b", "llama3-3b", true, false);
        assert_eq!(trace.retries, 1);
    }

    #[test]
    fn test_fallback_trace() {
        let mut store = TaskTraceStore::new();
        store.upsert_attempt("task-1", "peer-a", "llama3-3b", false, false);
        let trace = store.upsert_attempt("task-1", "peer-b", "mistral-7b", true, true);
        assert_eq!(trace.fallbacks, 1);
        assert_eq!(trace.model_history.last().unwrap(), "mistral-7b");
    }

    #[test]
    fn test_trace_persistence_roundtrip() {
        let path = temp_path();
        let manager = TaskTraceManager::new(&path).unwrap();
        manager
            .record_attempt("task-1", "peer-a", "llama3-3b", false, false)
            .unwrap();
        manager.record_latency("task-1", 123).unwrap();

        let reloaded = TaskTraceManager::new(&path).unwrap();
        let trace = reloaded.get("task-1").unwrap();
        assert_eq!(trace.total_latency_ms, 123);
        let _ = fs::remove_file(path);
    }
}
