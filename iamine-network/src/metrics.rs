use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DistributedTaskMetrics {
    pub total_tasks: u64,
    pub failed_tasks: u64,
    pub retries_count: u64,
    pub fallback_count: u64,
    #[serde(default)]
    pub late_results_count: u64,
    pub avg_latency_ms: f64,
    #[serde(default)]
    latency_samples: u64,
}

impl DistributedTaskMetrics {
    pub fn task_started(&mut self) {
        self.total_tasks = self.total_tasks.saturating_add(1);
    }

    pub fn task_failed(&mut self) {
        self.failed_tasks = self.failed_tasks.saturating_add(1);
    }

    pub fn retry_recorded(&mut self) {
        self.retries_count = self.retries_count.saturating_add(1);
    }

    pub fn fallback_recorded(&mut self) {
        self.fallback_count = self.fallback_count.saturating_add(1);
    }

    pub fn late_result_recorded(&mut self) {
        self.late_results_count = self.late_results_count.saturating_add(1);
    }

    pub fn record_latency(&mut self, latency_ms: u64) {
        self.latency_samples = self.latency_samples.saturating_add(1);
        let n = self.latency_samples as f64;
        self.avg_latency_ms = (self.avg_latency_ms * (n - 1.0) + latency_ms as f64) / n;
    }
}

#[derive(Debug, Clone)]
pub struct DistributedTaskMetricsManager {
    path: PathBuf,
    metrics: Arc<RwLock<DistributedTaskMetrics>>,
}

impl DistributedTaskMetricsManager {
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let metrics = load_metrics_from_path(&path)?;
        Ok(Self {
            path,
            metrics: Arc::new(RwLock::new(metrics)),
        })
    }

    pub fn default() -> Self {
        Self::new(default_task_metrics_path()).unwrap_or_else(|_| Self {
            path: default_task_metrics_path(),
            metrics: Arc::new(RwLock::new(DistributedTaskMetrics::default())),
        })
    }

    pub fn metrics(&self) -> DistributedTaskMetrics {
        self.metrics
            .read()
            .expect("distributed metrics manager poisoned")
            .clone()
    }

    pub fn record_started(&self) -> io::Result<DistributedTaskMetrics> {
        self.update(|metrics| metrics.task_started())
    }

    pub fn record_failed(&self) -> io::Result<DistributedTaskMetrics> {
        self.update(|metrics| metrics.task_failed())
    }

    pub fn record_retry(&self) -> io::Result<DistributedTaskMetrics> {
        self.update(|metrics| metrics.retry_recorded())
    }

    pub fn record_fallback(&self) -> io::Result<DistributedTaskMetrics> {
        self.update(|metrics| metrics.fallback_recorded())
    }

    pub fn record_latency(&self, latency_ms: u64) -> io::Result<DistributedTaskMetrics> {
        self.update(|metrics| metrics.record_latency(latency_ms))
    }

    pub fn record_late_result(&self) -> io::Result<DistributedTaskMetrics> {
        self.update(|metrics| metrics.late_result_recorded())
    }

    fn update(
        &self,
        apply: impl FnOnce(&mut DistributedTaskMetrics),
    ) -> io::Result<DistributedTaskMetrics> {
        let mut latest = load_metrics_from_path(&self.path)?;
        apply(&mut latest);
        save_metrics_to_path(&self.path, &latest)?;
        *self
            .metrics
            .write()
            .expect("distributed metrics manager poisoned") = latest.clone();
        Ok(latest)
    }
}

static GLOBAL_DISTRIBUTED_TASK_METRICS_MANAGER: OnceLock<DistributedTaskMetricsManager> =
    OnceLock::new();

pub fn global_distributed_task_metrics_manager() -> &'static DistributedTaskMetricsManager {
    GLOBAL_DISTRIBUTED_TASK_METRICS_MANAGER.get_or_init(DistributedTaskMetricsManager::default)
}

pub fn distributed_task_metrics() -> DistributedTaskMetrics {
    global_distributed_task_metrics_manager().metrics()
}

pub fn record_distributed_task_started() -> Option<DistributedTaskMetrics> {
    global_distributed_task_metrics_manager()
        .record_started()
        .ok()
}

pub fn record_distributed_task_failed() -> Option<DistributedTaskMetrics> {
    global_distributed_task_metrics_manager()
        .record_failed()
        .ok()
}

pub fn record_distributed_task_retry() -> Option<DistributedTaskMetrics> {
    global_distributed_task_metrics_manager()
        .record_retry()
        .ok()
}

pub fn record_distributed_task_fallback() -> Option<DistributedTaskMetrics> {
    global_distributed_task_metrics_manager()
        .record_fallback()
        .ok()
}

pub fn record_distributed_task_latency(latency_ms: u64) -> Option<DistributedTaskMetrics> {
    global_distributed_task_metrics_manager()
        .record_latency(latency_ms)
        .ok()
}

pub fn record_distributed_task_late_result() -> Option<DistributedTaskMetrics> {
    global_distributed_task_metrics_manager()
        .record_late_result()
        .ok()
}

pub fn default_task_metrics_path() -> PathBuf {
    if let Some(path) = std::env::var_os("IAMINE_TASK_METRICS_PATH") {
        return PathBuf::from(path);
    }

    #[cfg(test)]
    {
        return std::env::temp_dir().join("iamine-task-metrics-tests.json");
    }

    #[cfg(not(test))]
    {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".iamine")
            .join("task_metrics.json");
    }
}

fn load_metrics_from_path(path: &Path) -> io::Result<DistributedTaskMetrics> {
    if !path.exists() {
        return Ok(DistributedTaskMetrics::default());
    }

    let data = fs::read(path)?;
    if data.is_empty() {
        return Ok(DistributedTaskMetrics::default());
    }

    serde_json::from_slice(&data).map_err(io::Error::other)
}

fn save_metrics_to_path(path: &Path, metrics: &DistributedTaskMetrics) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let serialized = serde_json::to_vec_pretty(metrics).map_err(io::Error::other)?;
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
    use super::{
        default_task_metrics_path, distributed_task_metrics, DistributedTaskMetricsManager,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-task-metrics-{}.json", suffix))
    }

    #[test]
    fn test_metrics_tracking() {
        let path = temp_path();
        let manager = DistributedTaskMetricsManager::new(&path).unwrap();
        manager.record_started().unwrap();
        manager.record_retry().unwrap();
        manager.record_fallback().unwrap();
        manager.record_late_result().unwrap();
        manager.record_latency(120).unwrap();
        manager.record_failed().unwrap();

        let metrics = manager.metrics();
        assert_eq!(metrics.total_tasks, 1);
        assert_eq!(metrics.failed_tasks, 1);
        assert_eq!(metrics.retries_count, 1);
        assert_eq!(metrics.fallback_count, 1);
        assert_eq!(metrics.late_results_count, 1);
        assert_eq!(metrics.avg_latency_ms, 120.0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_metrics_persistence_roundtrip() {
        let path = temp_path();
        let manager = DistributedTaskMetricsManager::new(&path).unwrap();
        manager.record_started().unwrap();
        manager.record_latency(200).unwrap();

        let reloaded = DistributedTaskMetricsManager::new(&path).unwrap();
        let metrics = reloaded.metrics();
        assert_eq!(metrics.total_tasks, 1);
        assert_eq!(metrics.avg_latency_ms, 200.0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_default_path_uses_iamine_location() {
        let path = default_task_metrics_path();
        let rendered = path.to_string_lossy();
        assert!(rendered.contains("iamine-task-metrics-tests.json"));
        let _ = distributed_task_metrics();
    }
}
