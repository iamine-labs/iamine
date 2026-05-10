use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, OnceLock, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_LOG_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone)]
struct RuntimeLogContext {
    mode: String,
    version: String,
    log_schema_version: String,
}

impl Default for RuntimeLogContext {
    fn default() -> Self {
        Self {
            mode: "unknown".to_string(),
            version: "unknown".to_string(),
            log_schema_version: DEFAULT_LOG_SCHEMA_VERSION.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructuredLogEntry {
    pub timestamp: String,
    pub level: String,
    pub event: String,
    pub trace_id: String,
    pub node_id: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub log_schema_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub fields: Map<String, Value>,
}

impl StructuredLogEntry {
    pub fn new(
        level: LogLevel,
        event: impl Into<String>,
        trace_id: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            timestamp: iso8601_now(),
            level: level.as_str().to_string(),
            event: event.into(),
            trace_id: trace_id.into(),
            node_id: node_id.into(),
            mode: String::new(),
            version: String::new(),
            log_schema_version: String::new(),
            task_id: None,
            model_id: None,
            error_code: None,
            fields: Map::new(),
        }
    }

    pub fn with_mode(mut self, mode: impl Into<String>) -> Self {
        self.mode = mode.into();
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn with_log_schema_version(mut self, version: impl Into<String>) -> Self {
        self.log_schema_version = version.into();
        self
    }

    pub fn with_task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    pub fn with_model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }

    pub fn with_error_code(mut self, error_code: impl Into<String>) -> Self {
        self.error_code = Some(error_code.into());
        self
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }
}

fn iso8601_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let total_secs = now.as_secs() as i64;
    let millis = now.subsec_millis();
    let days = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hour, minute, second, millis
    )
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

enum LogCommand {
    Entry(String),
    Flush(mpsc::Sender<()>),
}

#[derive(Clone)]
pub struct StructuredLogger {
    path: PathBuf,
    node_id: Arc<RwLock<String>>,
    runtime_context: Arc<RwLock<RuntimeLogContext>>,
    enabled: bool,
    tx: mpsc::Sender<LogCommand>,
}

impl StructuredLogger {
    pub fn new(path: impl Into<PathBuf>, node_id: impl Into<String>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let (tx, rx) = mpsc::channel::<LogCommand>();
        let writer_path = path.clone();
        thread::spawn(move || {
            let file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&writer_path)
            {
                Ok(file) => file,
                Err(_) => return,
            };
            let mut writer = BufWriter::new(file);
            while let Ok(command) = rx.recv() {
                match command {
                    LogCommand::Entry(line) => {
                        let _ = writeln!(writer, "{}", line);
                        let _ = writer.flush();
                    }
                    LogCommand::Flush(ack) => {
                        let _ = writer.flush();
                        let _ = ack.send(());
                    }
                }
            }
        });

        Ok(Self {
            path,
            node_id: Arc::new(RwLock::new(node_id.into())),
            runtime_context: Arc::new(RwLock::new(RuntimeLogContext::default())),
            enabled: true,
            tx,
        })
    }

    pub fn noop(path: impl Into<PathBuf>, node_id: impl Into<String>) -> Self {
        let (tx, _rx) = mpsc::channel::<LogCommand>();
        Self {
            path: path.into(),
            node_id: Arc::new(RwLock::new(node_id.into())),
            runtime_context: Arc::new(RwLock::new(RuntimeLogContext::default())),
            enabled: false,
            tx,
        }
    }

    pub fn set_node_id(&self, node_id: impl Into<String>) {
        *self.node_id.write().expect("structured logger poisoned") = node_id.into();
    }

    pub fn set_runtime_context(&self, mode: impl Into<String>, version: impl Into<String>) {
        let mut context = self
            .runtime_context
            .write()
            .expect("structured logger poisoned");
        context.mode = mode.into();
        context.version = version.into();
        if context.log_schema_version.is_empty() {
            context.log_schema_version = DEFAULT_LOG_SCHEMA_VERSION.to_string();
        }
    }

    pub fn log(&self, mut entry: StructuredLogEntry) -> io::Result<()> {
        if entry.node_id.is_empty() || entry.node_id == "-" {
            entry.node_id = self
                .node_id
                .read()
                .expect("structured logger poisoned")
                .clone();
        }
        {
            let context = self
                .runtime_context
                .read()
                .expect("structured logger poisoned");
            if entry.mode.trim().is_empty() {
                entry.mode = context.mode.clone();
            }
            if entry.version.trim().is_empty() {
                entry.version = context.version.clone();
            }
            if entry.log_schema_version.trim().is_empty() {
                entry.log_schema_version = context.log_schema_version.clone();
            }
        }
        if entry.log_schema_version.trim().is_empty() {
            entry.log_schema_version = DEFAULT_LOG_SCHEMA_VERSION.to_string();
        }
        if !self.enabled {
            return Ok(());
        }
        let serialized = serde_json::to_string(&entry).map_err(io::Error::other)?;
        let _ = self.tx.send(LogCommand::Entry(serialized));
        Ok(())
    }

    pub fn flush(&self) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.tx.send(LogCommand::Flush(ack_tx)).is_err() {
            return Ok(());
        }
        let _ = ack_rx.recv_timeout(Duration::from_secs(1));
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

static GLOBAL_LOGGER: OnceLock<StructuredLogger> = OnceLock::new();

fn ndjson_logging_requested() -> bool {
    std::env::var("IAMINE_LOG_FORMAT")
        .map(|value| value.trim().eq_ignore_ascii_case("ndjson"))
        .unwrap_or(true)
}

pub fn default_node_log_path() -> PathBuf {
    if let Some(path) = std::env::var_os("IAMINE_LOG_PATH") {
        return PathBuf::from(path);
    }

    if let Some(path) = std::env::var_os("IAMINE_NODE_LOG_PATH") {
        return PathBuf::from(path);
    }

    #[cfg(test)]
    {
        return std::env::temp_dir().join("iamine-node-log-tests.ndjson");
    }

    #[cfg(not(test))]
    {
        return PathBuf::from("logs").join("iamine-node.ndjson");
    }
}

pub fn global_structured_logger() -> &'static StructuredLogger {
    GLOBAL_LOGGER.get_or_init(|| {
        let path = default_node_log_path();
        if !ndjson_logging_requested() {
            return StructuredLogger::noop(path, "-");
        }
        match StructuredLogger::new(path.clone(), "-") {
            Ok(logger) => logger,
            Err(error) => {
                eprintln!("[StructuredLogger] disabled: {}", error);
                StructuredLogger::noop(path, "-")
            }
        }
    })
}

pub fn set_global_node_id(node_id: &str) {
    global_structured_logger().set_node_id(node_id.to_string());
}

pub fn set_global_runtime_context(mode: &str, version: &str) {
    global_structured_logger().set_runtime_context(mode, version);
}

pub fn log_structured(entry: StructuredLogEntry) -> io::Result<()> {
    global_structured_logger().log(entry)
}

pub fn flush_structured_logs() -> io::Result<()> {
    global_structured_logger().flush()
}

pub fn read_log_entries(path: &Path) -> io::Result<Vec<StructuredLogEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = OpenOptions::new().read(true).open(path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        entries.push(serde_json::from_str(&line).map_err(io::Error::other)?);
    }
    Ok(entries)
}

pub fn normalize_prompt_for_log(prompt: &str) -> String {
    prompt
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase()
}

pub fn prompt_log_entry(
    trace_id: &str,
    prompt: &str,
    language: &str,
    task_type: &str,
    confidence: f32,
) -> StructuredLogEntry {
    StructuredLogEntry::new(LogLevel::Info, "prompt_received", trace_id, "-")
        .with_field("prompt", normalize_prompt_for_log(prompt))
        .with_field("language", language)
        .with_field("task_type", task_type)
        .with_field("prompt_length", prompt.trim().chars().count() as u64)
        .with_field("semantic_confidence", confidence as f64)
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_prompt_for_log, prompt_log_entry, read_log_entries, LogLevel, StructuredLogEntry,
        StructuredLogger,
    };
    use std::fs;
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-observability-{}.ndjson", suffix))
    }

    #[test]
    fn test_log_written() {
        let path = temp_path();
        let logger = StructuredLogger::new(&path, "node-a").unwrap();
        logger.set_runtime_context("worker", "v0.6.35");
        logger
            .log(
                StructuredLogEntry::new(LogLevel::Info, "task_created", "trace-1", "-")
                    .with_task_id("task-1"),
            )
            .unwrap();
        logger.flush().unwrap();
        let entries = read_log_entries(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event, "task_created");
        assert!(!entries[0].timestamp.is_empty());
        assert_eq!(entries[0].level, "INFO");
        assert_eq!(entries[0].node_id, "node-a");
        assert_eq!(entries[0].mode, "worker");
        assert_eq!(entries[0].version, "v0.6.35");
        assert_eq!(entries[0].log_schema_version, "1.0.0");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_prompt_anonymization() {
        let entry = prompt_log_entry(
            "trace-2",
            "  Explica   La Relatividad  ",
            "es",
            "Conceptual",
            0.82,
        );
        assert_eq!(
            entry.fields.get("prompt").and_then(|value| value.as_str()),
            Some("explica la relatividad")
        );
        assert!(entry.fields.get("user_id").is_none());
        assert_eq!(normalize_prompt_for_log("  Hola   MUNDO "), "hola mundo");
    }

    #[test]
    fn test_trace_id_propagation() {
        let path = temp_path();
        let logger = StructuredLogger::new(&path, "node-a").unwrap();
        logger
            .log(StructuredLogEntry::new(
                LogLevel::Info,
                "task_created",
                "trace-shared",
                "-",
            ))
            .unwrap();
        logger
            .log(StructuredLogEntry::new(
                LogLevel::Info,
                "task_sent",
                "trace-shared",
                "-",
            ))
            .unwrap();
        logger.flush().unwrap();
        let entries = read_log_entries(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| entry.trace_id == "trace-shared"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_health_log_entries() {
        let path = temp_path();
        let logger = StructuredLogger::new(&path, "node-a").unwrap();
        logger.set_runtime_context("worker", "v0.6.35");
        logger
            .log(
                StructuredLogEntry::new(LogLevel::Warn, "health_update", "trace-3", "-")
                    .with_field("success_rate", 0.4)
                    .with_field("timeout_count", 2)
                    .with_field("blacklisted", true),
            )
            .unwrap();
        logger.flush().unwrap();
        let entries = read_log_entries(&path).unwrap();
        assert_eq!(entries[0].event, "health_update");
        assert_eq!(
            entries[0]
                .fields
                .get("blacklisted")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_bad_json_lines_are_zero() {
        let path = temp_path();
        let logger = StructuredLogger::new(&path, "node-a").unwrap();
        logger.set_runtime_context("infer", "v0.6.35");
        logger
            .log(
                StructuredLogEntry::new(
                    LogLevel::Info,
                    "task_dispatch_context",
                    "trace-json-1",
                    "-",
                )
                .with_task_id("task-1")
                .with_model_id("tinyllama-1b")
                .with_field("attempt_id", "attempt-1")
                .with_field("selected_peer_id", "peer-1"),
            )
            .unwrap();
        logger
            .log(
                StructuredLogEntry::new(LogLevel::Error, "task_failed", "trace-json-1", "-")
                    .with_task_id("task-1")
                    .with_model_id("tinyllama-1b")
                    .with_error_code("TASK_FAILED_002")
                    .with_field("attempt_id", "attempt-1")
                    .with_field("error_kind", "validation")
                    .with_field("recoverable", true),
            )
            .unwrap();
        logger.flush().unwrap();

        let file = fs::File::open(&path).unwrap();
        let reader = BufReader::new(file);
        let mut bad_json_lines = 0u64;
        let mut total_lines = 0u64;
        for line in reader.lines() {
            let line = line.unwrap();
            if line.trim().is_empty() {
                continue;
            }
            total_lines += 1;
            if serde_json::from_str::<serde_json::Value>(&line).is_err() {
                bad_json_lines += 1;
            }
        }

        assert!(total_lines >= 2);
        assert_eq!(bad_json_lines, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_logger_does_not_panic_on_sink_issues() {
        let path = temp_path();
        fs::create_dir_all(&path).unwrap();
        let logger = StructuredLogger::new(&path, "node-a").unwrap();
        logger.set_runtime_context("worker", "v0.6.35");

        let result = std::panic::catch_unwind(|| {
            logger
                .log(
                    StructuredLogEntry::new(LogLevel::Info, "worker_started", "trace-io-1", "-")
                        .with_field("peer_id", "peer-a"),
                )
                .unwrap();
            logger.flush().unwrap();
        });

        assert!(result.is_ok());
        let _ = fs::remove_dir_all(path);
    }
}
