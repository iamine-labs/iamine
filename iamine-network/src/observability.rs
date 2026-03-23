use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, OnceLock, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
            task_id: None,
            model_id: None,
            error_code: None,
            fields: Map::new(),
        }
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
            tx,
        })
    }

    pub fn set_node_id(&self, node_id: impl Into<String>) {
        *self.node_id.write().expect("structured logger poisoned") = node_id.into();
    }

    pub fn log(&self, mut entry: StructuredLogEntry) -> io::Result<()> {
        if entry.node_id.is_empty() || entry.node_id == "-" {
            entry.node_id = self
                .node_id
                .read()
                .expect("structured logger poisoned")
                .clone();
        }
        let serialized = serde_json::to_string(&entry).map_err(io::Error::other)?;
        self.tx
            .send(LogCommand::Entry(serialized))
            .map_err(io::Error::other)
    }

    pub fn flush(&self) -> io::Result<()> {
        let (ack_tx, ack_rx) = mpsc::channel();
        self.tx
            .send(LogCommand::Flush(ack_tx))
            .map_err(io::Error::other)?;
        ack_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(io::Error::other)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

static GLOBAL_LOGGER: OnceLock<StructuredLogger> = OnceLock::new();

pub fn default_node_log_path() -> PathBuf {
    if let Some(path) = std::env::var_os("IAMINE_NODE_LOG_PATH") {
        return PathBuf::from(path);
    }

    #[cfg(test)]
    {
        return std::env::temp_dir().join("iamine-node-log-tests.ndjson");
    }

    #[cfg(not(test))]
    {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".iamine")
            .join("logs")
            .join("node.log");
    }
}

pub fn global_structured_logger() -> &'static StructuredLogger {
    GLOBAL_LOGGER.get_or_init(|| {
        StructuredLogger::new(default_node_log_path(), "-")
            .expect("failed to initialize structured logger")
    })
}

pub fn set_global_node_id(node_id: &str) {
    global_structured_logger().set_node_id(node_id.to_string());
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
}
