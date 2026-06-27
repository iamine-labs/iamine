use serde::Serialize;
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) const NODE_CONFIG_SCHEMA_VERSION: &str = "1.0.0";
pub(crate) const NODE_CONFIG_FEATURE: &str = "NODE-CONFIG-SCHEMA-MIGRATION-001";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeConfigAction {
    Status,
    Migrate,
    Rollback,
}

impl NodeConfigAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Migrate => "migrate",
            Self::Rollback => "rollback",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NodeConfigCommand {
    pub(crate) action: NodeConfigAction,
    pub(crate) json: bool,
    pub(crate) yes: bool,
    pub(crate) path: Option<PathBuf>,
}

impl NodeConfigCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        let action = match args.first().map(String::as_str) {
            Some("status") | None => NodeConfigAction::Status,
            Some("migrate") => NodeConfigAction::Migrate,
            Some("rollback") => NodeConfigAction::Rollback,
            Some(_) => return Err(node_config_usage()),
        };

        Ok(Self {
            action,
            json: args.iter().any(|arg| arg == "--json"),
            yes: args.iter().any(|arg| arg == "--yes"),
            path: parse_path_arg(args)?,
        })
    }

    fn resolved_path(&self) -> PathBuf {
        self.path.clone().unwrap_or_else(default_node_config_path)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeConfigState {
    Missing,
    Legacy,
    Current,
    Unsupported,
    InvalidJson,
}

impl NodeConfigState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Legacy => "legacy",
            Self::Current => "current",
            Self::Unsupported => "unsupported",
            Self::InvalidJson => "invalid_json",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeConfigReportStatus {
    Pass,
    Warn,
    Fail,
}

impl NodeConfigReportStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NodeConfigInspection {
    pub(crate) state: NodeConfigState,
    pub(crate) schema_version: Option<String>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NodeConfigReport {
    pub(crate) report_schema_version: &'static str,
    pub(crate) feature: &'static str,
    pub(crate) command: &'static str,
    pub(crate) action: &'static str,
    pub(crate) status: NodeConfigReportStatus,
    pub(crate) config_state: NodeConfigState,
    pub(crate) expected_schema_version: &'static str,
    pub(crate) detected_schema_version: Option<String>,
    pub(crate) path_label: String,
    pub(crate) backup_label: String,
    pub(crate) write_performed: bool,
    pub(crate) backup_created: bool,
    pub(crate) rollback_performed: bool,
    pub(crate) requires_confirmation: bool,
    pub(crate) message: String,
    pub(crate) runtime_side_effects: NodeConfigRuntimeEffects,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct NodeConfigRuntimeEffects {
    pub(crate) worker_started: bool,
    pub(crate) p2p_started: bool,
    pub(crate) pubsub_started: bool,
    pub(crate) model_download_started: bool,
    pub(crate) model_load_started: bool,
    pub(crate) inference_started: bool,
    pub(crate) dynamic_hardware_probe_started: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ReportWriteFlags {
    write_performed: bool,
    backup_created: bool,
    rollback_performed: bool,
    requires_confirmation: bool,
}

pub(crate) fn node_config_usage() -> String {
    "Uso: iamine-node node config [status|migrate|rollback] [--path PATH] [--yes] [--json]"
        .to_string()
}

pub(crate) fn default_node_config_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".iamine")
        .join("config")
        .join("node_config.json")
}

pub(crate) fn inspect_node_config(path: &Path) -> NodeConfigInspection {
    let Ok(contents) = fs::read_to_string(path) else {
        return NodeConfigInspection {
            state: NodeConfigState::Missing,
            schema_version: None,
            reason: None,
        };
    };

    let Ok(value) = serde_json::from_str::<Value>(&contents) else {
        return NodeConfigInspection {
            state: NodeConfigState::InvalidJson,
            schema_version: None,
            reason: Some("node config is not parseable JSON".to_string()),
        };
    };

    let Some(object) = value.as_object() else {
        return NodeConfigInspection {
            state: NodeConfigState::InvalidJson,
            schema_version: None,
            reason: Some("node config root must be a JSON object".to_string()),
        };
    };

    match object.get("schema_version").and_then(Value::as_str) {
        None => NodeConfigInspection {
            state: NodeConfigState::Legacy,
            schema_version: None,
            reason: Some("legacy node config has no schema_version".to_string()),
        },
        Some(NODE_CONFIG_SCHEMA_VERSION) => NodeConfigInspection {
            state: NodeConfigState::Current,
            schema_version: Some(NODE_CONFIG_SCHEMA_VERSION.to_string()),
            reason: None,
        },
        Some(version) => NodeConfigInspection {
            state: NodeConfigState::Unsupported,
            schema_version: Some(version.to_string()),
            reason: Some("node config schema_version is not supported".to_string()),
        },
    }
}

pub(crate) fn run_node_config_cli(command: &NodeConfigCommand) -> Result<(), Box<dyn Error>> {
    let report = execute_node_config_command(command)?;
    if command.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", render_node_config_human(&report));
    }

    if report.status == NodeConfigReportStatus::Fail {
        Err(report.message.into())
    } else {
        Ok(())
    }
}

pub(crate) fn execute_node_config_command(
    command: &NodeConfigCommand,
) -> Result<NodeConfigReport, String> {
    let path = command.resolved_path();
    let backup_path = backup_path_for(&path);
    let inspection = inspect_node_config(&path);

    match command.action {
        NodeConfigAction::Status => Ok(report_from_inspection(
            command,
            &path,
            &backup_path,
            inspection,
        )),
        NodeConfigAction::Migrate => migrate_node_config(command, &path, &backup_path, inspection),
        NodeConfigAction::Rollback => {
            rollback_node_config(command, &path, &backup_path, inspection)
        }
    }
}

fn migrate_node_config(
    command: &NodeConfigCommand,
    path: &Path,
    backup_path: &Path,
    inspection: NodeConfigInspection,
) -> Result<NodeConfigReport, String> {
    match inspection.state {
        NodeConfigState::Legacy => {
            if !command.yes {
                return Ok(base_report(
                    command,
                    path,
                    backup_path,
                    inspection,
                    NodeConfigReportStatus::Warn,
                    ReportWriteFlags {
                        requires_confirmation: true,
                        ..ReportWriteFlags::default()
                    },
                    "legacy node config detected; rerun with --yes to write schema_version"
                        .to_string(),
                ));
            }

            let contents = fs::read_to_string(path).map_err(|error| error.to_string())?;
            let backup_created = !backup_path.exists();
            if backup_created {
                fs::write(backup_path, contents.as_bytes()).map_err(|error| error.to_string())?;
            }

            let mut value: Value =
                serde_json::from_str(&contents).map_err(|error| error.to_string())?;
            let Some(object) = value.as_object_mut() else {
                return Ok(base_report(
                    command,
                    path,
                    backup_path,
                    inspection,
                    NodeConfigReportStatus::Fail,
                    ReportWriteFlags::default(),
                    "node config root must be a JSON object".to_string(),
                ));
            };
            object.insert(
                "schema_version".to_string(),
                Value::String(NODE_CONFIG_SCHEMA_VERSION.to_string()),
            );

            write_json(path, &value)?;
            Ok(base_report(
                command,
                path,
                backup_path,
                NodeConfigInspection {
                    state: NodeConfigState::Current,
                    schema_version: Some(NODE_CONFIG_SCHEMA_VERSION.to_string()),
                    reason: None,
                },
                NodeConfigReportStatus::Pass,
                ReportWriteFlags {
                    write_performed: true,
                    backup_created,
                    ..ReportWriteFlags::default()
                },
                "legacy node config migrated to schema_version 1.0.0".to_string(),
            ))
        }
        NodeConfigState::Current => Ok(base_report(
            command,
            path,
            backup_path,
            inspection,
            NodeConfigReportStatus::Pass,
            ReportWriteFlags::default(),
            "node config already uses schema_version 1.0.0".to_string(),
        )),
        NodeConfigState::Missing => Ok(base_report(
            command,
            path,
            backup_path,
            inspection,
            NodeConfigReportStatus::Warn,
            ReportWriteFlags::default(),
            "node config file does not exist yet; nothing to migrate".to_string(),
        )),
        NodeConfigState::Unsupported | NodeConfigState::InvalidJson => Ok(base_report(
            command,
            path,
            backup_path,
            inspection,
            NodeConfigReportStatus::Fail,
            ReportWriteFlags::default(),
            "node config cannot be migrated automatically".to_string(),
        )),
    }
}

fn rollback_node_config(
    command: &NodeConfigCommand,
    path: &Path,
    backup_path: &Path,
    inspection: NodeConfigInspection,
) -> Result<NodeConfigReport, String> {
    if !backup_path.exists() {
        return Ok(base_report(
            command,
            path,
            backup_path,
            inspection,
            NodeConfigReportStatus::Warn,
            ReportWriteFlags::default(),
            "no legacy backup exists for rollback".to_string(),
        ));
    }

    if !command.yes {
        return Ok(base_report(
            command,
            path,
            backup_path,
            inspection,
            NodeConfigReportStatus::Warn,
            ReportWriteFlags {
                requires_confirmation: true,
                ..ReportWriteFlags::default()
            },
            "legacy backup detected; rerun with --yes to restore it".to_string(),
        ));
    }

    let backup_contents = fs::read(backup_path).map_err(|error| error.to_string())?;
    fs::write(path, backup_contents).map_err(|error| error.to_string())?;
    Ok(base_report(
        command,
        path,
        backup_path,
        inspect_node_config(path),
        NodeConfigReportStatus::Pass,
        ReportWriteFlags {
            write_performed: true,
            rollback_performed: true,
            ..ReportWriteFlags::default()
        },
        "legacy node config backup restored".to_string(),
    ))
}

fn report_from_inspection(
    command: &NodeConfigCommand,
    path: &Path,
    backup_path: &Path,
    inspection: NodeConfigInspection,
) -> NodeConfigReport {
    let (status, message) = match inspection.state {
        NodeConfigState::Current => (
            NodeConfigReportStatus::Pass,
            "node config is current".to_string(),
        ),
        NodeConfigState::Missing => (
            NodeConfigReportStatus::Pass,
            "node config schema is available; config file does not exist yet".to_string(),
        ),
        NodeConfigState::Legacy => (
            NodeConfigReportStatus::Warn,
            "legacy node config can be migrated".to_string(),
        ),
        NodeConfigState::Unsupported => (
            NodeConfigReportStatus::Fail,
            "node config schema_version is unsupported".to_string(),
        ),
        NodeConfigState::InvalidJson => (
            NodeConfigReportStatus::Fail,
            "node config is not valid JSON".to_string(),
        ),
    };

    base_report(
        command,
        path,
        backup_path,
        inspection,
        status,
        ReportWriteFlags::default(),
        message,
    )
}

fn base_report(
    command: &NodeConfigCommand,
    path: &Path,
    backup_path: &Path,
    inspection: NodeConfigInspection,
    status: NodeConfigReportStatus,
    write_flags: ReportWriteFlags,
    message: String,
) -> NodeConfigReport {
    NodeConfigReport {
        report_schema_version: NODE_CONFIG_SCHEMA_VERSION,
        feature: NODE_CONFIG_FEATURE,
        command: "iamine-node node config",
        action: command.action.as_str(),
        status,
        config_state: inspection.state,
        expected_schema_version: NODE_CONFIG_SCHEMA_VERSION,
        detected_schema_version: inspection.schema_version,
        path_label: redacted_node_config_path_label(path),
        backup_label: redacted_node_config_path_label(backup_path),
        write_performed: write_flags.write_performed,
        backup_created: write_flags.backup_created,
        rollback_performed: write_flags.rollback_performed,
        requires_confirmation: write_flags.requires_confirmation,
        message,
        runtime_side_effects: NodeConfigRuntimeEffects::default(),
    }
}

fn render_node_config_human(report: &NodeConfigReport) -> String {
    format!(
        "node_config_schema: {}\n  action: {}\n  state: {}\n  schema_version: {}\n  path: {} (redacted)\n  backup: {} (redacted)\n  write_performed: {}\n  backup_created: {}\n  rollback_performed: {}\n  requires_confirmation: {}\n  message: {}",
        report.status.as_str(),
        report.action,
        report.config_state.as_str(),
        report
            .detected_schema_version
            .as_deref()
            .unwrap_or(report.expected_schema_version),
        report.path_label,
        report.backup_label,
        report.write_performed,
        report.backup_created,
        report.rollback_performed,
        report.requires_confirmation,
        report.message
    )
}

fn parse_path_arg(args: &[String]) -> Result<Option<PathBuf>, String> {
    let Some(index) = args.iter().position(|arg| arg == "--path") else {
        return Ok(None);
    };

    let Some(raw) = args.get(index + 1) else {
        return Err("Falta valor para --path".to_string());
    };

    if raw.trim().is_empty() {
        Err("Valor invalido para --path".to_string())
    } else {
        Ok(Some(PathBuf::from(raw)))
    }
}

fn backup_path_for(path: &Path) -> PathBuf {
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("node_config");
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("json");
    path.with_file_name(format!("{stem}.legacy-backup.{extension}"))
}

fn write_json(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let data = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    fs::write(path, data).map_err(|error| error.to_string())
}

pub(crate) fn redacted_node_config_path_label(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("node_config_path")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_config_path(name: &str) -> PathBuf {
        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("iamine-node-config-schema-{suffix}-{counter}"))
            .join(name)
    }

    fn command(action: NodeConfigAction, path: PathBuf, yes: bool) -> NodeConfigCommand {
        NodeConfigCommand {
            action,
            json: true,
            yes,
            path: Some(path),
        }
    }

    #[test]
    fn node_config_status_missing_is_schema_ready() {
        let path = temp_config_path("node_config.json");
        let report = execute_node_config_command(&command(NodeConfigAction::Status, path, false))
            .expect("status should not fail");

        assert_eq!(report.status, NodeConfigReportStatus::Pass);
        assert_eq!(report.config_state, NodeConfigState::Missing);
        assert!(!report.write_performed);
    }

    #[test]
    fn node_config_detects_legacy_without_writing() {
        let path = temp_config_path("node_config.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, br#"{"first_run_completed":true}"#).unwrap();

        let report =
            execute_node_config_command(&command(NodeConfigAction::Migrate, path.clone(), false))
                .expect("dry-run migrate should not fail");
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();

        assert_eq!(report.status, NodeConfigReportStatus::Warn);
        assert_eq!(report.config_state, NodeConfigState::Legacy);
        assert!(report.requires_confirmation);
        assert!(value.get("schema_version").is_none());

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn node_config_migrates_legacy_with_backup() {
        let path = temp_config_path("node_config.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, br#"{"first_run_completed":true}"#).unwrap();

        let report =
            execute_node_config_command(&command(NodeConfigAction::Migrate, path.clone(), true))
                .expect("migrate should pass");
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let backup = backup_path_for(&path);

        assert_eq!(report.status, NodeConfigReportStatus::Pass);
        assert_eq!(report.config_state, NodeConfigState::Current);
        assert!(report.write_performed);
        assert!(backup.exists());
        assert_eq!(value["schema_version"], NODE_CONFIG_SCHEMA_VERSION);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn node_config_rollback_requires_confirmation() {
        let path = temp_config_path("node_config.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            json!({"schema_version": NODE_CONFIG_SCHEMA_VERSION, "first_run_completed": true})
                .to_string(),
        )
        .unwrap();
        fs::write(backup_path_for(&path), br#"{"first_run_completed":true}"#).unwrap();

        let report =
            execute_node_config_command(&command(NodeConfigAction::Rollback, path.clone(), false))
                .expect("rollback dry-run should pass");
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();

        assert_eq!(report.status, NodeConfigReportStatus::Warn);
        assert!(report.requires_confirmation);
        assert_eq!(value["schema_version"], NODE_CONFIG_SCHEMA_VERSION);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn node_config_rolls_back_from_backup() {
        let path = temp_config_path("node_config.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            json!({"schema_version": NODE_CONFIG_SCHEMA_VERSION, "first_run_completed": true})
                .to_string(),
        )
        .unwrap();
        fs::write(backup_path_for(&path), br#"{"first_run_completed":true}"#).unwrap();

        let report =
            execute_node_config_command(&command(NodeConfigAction::Rollback, path.clone(), true))
                .expect("rollback should pass");
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();

        assert_eq!(report.status, NodeConfigReportStatus::Pass);
        assert!(report.write_performed);
        assert!(report.rollback_performed);
        assert!(value.get("schema_version").is_none());

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn node_config_rejects_unsupported_schema() {
        let path = temp_config_path("node_config.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, br#"{"schema_version":"9.9.9"}"#).unwrap();

        let report =
            execute_node_config_command(&command(NodeConfigAction::Status, path.clone(), false))
                .expect("status should return report");

        assert_eq!(report.status, NodeConfigReportStatus::Fail);
        assert_eq!(report.config_state, NodeConfigState::Unsupported);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn node_config_cli_parses_path_json_yes() {
        let args = vec![
            "migrate".to_string(),
            "--path".to_string(),
            "/tmp/iamine-node-config.json".to_string(),
            "--yes".to_string(),
            "--json".to_string(),
        ];

        let command = NodeConfigCommand::from_args(&args).expect("command should parse");

        assert_eq!(command.action, NodeConfigAction::Migrate);
        assert_eq!(
            command.path.as_deref(),
            Some(Path::new("/tmp/iamine-node-config.json"))
        );
        assert!(command.yes);
        assert!(command.json);
    }
}
