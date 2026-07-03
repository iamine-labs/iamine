use crate::node_identity::{
    create_identity_at_path, default_identity_key_path, key_permissions_private,
    load_identity_from_path, repair_key_permissions, NodeIdentity,
};
use serde::Serialize;
use std::error::Error;
use std::path::{Path, PathBuf};

pub(crate) const NODE_IDENTITY_SCHEMA_VERSION: &str = "1.0.0";
pub(crate) const NODE_IDENTITY_FEATURE: &str = "NODE-IDENTITY-REGISTRATION-001";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeIdentityAction {
    Status,
    Init,
}

impl NodeIdentityAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Init => "init",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NodeIdentityCommand {
    pub(crate) action: NodeIdentityAction,
    pub(crate) json: bool,
    pub(crate) path: Option<PathBuf>,
}

impl NodeIdentityCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        let action = match args.first().map(String::as_str) {
            Some("status") | None => NodeIdentityAction::Status,
            Some("init") => NodeIdentityAction::Init,
            Some(_) => return Err(node_identity_usage()),
        };

        Ok(Self {
            action,
            json: args.iter().any(|arg| arg == "--json"),
            path: parse_path_arg(args)?,
        })
    }

    fn resolved_path(&self) -> PathBuf {
        match &self.path {
            Some(path) => path.clone(),
            None => default_identity_key_path(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeIdentityState {
    Missing,
    Registered,
    InvalidKey,
}

impl NodeIdentityState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Registered => "registered",
            Self::InvalidKey => "invalid_key",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeIdentityReportStatus {
    Pass,
    Warn,
    Fail,
}

impl NodeIdentityReportStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct NodeIdentityRuntimeEffects {
    pub(crate) worker_started: bool,
    pub(crate) p2p_started: bool,
    pub(crate) pubsub_started: bool,
    pub(crate) model_download_started: bool,
    pub(crate) model_load_started: bool,
    pub(crate) inference_started: bool,
    pub(crate) dynamic_hardware_probe_started: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NodeIdentityReport {
    pub(crate) report_schema_version: &'static str,
    pub(crate) feature: &'static str,
    pub(crate) command: &'static str,
    pub(crate) action: &'static str,
    pub(crate) status: NodeIdentityReportStatus,
    pub(crate) identity_state: NodeIdentityState,
    pub(crate) node_id: Option<String>,
    pub(crate) peer_id: Option<String>,
    pub(crate) wallet_address: Option<String>,
    pub(crate) public_key_fingerprint: Option<String>,
    pub(crate) key_path_label: String,
    pub(crate) write_performed: bool,
    pub(crate) permissions_repaired: bool,
    pub(crate) key_permissions_private: bool,
    pub(crate) message: String,
    pub(crate) runtime_side_effects: NodeIdentityRuntimeEffects,
}

struct NodeIdentityReportInput {
    state: NodeIdentityState,
    status: NodeIdentityReportStatus,
    identity: Option<NodeIdentity>,
    write_performed: bool,
    permissions_repaired: bool,
    permissions_private: bool,
    message: String,
}

pub(crate) fn node_identity_usage() -> String {
    "Uso: iamine-node node identity [status|init] [--path PATH] [--json]".to_string()
}

pub(crate) fn run_node_identity_cli(command: &NodeIdentityCommand) -> Result<(), Box<dyn Error>> {
    let report = execute_node_identity_command(command)?;
    if command.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", render_node_identity_human(&report));
    }

    if report.status == NodeIdentityReportStatus::Fail {
        Err(report.message.into())
    } else {
        Ok(())
    }
}

pub(crate) fn execute_node_identity_command(
    command: &NodeIdentityCommand,
) -> Result<NodeIdentityReport, String> {
    let path = command.resolved_path();
    match command.action {
        NodeIdentityAction::Status => inspect_node_identity(command, &path),
        NodeIdentityAction::Init => init_node_identity(command, &path),
    }
}

fn inspect_node_identity(
    command: &NodeIdentityCommand,
    path: &Path,
) -> Result<NodeIdentityReport, String> {
    if !path.exists() {
        return Ok(base_report(
            command,
            path,
            NodeIdentityReportInput {
                state: NodeIdentityState::Missing,
                status: NodeIdentityReportStatus::Warn,
                identity: None,
                write_performed: false,
                permissions_repaired: false,
                permissions_private: false,
                message:
                    "node identity is not registered; run init to create a durable operator identity"
                        .to_string(),
            },
        ));
    }

    let identity = match load_identity_from_path(path) {
        Ok(identity) => identity,
        Err(_) => {
            return Ok(base_report(
                command,
                path,
                NodeIdentityReportInput {
                    state: NodeIdentityState::InvalidKey,
                    status: NodeIdentityReportStatus::Fail,
                    identity: None,
                    write_performed: false,
                    permissions_repaired: false,
                    permissions_private: false,
                    message: "node identity key is not parseable; refusing to overwrite it"
                        .to_string(),
                },
            ));
        }
    };
    let permissions_private = key_permissions_private(path);
    let status = if permissions_private {
        NodeIdentityReportStatus::Pass
    } else {
        NodeIdentityReportStatus::Warn
    };
    let message = if permissions_private {
        "node identity is registered".to_string()
    } else {
        "node identity key exists but permissions should be private".to_string()
    };

    Ok(base_report(
        command,
        path,
        NodeIdentityReportInput {
            state: NodeIdentityState::Registered,
            status,
            identity: Some(identity),
            write_performed: false,
            permissions_repaired: false,
            permissions_private,
            message,
        },
    ))
}

fn init_node_identity(
    command: &NodeIdentityCommand,
    path: &Path,
) -> Result<NodeIdentityReport, String> {
    let (identity, write_performed, permissions_repaired) = if path.exists() {
        let identity = load_identity_from_path(path).map_err(|_| {
            "node identity key is not parseable; refusing to overwrite it".to_string()
        })?;
        let permissions_repaired = repair_key_permissions(path)?;
        (identity, false, permissions_repaired)
    } else {
        let identity = create_identity_at_path(path)?;
        (identity, true, false)
    };

    Ok(base_report(
        command,
        path,
        NodeIdentityReportInput {
            state: NodeIdentityState::Registered,
            status: NodeIdentityReportStatus::Pass,
            identity: Some(identity),
            write_performed,
            permissions_repaired,
            permissions_private: key_permissions_private(path),
            message: if write_performed {
                "node identity registered".to_string()
            } else if permissions_repaired {
                "node identity already registered; key permissions repaired".to_string()
            } else {
                "node identity already registered".to_string()
            },
        },
    ))
}

fn base_report(
    command: &NodeIdentityCommand,
    path: &Path,
    input: NodeIdentityReportInput,
) -> NodeIdentityReport {
    let identity = input.identity;
    let node_id = identity.as_ref().map(|identity| identity.node_id.clone());
    let peer_id = identity
        .as_ref()
        .map(|identity| identity.peer_id.to_string());
    let wallet_address = identity
        .as_ref()
        .map(|identity| identity.wallet_address.clone());
    let public_key_fingerprint = identity.as_ref().map(NodeIdentity::public_key_fingerprint);

    NodeIdentityReport {
        report_schema_version: NODE_IDENTITY_SCHEMA_VERSION,
        feature: NODE_IDENTITY_FEATURE,
        command: "iamine-node node identity",
        action: command.action.as_str(),
        status: input.status,
        identity_state: input.state,
        node_id,
        peer_id,
        wallet_address,
        public_key_fingerprint,
        key_path_label: redacted_identity_path_label(path),
        write_performed: input.write_performed,
        permissions_repaired: input.permissions_repaired,
        key_permissions_private: input.permissions_private,
        message: input.message,
        runtime_side_effects: NodeIdentityRuntimeEffects::default(),
    }
}

fn render_node_identity_human(report: &NodeIdentityReport) -> String {
    format!(
        "node_identity_registration: {}\n  action: {}\n  state: {}\n  node_id: {}\n  peer_id: {}\n  wallet: {}\n  public_key_fingerprint: {}\n  key_path: {} (redacted)\n  write_performed: {}\n  permissions_repaired: {}\n  key_permissions_private: {}\n  message: {}",
        report.status.as_str(),
        report.action,
        report.identity_state.as_str(),
        option_text(report.node_id.as_deref()),
        option_text(report.peer_id.as_deref()),
        option_text(report.wallet_address.as_deref()),
        option_text(report.public_key_fingerprint.as_deref()),
        report.key_path_label,
        report.write_performed,
        report.permissions_repaired,
        report.key_permissions_private,
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

fn redacted_identity_path_label(path: &Path) -> String {
    match path.file_name().and_then(|value| value.to_str()) {
        Some(value) => value.to_string(),
        None => "node_key".to_string(),
    }
}

fn option_text(value: Option<&str>) -> &str {
    match value {
        Some(value) => value,
        None => "-",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_identity_path(name: &str) -> PathBuf {
        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let suffix = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos(),
            Err(_) => 0,
        };
        let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("iamine-node-identity-{suffix}-{counter}"))
            .join(name)
    }

    fn command(action: NodeIdentityAction, path: PathBuf) -> NodeIdentityCommand {
        NodeIdentityCommand {
            action,
            json: true,
            path: Some(path),
        }
    }

    #[test]
    fn node_identity_status_missing_does_not_write() {
        let path = temp_identity_path("node_key");
        let report =
            match execute_node_identity_command(&command(NodeIdentityAction::Status, path.clone()))
            {
                Ok(report) => report,
                Err(error) => {
                    assert!(error.is_empty(), "status returned error");
                    return;
                }
            };

        assert_eq!(report.status, NodeIdentityReportStatus::Warn);
        assert_eq!(report.identity_state, NodeIdentityState::Missing);
        assert!(!report.write_performed);
        assert!(!path.exists());
    }

    #[test]
    fn node_identity_init_creates_private_durable_identity() {
        let path = temp_identity_path("node_key");
        let init_report =
            match execute_node_identity_command(&command(NodeIdentityAction::Init, path.clone())) {
                Ok(report) => report,
                Err(error) => {
                    assert!(error.is_empty(), "init returned error");
                    return;
                }
            };
        let status_report =
            match execute_node_identity_command(&command(NodeIdentityAction::Status, path.clone()))
            {
                Ok(report) => report,
                Err(error) => {
                    assert!(error.is_empty(), "status returned error");
                    return;
                }
            };

        assert_eq!(init_report.status, NodeIdentityReportStatus::Pass);
        assert_eq!(init_report.identity_state, NodeIdentityState::Registered);
        assert!(init_report.write_performed);
        assert!(init_report.key_permissions_private);
        assert_eq!(status_report.status, NodeIdentityReportStatus::Pass);
        assert_eq!(init_report.peer_id, status_report.peer_id);
        assert_eq!(
            init_report.public_key_fingerprint,
            status_report.public_key_fingerprint
        );

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn node_identity_invalid_key_fails_without_overwrite() {
        let path = temp_identity_path("node_key");
        if let Some(parent) = path.parent() {
            assert!(fs::create_dir_all(parent).is_ok());
        }
        assert!(fs::write(&path, b"not-a-key").is_ok());

        let report =
            match execute_node_identity_command(&command(NodeIdentityAction::Status, path.clone()))
            {
                Ok(report) => report,
                Err(error) => {
                    assert!(error.is_empty(), "status returned error");
                    return;
                }
            };
        let init_result =
            execute_node_identity_command(&command(NodeIdentityAction::Init, path.clone()));
        let stored = match fs::read_to_string(&path) {
            Ok(value) => value,
            Err(error) => {
                assert!(error.to_string().is_empty(), "read failed");
                return;
            }
        };

        assert_eq!(report.status, NodeIdentityReportStatus::Fail);
        assert_eq!(report.identity_state, NodeIdentityState::InvalidKey);
        assert!(init_result.is_err());
        assert_eq!(stored, "not-a-key");

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn node_identity_init_repairs_existing_key_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_identity_path("node_key");
        let first = execute_node_identity_command(&command(NodeIdentityAction::Init, path.clone()));
        assert!(first.is_ok());
        assert!(fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).is_ok());

        let report =
            match execute_node_identity_command(&command(NodeIdentityAction::Init, path.clone())) {
                Ok(report) => report,
                Err(error) => {
                    assert!(error.is_empty(), "init returned error");
                    return;
                }
            };

        assert!(!report.write_performed);
        assert!(report.permissions_repaired);
        assert!(report.key_permissions_private);

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn node_identity_cli_parses_path_and_json() {
        let args = vec![
            "init".to_string(),
            "--path".to_string(),
            "/tmp/iamine-node-key".to_string(),
            "--json".to_string(),
        ];

        let command = match NodeIdentityCommand::from_args(&args) {
            Ok(command) => command,
            Err(error) => {
                assert!(error.is_empty(), "parse returned error");
                return;
            }
        };

        assert_eq!(command.action, NodeIdentityAction::Init);
        assert_eq!(
            command.path.as_deref(),
            Some(Path::new("/tmp/iamine-node-key"))
        );
        assert!(command.json);
    }
}
