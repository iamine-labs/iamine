use crate::lan_node_doctor::{build_lan_node_doctor_report, DoctorCheck, DoctorStatus};
use serde::Serialize;
use std::fs;
#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::Write;
use std::path::{Path, PathBuf};

const SUPPORT_BUNDLE_SCHEMA_VERSION: &str = "1.0.0";
const SUPPORT_BUNDLE_COMMAND: &str = "iamine-node support bundle";
const SUPPORT_BUNDLE_FEATURE: &str = "USER-DIAGNOSTICS-SUPPORT-001";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SupportAction {
    Bundle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SupportCommand {
    pub(crate) action: SupportAction,
    pub(crate) json: bool,
    pub(crate) output: Option<PathBuf>,
}

impl SupportCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        match args.first().map(|value| value.as_str()) {
            Some("bundle") => Ok(Self {
                action: SupportAction::Bundle,
                json: args.iter().any(|arg| arg == "--json"),
                output: parse_output_arg(args)?,
            }),
            _ => Err(support_usage()),
        }
    }
}

pub(crate) fn support_usage() -> String {
    "Uso: iamine-node support bundle [--output PATH] [--json]".to_string()
}

pub(crate) fn run_support_cli(command: &SupportCommand) -> Result<(), String> {
    match command.action {
        SupportAction::Bundle => {
            let report = build_support_bundle_report(command.output.as_deref());
            if let Some(output) = &command.output {
                write_support_bundle(output, &report)?;
            }

            if command.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
                );
            } else {
                print_support_bundle_report(&report);
            }

            Ok(())
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SupportBundleReport {
    schema_version: &'static str,
    feature: &'static str,
    command: &'static str,
    write_performed: bool,
    bundle_path_label: Option<String>,
    privacy: SupportPrivacyPolicy,
    runtime_side_effects: SupportRuntimeSideEffects,
    diagnostic_summary: SupportDiagnosticSummary,
    checks: Vec<SupportCheckSummary>,
    action_items: Vec<SupportActionItem>,
}

#[derive(Debug, Clone, Serialize)]
struct SupportPrivacyPolicy {
    usernames_collected: bool,
    home_directories_collected: bool,
    full_hostnames_collected: bool,
    mac_addresses_collected: bool,
    ip_addresses_collected: bool,
    serial_numbers_collected: bool,
    disk_uuids_collected: bool,
    machine_ids_collected: bool,
    user_process_lists_collected: bool,
    personal_paths_collected: bool,
    raw_logs_collected: bool,
    secrets_collected: bool,
    path_strategy: &'static str,
}

impl SupportPrivacyPolicy {
    fn redacted_by_default() -> Self {
        Self {
            usernames_collected: false,
            home_directories_collected: false,
            full_hostnames_collected: false,
            mac_addresses_collected: false,
            ip_addresses_collected: false,
            serial_numbers_collected: false,
            disk_uuids_collected: false,
            machine_ids_collected: false,
            user_process_lists_collected: false,
            personal_paths_collected: false,
            raw_logs_collected: false,
            secrets_collected: false,
            path_strategy: "file_name_label_only",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SupportRuntimeSideEffects {
    workers_started: bool,
    p2p_started: bool,
    pubsub_started: bool,
    model_download_started: bool,
    model_load_started: bool,
    inference_started: bool,
    dynamic_hardware_probe_started: bool,
}

impl SupportRuntimeSideEffects {
    fn diagnostic_only() -> Self {
        Self {
            workers_started: false,
            p2p_started: false,
            pubsub_started: false,
            model_download_started: false,
            model_load_started: false,
            inference_started: false,
            dynamic_hardware_probe_started: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SupportDiagnosticSummary {
    source: &'static str,
    overall_status: DoctorStatus,
    pass_count: usize,
    warn_count: usize,
    fail_count: usize,
    not_run_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct SupportCheckSummary {
    id: &'static str,
    status: DoctorStatus,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct SupportActionItem {
    id: &'static str,
    severity: &'static str,
    check_id: &'static str,
    message: &'static str,
    next_command: Option<&'static str>,
}

pub(crate) fn build_support_bundle_report(output: Option<&Path>) -> SupportBundleReport {
    let doctor_report = build_lan_node_doctor_report(false);
    let checks = doctor_report
        .checks()
        .iter()
        .map(support_check_summary)
        .collect::<Vec<_>>();
    let action_items = doctor_report
        .checks()
        .iter()
        .filter_map(support_action_item)
        .collect::<Vec<_>>();

    SupportBundleReport {
        schema_version: SUPPORT_BUNDLE_SCHEMA_VERSION,
        feature: SUPPORT_BUNDLE_FEATURE,
        command: SUPPORT_BUNDLE_COMMAND,
        write_performed: output.is_some(),
        bundle_path_label: output.map(redacted_path_label),
        privacy: SupportPrivacyPolicy::redacted_by_default(),
        runtime_side_effects: SupportRuntimeSideEffects::diagnostic_only(),
        diagnostic_summary: support_diagnostic_summary(doctor_report.overall_status(), &checks),
        checks,
        action_items,
    }
}

fn support_diagnostic_summary(
    overall_status: DoctorStatus,
    checks: &[SupportCheckSummary],
) -> SupportDiagnosticSummary {
    SupportDiagnosticSummary {
        source: "lan_node_doctor",
        overall_status,
        pass_count: count_status(checks, DoctorStatus::Pass),
        warn_count: count_status(checks, DoctorStatus::Warn),
        fail_count: count_status(checks, DoctorStatus::Fail),
        not_run_count: count_status(checks, DoctorStatus::NotRun),
    }
}

fn support_check_summary(check: &DoctorCheck) -> SupportCheckSummary {
    SupportCheckSummary {
        id: check.id,
        status: check.status,
        message: check.message.clone(),
    }
}

fn count_status(checks: &[SupportCheckSummary], status: DoctorStatus) -> usize {
    checks.iter().filter(|check| check.status == status).count()
}

fn support_action_item(check: &DoctorCheck) -> Option<SupportActionItem> {
    match (check.id, check.status) {
        (_, DoctorStatus::Pass) => None,
        ("hardware_profile_visibility", DoctorStatus::Fail) => Some(SupportActionItem {
            id: "inspect_hardware_visibility",
            severity: "error",
            check_id: check.id,
            message: "Local hardware inspection failed or returned an unsupported schema.",
            next_command: Some("iamine-node hardware inspect --json"),
        }),
        ("model_catalog_gates", DoctorStatus::Fail | DoctorStatus::Warn) => {
            Some(SupportActionItem {
                id: "review_model_catalog",
                severity: severity_for_status(check.status),
                check_id: check.id,
                message: "Model catalog gates did not find a ready local model.",
                next_command: Some("iamine-node models catalog"),
            })
        }
        ("backend_availability", DoctorStatus::Warn | DoctorStatus::Fail) => {
            Some(SupportActionItem {
                id: "review_backend_availability",
                severity: severity_for_status(check.status),
                check_id: check.id,
                message: "Backend policy does not currently permit real inference.",
                next_command: Some("iamine-node lan doctor --json"),
            })
        }
        ("worker_startup_policy", DoctorStatus::Warn | DoctorStatus::Fail) => {
            Some(SupportActionItem {
                id: "review_worker_startup_policy",
                severity: severity_for_status(check.status),
                check_id: check.id,
                message: "Worker startup policy is in diagnostic or degraded mode.",
                next_command: Some("iamine-node worker lifecycle readiness --json"),
            })
        }
        ("metrics_availability", DoctorStatus::Warn | DoctorStatus::Fail) => {
            Some(SupportActionItem {
                id: "review_metrics_availability",
                severity: severity_for_status(check.status),
                check_id: check.id,
                message: "Metrics endpoint policy would not start a normal metrics server.",
                next_command: Some("iamine-node worker lifecycle readiness --json"),
            })
        }
        ("node_config_schema", DoctorStatus::Warn | DoctorStatus::Fail) => {
            Some(SupportActionItem {
                id: "review_node_config_schema",
                severity: severity_for_status(check.status),
                check_id: check.id,
                message: "Node config is legacy, invalid, or unsupported.",
                next_command: Some("iamine-node node config status --json"),
            })
        }
        ("lan_peer_pubsub_readiness", DoctorStatus::NotRun) => Some(SupportActionItem {
            id: "optional_lan_network_diagnostic",
            severity: "info",
            check_id: check.id,
            message:
                "LAN network readiness was not probed because support bundles avoid starting P2P.",
            next_command: Some("iamine-node lan doctor --network --json"),
        }),
        (_, DoctorStatus::Warn | DoctorStatus::Fail | DoctorStatus::NotRun) => {
            Some(SupportActionItem {
                id: "review_diagnostic_check",
                severity: severity_for_status(check.status),
                check_id: check.id,
                message: "Review the diagnostic check status before opening a support request.",
                next_command: Some("iamine-node lan doctor --json"),
            })
        }
    }
}

fn severity_for_status(status: DoctorStatus) -> &'static str {
    match status {
        DoctorStatus::Fail => "error",
        DoctorStatus::Warn => "warning",
        DoctorStatus::NotRun => "info",
        DoctorStatus::Pass => "none",
    }
}

fn write_support_bundle(path: &Path, report: &SupportBundleReport) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let data = serde_json::to_vec_pretty(report).map_err(|error| error.to_string())?;
    write_private_file(path, &data)
}

#[cfg(unix)]
fn write_private_file(path: &Path, data: &[u8]) -> Result<(), String> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(data).map_err(|error| error.to_string())?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|error| error.to_string())
}

#[cfg(not(unix))]
fn write_private_file(path: &Path, data: &[u8]) -> Result<(), String> {
    fs::write(path, data).map_err(|error| error.to_string())
}

fn print_support_bundle_report(report: &SupportBundleReport) {
    println!("IAMINE support bundle");
    println!("schema_version={}", report.schema_version);
    println!("feature={}", report.feature);
    println!(
        "overall_status={}",
        report.diagnostic_summary.overall_status.as_str()
    );
    println!("write_performed={}", report.write_performed);
    if let Some(label) = &report.bundle_path_label {
        println!("bundle_path_label={label} (redacted)");
    }
    println!("action_items={}", report.action_items.len());
    for item in &report.action_items {
        println!("[{}] {} - {}", item.severity, item.check_id, item.message);
        if let Some(command) = item.next_command {
            println!("  next_command={command}");
        }
    }
}

fn parse_output_arg(args: &[String]) -> Result<Option<PathBuf>, String> {
    let Some(index) = args.iter().position(|arg| arg == "--output") else {
        return Ok(None);
    };

    let Some(raw) = args.get(index + 1) else {
        return Err("Falta valor para --output".to_string());
    };

    if raw.trim().is_empty() {
        Err("Valor invalido para --output".to_string())
    } else {
        Ok(Some(PathBuf::from(raw)))
    }
}

fn redacted_path_label(path: &Path) -> String {
    match path.file_name().and_then(|value| value.to_str()) {
        Some(value) => value.to_string(),
        None => "iamine-support-bundle.json".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn support_command_parses_bundle_json_output() {
        let command = match SupportCommand::from_args(&args(&[
            "bundle",
            "--output",
            "/tmp/private/operator/support.json",
            "--json",
        ])) {
            Ok(command) => command,
            Err(error) => {
                assert_eq!(error, "command parsed");
                return;
            }
        };

        assert_eq!(command.action, SupportAction::Bundle);
        assert!(command.json);
        assert_eq!(
            command.output.as_deref(),
            Some(Path::new("/tmp/private/operator/support.json"))
        );
    }

    #[test]
    fn support_bundle_redacts_output_path_to_file_label() {
        let report =
            build_support_bundle_report(Some(Path::new("/tmp/private/operator/support.json")));

        assert!(report.write_performed);
        assert_eq!(report.bundle_path_label.as_deref(), Some("support.json"));
    }

    #[test]
    fn support_bundle_privacy_policy_blocks_sensitive_sources() {
        let report = build_support_bundle_report(None);

        assert!(!report.privacy.usernames_collected);
        assert!(!report.privacy.home_directories_collected);
        assert!(!report.privacy.full_hostnames_collected);
        assert!(!report.privacy.mac_addresses_collected);
        assert!(!report.privacy.ip_addresses_collected);
        assert!(!report.privacy.raw_logs_collected);
        assert!(!report.privacy.secrets_collected);
    }

    #[test]
    fn support_bundle_does_not_start_runtime_services() {
        let report = build_support_bundle_report(None);

        assert!(!report.runtime_side_effects.workers_started);
        assert!(!report.runtime_side_effects.p2p_started);
        assert!(!report.runtime_side_effects.pubsub_started);
        assert!(!report.runtime_side_effects.model_download_started);
        assert!(!report.runtime_side_effects.model_load_started);
        assert!(!report.runtime_side_effects.inference_started);
    }

    #[test]
    fn support_bundle_includes_actionable_diagnostic_summary() {
        let report = build_support_bundle_report(None);

        assert_eq!(report.diagnostic_summary.source, "lan_node_doctor");
        assert!(!report.checks.is_empty());
        assert!(report
            .action_items
            .iter()
            .any(|item| item.next_command.is_some()));
    }

    #[test]
    fn support_bundle_rejects_empty_output_path() {
        match SupportCommand::from_args(&args(&["bundle", "--output", ""])) {
            Ok(command) => assert_eq!(command.output, Some(PathBuf::from("rejected"))),
            Err(error) => assert_eq!(error, "Valor invalido para --output"),
        }
    }

    #[test]
    fn support_bundle_writes_json_file() {
        let temp = match tempfile::tempdir() {
            Ok(temp) => temp,
            Err(error) => {
                assert_eq!(error.to_string(), "tempdir ok");
                return;
            }
        };
        let path = temp.path().join("support.json");
        let report = build_support_bundle_report(Some(&path));

        if let Err(error) = write_support_bundle(&path, &report) {
            assert_eq!(error, "bundle wrote");
            return;
        }

        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(error) => {
                assert_eq!(error.to_string(), "bundle read");
                return;
            }
        };
        assert!(data.contains("\"schema_version\": \"1.0.0\""));
        assert!(data.contains("\"path_strategy\": \"file_name_label_only\""));
        assert!(!data.contains(temp.path().to_string_lossy().as_ref()));
    }

    #[cfg(unix)]
    #[test]
    fn support_bundle_file_permissions_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp = match tempfile::tempdir() {
            Ok(temp) => temp,
            Err(error) => {
                assert_eq!(error.to_string(), "tempdir ok");
                return;
            }
        };
        let path = temp.path().join("support.json");
        let report = build_support_bundle_report(Some(&path));

        if let Err(error) = write_support_bundle(&path, &report) {
            assert_eq!(error, "bundle wrote");
            return;
        }

        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) => {
                assert_eq!(error.to_string(), "bundle metadata");
                return;
            }
        };
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
