use crate::metrics_policy::{metrics_startup_decision, MetricsStartupDecision};
use crate::model_backend_availability::{
    ModelBackendAvailabilityReason, ModelBackendAvailabilityStatus,
};
use crate::node_capability_snapshot::{
    capabilities_from_hardware_profile, LOCAL_DIAGNOSTIC_NODE_ID,
};
use crate::worker_startup_policy::WorkerStartupPolicy;
use iamine_hardware::{inspect_hardware, HardwareProfilerConfig};
use iamine_models::ModelStorage;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::error::Error;

const REPORT_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkerLifecycleAction {
    Install,
    Start,
    Stop,
    Restart,
    Readiness,
    Recover,
    Status,
}

impl WorkerLifecycleAction {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "install" => Some(Self::Install),
            "start" => Some(Self::Start),
            "stop" => Some(Self::Stop),
            "restart" => Some(Self::Restart),
            "readiness" => Some(Self::Readiness),
            "recover" => Some(Self::Recover),
            "status" => Some(Self::Status),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
            Self::Readiness => "readiness",
            Self::Recover => "recover",
            Self::Status => "status",
        }
    }

    fn is_manual_operation(self) -> bool {
        matches!(
            self,
            Self::Install | Self::Start | Self::Stop | Self::Restart | Self::Recover | Self::Status
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerLifecycleCommand {
    pub(crate) action: WorkerLifecycleAction,
    pub(crate) json: bool,
    pub(crate) port: u16,
}

impl WorkerLifecycleCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        let Some(action_raw) = args.first() else {
            return Err(worker_lifecycle_usage());
        };
        let Some(action) = WorkerLifecycleAction::from_str(action_raw.as_str()) else {
            return Err(worker_lifecycle_usage());
        };

        Ok(Self {
            action,
            json: args.iter().any(|arg| arg == "--json"),
            port: parse_lifecycle_port(args)?,
        })
    }
}

pub(crate) fn worker_lifecycle_usage() -> String {
    "Uso: iamine-node worker lifecycle [install|start|stop|restart|readiness|recover|status] [--port=N] [--json]"
        .to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleStatus {
    Pass,
    Warn,
    Blocked,
    Manual,
}

impl LifecycleStatus {
    fn human(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Warn => "WARN",
            Self::Blocked => "BLOCKED",
            Self::Manual => "MANUAL",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct LifecycleRuntimeEffects {
    worker_started: bool,
    worker_stopped: bool,
    p2p_started: bool,
    pubsub_started: bool,
    model_download_started: bool,
    model_load_started: bool,
    inference_started: bool,
}

impl LifecycleRuntimeEffects {
    fn none() -> Self {
        Self {
            worker_started: false,
            worker_stopped: false,
            p2p_started: false,
            pubsub_started: false,
            model_download_started: false,
            model_load_started: false,
            inference_started: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct LifecycleCheck {
    id: &'static str,
    status: LifecycleStatus,
    message: String,
    details: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
struct LifecycleStep {
    id: &'static str,
    message: String,
    command: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkerLifecycleReport {
    schema_version: &'static str,
    command: &'static str,
    action: WorkerLifecycleAction,
    overall_status: LifecycleStatus,
    worker_port: u16,
    runtime_effects: LifecycleRuntimeEffects,
    checks: Vec<LifecycleCheck>,
    steps: Vec<LifecycleStep>,
}

pub(crate) fn run_worker_lifecycle_cli(
    command: &WorkerLifecycleCommand,
) -> Result<(), Box<dyn Error>> {
    let report = build_worker_lifecycle_report(command);

    if command.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_worker_lifecycle_report(&report);
    }

    Ok(())
}

fn build_worker_lifecycle_report(command: &WorkerLifecycleCommand) -> WorkerLifecycleReport {
    let hardware_profile = inspect_hardware(HardwareProfilerConfig::default());
    let storage = ModelStorage::new();
    let capabilities = capabilities_from_hardware_profile(
        LOCAL_DIAGNOSTIC_NODE_ID,
        hardware_profile.as_ref().ok(),
        &storage,
    );
    let startup_policy = WorkerStartupPolicy::from_env(&capabilities);

    let checks = vec![
        hardware_check(&hardware_profile),
        backend_check(&startup_policy),
        worker_start_command_check(command.port),
        metrics_check(command.port),
        process_observation_check(),
        service_manager_check(command.action),
        runtime_effects_check(),
    ];

    let steps = lifecycle_steps(command.action, command.port);
    let overall_status = overall_status(command.action, &checks);

    WorkerLifecycleReport {
        schema_version: REPORT_SCHEMA_VERSION,
        command: "iamine-node worker lifecycle",
        action: command.action,
        overall_status,
        worker_port: command.port,
        runtime_effects: LifecycleRuntimeEffects::none(),
        checks,
        steps,
    }
}

fn hardware_check(
    hardware_profile: &Result<iamine_hardware::NodeHardwareProfile, String>,
) -> LifecycleCheck {
    match hardware_profile {
        Ok(profile) => {
            let mut details = details_map();
            details.insert("schema_version".to_string(), json!(profile.schema_version));
            details.insert(
                "collection_mode".to_string(),
                json!(profile.collection_mode),
            );
            details.insert(
                "effective_worker_slots".to_string(),
                json!(profile.static_profile.effective.effective_worker_slots),
            );
            details.insert(
                "effective_cpu_threads".to_string(),
                json!(profile.static_profile.effective.effective_cpu_threads),
            );
            details.insert(
                "effective_accelerator".to_string(),
                json!(profile.static_profile.effective.effective_accelerator),
            );

            let schema_valid = profile.validate_schema().is_ok();
            LifecycleCheck {
                id: "hardware_profile",
                status: if schema_valid {
                    LifecycleStatus::Pass
                } else {
                    LifecycleStatus::Blocked
                },
                message: if schema_valid {
                    "static hardware profile is visible".to_string()
                } else {
                    "hardware profile schema is not supported".to_string()
                },
                details,
            }
        }
        Err(error) => LifecycleCheck {
            id: "hardware_profile",
            status: LifecycleStatus::Blocked,
            message: format!("hardware inspection failed: {error}"),
            details: details_map(),
        },
    }
}

fn backend_check(policy: &WorkerStartupPolicy) -> LifecycleCheck {
    let decision = policy.backend_availability_decision();
    let mut details = details_map();
    details.insert("backend".to_string(), json!(policy.backend.as_str()));
    details.insert(
        "skip_model_load_on_startup".to_string(),
        json!(policy.skip_model_load_on_startup),
    );
    details.insert(
        "legacy_cpu_real_backend_mode".to_string(),
        json!(policy.legacy_cpu_real_backend_mode.as_str()),
    );
    details.insert(
        "real_inference_available".to_string(),
        json!(policy.real_inference_available),
    );
    details.insert(
        "backend_availability_status".to_string(),
        json!(backend_status_code(decision.status)),
    );
    details.insert(
        "backend_availability_reason".to_string(),
        json!(backend_reason_code(decision.reason)),
    );

    LifecycleCheck {
        id: "backend_availability",
        status: if decision.permits_real_inference() {
            LifecycleStatus::Pass
        } else {
            LifecycleStatus::Warn
        },
        message: if decision.permits_real_inference() {
            "backend policy permits real inference".to_string()
        } else {
            format!(
                "worker can start degraded but real inference is unavailable: {}",
                backend_reason_code(decision.reason)
            )
        },
        details,
    }
}

fn worker_start_command_check(port: u16) -> LifecycleCheck {
    let mut details = details_map();
    details.insert("port".to_string(), json!(port));
    details.insert("command".to_string(), json!(worker_start_command(port)));

    LifecycleCheck {
        id: "explicit_start_command",
        status: LifecycleStatus::Pass,
        message: "worker start is explicit and maps to the existing worker runtime command"
            .to_string(),
        details,
    }
}

fn metrics_check(port: u16) -> LifecycleCheck {
    let mut details = details_map();
    details.insert("worker_port".to_string(), json!(port));
    details.insert("bind_probe".to_string(), json!("not_run"));

    match metrics_startup_decision(port) {
        MetricsStartupDecision::StartMetrics { port } => {
            details.insert("metrics_port".to_string(), json!(port));
            details.insert(
                "fallback_behavior".to_string(),
                json!("start_metrics_server"),
            );
            LifecycleCheck {
                id: "metrics_policy",
                status: LifecycleStatus::Pass,
                message: "metrics policy derives an endpoint without binding it".to_string(),
                details,
            }
        }
        MetricsStartupDecision::ContinueWithoutMetrics { reason, error } => {
            details.insert("reason".to_string(), json!(metrics_reason_code(reason)));
            details.insert("error_reason".to_string(), json!(error.reason));
            details.insert(
                "fallback_behavior".to_string(),
                json!("continue_without_metrics_server"),
            );
            LifecycleCheck {
                id: "metrics_policy",
                status: LifecycleStatus::Warn,
                message: "metrics policy would continue without a metrics server".to_string(),
                details,
            }
        }
        MetricsStartupDecision::Disabled { reason } => {
            details.insert("reason".to_string(), json!(metrics_reason_code(reason)));
            details.insert(
                "fallback_behavior".to_string(),
                json!("continue_without_metrics_server"),
            );
            LifecycleCheck {
                id: "metrics_policy",
                status: LifecycleStatus::Warn,
                message: "metrics policy is disabled".to_string(),
                details,
            }
        }
    }
}

fn process_observation_check() -> LifecycleCheck {
    let mut details = details_map();
    details.insert("process_scan".to_string(), json!("not_run"));
    details.insert("reason".to_string(), json!("privacy_preserving_cli"));

    LifecycleCheck {
        id: "worker_process_observation",
        status: LifecycleStatus::Manual,
        message: "process discovery is not performed to avoid collecting user process lists"
            .to_string(),
        details,
    }
}

fn service_manager_check(action: WorkerLifecycleAction) -> LifecycleCheck {
    let mut details = details_map();
    details.insert("service_manager".to_string(), json!("not_configured"));
    details.insert(
        "packaging_dependency".to_string(),
        json!("LAN-INFERENCE-BETA-PACKAGING-001"),
    );
    details.insert("action".to_string(), json!(action.as_str()));

    LifecycleCheck {
        id: "service_manager",
        status: LifecycleStatus::Manual,
        message: "service manager integration is deferred to packaging; use explicit commands"
            .to_string(),
        details,
    }
}

fn runtime_effects_check() -> LifecycleCheck {
    let mut details = details_map();
    details.insert("worker_started".to_string(), json!(false));
    details.insert("worker_stopped".to_string(), json!(false));
    details.insert("p2p_started".to_string(), json!(false));
    details.insert("pubsub_started".to_string(), json!(false));
    details.insert("model_download_started".to_string(), json!(false));
    details.insert("model_load_started".to_string(), json!(false));
    details.insert("inference_started".to_string(), json!(false));

    LifecycleCheck {
        id: "runtime_effects",
        status: LifecycleStatus::Pass,
        message: "lifecycle CLI reports actions without starting or stopping runtime services"
            .to_string(),
        details,
    }
}

fn lifecycle_steps(action: WorkerLifecycleAction, port: u16) -> Vec<LifecycleStep> {
    match action {
        WorkerLifecycleAction::Install => vec![
            LifecycleStep {
                id: "verify_binary",
                message: "build or install the iamine-node binary for this host".to_string(),
                command: Some(vec!["cargo".to_string(), "build".to_string(), "-p".to_string(), "iamine-node".to_string()]),
            },
            LifecycleStep {
                id: "run_readiness",
                message: "validate local readiness before starting the worker".to_string(),
                command: Some(worker_lifecycle_command("readiness", port)),
            },
        ],
        WorkerLifecycleAction::Start => vec![LifecycleStep {
            id: "start_worker",
            message: "start the worker with an explicit runtime command".to_string(),
            command: Some(worker_start_command(port)),
        }],
        WorkerLifecycleAction::Stop => vec![LifecycleStep {
            id: "stop_worker",
            message: "stop the foreground worker process or configured service; no process is killed by this CLI"
                .to_string(),
            command: None,
        }],
        WorkerLifecycleAction::Restart => vec![
            LifecycleStep {
                id: "stop_worker",
                message: "stop the existing worker process through the operator-controlled process manager"
                    .to_string(),
                command: None,
            },
            LifecycleStep {
                id: "start_worker",
                message: "start a fresh worker after stop is confirmed".to_string(),
                command: Some(worker_start_command(port)),
            },
        ],
        WorkerLifecycleAction::Readiness => vec![LifecycleStep {
            id: "readiness",
            message: "inspect checks in this report before starting or restarting the worker"
                .to_string(),
            command: Some(worker_lifecycle_command("readiness", port)),
        }],
        WorkerLifecycleAction::Recover => vec![
            LifecycleStep {
                id: "diagnose",
                message: "run the LAN doctor before recovery actions".to_string(),
                command: Some(vec![
                    "iamine-node".to_string(),
                    "lan".to_string(),
                    "doctor".to_string(),
                ]),
            },
            LifecycleStep {
                id: "restart",
                message: "restart only through explicit operator action".to_string(),
                command: Some(worker_lifecycle_command("restart", port)),
            },
            LifecycleStep {
                id: "degraded_start",
                message: "for safe degraded startup, use mock backend or skip model load explicitly"
                    .to_string(),
                command: Some(vec![
                    "IAMINE_INFERENCE_BACKEND=mock".to_string(),
                    "IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1".to_string(),
                    "iamine-node".to_string(),
                    "--worker".to_string(),
                    format!("--port={port}"),
                ]),
            },
        ],
        WorkerLifecycleAction::Status => vec![LifecycleStep {
            id: "status",
            message: "review readiness checks; runtime process observation is intentionally manual"
                .to_string(),
            command: Some(worker_lifecycle_command("status", port)),
        }],
    }
}

fn overall_status(action: WorkerLifecycleAction, checks: &[LifecycleCheck]) -> LifecycleStatus {
    if checks
        .iter()
        .any(|check| check.status == LifecycleStatus::Blocked)
    {
        LifecycleStatus::Blocked
    } else if checks
        .iter()
        .any(|check| check.status == LifecycleStatus::Warn)
    {
        LifecycleStatus::Warn
    } else if action.is_manual_operation()
        || checks
            .iter()
            .any(|check| check.status == LifecycleStatus::Manual)
    {
        LifecycleStatus::Manual
    } else {
        LifecycleStatus::Pass
    }
}

fn parse_lifecycle_port(args: &[String]) -> Result<u16, String> {
    for (index, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix("--port=") {
            return parse_port_value(raw);
        }
        if arg == "--port" {
            let Some(raw) = args.get(index + 1) else {
                return Err("Falta valor para --port".to_string());
            };
            return parse_port_value(raw);
        }
    }
    Ok(9000)
}

fn parse_port_value(raw: &str) -> Result<u16, String> {
    raw.parse::<u16>()
        .map_err(|_| format!("Valor invalido para --port: {raw}"))
}

fn worker_start_command(port: u16) -> Vec<String> {
    vec![
        "iamine-node".to_string(),
        "--worker".to_string(),
        format!("--port={port}"),
    ]
}

fn worker_lifecycle_command(action: &str, port: u16) -> Vec<String> {
    vec![
        "iamine-node".to_string(),
        "worker".to_string(),
        "lifecycle".to_string(),
        action.to_string(),
        format!("--port={port}"),
    ]
}

fn backend_status_code(status: ModelBackendAvailabilityStatus) -> &'static str {
    match status {
        ModelBackendAvailabilityStatus::Available => "available",
        ModelBackendAvailabilityStatus::Unavailable => "unavailable",
    }
}

fn backend_reason_code(reason: ModelBackendAvailabilityReason) -> &'static str {
    match reason {
        ModelBackendAvailabilityReason::Available => "available",
        ModelBackendAvailabilityReason::MockBackend => "mock_backend",
        ModelBackendAvailabilityReason::ModelLoadSkipped => "model_load_skipped",
        ModelBackendAvailabilityReason::CpuFeatureIncompatible => "cpu_feature_incompatible",
        ModelBackendAvailabilityReason::LegacyCpuDaemonOnly => "legacy_cpu_daemon_only",
        ModelBackendAvailabilityReason::RealInferenceUnavailable => "real_inference_unavailable",
    }
}

fn metrics_reason_code(reason: crate::metrics_policy::MetricsUnavailableReason) -> &'static str {
    match reason {
        crate::metrics_policy::MetricsUnavailableReason::InvalidPortMath => "invalid_port_math",
        crate::metrics_policy::MetricsUnavailableReason::PortBelowBase => {
            "worker_port_below_metrics_base"
        }
        crate::metrics_policy::MetricsUnavailableReason::PortInUse => "port_in_use",
        crate::metrics_policy::MetricsUnavailableReason::DisabledByConfig => "disabled_by_config",
        crate::metrics_policy::MetricsUnavailableReason::Unknown => "unknown",
    }
}

fn print_worker_lifecycle_report(report: &WorkerLifecycleReport) {
    println!("IaMine worker lifecycle");
    println!("action={}", report.action.as_str());
    println!("overall_status={}", report.overall_status.human());
    println!("worker_port={}", report.worker_port);
    println!();

    for check in &report.checks {
        println!(
            "[{}] {} - {}",
            check.status.human(),
            check.id,
            check.message
        );
        for (key, value) in &check.details {
            println!("  {key}={}", format_value(value));
        }
    }

    println!();
    println!("steps:");
    for step in &report.steps {
        println!("- {}: {}", step.id, step.message);
        if let Some(command) = &step.command {
            println!("  command={}", command.join(" "));
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

fn details_map() -> BTreeMap<String, Value> {
    BTreeMap::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn check(id: &'static str, status: LifecycleStatus) -> LifecycleCheck {
        LifecycleCheck {
            id,
            status,
            message: "test".to_string(),
            details: details_map(),
        }
    }

    #[test]
    fn worker_lifecycle_command_parses_start_json_port() -> Result<(), String> {
        let command =
            WorkerLifecycleCommand::from_args(&args(&["start", "--port=4101", "--json"]))?;

        assert_eq!(command.action, WorkerLifecycleAction::Start);
        assert_eq!(command.port, 4101);
        assert!(command.json);
        Ok(())
    }

    #[test]
    fn worker_lifecycle_rejects_unknown_action() {
        let result = WorkerLifecycleCommand::from_args(&args(&["launch"]));

        assert!(result.is_err());
    }

    #[test]
    fn worker_lifecycle_start_plan_does_not_start_runtime() {
        let command = WorkerLifecycleCommand {
            action: WorkerLifecycleAction::Start,
            json: true,
            port: 4101,
        };
        let report = build_worker_lifecycle_report(&command);

        assert!(!report.runtime_effects.worker_started);
        assert!(!report.runtime_effects.p2p_started);
        assert!(!report.runtime_effects.model_load_started);
        assert!(report.steps.iter().any(|step| match &step.command {
            Some(command) => command == &worker_start_command(4101),
            None => false,
        }));
    }

    #[test]
    fn worker_lifecycle_overall_blocks_on_blocked_check() {
        let checks = vec![
            check("one", LifecycleStatus::Manual),
            check("two", LifecycleStatus::Blocked),
        ];

        assert_eq!(
            overall_status(WorkerLifecycleAction::Start, &checks),
            LifecycleStatus::Blocked
        );
    }

    #[test]
    fn worker_lifecycle_overall_reports_manual_for_manual_actions() {
        let checks = vec![
            check("one", LifecycleStatus::Pass),
            check("two", LifecycleStatus::Manual),
        ];

        assert_eq!(
            overall_status(WorkerLifecycleAction::Start, &checks),
            LifecycleStatus::Manual
        );
    }
}
