use crate::metrics_policy::{
    allocate_metrics_port, metrics_startup_decision, MetricsStartupDecision,
};
use crate::model_backend_availability::{
    ModelBackendAvailabilityReason, ModelBackendAvailabilityStatus,
};
use crate::node_capability_snapshot::{
    capabilities_from_hardware_profile, LOCAL_DIAGNOSTIC_NODE_ID,
};
use crate::node_config_schema::{
    default_node_config_path, inspect_node_config, NodeConfigState, NODE_CONFIG_FEATURE,
    NODE_CONFIG_SCHEMA_VERSION,
};
use crate::worker_startup_policy::WorkerStartupPolicy;
use iamine_hardware::{inspect_hardware, HardwareProfilerConfig, NodeHardwareProfile};
use iamine_models::{
    build_model_catalog_entries, LicenseAcceptanceStore, ModelCatalogDownloadAction,
    ModelCatalogEntry, ModelNodeCapabilities, ModelRegistry, ModelStorage,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::error::Error;

const REPORT_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorStatus {
    Pass,
    Warn,
    Fail,
    NotRun,
}

impl DoctorStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Warn => "WARN",
            Self::Fail => "FAIL",
            Self::NotRun => "NOT_RUN",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorCheck {
    pub(crate) id: &'static str,
    pub(crate) status: DoctorStatus,
    pub(crate) message: String,
    pub(crate) details: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeSideEffectPolicy {
    workers_started: bool,
    p2p_started: bool,
    pubsub_started: bool,
    model_download_started: bool,
    model_load_started: bool,
    inference_started: bool,
    dynamic_hardware_probe_started: bool,
}

impl RuntimeSideEffectPolicy {
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
pub(crate) struct LanNodeDoctorReport {
    schema_version: &'static str,
    command: &'static str,
    overall_status: DoctorStatus,
    runtime_side_effects: RuntimeSideEffectPolicy,
    checks: Vec<DoctorCheck>,
}

impl LanNodeDoctorReport {
    pub(crate) fn overall_status(&self) -> DoctorStatus {
        self.overall_status
    }

    pub(crate) fn checks(&self) -> &[DoctorCheck] {
        &self.checks
    }
}

pub(crate) fn run_lan_node_doctor(
    json_output: bool,
    network_checks_requested: bool,
) -> Result<(), Box<dyn Error>> {
    let report = build_lan_node_doctor_report(network_checks_requested);

    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human_report(&report);
    }

    Ok(())
}

pub(crate) fn build_lan_node_doctor_report(network_checks_requested: bool) -> LanNodeDoctorReport {
    let mut checks = Vec::new();
    let hardware_profile = inspect_hardware(HardwareProfilerConfig::default());
    checks.push(hardware_visibility_check(&hardware_profile));

    let catalog = collect_model_catalog(hardware_profile.as_ref().ok());
    checks.push(model_catalog_check(&catalog));

    let policy = WorkerStartupPolicy::from_env(&catalog.capabilities);
    checks.push(backend_availability_check(&policy));
    checks.push(worker_startup_policy_check(&policy));
    checks.push(metrics_availability_check());
    checks.push(config_schema_check());
    checks.push(network_readiness_check(network_checks_requested));
    checks.push(runtime_side_effect_check());

    let overall_status = overall_status(&checks);

    LanNodeDoctorReport {
        schema_version: REPORT_SCHEMA_VERSION,
        command: "iamine-node lan doctor",
        overall_status,
        runtime_side_effects: RuntimeSideEffectPolicy::diagnostic_only(),
        checks,
    }
}

fn hardware_visibility_check(
    hardware_profile: &Result<NodeHardwareProfile, String>,
) -> DoctorCheck {
    match hardware_profile {
        Ok(profile) => {
            let schema_valid = profile.validate_schema().is_ok();
            let effective = &profile.static_profile.effective;
            let mut details = details_map();
            details.insert("schema_version".to_string(), json!(profile.schema_version));
            details.insert("schema_valid".to_string(), json!(schema_valid));
            details.insert(
                "collection_mode".to_string(),
                json!(profile.collection_mode),
            );
            details.insert(
                "effective_cpu_threads".to_string(),
                json!(effective.effective_cpu_threads),
            );
            details.insert(
                "effective_worker_slots".to_string(),
                json!(effective.effective_worker_slots),
            );
            details.insert(
                "effective_ram_gb".to_string(),
                json!(bytes_to_gb(effective.effective_ram_bytes)),
            );
            details.insert(
                "effective_accelerator".to_string(),
                json!(effective.effective_accelerator),
            );
            details.insert(
                "accelerator_count".to_string(),
                json!(profile.static_profile.accelerators.len()),
            );
            details.insert("warning_count".to_string(), json!(profile.warnings.len()));

            DoctorCheck {
                id: "hardware_profile_visibility",
                status: if schema_valid {
                    DoctorStatus::Pass
                } else {
                    DoctorStatus::Fail
                },
                message: if schema_valid {
                    "static hardware profile is visible and schema is supported".to_string()
                } else {
                    "static hardware profile is visible but schema is unsupported".to_string()
                },
                details,
            }
        }
        Err(error) => DoctorCheck {
            id: "hardware_profile_visibility",
            status: DoctorStatus::Fail,
            message: format!("hardware inspection failed: {error}"),
            details: details_map(),
        },
    }
}

#[derive(Debug)]
struct CatalogDiagnostic {
    entries: Vec<ModelCatalogEntry>,
    capabilities: ModelNodeCapabilities,
}

fn collect_model_catalog(hardware_profile: Option<&NodeHardwareProfile>) -> CatalogDiagnostic {
    let registry = ModelRegistry::new();
    let storage = ModelStorage::new();
    let acceptance = LicenseAcceptanceStore::new();
    let capabilities =
        capabilities_from_hardware_profile(LOCAL_DIAGNOSTIC_NODE_ID, hardware_profile, &storage);
    let entries = build_model_catalog_entries(&registry, &storage, &acceptance, &capabilities);

    CatalogDiagnostic {
        entries,
        capabilities,
    }
}

fn model_catalog_check(catalog: &CatalogDiagnostic) -> DoctorCheck {
    let approved_models = catalog.entries.len();
    let ready_models = count_download_action(&catalog.entries, ModelCatalogDownloadAction::Ready);
    let installed_models = count_download_action(
        &catalog.entries,
        ModelCatalogDownloadAction::AlreadyInstalled,
    );
    let license_acceptance_required = count_download_action(
        &catalog.entries,
        ModelCatalogDownloadAction::LicenseAcceptanceRequired,
    );
    let incompatible_models =
        count_download_action(&catalog.entries, ModelCatalogDownloadAction::Incompatible);
    let blocked_models =
        count_download_action(&catalog.entries, ModelCatalogDownloadAction::Blocked);
    let ready_or_installed = ready_models + installed_models;

    let mut details = details_map();
    details.insert("approved_models".to_string(), json!(approved_models));
    details.insert("ready_models".to_string(), json!(ready_models));
    details.insert("installed_models".to_string(), json!(installed_models));
    details.insert(
        "license_acceptance_required".to_string(),
        json!(license_acceptance_required),
    );
    details.insert(
        "incompatible_models".to_string(),
        json!(incompatible_models),
    );
    details.insert("blocked_models".to_string(), json!(blocked_models));
    details.insert(
        "cpu_cores".to_string(),
        json!(catalog.capabilities.cpu_cores),
    );
    details.insert("ram_gb".to_string(), json!(catalog.capabilities.ram_gb));
    details.insert(
        "storage_available_gb".to_string(),
        json!(catalog.capabilities.storage_available_gb),
    );
    details.insert(
        "accelerator".to_string(),
        json!(catalog.capabilities.accelerator),
    );

    let (status, message) = if approved_models == 0 {
        (
            DoctorStatus::Fail,
            "approved model catalog is empty".to_string(),
        )
    } else if ready_or_installed == 0 {
        (
            DoctorStatus::Warn,
            "approved catalog is present but no model is ready or installed locally".to_string(),
        )
    } else {
        (
            DoctorStatus::Pass,
            "approved catalog gates were evaluated and at least one model is ready or installed"
                .to_string(),
        )
    };

    DoctorCheck {
        id: "model_catalog_gates",
        status,
        message,
        details,
    }
}

fn backend_availability_check(policy: &WorkerStartupPolicy) -> DoctorCheck {
    let decision = policy.backend_availability_decision();
    let mut details = worker_policy_details(policy);
    details.insert(
        "backend_availability_status".to_string(),
        json!(backend_availability_status_code(decision.status)),
    );
    details.insert(
        "backend_availability_reason".to_string(),
        json!(backend_availability_reason_code(decision.reason)),
    );
    details.insert(
        "permits_real_inference".to_string(),
        json!(decision.permits_real_inference()),
    );
    details.insert(
        "permits_local_backend_load".to_string(),
        json!(decision.permits_local_backend_load()),
    );

    if decision.permits_real_inference() {
        DoctorCheck {
            id: "backend_availability",
            status: DoctorStatus::Pass,
            message: "backend policy permits real inference".to_string(),
            details,
        }
    } else {
        DoctorCheck {
            id: "backend_availability",
            status: DoctorStatus::Warn,
            message: format!(
                "backend policy does not currently permit real inference: {}",
                backend_availability_reason_code(decision.reason)
            ),
            details,
        }
    }
}

fn worker_startup_policy_check(policy: &WorkerStartupPolicy) -> DoctorCheck {
    let mut details = worker_policy_details(policy);
    details.insert(
        "legacy_cpu_daemon_only_real_inference".to_string(),
        json!(policy.legacy_cpu_daemon_only_real_inference()),
    );

    let status = if policy.model_load_skip_reason.is_some() || policy.mock_backend() {
        DoctorStatus::Warn
    } else {
        DoctorStatus::Pass
    };

    DoctorCheck {
        id: "worker_startup_policy",
        status,
        message: "worker startup policy evaluated without starting a worker".to_string(),
        details,
    }
}

fn metrics_availability_check() -> DoctorCheck {
    let mut details = details_map();
    let worker_port_basis = 9000;
    details.insert("worker_port_basis".to_string(), json!(worker_port_basis));
    details.insert("bind_probe".to_string(), json!("not_run"));

    match metrics_startup_decision(worker_port_basis) {
        MetricsStartupDecision::StartMetrics { port: metrics_port } => {
            details.insert("metrics_port".to_string(), json!(metrics_port));
            if let Ok(allocation) = allocate_metrics_port(worker_port_basis) {
                details.insert(
                    "allocation_strategy".to_string(),
                    json!(allocation.strategy.as_str()),
                );
                details.insert(
                    "allocation_offset".to_string(),
                    json!(allocation
                        .metrics_port
                        .saturating_sub(allocation.worker_port)),
                );
            }
            details.insert(
                "fallback_behavior".to_string(),
                json!("start_metrics_server"),
            );
            DoctorCheck {
                id: "metrics_availability",
                status: DoctorStatus::Pass,
                message: "metrics port policy derives a default endpoint without binding it"
                    .to_string(),
                details,
            }
        }
        MetricsStartupDecision::ContinueWithoutMetrics { reason, error } => {
            details.insert("reason".to_string(), json!(format!("{reason:?}")));
            details.insert("error_reason".to_string(), json!(error.reason));
            details.insert(
                "fallback_behavior".to_string(),
                json!("continue_without_metrics_server"),
            );
            DoctorCheck {
                id: "metrics_availability",
                status: DoctorStatus::Warn,
                message: "metrics policy would continue without a metrics server".to_string(),
                details,
            }
        }
        MetricsStartupDecision::Disabled { reason } => {
            details.insert("reason".to_string(), json!(format!("{reason:?}")));
            details.insert(
                "fallback_behavior".to_string(),
                json!("continue_without_metrics_server"),
            );
            DoctorCheck {
                id: "metrics_availability",
                status: DoctorStatus::Warn,
                message: "metrics policy is disabled".to_string(),
                details,
            }
        }
    }
}

fn config_schema_check() -> DoctorCheck {
    let mut details = details_map();
    let inspection = inspect_node_config(&default_node_config_path());
    details.insert("feature".to_string(), json!(NODE_CONFIG_FEATURE));
    details.insert(
        "expected_schema_version".to_string(),
        json!(NODE_CONFIG_SCHEMA_VERSION),
    );
    details.insert("config_state".to_string(), json!(inspection.state.as_str()));
    details.insert(
        "detected_schema_version".to_string(),
        json!(inspection.schema_version),
    );
    details.insert("path_redacted".to_string(), json!(true));
    details.insert("migration_available".to_string(), json!(true));
    details.insert("rollback_available".to_string(), json!(true));

    match inspection.state {
        NodeConfigState::Missing => DoctorCheck {
            id: "node_config_schema",
            status: DoctorStatus::Pass,
            message: "versioned node config schema is available; no config file exists yet"
                .to_string(),
            details,
        },
        NodeConfigState::Current => DoctorCheck {
            id: "node_config_schema",
            status: DoctorStatus::Pass,
            message: "node config uses the current schema".to_string(),
            details,
        },
        NodeConfigState::Legacy => DoctorCheck {
            id: "node_config_schema",
            status: DoctorStatus::Warn,
            message: "legacy node config can be migrated with iamine-node node config migrate"
                .to_string(),
            details,
        },
        NodeConfigState::Unsupported => DoctorCheck {
            id: "node_config_schema",
            status: DoctorStatus::Fail,
            message: "node config schema_version is not supported".to_string(),
            details,
        },
        NodeConfigState::InvalidJson => DoctorCheck {
            id: "node_config_schema",
            status: DoctorStatus::Fail,
            message: "node config is not valid JSON".to_string(),
            details,
        },
    }
}

fn network_readiness_check(network_checks_requested: bool) -> DoctorCheck {
    let mut details = details_map();
    details.insert(
        "network_checks_requested".to_string(),
        json!(network_checks_requested),
    );
    details.insert("p2p_started".to_string(), json!(false));
    details.insert("pubsub_started".to_string(), json!(false));

    DoctorCheck {
        id: "lan_peer_pubsub_readiness",
        status: DoctorStatus::NotRun,
        message: if network_checks_requested {
            "network readiness probe is not implemented without starting P2P or PubSub".to_string()
        } else {
            "network checks were not requested".to_string()
        },
        details,
    }
}

fn runtime_side_effect_check() -> DoctorCheck {
    let mut details = details_map();
    details.insert("workers_started".to_string(), json!(false));
    details.insert("p2p_started".to_string(), json!(false));
    details.insert("pubsub_started".to_string(), json!(false));
    details.insert("model_download_started".to_string(), json!(false));
    details.insert("model_load_started".to_string(), json!(false));
    details.insert("inference_started".to_string(), json!(false));
    details.insert("dynamic_hardware_probe_started".to_string(), json!(false));

    DoctorCheck {
        id: "runtime_side_effects",
        status: DoctorStatus::Pass,
        message: "diagnostic path does not start runtime services".to_string(),
        details,
    }
}

fn worker_policy_details(policy: &WorkerStartupPolicy) -> BTreeMap<String, Value> {
    let mut details = details_map();
    details.insert("backend".to_string(), json!(policy.backend.as_str()));
    details.insert(
        "skip_model_load_on_startup".to_string(),
        json!(policy.skip_model_load_on_startup),
    );
    details.insert(
        "cpu_feature_compatible".to_string(),
        json!(policy.cpu_feature_compatible),
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
        "model_load_skip_reason".to_string(),
        json!(policy.model_load_skip_reason),
    );
    details
}

fn count_download_action(
    entries: &[ModelCatalogEntry],
    action: ModelCatalogDownloadAction,
) -> usize {
    entries
        .iter()
        .filter(|entry| entry.download_action == action)
        .count()
}

fn overall_status(checks: &[DoctorCheck]) -> DoctorStatus {
    if checks
        .iter()
        .any(|check| check.status == DoctorStatus::Fail)
    {
        DoctorStatus::Fail
    } else if checks
        .iter()
        .any(|check| check.status == DoctorStatus::Warn)
    {
        DoctorStatus::Warn
    } else {
        DoctorStatus::Pass
    }
}

fn backend_availability_status_code(status: ModelBackendAvailabilityStatus) -> &'static str {
    match status {
        ModelBackendAvailabilityStatus::Available => "available",
        ModelBackendAvailabilityStatus::Unavailable => "unavailable",
    }
}

fn backend_availability_reason_code(reason: ModelBackendAvailabilityReason) -> &'static str {
    match reason {
        ModelBackendAvailabilityReason::Available => "available",
        ModelBackendAvailabilityReason::MockBackend => "mock_backend",
        ModelBackendAvailabilityReason::ModelLoadSkipped => "model_load_skipped",
        ModelBackendAvailabilityReason::CpuFeatureIncompatible => "cpu_feature_incompatible",
        ModelBackendAvailabilityReason::LegacyCpuDaemonOnly => "legacy_cpu_daemon_only",
        ModelBackendAvailabilityReason::RealInferenceUnavailable => "real_inference_unavailable",
    }
}

fn print_human_report(report: &LanNodeDoctorReport) {
    println!("IaMine LAN node doctor");
    println!("overall_status={}", report.overall_status.as_str());
    println!("schema_version={}", report.schema_version);
    println!();

    for check in &report.checks {
        println!(
            "[{}] {} - {}",
            check.status.as_str(),
            check.id,
            check.message
        );
        for (key, value) in &check.details {
            println!("  {key}={}", format_value(value));
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

fn bytes_to_gb(bytes: u64) -> u64 {
    bytes / 1_073_741_824
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(id: &'static str, status: DoctorStatus) -> DoctorCheck {
        DoctorCheck {
            id,
            status,
            message: "test".to_string(),
            details: details_map(),
        }
    }

    #[test]
    fn doctor_overall_status_prioritizes_failures() {
        let checks = vec![
            check("one", DoctorStatus::Pass),
            check("two", DoctorStatus::Warn),
            check("three", DoctorStatus::Fail),
        ];

        assert_eq!(overall_status(&checks), DoctorStatus::Fail);
    }

    #[test]
    fn doctor_overall_status_reports_warnings_without_failures() {
        let checks = vec![
            check("one", DoctorStatus::Pass),
            check("two", DoctorStatus::Warn),
            check("three", DoctorStatus::NotRun),
        ];

        assert_eq!(overall_status(&checks), DoctorStatus::Warn);
    }

    #[test]
    fn network_readiness_probe_does_not_start_network_runtime() {
        let check = network_readiness_check(true);

        assert_eq!(check.status, DoctorStatus::NotRun);
        assert_eq!(check.details.get("p2p_started"), Some(&json!(false)));
        assert_eq!(check.details.get("pubsub_started"), Some(&json!(false)));
    }

    #[test]
    fn metrics_availability_reports_allocated_default_endpoint() {
        let check = metrics_availability_check();

        assert_eq!(check.status, DoctorStatus::Pass);
        assert_eq!(check.details.get("worker_port_basis"), Some(&json!(9000)));
        assert_eq!(check.details.get("metrics_port"), Some(&json!(9090)));
        assert_eq!(
            check.details.get("allocation_strategy"),
            Some(&json!("legacy_worker_base"))
        );
        assert_eq!(check.details.get("allocation_offset"), Some(&json!(90)));
        assert_eq!(check.details.get("bind_probe"), Some(&json!("not_run")));
    }

    #[test]
    fn runtime_side_effect_policy_is_diagnostic_only() {
        let check = runtime_side_effect_check();

        assert_eq!(check.status, DoctorStatus::Pass);
        assert_eq!(check.details.get("workers_started"), Some(&json!(false)));
        assert_eq!(check.details.get("model_load_started"), Some(&json!(false)));
        assert_eq!(check.details.get("inference_started"), Some(&json!(false)));
    }
}
