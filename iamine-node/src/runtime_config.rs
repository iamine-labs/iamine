use crate::benchmark::NodeBenchmark;
use crate::cli::{parse_args_from, parse_worker_port};
use crate::cluster_stress::IAMINE_STRESS_CHILD;
use crate::env_config::env_bool_or_default;
use crate::log_observability_event;
use crate::mode_dispatch::is_control_plane_mode;
use crate::network_config;
use crate::node_identity::NodeIdentity;
use crate::node_modes::{DebugFlags, NodeMode};
use crate::resource_policy::ResourcePolicy;
use crate::wallet::Wallet;
use crate::worker_startup_policy::emit_worker_startup_started_event;
use iamine_network::{set_global_node_id, LogLevel};
use libp2p::{identity, PeerId};
use serde_json::Map;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const INFER_TIMEOUT_MS: u64 = 5_000;
pub(crate) const INFER_FALLBACK_AFTER_MS: u64 = 2_000;
pub(crate) const MAX_DISTRIBUTED_RETRIES: u8 = 2;
pub(crate) const MIN_ADAPTIVE_TIMEOUT_MS: u64 = 7_500;
pub(crate) const MAX_ADAPTIVE_TIMEOUT_MS: u64 = 45_000;
pub(crate) const WATCHDOG_EXTENSION_STEP_MS: u64 = 4_000;
pub(crate) const WATCHDOG_STALL_FACTOR_NUM: u64 = 3;
pub(crate) const WATCHDOG_STALL_FACTOR_DEN: u64 = 5;
pub(crate) const WATCHDOG_MIN_STALL_MS: u64 = 6_000;
pub(crate) const LATE_RESULT_ACCEPTANCE_WINDOW_MS: u64 = 15_000;
pub(crate) const MIN_REMOTE_EXECUTION_TIMEOUT_MS: u64 = 120_000;
pub(crate) const MAX_REMOTE_EXECUTION_TIMEOUT_MS: u64 = 900_000;
pub(crate) const UNCLAIMED_WORKER_PEER_ID: &str = "-";

#[derive(Debug, Clone)]
pub(crate) struct RuntimeArgsConfig {
    pub(crate) args: Vec<String>,
    pub(crate) auto_model: bool,
    pub(crate) debug_flags: DebugFlags,
    pub(crate) mode: NodeMode,
    pub(crate) worker_port: u16,
}

pub(crate) struct RuntimeStartupConfig {
    pub(crate) node_identity: NodeIdentity,
    pub(crate) peer_id: PeerId,
    pub(crate) id_keys: identity::Keypair,
    pub(crate) wallet: Wallet,
    pub(crate) benchmark: Option<NodeBenchmark>,
    pub(crate) resource_policy: ResourcePolicy,
    pub(crate) worker_slots: usize,
}

impl RuntimeArgsConfig {
    pub(crate) fn from_args(args: Vec<String>) -> Result<Self, String> {
        let auto_model = args.iter().any(|arg| arg == "--auto-model");
        let debug_flags = DebugFlags::from_args(&args);
        let mode = parse_args_from(args.clone())?;
        let worker_port = parse_worker_port(&args);
        Ok(Self {
            args,
            auto_model,
            debug_flags,
            mode,
            worker_port,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeModeConfig {
    pub(crate) is_cluster_status_mode: bool,
    pub(crate) control_plane_only: bool,
}

impl RuntimeModeConfig {
    pub(crate) fn from_mode(mode: &NodeMode) -> Self {
        Self {
            is_cluster_status_mode: matches!(mode, NodeMode::ClusterStatus { .. }),
            control_plane_only: is_control_plane_mode(mode),
        }
    }
}

pub(crate) fn load_runtime_args() -> Result<RuntimeArgsConfig, String> {
    RuntimeArgsConfig::from_args(std::env::args().collect())
}

pub(crate) fn maybe_print_debug_flags(debug_flags: DebugFlags) {
    if debug_flags.network || debug_flags.scheduler || debug_flags.tasks {
        println!(
            "[Debug] network={} scheduler={} tasks={}",
            debug_flags.network, debug_flags.scheduler, debug_flags.tasks
        );
    }
}

pub(crate) fn uuid_simple() -> String {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{}{}", t.as_secs(), t.subsec_nanos())
}

pub(crate) fn prepare_runtime_startup_config(
    mode: &NodeMode,
    args: &[String],
    is_cluster_status_mode: bool,
    runtime_version: &str,
    worker_port: u16,
) -> RuntimeStartupConfig {
    if matches!(mode, NodeMode::Worker) {
        println!("╔══════════════════════════════════╗");
        println!("║    IaMine Worker Runtime         ║");
        println!("╚══════════════════════════════════╝\n");
        println!("🏷️  Version: {}", runtime_version);
    }

    let ephemeral_identity = if matches!(mode, NodeMode::Infer { .. }) {
        Some(("infer", "avoid_peer_id_conflict_with_local_worker"))
    } else if matches!(mode, NodeMode::Broadcast { .. })
        && env_bool_or_default(IAMINE_STRESS_CHILD, false)
    {
        Some((
            "cluster_stress_child",
            "avoid_peer_id_conflict_between_concurrent_stress_controllers",
        ))
    } else {
        None
    };
    let node_identity = if is_cluster_status_mode {
        NodeIdentity::load_or_create_quiet()
    } else if let Some((mode, _)) = ephemeral_identity {
        NodeIdentity::ephemeral(mode)
    } else {
        NodeIdentity::load_or_create()
    };
    let peer_id = node_identity.peer_id;
    set_global_node_id(&peer_id.to_string());
    let id_keys = node_identity.keypair.clone();

    if matches!(mode, NodeMode::Worker) {
        emit_worker_startup_started_event(&peer_id.to_string(), worker_port);
    }

    if let Some((mode, reason)) = ephemeral_identity {
        log_observability_event(
            LogLevel::Info,
            "identity_mode_selected",
            "startup",
            None,
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("mode".to_string(), mode.into());
                fields.insert("identity_kind".to_string(), "ephemeral".into());
                fields.insert("reason".to_string(), reason.into());
                fields.insert("peer_id".to_string(), peer_id.to_string().into());
                fields
            },
        );
    }

    let wallet = if is_cluster_status_mode {
        Wallet {
            address: node_identity.wallet_address.clone(),
            balance: 0,
            reputation: 100.0,
            tasks_completed: 0,
            tasks_failed: 0,
            total_uptime_secs: 0,
        }
    } else {
        Wallet::load_or_create(&node_identity.wallet_address)
    };

    let benchmark = if matches!(mode, NodeMode::Worker) {
        Some(NodeBenchmark::run())
    } else {
        None
    };

    let resource_policy = ResourcePolicy::from_args(args);
    if matches!(mode, NodeMode::Worker) {
        resource_policy.display();
    }

    let worker_slots = if let Some(ref benchmark) = benchmark {
        benchmark.calculate_slots(&resource_policy)
    } else {
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(2)
    };

    RuntimeStartupConfig {
        node_identity,
        peer_id,
        id_keys,
        wallet,
        benchmark,
        resource_policy,
        worker_slots,
    }
}

pub(crate) async fn simulate_workers(count: usize, _base_peer_id: String) {
    println!("🔥 Iniciando simulación de {} workers...", count);
    let mut handles = vec![];
    for i in 0..count {
        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(network_config::simulated_worker_tick_interval());
            let mut tasks_done = 0u64;
            loop {
                interval.tick().await;
                tasks_done += 1;
                if tasks_done.is_multiple_of(10) {
                    println!("👷 Worker-{}: {} tareas simuladas", i, tasks_done);
                }
            }
        });
        handles.push(handle);
    }
    println!(
        "✅ {} workers simulados activos. Ctrl+C para detener.",
        count
    );
    tokio::time::sleep(network_config::simulated_worker_run_duration()).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_args_preserve_worker_port_default() {
        let config =
            RuntimeArgsConfig::from_args(vec!["iamine-node".to_string(), "--worker".to_string()])
                .expect("runtime args");
        assert!(matches!(config.mode, NodeMode::Worker));
        assert_eq!(config.worker_port, 9000);
    }

    #[test]
    fn cli_help_does_not_read_runtime_config_destructively() {
        let config =
            RuntimeArgsConfig::from_args(vec!["iamine-node".to_string(), "--help".to_string()])
                .expect("help args");
        assert!(matches!(config.mode, NodeMode::Help));
    }

    #[test]
    fn infer_config_defaults_preserved() {
        assert_eq!(INFER_TIMEOUT_MS, 5_000);
        assert_eq!(INFER_FALLBACK_AFTER_MS, 2_000);
        assert_eq!(MAX_DISTRIBUTED_RETRIES, 2);
        assert_eq!(MIN_ADAPTIVE_TIMEOUT_MS, 7_500);
        assert_eq!(MAX_ADAPTIVE_TIMEOUT_MS, 45_000);
        assert_eq!(MIN_REMOTE_EXECUTION_TIMEOUT_MS, 120_000);
        assert_eq!(MAX_REMOTE_EXECUTION_TIMEOUT_MS, 900_000);
        assert_eq!(UNCLAIMED_WORKER_PEER_ID, "-");
    }
}
