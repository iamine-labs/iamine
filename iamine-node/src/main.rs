mod attempt_watchdog;
mod backend_runtime;
mod benchmark;
mod capability_advertising;
mod cli;
mod cli_mode_handlers;
mod code_quality;
mod daemon_runtime;
mod dispatch_runtime;
mod event_loop_control;
mod executor;
mod gossipsub_handlers;
mod heartbeat;
mod infer_cli_handlers;
mod infer_mode_handlers;
mod local_inference_runtime;
mod metrics;
mod mode_gate;
mod model_selector_cli;
mod network;
mod network_bootstrap;
mod network_events;
mod node_identity;
mod node_observability;
mod peer_tracker;
mod prompt_runtime;
mod protocol;
mod pubsub_tracker;
mod quality_gate;
mod rate_limiter;
mod regression_runner;
mod request_response_handlers;
mod resource_policy;
mod result_lifecycle;
mod result_protocol;
mod runtime_config;
mod runtime_state;
mod security_checks;
mod setup_wizard;
mod startup_bootstrap;
mod startup_math;
mod startup_model_validation;
mod task_cache;
mod task_codec;
mod task_protocol;
mod task_queue;
mod task_scheduler;
#[cfg(test)]
mod tests;
mod wallet;
mod worker_capabilities;
mod worker_heartbeat_runtime;
mod worker_pool;

use iamine_models::{
    can_node_run_model, normalize_output, AutoProvisionProfile, DirectInferenceRequest,
    HardwareAcceleration, InferenceTask, InferenceTaskResult, InstallResult, ModelAutoProvision,
    ModelInstaller, ModelNodeCapabilities, ModelRegistry, ModelRequirements, ModelStorage,
    RealInferenceEngine, RealInferenceRequest, RealInferenceResult, StorageConfig, StreamedToken,
};

use iamine_network::{
    analyze_prompt_semantics, default_semantic_log_path, describe_output_policy,
    detect_exact_subtype, distributed_task_metrics, evaluate_default_dataset, log_structured,
    normalize_expression, prompt_log_entry, ranked_models, record_distributed_task_failed,
    record_distributed_task_fallback, record_distributed_task_late_result,
    record_distributed_task_latency, record_distributed_task_retry,
    record_distributed_task_started, record_model_metrics, record_task_attempt,
    record_task_latency, select_retry_target, set_global_node_id, set_global_runtime_context,
    task_trace, validate_result, validate_semantic_decision, Complexity as PromptComplexityLevel,
    DeterministicLevel as PromptDeterministicLevel, DistributedTaskMetrics, DistributedTaskResult,
    Domain as PromptDomain, ExactSubtype as PromptExactSubtype, FailureKind, IntelligentScheduler,
    Language as PromptLanguage, LogLevel, ModelKarma, ModelMetrics, ModelPolicyEngine,
    NetworkTopology, NodeCapability, NodeCapabilityHeartbeat, NodeHealth, NodeRegistry,
    OutputPolicyDecision, OutputStyle as PromptOutputStyle, PromptProfile, ResultStatus,
    RetryPolicy, RetryState, SemanticFeedbackEngine, SemanticRoutingDecision,
    SharedNetworkTopology, SharedNodeRegistry, StructuredLogEntry, TaskClaim, TaskManager,
    TaskTrace, TaskType as PromptTaskType, ValidationResult as SemanticValidationResult,
    MODEL_LOAD_FAILED_001, MODEL_UNSUPPORTED_HW_002, NETWORK_NO_PUBSUB_PEERS_001,
    NET_PEER_DISCONNECTED_002, NODE_BLACKLISTED_001, NODE_UNHEALTHY_002,
    PUBSUB_TOPIC_NOT_READY_001, SCH_NO_NODE_001, TASK_DISPATCH_UNCONFIRMED_001,
    TASK_EMPTY_RESULT_003, TASK_FAILED_002, TASK_TIMEOUT_001, WORKER_STARTUP_OVERFLOW_001,
};

use attempt_watchdog::{
    is_meaningfully_in_flight, AttemptLifecycleState, AttemptTimeoutPolicy, AttemptWatchdog,
    WatchdogCheck,
};
use backend_runtime::choose_inference_runtime;
use benchmark::NodeBenchmark;
use cli::{
    is_control_plane_mode, mode_label, parse_args, DebugFlags, InferenceControlFlags, NodeMode,
};
use cli_mode_handlers::{
    handle_capabilities, handle_check_code, handle_check_security, handle_models_download,
    handle_models_list, handle_models_menu, handle_models_recommend, handle_models_remove,
    handle_models_search, handle_models_stats, handle_regression_run, handle_semantic_eval,
    handle_tasks_stats, handle_tasks_trace, handle_validate_release,
};
use daemon_runtime::{daemon_is_available, daemon_socket_path, infer_via_daemon, run_daemon};
#[cfg(test)]
use dispatch_runtime::{apply_retry_fallback_metrics, emit_attempt_progress_event};
use dispatch_runtime::{
    build_direct_inference_request_context, build_inference_request_context,
    emit_attempt_stalled_event, emit_attempt_state_changed_event,
    emit_attempt_timeout_extended_event, emit_dispatch_context_event,
    emit_dispatch_readiness_failure_event, emit_fallback_attempt_registered_event,
    emit_late_result_received_event, emit_result_received_event, emit_retry_scheduled_event,
    emit_task_publish_attempt_event, emit_task_publish_failed_event, emit_task_published_event,
    emit_worker_task_message_received_event, evaluate_dispatch_readiness,
    run_worker_inference_pipeline, should_print_result_output, topic_hash_string,
    DispatchContextEvent, DispatchReadinessSnapshot, LateResultReceivedEvent, PublishEventContext,
};
use event_loop_control::EventLoopDirective;
use gossipsub_handlers::{handle_gossipsub_message, GossipsubHandlerContext};
use heartbeat::HeartbeatService;
use infer_cli_handlers::handle_test_inference;
use local_inference_runtime::{run_local_inference_with_timeout, InferenceRuntime};
use metrics::{start_metrics_server, NodeMetrics};
use mode_gate::{handle_pre_runtime_mode, ModeGateOutcome};
use model_selector_cli::ModelSelectorCLI;
use network_bootstrap::{build_swarm_and_topics, dial_bootnodes_from_args};
use network_events::{
    handle_connection_closed, handle_connection_established, handle_kademlia_routing_updated,
    handle_mdns_discovered, handle_mdns_expired, handle_ping_event, handle_pubsub_subscribed,
    handle_pubsub_unsubscribed, handle_result_response_ack, handle_result_response_request,
};
use node_identity::NodeIdentity; // ← actualizado
use node_observability::{
    cluster_label_for_latency, debug_network_log, debug_scheduler_log, debug_task_log,
    finalize_distributed_task_observability, log_health_update, log_observability_event,
    print_distributed_task_stats, print_model_karma_stats, print_task_trace_entry,
};
use peer_tracker::PeerTracker;
use prompt_runtime::{
    exact_subtype_label, prompt_task_label, record_semantic_feedback, resolve_output_policy,
    resolve_policy_for_prompt, task_requires_validation, with_task_guard,
};
use pubsub_tracker::PubsubTopicTracker;
use quality_gate::current_release_version;
use rate_limiter::RateLimiter;
use request_response_handlers::{handle_request_response_event, RequestResponseHandlerContext};
use resource_policy::ResourcePolicy;
use result_lifecycle::{
    apply_late_result_lifecycle, apply_result_failure_lifecycle, apply_result_success_lifecycle,
    clear_stream_state, LateResultLifecycleInput, ResultFailureInput,
    ResultFailureLifecycleContext,
};
use result_protocol::{TaskResultRequest, TaskResultResponse};
use runtime_config::{listen_address_for_mode, worker_port_from_args};
use runtime_state::{ClientRuntimeState, DistributedInferState, InferRuntimeState};
use serde_json::{Map, Value};
use setup_wizard::{DetectedHardware, NodeSetupConfig};
use startup_bootstrap::{bootstrap_core_state, bootstrap_model_state};
#[cfg(test)]
use startup_math::{checked_sub_usize, StartupMathError};
use startup_math::{
    compute_metrics_port, emit_worker_startup_overflow_event, resource_policy_value,
    WorkerStartupOverflowContext,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use task_cache::TaskCache;
use task_queue::TaskQueue;
use task_scheduler::TaskScheduler;
use tokio::sync::RwLock;
use wallet::Wallet;
use worker_capabilities::WorkerCapabilities;
use worker_heartbeat_runtime::{handle_worker_heartbeat_tick, WorkerHeartbeatContext};
use worker_pool::WorkerPool;

use futures::StreamExt;
use libp2p::{
    gossipsub,
    identify,
    kad,
    mdns,
    noise,
    ping, // ← quitado identity de aquí
    request_response::{self, cbor, Event as RREvent, Message, ProtocolSupport},
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp,
    yamux,
    Multiaddr,
    PeerId,
    StreamProtocol,
    Transport,
};
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use executor::TaskExecutor;
use task_protocol::{TaskRequest, TaskResponse};

const TASK_TOPIC: &str = "iamine-tasks";
const CAP_TOPIC: &str = "iamine-capabilities";
const DIRECT_INF_TOPIC: &str = "iamine-direct-inference";
const RESULTS_TOPIC: &str = "iamine-results";
const INFER_TIMEOUT_MS: u64 = 5_000;
const INFER_FALLBACK_AFTER_MS: u64 = 2_000;
const MAX_DISTRIBUTED_RETRIES: u8 = 2;
const MIN_ADAPTIVE_TIMEOUT_MS: u64 = 7_500;
const MAX_ADAPTIVE_TIMEOUT_MS: u64 = 45_000;
const WATCHDOG_EXTENSION_STEP_MS: u64 = 4_000;
const WATCHDOG_STALL_FACTOR_NUM: u64 = 3;
const WATCHDOG_STALL_FACTOR_DEN: u64 = 5;
const WATCHDOG_MIN_STALL_MS: u64 = 6_000;

struct PendingTaskResponse {
    channel: request_response::ResponseChannel<TaskResponse>,
    response: TaskResponse,
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "IaMineEvent")]
struct IamineBehaviour {
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    request_response: cbor::Behaviour<TaskRequest, TaskResponse>,
    result_response: cbor::Behaviour<TaskResultRequest, TaskResultResponse>, // ← nuevo
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    mdns: mdns::tokio::Behaviour,
    gossipsub: gossipsub::Behaviour,
}

#[derive(Debug)]
enum IaMineEvent {
    Ping(ping::Event),
    #[allow(dead_code)]
    Identify(identify::Event),
    RequestResponse(RREvent<TaskRequest, TaskResponse>),
    ResultResponse(RREvent<TaskResultRequest, TaskResultResponse>), // ← nuevo
    Kademlia(kad::Event),
    Mdns(mdns::Event),
    Gossipsub(gossipsub::Event),
}

impl From<ping::Event> for IaMineEvent {
    fn from(e: ping::Event) -> Self {
        IaMineEvent::Ping(e)
    }
}
impl From<identify::Event> for IaMineEvent {
    fn from(e: identify::Event) -> Self {
        IaMineEvent::Identify(e)
    }
}
impl From<RREvent<TaskRequest, TaskResponse>> for IaMineEvent {
    fn from(e: RREvent<TaskRequest, TaskResponse>) -> Self {
        IaMineEvent::RequestResponse(e)
    }
}
impl From<RREvent<TaskResultRequest, TaskResultResponse>> for IaMineEvent {
    fn from(e: RREvent<TaskResultRequest, TaskResultResponse>) -> Self {
        IaMineEvent::ResultResponse(e)
    }
}
impl From<kad::Event> for IaMineEvent {
    fn from(e: kad::Event) -> Self {
        IaMineEvent::Kademlia(e)
    }
}
impl From<mdns::Event> for IaMineEvent {
    fn from(e: mdns::Event) -> Self {
        IaMineEvent::Mdns(e)
    }
}
impl From<gossipsub::Event> for IaMineEvent {
    fn from(e: gossipsub::Event) -> Self {
        IaMineEvent::Gossipsub(e)
    }
}

struct TimeoutContext {
    trace_task_id: String,
    attempt_id: String,
    worker_peer_id: String,
    model_id: String,
    elapsed_ms: u64,
    elapsed_since_progress_ms: u64,
    adaptive_timeout_ms: u64,
    max_wait_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let auto_model = args.iter().any(|a| a == "--auto-model");
    let debug_flags = DebugFlags::from_args(&args);
    let mode = match parse_args() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("❌ {}", e);
            eprintln!("Uso:");
            eprintln!("  iamine-node --worker [--port=N] [--cpu=N] [--ram=N] [--gpu]");
            eprintln!("  iamine-node --relay");
            eprintln!("  iamine-node --broadcast <type> <data>");
            eprintln!("  iamine-node models list");
            eprintln!("  iamine-node models stats");
            eprintln!("  iamine-node models download <model_id>");
            eprintln!("  iamine-node models remove <model_id>");
            eprintln!("  iamine-node semantic-eval");
            eprintln!("  iamine-node regression-run");
            eprintln!("  iamine-node check-code");
            eprintln!("  iamine-node check-security");
            eprintln!("  iamine-node validate-release");
            eprintln!("  iamine-node tasks stats");
            eprintln!("  iamine-node tasks trace <task_id>");
            eprintln!("  iamine-node --daemon");
            eprintln!("Flags:");
            eprintln!("  --debug-network");
            eprintln!("  --debug-scheduler");
            eprintln!("  --debug-tasks");
            eprintln!("  --force-network");
            eprintln!("  --no-local");
            eprintln!("  --prefer-local");
            // eprintln!("  iamine-node nodes");
            std::process::exit(1);
        }
    };
    if debug_flags.network || debug_flags.scheduler || debug_flags.tasks {
        println!(
            "[Debug] network={} scheduler={} tasks={}",
            debug_flags.network, debug_flags.scheduler, debug_flags.tasks
        );
    }
    set_global_runtime_context(mode_label(&mode), current_release_version());
    let control_plane_only = is_control_plane_mode(&mode);

    // v0.6.1: Registry global para smart routing (capabilities + selección de nodo)
    let registry: SharedNodeRegistry = Arc::new(RwLock::new(NodeRegistry::new()));
    let topology: SharedNetworkTopology = Arc::new(RwLock::new(NetworkTopology::new()));

    // ═══════════════════════════════════════════════
    // CLI/Control-plane gating — salida temprana
    // ═══════════════════════════════════════════════
    match handle_pre_runtime_mode(&mode).await? {
        ModeGateOutcome::Exit => return Ok(()),
        ModeGateOutcome::ContinueRuntime => {}
    }

    if control_plane_only {
        unreachable!("control plane mode should have returned before runtime startup");
    }

    // ════════════════════════════════════════
    // WORKER STARTUP FLOW
    // ════════════════════════════════════════
    let core_startup = bootstrap_core_state(&args, &mode);
    let node_identity = core_startup.node_identity;
    let peer_id = core_startup.peer_id;
    let id_keys = node_identity.keypair.clone();
    let wallet = core_startup.wallet;
    let benchmark = core_startup.benchmark;
    let resource_policy = core_startup.resource_policy;
    let worker_slots = core_startup.worker_slots;

    let model_startup =
        bootstrap_model_state(&mode, auto_model, &peer_id, benchmark.as_ref()).await?;
    let model_storage = model_startup.model_storage;
    let inference_engine = model_startup.inference_engine;
    let node_caps = model_startup.node_caps;
    let validated_advertised_models = model_startup.validated_advertised_models;

    // 7️⃣ Simulate workers — salida temprana
    if let NodeMode::SimulateWorkers { count } = &mode {
        println!("🔥 Simulando {} workers virtuales...", count);
        simulate_workers(*count, peer_id.to_string()).await;
        return Ok(());
    }

    let (mut swarm, mut pubsub_topics) = build_swarm_and_topics(peer_id, &id_keys)?;
    let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC);

    let pool = Arc::new(WorkerPool::with_slots(worker_slots));
    let queue = Arc::new(TaskQueue::new(peer_id.to_string()));
    let scheduler = Arc::new(TaskScheduler::new());
    let task_manager = Arc::new(TaskManager::new());
    let (task_response_tx, mut task_response_rx) =
        tokio::sync::mpsc::channel::<PendingTaskResponse>(64);
    let heartbeat = Arc::new(HeartbeatService::new());
    let metrics = Arc::new(RwLock::new(NodeMetrics::new()));
    let capabilities = WorkerCapabilities::detect();
    let mut task_cache = TaskCache::new(1000);
    let mut peer_tracker = PeerTracker::new();
    let mut rate_limiter = RateLimiter::new(100); // ← 100 msgs/sec max

    // ← Actualizar métricas con wallet
    {
        let mut m = metrics.write().await;
        m.reputation_score = wallet.reputation as u32;
    }

    // Relay mode
    if matches!(mode, NodeMode::Relay) {
        println!("🔀 Modo RELAY activo — ayudando a peers detrás de NAT");
        println!("   Peer ID (compartir con otros nodos): {}", peer_id);
    }

    println!("   Slots paralelos: {}", pool.max_concurrent);
    println!("   Task queue: activa (max 3 reintentos)");
    println!("   Scheduler: activo (bid window 1.5s)\n");

    dial_bootnodes_from_args(&mut swarm, &args);

    let worker_port = worker_port_from_args(&args);
    if matches!(mode, NodeMode::Worker) {
        log_observability_event(
            LogLevel::Info,
            "worker_started",
            "startup",
            None,
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("peer_id".to_string(), peer_id.to_string().into());
                fields.insert("port".to_string(), (worker_port as u64).into());
                fields.insert("worker_slots".to_string(), (worker_slots as u64).into());
                fields.insert(
                    "resource_policy".to_string(),
                    resource_policy_value(&resource_policy),
                );
                fields
            },
        );
    }

    let listen_addr = listen_address_for_mode(&mode, worker_port)?;
    swarm.listen_on(listen_addr)?;

    let metrics_port = if matches!(mode, NodeMode::Worker) {
        match compute_metrics_port(worker_port) {
            Ok(port) => Some(port),
            Err(error) => {
                emit_worker_startup_overflow_event(WorkerStartupOverflowContext {
                    trace_id: "startup",
                    node_id: &node_identity.node_id,
                    peer_id: &peer_id.to_string(),
                    port: worker_port,
                    resource_policy: &resource_policy,
                    worker_slots,
                    max_concurrent: pool.max_concurrent,
                    available_slots: pool.available_slots(),
                    error: &error,
                    fallback_behavior: "continue_without_metrics_server",
                });
                println!(
                    "⚠️  [Startup] Cálculo de metrics port inválido: {}. Continuando en modo degradado (metrics deshabilitado).",
                    error.describe()
                );
                None
            }
        }
    } else {
        None
    };

    if let Some(metrics_port) = metrics_port {
        let m = Arc::clone(&metrics);
        let port = metrics_port;
        println!("📊 Metrics en http://localhost:{}/metrics", metrics_port);
        tokio::spawn(async move {
            start_metrics_server(m, port).await;
        });
    }

    let mut heartbeat_rx = HeartbeatService::start(5);
    let mut nodes_tick = tokio::time::interval(Duration::from_secs(5)); // ← nuevo ticker dedicado

    let mut client_state = ClientRuntimeState::new();
    client_state.prime_tasks_from_mode(&mode);

    let mut infer_runtime = InferRuntimeState::new();
    infer_runtime.initialize_infer_mode_if_needed(&mode, &model_storage);

    let is_client = !matches!(mode, NodeMode::Worker | NodeMode::Relay);

    loop {
        tokio::select! {
            // Ticker dedicado para mostrar nodos/capabilities/topology
            _ = nodes_tick.tick(), if matches!(mode, NodeMode::Nodes | NodeMode::Topology) => {
                if matches!(mode, NodeMode::Nodes) {
                    let snapshot = registry.read().await.all_nodes();
                    println!("\nPeer ID        Models                CPU Score    Load");
                    println!("-------------------------------------------------------");
                    if snapshot.is_empty() {
                        println!("(esperando capabilities de peers...)");
                    } else {
                        for (pid, c) in snapshot {
                            let models = if c.models.is_empty() { "-".to_string() } else { c.models.join(",") };
                            println!(
                                "{}  {:20} {:10} {}/{}",
                                &pid.to_string()[..10.min(pid.to_string().len())],
                                models,
                                c.cpu_score,
                                c.active_tasks,
                                c.worker_slots
                            );
                        }
                    }
                }
                if matches!(mode, NodeMode::Topology) {
                    // Assign clusters before display
                    {
                        let mut topo = topology.write().await;
                        topo.assign_clusters(&peer_id.to_string());
                    }
                    let topo = topology.read().await;
                    topo.display();
                    if topo.peer_count() > 0 {
                        println!("   ℹ️  Latencias: medidas reales (libp2p ping RTT)");
                        println!("\n(Ctrl+C para salir)");
                    } else {
                        println!("(esperando peers...)");
                    }
                }
            }

            Some(completed_response) = task_response_rx.recv() => {
                let task_id = completed_response.response.task_id.clone();
                if swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(completed_response.channel, completed_response.response)
                    .is_ok()
                {
                    println!("[Task] Completed {}", task_id);
                } else {
                    eprintln!("[Task] Failed to return result for {}", task_id);
                }
            }

            // Heartbeat tick
            Some(_) = heartbeat_rx.recv() => {
                handle_worker_heartbeat_tick(WorkerHeartbeatContext {
                    mode: &mode,
                    queue: &queue,
                    heartbeat: &heartbeat,
                    pool: &pool,
                    metrics: &metrics,
                    peer_tracker: &mut peer_tracker,
                    direct_peers_count: client_state.known_workers.len(),
                    rate_limiter: &mut rate_limiter,
                    swarm: &mut swarm,
                    peer_id,
                    capabilities: &capabilities,
                    validated_advertised_models: &validated_advertised_models,
                    benchmark: benchmark.as_ref(),
                    node_caps: &node_caps,
                    worker_slots,
                    topology: &topology,
                    registry: &registry,
                    resource_policy: &resource_policy,
                    node_identity: &node_identity,
                    worker_port,
                }).await?;

                // Smart routing: intento envío directo cuando ya hay registry
                if matches!(mode, NodeMode::Infer { .. }) && !infer_runtime.infer_broadcast_sent {
                    if let Some(infer_state) = infer_runtime.distributed_infer_state.as_mut() {
                        let local_models = model_storage.list_local_models();
                        let start = std::time::Instant::now();
                        let local_cluster = {
                            let topo = topology.read().await;
                            topo.cluster_for_peer(&peer_id.to_string()).map(|s| s.to_string())
                        };
                        let reg = registry.read().await;
                        let total_nodes = reg.all_nodes().len();
                        let network_models = reg.available_models();
                        let mut available_models = local_models.clone();
                        for model in network_models {
                            if !available_models.contains(&model) {
                                available_models.push(model);
                            }
                        }

                        let resolution = resolve_policy_for_prompt(
                            &infer_state.prompt,
                            infer_state.model_override.as_deref(),
                            &available_models,
                        );
                        let profile = resolution.profile;
                        let mut candidates = resolution.candidate_models;
                        let selected_model = resolution.selected_model;
                        let semantic_prompt = resolution.semantic_prompt;
                        let output_policy = resolve_output_policy(
                            &profile,
                            &semantic_prompt,
                            infer_state.max_tokens_override,
                        );
                        if !candidates.contains(&selected_model) {
                            candidates.insert(0, selected_model.clone());
                        }

                        let scheduler = IntelligentScheduler::new();
                        let selected = scheduler
                            .select_best_node_for_models_excluding(
                                &reg,
                                &candidates,
                                local_cluster.as_deref(),
                                &infer_state.retry_state.failed_peers,
                                &infer_state.retry_state.failed_models,
                            )
                            .map(|decision| {
                                let selected_capability = reg
                                    .all_nodes()
                                    .into_iter()
                                    .find(|(peer_id, _)| peer_id == &decision.peer_id)
                                    .map(|(_, capability)| capability);
                                let selected_cluster_id = selected_capability
                                    .as_ref()
                                    .and_then(|capability| capability.cluster_id.clone());
                                (
                                    decision.peer_id,
                                    decision.model_id,
                                    decision.score,
                                    selected_cluster_id,
                                    selected_capability,
                                )
                            });
                        drop(reg);

                        if let Some((
                            best_peer,
                            routed_model,
                            node_score,
                            selected_cluster_id,
                            selected_capability,
                        )) = selected
                        {
                            let rid = infer_state.current_request_id.clone();
                            let trace_task_id = infer_state.trace_task_id.clone();
                            let duplicate_inflight = infer_runtime.attempt_watchdogs.values().any(|watchdog| {
                                watchdog.task_id == trace_task_id
                                    && watchdog.worker_peer_id == best_peer
                                    && watchdog.model_id == routed_model
                                    && watchdog.attempt_id != rid
                                    && is_meaningfully_in_flight(watchdog.state)
                            });
                            if duplicate_inflight {
                                log_observability_event(
                                    LogLevel::Info,
                                    "dispatch_deduplicated_inflight",
                                    &trace_task_id,
                                    Some(&trace_task_id),
                                    Some(&routed_model),
                                    None,
                                    {
                                        let mut fields = Map::new();
                                        fields.insert("attempt_id".to_string(), rid.clone().into());
                                        fields.insert("selected_peer_id".to_string(), best_peer.clone().into());
                                        fields.insert("selected_model".to_string(), routed_model.clone().into());
                                        fields.insert(
                                            "reason".to_string(),
                                            "inflight_attempt_same_worker_model".into(),
                                        );
                                        fields
                                    },
                                );
                                continue;
                            }
                            let is_retry_attempt = infer_state.retry_state.retry_count > 0;
                            let is_model_switch_attempt = infer_state
                                .current_model
                                .as_ref()
                                .map(|previous_model| previous_model != &routed_model)
                                .unwrap_or(false);

                            if is_retry_attempt {
                                println!(
                                    "[Retry] Dispatching retry attempt {}/{}",
                                    infer_state.retry_state.retry_count,
                                    infer_state.retry_policy.max_retries
                                );
                                if is_model_switch_attempt {
                                    if let Some(previous_model) = infer_state.current_model.as_ref() {
                                        println!(
                                            "[Fallback] Switching model {} -> {}",
                                            previous_model, routed_model
                                        );
                                    }
                                }
                            }

                            println!(
                                "[Trace] task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                                trace_task_id,
                                rid,
                                best_peer,
                                selected_cluster_id.as_deref().unwrap_or("-"),
                                routed_model
                            );
                            log_observability_event(
                                LogLevel::Info,
                                "scheduler_node_selected",
                                &trace_task_id,
                                Some(&trace_task_id),
                                Some(&routed_model),
                                None,
                                {
                                    let mut fields = Map::new();
                                    fields.insert(
                                        "selected_peer_id".to_string(),
                                        best_peer.clone().into(),
                                    );
                                    fields.insert(
                                        "cluster_id".to_string(),
                                        selected_cluster_id
                                            .clone()
                                            .unwrap_or_else(|| "-".to_string())
                                            .into(),
                                    );
                                    fields.insert("score".to_string(), node_score.into());
                                    fields.insert(
                                        "reason".to_string(),
                                        "high_success_low_latency".into(),
                                    );
                                    fields.insert(
                                        "candidate_models".to_string(),
                                        serde_json::json!(candidates),
                                    );
                                    fields
                                },
                            );
                            debug_scheduler_log(
                                debug_flags,
                                format!(
                                    "task_id={} attempt_id={} peer_id={} cluster_id={} model_id={} score={:.3} retries={} total_nodes={}",
                                    trace_task_id,
                                    rid,
                                    best_peer,
                                    selected_cluster_id.as_deref().unwrap_or("-"),
                                    routed_model,
                                    node_score,
                                    infer_state.retry_state.retry_count,
                                    total_nodes
                                ),
                            );
                            debug_task_log(
                                debug_flags,
                                &best_peer,
                                selected_cluster_id.as_deref(),
                                &routed_model,
                                &trace_task_id,
                                &rid,
                                "dispatching distributed task",
                            );
                            let topic_peer_count = pubsub_topics.topic_peer_count(DIRECT_INF_TOPIC);
                            let readiness_snapshot = DispatchReadinessSnapshot {
                                connected_peer_count: client_state.known_workers.len(),
                                mesh_peer_count: pubsub_topics.mesh_peer_count(),
                                topic_peer_count,
                                joined_task_topic: pubsub_topics.joined(TASK_TOPIC),
                                joined_direct_topic: pubsub_topics.joined(DIRECT_INF_TOPIC),
                                joined_results_topic: pubsub_topics.joined(RESULTS_TOPIC),
                                selected_topic: DIRECT_INF_TOPIC,
                            };
                            emit_dispatch_context_event(DispatchContextEvent {
                                trace_task_id: &trace_task_id,
                                attempt_id: &rid,
                                selected_model: &routed_model,
                                candidates: &candidates,
                                connected_peer_count: readiness_snapshot.connected_peer_count,
                                topic_peer_count: readiness_snapshot.topic_peer_count,
                                selected_topic: DIRECT_INF_TOPIC,
                                target_peer_id: Some(&best_peer),
                            });
                            if let Err(readiness_error) = evaluate_dispatch_readiness(&readiness_snapshot)
                            {
                                emit_dispatch_readiness_failure_event(
                                    &trace_task_id,
                                    &rid,
                                    &routed_model,
                                    &candidates,
                                    Some(&best_peer),
                                    &readiness_snapshot,
                                    &readiness_error,
                                );
                                let elapsed_ms = infer_runtime.infer_started_at
                                    .map(|started| started.elapsed().as_millis() as u64)
                                    .unwrap_or_default();
                                if elapsed_ms < INFER_TIMEOUT_MS {
                                    debug_network_log(
                                        debug_flags,
                                        format!(
                                            "dispatch waiting for readiness task_id={} attempt_id={} reason={} elapsed_ms={}",
                                            trace_task_id, rid, readiness_error.reason, elapsed_ms
                                        ),
                                    );
                                    continue;
                                }
                                return Err(format!(
                                    "Dispatch readiness timeout: task_id={} attempt_id={} reason={} [{}]",
                                    trace_task_id, rid, readiness_error.reason, readiness_error.code
                                )
                                .into());
                            }

                            let direct = DirectInferenceRequest {
                                request_id: rid.clone(),
                                target_peer: best_peer.to_string(),
                                model: routed_model.clone(),
                                prompt: semantic_prompt.clone(),
                                max_tokens: output_policy.max_tokens as u32,
                            };
                            let mut direct_payload = direct.to_gossip_json();
                            if let Some(map) = direct_payload.as_object_mut() {
                                map.insert("task_id".to_string(), trace_task_id.clone().into());
                                map.insert("attempt_id".to_string(), rid.clone().into());
                            }
                            let payload = serde_json::to_vec(&direct_payload)
                                .map_err(|error| format!("Direct dispatch payload error: {}", error))?;
                            let payload_size = payload.len();
                            emit_task_publish_attempt_event(
                                &trace_task_id,
                                &rid,
                                &routed_model,
                                DIRECT_INF_TOPIC,
                                topic_peer_count,
                                payload_size,
                                Some(&best_peer),
                            );
                            let publish_result = swarm.behaviour_mut().gossipsub.publish(
                                gossipsub::IdentTopic::new(DIRECT_INF_TOPIC),
                                payload,
                            );
                            match publish_result {
                                Ok(message_id) => {
                                    let message_id = message_id.to_string();
                                    infer_state.record_attempt(
                                        best_peer.clone(),
                                        routed_model.clone(),
                                        profile.task_type,
                                        semantic_prompt.clone(),
                                        local_cluster.clone(),
                                        candidates.clone(),
                                    );
                                    let _ = record_task_attempt(
                                        &trace_task_id,
                                        &best_peer,
                                        &routed_model,
                                        is_retry_attempt,
                                        is_model_switch_attempt,
                                    );
                                    if is_retry_attempt {
                                        let _ = record_distributed_task_retry();
                                    }
                                    emit_task_published_event(
                                        PublishEventContext {
                                            trace_task_id: &trace_task_id,
                                            attempt_id: &rid,
                                            model_id: &routed_model,
                                            topic: DIRECT_INF_TOPIC,
                                            publish_peer_count: topic_peer_count,
                                            payload_size,
                                            selected_peer_id: Some(&best_peer),
                                        },
                                        &message_id,
                                    );
                                    let timeout_policy = AttemptTimeoutPolicy::from_model_and_node(
                                        &routed_model,
                                        selected_capability.as_ref(),
                                    );
                                    log_observability_event(
                                        LogLevel::Info,
                                        "attempt_timeout_policy",
                                        &trace_task_id,
                                        Some(&trace_task_id),
                                        Some(&routed_model),
                                        None,
                                        {
                                            let mut fields = Map::new();
                                            fields.insert("attempt_id".to_string(), rid.clone().into());
                                            fields.insert(
                                                "worker_peer_id".to_string(),
                                                best_peer.clone().into(),
                                            );
                                            fields.insert(
                                                "timeout_ms".to_string(),
                                                timeout_policy.timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "stall_timeout_ms".to_string(),
                                                timeout_policy.stall_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "max_wait_ms".to_string(),
                                                timeout_policy.max_wait_ms.into(),
                                            );
                                            fields.insert(
                                                "latency_class".to_string(),
                                                timeout_policy.latency_class.into(),
                                            );
                                            fields
                                        },
                                    );
                                    let mut watchdog = AttemptWatchdog::new(
                                        trace_task_id.clone(),
                                        rid.clone(),
                                        best_peer.clone(),
                                        routed_model.clone(),
                                        timeout_policy,
                                    );
                                    let _ = watchdog.transition_state_with_event(
                                        AttemptLifecycleState::Starting,
                                        Some(&best_peer),
                                    );
                                    infer_runtime.attempt_watchdogs.insert(rid.clone(), watchdog);
                                    metrics
                                        .write()
                                        .await
                                        .routing_decision(start.elapsed().as_millis() as u64);
                                    infer_runtime.infer_broadcast_sent = true;
                                    client_state.waiting_for_response = true;
                                    infer_runtime.pending_inference.insert(
                                        rid.clone(),
                                        tokio::time::Instant::now(),
                                    );
                                    infer_runtime.infer_request_id = Some(rid.clone());
                                    println!(
                                        "[Routing] task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                                        trace_task_id,
                                        rid,
                                        best_peer,
                                        selected_cluster_id.as_deref().unwrap_or("-"),
                                        routed_model
                                    );
                                    println!(
                                        "[Scheduler] Selected node {} with score {:.3}",
                                        best_peer, node_score
                                    );
                                    println!(
                                        "🧠 DirectInferenceRequest enviado [{}] → {}",
                                        &rid[..8.min(rid.len())],
                                        best_peer
                                    );
                                }
                                Err(error) => {
                                    let error_text = error.to_string();
                                    emit_task_publish_failed_event(
                                        PublishEventContext {
                                            trace_task_id: &trace_task_id,
                                            attempt_id: &rid,
                                            model_id: &routed_model,
                                            topic: DIRECT_INF_TOPIC,
                                            publish_peer_count: topic_peer_count,
                                            payload_size,
                                            selected_peer_id: Some(&best_peer),
                                        },
                                        &error_text,
                                    );
                                    return Err(format!(
                                        "Dispatch publish failed: task_id={} attempt_id={} error={} [{}]",
                                        trace_task_id, rid, error_text, TASK_DISPATCH_UNCONFIRMED_001
                                    )
                                    .into());
                                }
                            }
                        } else if total_nodes > 0
                            && candidates.iter().all(|m| !available_models.contains(m))
                        {
                            log_observability_event(
                                LogLevel::Error,
                                "node_rejected",
                                &infer_state.trace_task_id,
                                Some(&infer_state.trace_task_id),
                                None,
                                Some(SCH_NO_NODE_001),
                                {
                                    let mut fields = Map::new();
                                    fields.insert("reason".to_string(), "no_compatible_model".into());
                                    fields.insert("known_nodes".to_string(), (total_nodes as u64).into());
                                    fields.insert(
                                        "candidate_models".to_string(),
                                        serde_json::json!(candidates),
                                    );
                                    fields
                                },
                            );
                            eprintln!("❌ Ningún nodo en la red tiene un modelo compatible instalado.");
                            eprintln!("   Nodos conocidos: {}", total_nodes);
                            eprintln!("   Candidates: {}", candidates.join(", "));
                            return Err("No compatible node available for distributed inference".into());
                        } else if infer_runtime.infer_started_at
                            .map(|t| t.elapsed().as_millis() as u64)
                            .unwrap_or(0)
                            >= INFER_FALLBACK_AFTER_MS
                        {
                            // Fallback automático a broadcast legacy
                            let rid = infer_state.current_request_id.clone();
                            let mid = selected_model.clone();
                            let trace_task_id = infer_state.trace_task_id.clone();
                            let task = InferenceTask::new(
                                rid.clone(),
                                mid,
                                semantic_prompt.clone(),
                                output_policy.max_tokens as u32,
                                peer_id.to_string(),
                            );
                            let topic_peer_count = pubsub_topics.topic_peer_count(TASK_TOPIC);
                            let readiness_snapshot = DispatchReadinessSnapshot {
                                connected_peer_count: client_state.known_workers.len(),
                                mesh_peer_count: pubsub_topics.mesh_peer_count(),
                                topic_peer_count,
                                joined_task_topic: pubsub_topics.joined(TASK_TOPIC),
                                joined_direct_topic: pubsub_topics.joined(DIRECT_INF_TOPIC),
                                joined_results_topic: pubsub_topics.joined(RESULTS_TOPIC),
                                selected_topic: TASK_TOPIC,
                            };
                            emit_dispatch_context_event(DispatchContextEvent {
                                trace_task_id: &trace_task_id,
                                attempt_id: &rid,
                                selected_model: &selected_model,
                                candidates: &candidates,
                                connected_peer_count: readiness_snapshot.connected_peer_count,
                                topic_peer_count: readiness_snapshot.topic_peer_count,
                                selected_topic: TASK_TOPIC,
                                target_peer_id: None,
                            });
                            if let Err(readiness_error) = evaluate_dispatch_readiness(&readiness_snapshot)
                            {
                                emit_dispatch_readiness_failure_event(
                                    &trace_task_id,
                                    &rid,
                                    &selected_model,
                                    &candidates,
                                    None,
                                    &readiness_snapshot,
                                    &readiness_error,
                                );
                                let elapsed_ms = infer_runtime.infer_started_at
                                    .map(|started| started.elapsed().as_millis() as u64)
                                    .unwrap_or_default();
                                if elapsed_ms < INFER_TIMEOUT_MS {
                                    debug_network_log(
                                        debug_flags,
                                        format!(
                                            "fallback waiting for readiness task_id={} attempt_id={} reason={} elapsed_ms={}",
                                            trace_task_id, rid, readiness_error.reason, elapsed_ms
                                        ),
                                    );
                                    continue;
                                }
                                return Err(format!(
                                    "Fallback dispatch readiness timeout: task_id={} attempt_id={} reason={} [{}]",
                                    trace_task_id, rid, readiness_error.reason, readiness_error.code
                                )
                                .into());
                            }

                            let mut fallback_payload = task.to_gossip_json();
                            if let Some(map) = fallback_payload.as_object_mut() {
                                map.insert("task_id".to_string(), trace_task_id.clone().into());
                                map.insert("attempt_id".to_string(), rid.clone().into());
                            }
                            let payload = serde_json::to_vec(&fallback_payload)
                                .map_err(|error| format!("Fallback dispatch payload error: {}", error))?;
                            let payload_size = payload.len();
                            emit_task_publish_attempt_event(
                                &trace_task_id,
                                &rid,
                                &selected_model,
                                TASK_TOPIC,
                                topic_peer_count,
                                payload_size,
                                None,
                            );
                            let publish_result = swarm.behaviour_mut().gossipsub.publish(
                                gossipsub::IdentTopic::new(TASK_TOPIC),
                                payload,
                            );
                            match publish_result {
                                Ok(message_id) => {
                                    let message_id = message_id.to_string();
                                    emit_task_published_event(
                                        PublishEventContext {
                                            trace_task_id: &trace_task_id,
                                            attempt_id: &rid,
                                            model_id: &selected_model,
                                            topic: TASK_TOPIC,
                                            publish_peer_count: topic_peer_count,
                                            payload_size,
                                            selected_peer_id: None,
                                        },
                                        &message_id,
                                    );
                                    let timeout_policy =
                                        AttemptTimeoutPolicy::from_model_and_node(
                                            &selected_model,
                                            None,
                                        );
                                    log_observability_event(
                                        LogLevel::Info,
                                        "attempt_timeout_policy",
                                        &trace_task_id,
                                        Some(&trace_task_id),
                                        Some(&selected_model),
                                        None,
                                        {
                                            let mut fields = Map::new();
                                            fields.insert("attempt_id".to_string(), rid.clone().into());
                                            fields.insert("worker_peer_id".to_string(), "-".into());
                                            fields.insert(
                                                "timeout_ms".to_string(),
                                                timeout_policy.timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "stall_timeout_ms".to_string(),
                                                timeout_policy.stall_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "max_wait_ms".to_string(),
                                                timeout_policy.max_wait_ms.into(),
                                            );
                                            fields.insert(
                                                "latency_class".to_string(),
                                                timeout_policy.latency_class.into(),
                                            );
                                            fields
                                        },
                                    );
                                    let mut watchdog = AttemptWatchdog::new(
                                        trace_task_id.clone(),
                                        rid.clone(),
                                        "-".to_string(),
                                        selected_model.clone(),
                                        timeout_policy,
                                    );
                                    let _ = watchdog.transition_state_with_event(
                                        AttemptLifecycleState::Starting,
                                        Some("-"),
                                    );
                                    infer_runtime.attempt_watchdogs.insert(rid.clone(), watchdog);
                                    let is_retry_attempt = infer_state.retry_state.retry_count > 0;
                                    infer_state.record_attempt(
                                        "-".to_string(),
                                        selected_model.clone(),
                                        profile.task_type,
                                        semantic_prompt.clone(),
                                        local_cluster.clone(),
                                        candidates.clone(),
                                    );
                                    let _ = record_task_attempt(
                                        &trace_task_id,
                                        "broadcast",
                                        &selected_model,
                                        is_retry_attempt,
                                        true,
                                    );
                                    emit_fallback_attempt_registered_event(
                                        &trace_task_id,
                                        &rid,
                                        &selected_model,
                                        TASK_TOPIC,
                                        &candidates,
                                    );
                                    let _ = record_distributed_task_fallback();
                                    infer_runtime.infer_broadcast_sent = true;
                                    client_state.waiting_for_response = true;
                                    infer_runtime.pending_inference.insert(rid.clone(), tokio::time::Instant::now());
                                    infer_runtime.infer_request_id = Some(rid.clone());
                                    println!(
                                        "↪️  Fallback broadcast enviado: task_id={} attempt_id={} message_id={}",
                                        trace_task_id, rid, message_id
                                    );
                                }
                                Err(error) => {
                                    let error_text = error.to_string();
                                    emit_task_publish_failed_event(
                                        PublishEventContext {
                                            trace_task_id: &trace_task_id,
                                            attempt_id: &rid,
                                            model_id: &selected_model,
                                            topic: TASK_TOPIC,
                                            publish_peer_count: topic_peer_count,
                                            payload_size,
                                            selected_peer_id: None,
                                        },
                                        &error_text,
                                    );
                                    return Err(format!(
                                        "Fallback dispatch publish failed: task_id={} attempt_id={} error={} [{}]",
                                        trace_task_id, rid, error_text, TASK_DISPATCH_UNCONFIRMED_001
                                    )
                                    .into());
                                }
                            }
                        }
                    }
                }

                // Timeout total de inferencia distribuida
                if matches!(mode, NodeMode::Infer { .. }) {
                    if let Some(rid) = infer_runtime.infer_request_id.clone() {
                        if infer_runtime.pending_inference.contains_key(&rid) {
                            let mut timeout_context: Option<TimeoutContext> = None;
                            let mut should_retry = false;
                            if let Some(watchdog) = infer_runtime.attempt_watchdogs.get_mut(&rid) {
                                match watchdog.check() {
                                    WatchdogCheck::Healthy => {}
                                    WatchdogCheck::Extended => {
                                        emit_attempt_timeout_extended_event(
                                            &watchdog.task_id,
                                            &watchdog.attempt_id,
                                            Some(&watchdog.model_id),
                                            Some(&watchdog.worker_peer_id),
                                            watchdog.deadline_at.duration_since(watchdog.started_at).as_millis() as u64,
                                            "real_progress",
                                        );
                                    }
                                    WatchdogCheck::Stalled | WatchdogCheck::TimedOut => {
                                        let worker_peer_for_event =
                                            watchdog.worker_peer_id.clone();
                                        let _ = watchdog.transition_state_with_event(
                                            AttemptLifecycleState::Stalled,
                                            Some(&worker_peer_for_event),
                                        );
                                        emit_attempt_stalled_event(
                                            &watchdog.task_id,
                                            &watchdog.attempt_id,
                                            &watchdog.worker_peer_id,
                                            Some(&watchdog.model_id),
                                            &watchdog.last_progress_stage,
                                            watchdog.elapsed_since_last_progress_ms(),
                                        );
                                        let _ = watchdog.transition_state_with_event(
                                            AttemptLifecycleState::TimedOut,
                                            Some(&worker_peer_for_event),
                                        );
                                        timeout_context = Some(TimeoutContext {
                                            trace_task_id: watchdog.task_id.clone(),
                                            attempt_id: watchdog.attempt_id.clone(),
                                            worker_peer_id: watchdog.worker_peer_id.clone(),
                                            model_id: watchdog.model_id.clone(),
                                            elapsed_ms: watchdog.elapsed_ms(),
                                            elapsed_since_progress_ms: watchdog
                                                .elapsed_since_last_progress_ms(),
                                            adaptive_timeout_ms: watchdog.policy.timeout_ms,
                                            max_wait_ms: watchdog.policy.max_wait_ms,
                                        });
                                        should_retry = true;
                                    }
                                }
                            } else if let Some(t0) = infer_runtime.pending_inference.get(&rid) {
                                if t0.elapsed().as_millis() as u64 >= INFER_TIMEOUT_MS {
                                    timeout_context = Some(TimeoutContext {
                                        trace_task_id: infer_runtime
                                            .distributed_infer_state
                                            .as_ref()
                                            .map(|state| state.trace_task_id.clone())
                                            .unwrap_or_else(|| "-".to_string()),
                                        attempt_id: rid.clone(),
                                        worker_peer_id: infer_runtime
                                            .distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_peer.clone())
                                            .unwrap_or_else(|| "-".to_string()),
                                        model_id: infer_runtime
                                            .distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.clone())
                                            .unwrap_or_else(|| "-".to_string()),
                                        elapsed_ms: INFER_TIMEOUT_MS,
                                        elapsed_since_progress_ms: INFER_TIMEOUT_MS,
                                        adaptive_timeout_ms: INFER_TIMEOUT_MS,
                                        max_wait_ms: INFER_TIMEOUT_MS,
                                    });
                                    should_retry = true;
                                }
                            }

                            if should_retry {
                                let failure_kind = FailureKind::Timeout;
                                if let Some(timeout_context) = timeout_context {
                                    let TimeoutContext {
                                        trace_task_id,
                                        attempt_id,
                                        worker_peer_id,
                                        model_id,
                                        elapsed_ms,
                                        elapsed_since_progress_ms,
                                        adaptive_timeout_ms,
                                        max_wait_ms,
                                    } = timeout_context;
                                    eprintln!(
                                        "\n[Fault] task_id={} attempt_id={} kind={:?} timeout_ms={}",
                                        trace_task_id, attempt_id, failure_kind, adaptive_timeout_ms
                                    );
                                    if let Some(infer_state) = infer_runtime.distributed_infer_state.as_mut() {
                                        if worker_peer_id != "-" {
                                            if let Some(health) =
                                                registry.write().await.record_timeout(&worker_peer_id)
                                            {
                                                log_health_update(
                                                    &trace_task_id,
                                                    &worker_peer_id,
                                                    infer_state.current_model.as_deref(),
                                                    &health,
                                                    Some(NODE_UNHEALTHY_002),
                                                );
                                            }
                                        }
                                        log_observability_event(
                                            LogLevel::Error,
                                            "task_timeout",
                                            &trace_task_id,
                                            Some(&trace_task_id),
                                            Some(&model_id),
                                            Some(TASK_TIMEOUT_001),
                                            {
                                                let mut fields = Map::new();
                                                fields.insert("attempt_id".to_string(), attempt_id.clone().into());
                                                fields.insert("latency_ms".to_string(), elapsed_ms.into());
                                                fields.insert(
                                                    "elapsed_since_last_progress_ms".to_string(),
                                                    elapsed_since_progress_ms.into(),
                                                );
                                                fields.insert(
                                                    "adaptive_timeout_ms".to_string(),
                                                    adaptive_timeout_ms.into(),
                                                );
                                                fields.insert("max_wait_ms".to_string(), max_wait_ms.into());
                                                fields.insert("retry".to_string(), true.into());
                                                fields.insert("error_kind".to_string(), "timeout".into());
                                                fields.insert("recoverable".to_string(), true.into());
                                                fields.insert("watchdog_reason".to_string(), "no_progress".into());
                                                if worker_peer_id != "-" {
                                                    fields.insert("peer_id".to_string(), worker_peer_id.clone().into());
                                                }
                                                fields
                                            },
                                        );
                                        let failed_peer_opt = if worker_peer_id == "-" {
                                            infer_state.current_peer.as_deref()
                                        } else {
                                            Some(worker_peer_id.as_str())
                                        };
                                        let failed_model_opt = if model_id == "-" {
                                            infer_state.current_model.as_deref()
                                        } else {
                                            Some(model_id.as_str())
                                        };
                                        let mut preview_retry = infer_state.retry_state.clone();
                                        preview_retry.record_failure(
                                            failed_peer_opt,
                                            failed_model_opt,
                                        );
                                        let reg = registry.read().await;
                                        let retry_target = select_retry_target(
                                            &reg,
                                            &IntelligentScheduler::new(),
                                            &infer_state.candidate_models,
                                            infer_state.local_cluster_id.as_deref(),
                                            &preview_retry,
                                        );
                                        drop(reg);
                                        task_manager
                                            .fail(&trace_task_id, "distributed timeout")
                                            .await;
                                        let failed_peer = infer_state.current_peer.clone();
                                        let failed_model = infer_state.current_model.clone();
                                        let retried = infer_state.schedule_retry(
                                            failed_peer.as_deref(),
                                            failed_model.as_deref(),
                                        );
                                        infer_runtime.pending_inference.remove(&rid);
                                        clear_stream_state(
                                            &mut infer_runtime.token_buffer,
                                            &mut infer_runtime.next_token_idx,
                                            &mut infer_runtime.rendered_output,
                                        );
                                        if retried {
                                            emit_retry_scheduled_event(
                                                &trace_task_id,
                                                &infer_state.current_request_id,
                                                failed_model.as_deref(),
                                                failed_peer.as_deref(),
                                                retry_target
                                                    .as_ref()
                                                    .map(|target| target.peer_id.as_str()),
                                                "timeout",
                                            );
                                            if let Some(target) = retry_target {
                                                println!(
                                                    "[Retry] task_id={} attempt_id={} kind={:?} peer_id={} model_id={}",
                                                    trace_task_id,
                                                    infer_state.current_request_id,
                                                    failure_kind,
                                                    target.peer_id,
                                                    target.model_id
                                                );
                                            } else {
                                                println!(
                                                    "[Retry] task_id={} attempt_id={} retrying on new node",
                                                    trace_task_id, infer_state.current_request_id
                                                );
                                            }
                                            infer_runtime.infer_request_id =
                                                Some(infer_state.current_request_id.clone());
                                            infer_runtime.infer_broadcast_sent = false;
                                            client_state.waiting_for_response = false;
                                            continue;
                                        }

                                        let (trace, aggregate_metrics) =
                                            finalize_distributed_task_observability(
                                                &trace_task_id,
                                                infer_runtime.infer_started_at,
                                                true,
                                            );
                                        if let Some(trace) = trace {
                                            println!(
                                                "[Metrics] latency={} retries={} fallbacks={}",
                                                trace.total_latency_ms, trace.retries, trace.fallbacks
                                            );
                                        }
                                        println!(
                                            "[Metrics] totals: tasks={} failed={} avg_latency_ms={:.1}",
                                            aggregate_metrics.total_tasks,
                                            aggregate_metrics.failed_tasks,
                                            aggregate_metrics.avg_latency_ms
                                        );
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            event = swarm.select_next_some() => match event {

                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("🌐 Escuchando en: {}", address);
                    swarm.behaviour_mut().kademlia.add_address(&peer_id, address);
                }

                SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Discovered(peers))) => {
                    handle_mdns_discovered(
                        &mut swarm,
                        &mut client_state,
                        is_client,
                        debug_flags,
                        peer_id,
                        peers,
                    );
                }

                SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Expired(peers))) => {
                    handle_mdns_expired(&mut swarm, &mut client_state, &mut pubsub_topics, peers);
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source,
                    message,
                    ..
                })) => {
                    match handle_gossipsub_message(GossipsubHandlerContext {
                        swarm: &mut swarm,
                        mode: &mode,
                        propagation_source,
                        message,
                        registry: &registry,
                        task_cache: &mut task_cache,
                        capabilities: &capabilities,
                        pool: &pool,
                        queue: &queue,
                        peer_id,
                        scheduler: &scheduler,
                        client_state: &mut client_state,
                        task_topic: &task_topic,
                        peer_tracker: &mut peer_tracker,
                        metrics: &metrics,
                        infer_runtime: &mut infer_runtime,
                        model_storage: &model_storage,
                        node_caps: &node_caps,
                        inference_engine: &inference_engine,
                        task_manager: &task_manager,
                    })
                    .await
                    {
                        EventLoopDirective::None => {}
                        EventLoopDirective::Continue => continue,
                        EventLoopDirective::Break => break,
                    }
                }

                // ← Direct result routing — origin recibe resultado
                SwarmEvent::Behaviour(IaMineEvent::ResultResponse(RREvent::Message {
                    peer,
                    message: Message::Request { request, channel, .. },
                })) => {
                    handle_result_response_request(&mut swarm, &peer, request, channel);
                }

                SwarmEvent::Behaviour(IaMineEvent::ResultResponse(RREvent::Message {
                    peer,
                    message: Message::Response { response, .. },
                })) => {
                    handle_result_response_ack(&peer, response);
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id: pid, topic })) => {
                    handle_pubsub_subscribed(&mut pubsub_topics, pid, topic);
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id: pid, topic })) => {
                    handle_pubsub_unsubscribed(&mut pubsub_topics, pid, topic);
                }

                SwarmEvent::ConnectionEstablished { peer_id: pid, endpoint, .. } => {
                    handle_connection_established(
                        &mut swarm,
                        &mut client_state,
                        is_client,
                        &mode,
                        pid,
                        endpoint.get_remote_address().clone(),
                    );
                }

                SwarmEvent::ConnectionClosed { peer_id: pid, .. } => {
                    if handle_connection_closed(
                        &mut client_state,
                        &mut pubsub_topics,
                        is_client,
                        &mode,
                        pid,
                    ) {
                        break;
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
                    handle_ping_event(&mut peer_tracker, &topology, peer, result).await;
                }

                SwarmEvent::Behaviour(IaMineEvent::Kademlia(kad::Event::RoutingUpdated { peer, .. })) => {
                    handle_kademlia_routing_updated(debug_flags, peer);
                }

                SwarmEvent::Behaviour(IaMineEvent::RequestResponse(event)) => {
                    match handle_request_response_event(RequestResponseHandlerContext {
                        swarm: &mut swarm,
                        event,
                        peer_id,
                        debug_flags,
                        is_client,
                        topology: &topology,
                        queue: &queue,
                        task_manager: &task_manager,
                        task_response_tx: &task_response_tx,
                        registry: &registry,
                        model_storage: &model_storage,
                        infer_runtime: &mut infer_runtime,
                        client_state: &mut client_state,
                    })
                    .await
                    {
                        EventLoopDirective::None => {}
                        EventLoopDirective::Continue => continue,
                        EventLoopDirective::Break => break,
                    }
                }

                _ => {}
            }
        }
    }

    #[allow(unreachable_code)]
    Ok(())
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{}{}", t.as_secs(), t.subsec_nanos())
}

async fn simulate_workers(count: usize, _base_peer_id: String) {
    println!("🔥 Iniciando simulación de {} workers...", count);
    let mut handles = vec![];
    for i in 0..count {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
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
    tokio::time::sleep(Duration::from_secs(60)).await;
}
