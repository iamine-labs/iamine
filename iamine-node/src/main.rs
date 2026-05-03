mod attempt_watchdog;
mod backend_runtime;
mod benchmark;
mod capability_advertising;
mod cli;
mod cli_mode_handlers;
mod code_quality;
mod daemon_runtime;
mod dispatch_runtime;
mod distributed_infer_runtime;
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
mod nodes_mode_runtime;
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
mod runtime_ids;
mod runtime_state;
mod security_checks;
mod setup_wizard;
mod startup_bootstrap;
mod startup_math;
mod startup_model_validation;
mod swarm_event_runtime;
mod task_cache;
mod task_codec;
mod task_protocol;
mod task_queue;
mod task_response_runtime;
mod task_scheduler;
#[cfg(test)]
mod tests;
mod wallet;
mod worker_capabilities;
mod worker_heartbeat_runtime;
mod worker_pool;
mod worker_simulation;

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
use distributed_infer_runtime::{handle_distributed_infer_tick, DistributedInferTickContext};
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
use nodes_mode_runtime::handle_nodes_or_topology_tick;
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
use runtime_ids::uuid_simple;
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
use swarm_event_runtime::{handle_swarm_event, SwarmEventRuntimeContext};
use task_cache::TaskCache;
use task_queue::TaskQueue;
use task_response_runtime::handle_completed_task_response;
use task_scheduler::TaskScheduler;
use tokio::sync::RwLock;
use wallet::Wallet;
use worker_capabilities::WorkerCapabilities;
use worker_heartbeat_runtime::{handle_worker_heartbeat_tick, WorkerHeartbeatContext};
use worker_pool::WorkerPool;
use worker_simulation::simulate_workers;

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
                handle_nodes_or_topology_tick(&mode, &registry, &topology, peer_id).await;
            }

            Some(completed_response) = task_response_rx.recv() => {
                handle_completed_task_response(&mut swarm, completed_response);
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

                match handle_distributed_infer_tick(DistributedInferTickContext {
                    mode: &mode,
                    infer_runtime: &mut infer_runtime,
                    model_storage: &model_storage,
                    topology: &topology,
                    peer_id,
                    registry: &registry,
                    pubsub_topics: &pubsub_topics,
                    client_state: &mut client_state,
                    debug_flags,
                    swarm: &mut swarm,
                    metrics: &metrics,
                    task_manager: &task_manager,
                })
                .await?
                {
                    EventLoopDirective::None => {}
                    EventLoopDirective::Continue => continue,
                    EventLoopDirective::Break => break,
                }
            }

            event = swarm.select_next_some() => {
                match handle_swarm_event(SwarmEventRuntimeContext {
                    swarm: &mut swarm,
                    event,
                    mode: &mode,
                    peer_id,
                    debug_flags,
                    is_client,
                    client_state: &mut client_state,
                    pubsub_topics: &mut pubsub_topics,
                    registry: &registry,
                    task_cache: &mut task_cache,
                    capabilities: &capabilities,
                    pool: &pool,
                    queue: &queue,
                    scheduler: &scheduler,
                    peer_tracker: &mut peer_tracker,
                    metrics: &metrics,
                    infer_runtime: &mut infer_runtime,
                    model_storage: &model_storage,
                    node_caps: &node_caps,
                    inference_engine: &inference_engine,
                    task_manager: &task_manager,
                    topology: &topology,
                    task_response_tx: &task_response_tx,
                    task_topic: &task_topic,
                })
                .await
                {
                    EventLoopDirective::None => {}
                    EventLoopDirective::Continue => continue,
                    EventLoopDirective::Break => break,
                }
            }
        }
    }

    #[allow(unreachable_code)]
    Ok(())
}
