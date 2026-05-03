mod attempt_watchdog;
mod backend_runtime;
mod benchmark;
mod capability_advertising;
mod cli;
mod code_quality;
mod daemon_runtime;
mod dispatch_runtime;
mod event_loop_control;
mod executor;
mod gossipsub_handlers;
mod heartbeat;
mod metrics;
mod model_selector_cli;
mod network;
mod network_events;
mod node_identity;
mod node_observability;
mod peer_tracker;
mod prompt_runtime;
mod protocol;
mod quality_gate;
mod rate_limiter;
mod regression_runner;
mod request_response_handlers;
mod resource_policy;
mod result_lifecycle;
mod result_protocol;
mod runtime_state;
mod security_checks;
mod setup_wizard;
mod startup_math;
mod task_cache;
mod task_codec;
mod task_protocol;
mod task_queue;
mod task_scheduler;
#[cfg(test)]
mod tests;
mod wallet;
mod worker_capabilities;
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
use capability_advertising::{
    CapabilityAdvertisementValidator, CapabilityRejectionReason, CapabilityValidationResult,
};
use cli::{
    is_control_plane_mode, mode_label, parse_args, DebugFlags, InferenceControlFlags, NodeMode,
};
use code_quality::run_code_quality_checks;
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
use metrics::{start_metrics_server, NodeMetrics};
use model_selector_cli::ModelSelectorCLI;
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
use quality_gate::{current_release_version, run_release_validation};
use rate_limiter::RateLimiter;
use regression_runner::run_default_regression_suite;
use request_response_handlers::{handle_request_response_event, RequestResponseHandlerContext};
use resource_policy::ResourcePolicy;
use result_lifecycle::{
    apply_late_result_lifecycle, apply_result_failure_lifecycle, apply_result_success_lifecycle,
    clear_stream_state, LateResultLifecycleInput, ResultFailureInput,
    ResultFailureLifecycleContext,
};
use result_protocol::{TaskResultRequest, TaskResultResponse};
use runtime_state::{ClientRuntimeState, DistributedInferState, InferRuntimeState};
use security_checks::run_security_checks;
use serde_json::{Map, Value};
use setup_wizard::{DetectedHardware, NodeSetupConfig};
#[cfg(test)]
use startup_math::{checked_sub_usize, StartupMathError};
use startup_math::{
    compute_active_tasks, compute_metrics_port, emit_worker_startup_overflow_event,
    resource_policy_value, WorkerStartupOverflowContext,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use task_cache::TaskCache;
use task_queue::TaskQueue;
use task_scheduler::TaskScheduler;
use tokio::sync::RwLock;
use wallet::Wallet;
use worker_capabilities::WorkerCapabilities;
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

#[derive(Clone)]
enum InferenceRuntime {
    Engine(Arc<RealInferenceEngine>),
    Daemon(PathBuf),
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

async fn run_local_inference_with_validation(
    runtime: InferenceRuntime,
    task_id: String,
    model_id: String,
    prompt: String,
    task_type: PromptTaskType,
    max_tokens: u32,
    temperature: f32,
) -> Result<RealInferenceResult, String> {
    let started_at = std::time::Instant::now();
    let mut last_result: Option<RealInferenceResult> = None;
    let mut final_success = false;
    let mut final_semantic_success = false;
    let mut retry_count = 0u32;

    for attempt in 0..=1 {
        retry_count = attempt as u32;
        let guarded_prompt = with_task_guard(&prompt, task_type, attempt == 1);
        let attempt_temperature = if task_requires_validation(task_type) {
            temperature.min(0.2)
        } else {
            temperature
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
        if attempt > 0 {
            print!("\n[Validator] Retrying once with stronger formatting guard...\n");
        }

        let req = RealInferenceRequest {
            task_id: task_id.clone(),
            model_id: model_id.clone(),
            prompt: guarded_prompt,
            max_tokens,
            temperature: attempt_temperature,
        };

        let result: Result<RealInferenceResult, String> = match &runtime {
            InferenceRuntime::Engine(engine) => {
                let engine_clone = Arc::clone(engine);
                let inference_handle =
                    tokio::spawn(async move { engine_clone.run_inference(req, Some(tx)).await });

                while let Some(token) = rx.recv().await {
                    print!("{}", token);
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                }

                Ok(inference_handle
                    .await
                    .map_err(|e| format!("Inference task join error: {}", e))?)
            }
            InferenceRuntime::Daemon(socket_path) => {
                drop(rx);
                let daemon_response = infer_via_daemon(socket_path, req, |token| {
                    print!("{}", token);
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                })
                .await?;
                println!(
                    "\n[Daemon] requests_handled={} model_load_ms={} actual_loads={} reuse_hits={}",
                    daemon_response.requests_handled,
                    daemon_response.model_load_ms,
                    daemon_response.actual_model_loads,
                    daemon_response.reuse_hits
                );
                Ok(daemon_response.result)
            }
        };

        let mut result = match result {
            Ok(result) => result,
            Err(error) => {
                record_model_metrics(
                    &model_id,
                    ModelMetrics::new(
                        false,
                        started_at.elapsed().as_millis() as u64,
                        false,
                        retry_count,
                    ),
                );
                return Err(error);
            }
        };
        let task_label = prompt_task_label(task_type);
        let exact_subtype = if matches!(task_type, PromptTaskType::ExactMath) {
            Some(detect_exact_subtype(&prompt, &result.output))
        } else {
            None
        };
        let exact_subtype_label = exact_subtype.map(exact_subtype_label);
        if matches!(task_type, PromptTaskType::ExactMath) {
            let (normalized_output, normalization_reason) =
                normalize_output(task_label, exact_subtype_label, &result.output);
            if let Some(reason) = normalization_reason {
                println!("\n[Normalizer] Applied: {}", reason);
                println!("[Normalizer] Output corrected");
                result.output = normalized_output;
            }
        }

        let valid = if task_requires_validation(task_type) {
            iamine_models::output_validator::validate_structure(task_label, &result.output)
                && iamine_models::output_validator::validate_exactness(
                    task_label,
                    exact_subtype_label,
                    &result.output,
                )
        } else {
            true
        };

        if task_requires_validation(task_type) {
            println!("\n[Validator] Output valid: {}", valid);
        }
        if task_requires_validation(task_type) && !result.output.trim().is_empty() {
            println!("[Validator] Final output:\n{}", result.output);
        }

        final_semantic_success = if task_requires_validation(task_type) {
            valid
        } else {
            true
        };
        final_success = result.success
            && final_semantic_success
            && result.error.is_none()
            && !result.output.trim().is_empty();
        let should_retry = task_requires_validation(task_type) && !valid && attempt == 0;
        last_result = Some(result);
        if !should_retry {
            break;
        }
    }

    if let Some(result) = last_result {
        record_model_metrics(
            &model_id,
            ModelMetrics::new(
                final_success,
                started_at.elapsed().as_millis() as u64,
                final_semantic_success,
                retry_count,
            ),
        );
        Ok(result)
    } else {
        record_model_metrics(
            &model_id,
            ModelMetrics::new(
                false,
                started_at.elapsed().as_millis() as u64,
                false,
                retry_count,
            ),
        );
        Err("Inference returned no result".to_string())
    }
}

#[derive(Debug, Clone, Default)]
struct PubsubTopicTracker {
    joined_topic_hashes: HashSet<String>,
    topic_peers: HashMap<String, HashSet<String>>,
}

impl PubsubTopicTracker {
    fn register_local_subscription(&mut self, topic_name: &str) {
        self.joined_topic_hashes
            .insert(topic_hash_string(topic_name));
    }

    fn register_peer_subscription(&mut self, peer_id: &PeerId, topic: &gossipsub::TopicHash) {
        self.topic_peers
            .entry(topic.to_string())
            .or_default()
            .insert(peer_id.to_string());
    }

    fn unregister_peer_subscription(&mut self, peer_id: &PeerId, topic: &gossipsub::TopicHash) {
        if let Some(peers) = self.topic_peers.get_mut(&topic.to_string()) {
            peers.remove(&peer_id.to_string());
            if peers.is_empty() {
                self.topic_peers.remove(&topic.to_string());
            }
        }
    }

    fn unregister_peer(&mut self, peer_id: &PeerId) {
        let peer = peer_id.to_string();
        self.topic_peers.retain(|_, peers| {
            peers.remove(&peer);
            !peers.is_empty()
        });
    }

    fn topic_peer_count(&self, topic_name: &str) -> usize {
        self.topic_peers
            .get(&topic_hash_string(topic_name))
            .map(|peers| peers.len())
            .unwrap_or(0)
    }

    fn mesh_peer_count(&self) -> usize {
        self.topic_peers
            .values()
            .flat_map(|peers| peers.iter().cloned())
            .collect::<HashSet<_>>()
            .len()
    }

    fn joined(&self, topic_name: &str) -> bool {
        self.joined_topic_hashes
            .contains(&topic_hash_string(topic_name))
    }
}

fn local_inference_timeout_ms(model_id: &str) -> u64 {
    AttemptTimeoutPolicy::from_model_and_node(model_id, None).timeout_ms
}

async fn run_local_inference_with_timeout(
    runtime: InferenceRuntime,
    task_id: String,
    model_id: String,
    prompt: String,
    task_type: PromptTaskType,
    max_tokens: u32,
    temperature: f32,
) -> Result<RealInferenceResult, String> {
    let timeout_ms = local_inference_timeout_ms(&model_id);
    match tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        run_local_inference_with_validation(
            runtime,
            task_id,
            model_id,
            prompt,
            task_type,
            max_tokens,
            temperature,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(format!("Inference exceeded timeout of {} ms", timeout_ms)),
    }
}

fn validate_models_for_advertising(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
) -> Vec<String> {
    let validator = CapabilityAdvertisementValidator::new(registry, storage, node_caps);
    validator
        .validate_local_models()
        .into_iter()
        .filter_map(|result| {
            if result.accepted {
                log_observability_event(
                    LogLevel::Info,
                    "model_validated",
                    "startup",
                    None,
                    Some(&result.model_id),
                    None,
                    {
                        let mut fields = Map::new();
                        fields.insert("validation_mode".to_string(), "metadata_only".into());
                        fields
                    },
                );
                return Some(result.model_id);
            }

            let CapabilityValidationResult {
                model_id,
                accepted: _,
                rejection,
            } = result;
            match rejection {
                Some(CapabilityRejectionReason::HardwareRequirements) => {
                    println!(
                        "[Health] Skipping advertisement for {}: hardware requirements not satisfied",
                        model_id
                    );
                    log_observability_event(
                        LogLevel::Warn,
                        "model_advertisement_rejected",
                        "startup",
                        None,
                        Some(&model_id),
                        Some(MODEL_UNSUPPORTED_HW_002),
                        {
                            let mut fields = Map::new();
                            fields.insert("reason".to_string(), "hardware_requirements".into());
                            fields
                        },
                    );
                }
                Some(reason) => {
                    println!(
                        "[Health] Skipping advertisement for {}: {}",
                        model_id,
                        reason.as_str()
                    );
                    log_observability_event(
                        LogLevel::Error,
                        "model_advertisement_rejected",
                        "startup",
                        None,
                        Some(&model_id),
                        Some(MODEL_LOAD_FAILED_001),
                        {
                            let mut fields = Map::new();
                            fields.insert("reason".to_string(), reason.as_str().into());
                            fields
                        },
                    );
                }
                None => {}
            }
            None
        })
        .collect()
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
    // CLI MODELS + CAPABILITIES — salida temprana
    // ═══════════════════════════════════════════════
    match &mode {
        NodeMode::ModelsList => {
            println!("╔══════════════════════════════════╗");
            println!("║       IaMine — Modelos           ║");
            println!("╚══════════════════════════════════╝\n");
            let installer = ModelInstaller::new();
            let models = installer.list_models();
            println!("📋 Modelos disponibles:\n");
            for m in &models {
                m.display();
            }
            let used = ModelStorage::new().total_size_bytes();
            let cfg = StorageConfig::load();
            println!(
                "\n💾 Storage: {:.1}/{} GB usados",
                used as f64 / 1_073_741_824.0,
                cfg.max_storage_gb
            );
            return Ok(());
        }

        NodeMode::ModelsStats => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Model Karma          ║");
            println!("╚══════════════════════════════════╝\n");
            print_model_karma_stats();
            return Ok(());
        }

        NodeMode::TasksStats => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Task Observability    ║");
            println!("╚══════════════════════════════════╝\n");
            let metrics = distributed_task_metrics();
            print_distributed_task_stats(&metrics);
            return Ok(());
        }

        NodeMode::TasksTrace { task_id } => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Task Trace           ║");
            println!("╚══════════════════════════════════╝\n");
            if let Some(trace) = task_trace(task_id) {
                print_task_trace_entry(&trace);
                return Ok(());
            }
            return Err(format!("No existe trace para task_id={}", task_id).into());
        }

        NodeMode::ModelsMenu => {
            let node_identity = NodeIdentity::load_or_create();
            ModelSelectorCLI::show_model_menu(&node_identity.peer_id.to_string())?;
            return Ok(());
        }

        NodeMode::ModelsSearch { query } => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Buscar en HF          ║");
            println!("╚══════════════════════════════════╝\n");

            println!("🔍 Buscando '{}' en HuggingFace...", query);

            match iamine_models::HuggingFaceSearch::search_gguf_models(query, 10).await {
                Ok(models) => {
                    let filtered = iamine_models::HuggingFaceSearch::filter_suitable(models);

                    if filtered.is_empty() {
                        println!("❌ Sin modelos encontrados para '{}'", query);
                    } else {
                        println!("\n📦 {} modelos encontrados:\n", filtered.len());
                        println!("{:<50} {:>12} {:>8}", "Modelo", "Downloads", "Likes");
                        println!("{}", "─".repeat(72));

                        for m in &filtered {
                            let name = if m.id.len() > 48 {
                                format!("{}…", &m.id[..47])
                            } else {
                                m.id.clone()
                            };
                            println!("{:<50} {:>12} {:>8}", name, m.downloads, m.likes);
                        }

                        println!("\n💡 Descarga:");
                        println!("   iamine-node models download <model_id>");
                    }
                }
                Err(e) => println!("❌ Error: {}", e),
            }
            return Ok(());
        }

        NodeMode::ModelsDownload { model_id } => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Descargar Modelo     ║");
            println!("╚══════════════════════════════════╝\n");
            let installer = ModelInstaller::new();
            let (tx, _rx) = tokio::sync::mpsc::channel(10);
            match installer.install(model_id, "local", Some(tx)).await {
                InstallResult::Installed(id) => {
                    println!("✅ Modelo {} instalado en ~/.iamine/models/", id)
                }
                InstallResult::AlreadyExists(id) => println!("ℹ️  Modelo {} ya está instalado", id),
                InstallResult::InsufficientStorage {
                    needed_gb,
                    available_gb,
                } => println!(
                    "❌ Espacio insuficiente: necesita {:.1} GB, disponible {:.1} GB",
                    needed_gb, available_gb
                ),
                InstallResult::DownloadFailed(e) => println!("❌ Descarga fallida: {}", e),
                InstallResult::ValidationFailed(e) => println!("❌ Validación fallida: {}", e),
            }
            return Ok(());
        }

        NodeMode::ModelsRemove { model_id } => {
            println!("╔══════════════════════════════════╗");
            println!("║     IaMine — Remove Model        ║");
            println!("╚══════════════════════════════════╝\n");
            let installer = ModelInstaller::new();
            installer.remove(model_id)?;
            let remaining = ModelStorage::new().list_local_models();
            println!("📦 Modelos locales restantes: {}", remaining.join(", "));
            return Ok(());
        }

        NodeMode::SemanticEval => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Semantic Eval         ║");
            println!("╚══════════════════════════════════╝\n");

            let report = evaluate_default_dataset()
                .map_err(|e| format!("No se pudo cargar semantic_dataset.json: {}", e))?;

            println!("📊 Accuracy: {:.1}%", report.accuracy * 100.0);
            println!("📉 Fallback rate: {:.1}%", report.fallback_rate * 100.0);
            println!(
                "⚠️  Low-confidence rate: {:.1}%",
                report.low_confidence_rate * 100.0
            );
            println!("🧪 Total prompts: {}", report.total);
            println!("❌ Error cases: {}", report.error_cases.len());

            if !report.error_cases.is_empty() {
                println!("\nError breakdown:");
                for error in &report.error_cases {
                    println!(
                        "- prompt='{}' expected={} predicted={} secondary(expected={:?}, predicted={:?}) style(expected={:?}, predicted={:?}) context(expected={:?}, predicted={}) normalize(expected={}, predicted={}) confidence={:.2} fallback={}",
                        error.prompt,
                        prompt_task_label(error.expected_task),
                        prompt_task_label(error.predicted_task),
                        error.expected_secondary,
                        error.predicted_secondary,
                        error.expected_output_style,
                        error.predicted_output_style,
                        error.expected_requires_context,
                        error.predicted_requires_context,
                        error.expected_normalize,
                        error.predicted_normalize,
                        error.confidence,
                        error.fallback_applied
                    );
                }
            }
            return Ok(());
        }

        NodeMode::RegressionRun => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Regression Runner     ║");
            println!("╚══════════════════════════════════╝\n");

            let report = run_default_regression_suite()?;
            println!("🧪 Versions covered: {}", report.total_versions);
            println!("🧪 Total prompts: {}", report.total_prompts);
            println!("❌ Total failures: {}", report.total_failures);

            for version in &report.version_results {
                println!(
                    "- {}: {}/{} passed",
                    version.version, version.passed, version.total
                );
                for failure in &version.failures {
                    println!(
                        "  prompt='{}' expected={} predicted={} normalize(expected={}, predicted={})",
                        failure.prompt,
                        prompt_task_label(failure.expected_task),
                        prompt_task_label(failure.predicted_task),
                        failure.expected_normalize,
                        failure.predicted_normalize
                    );
                }
            }

            if !report.passed() {
                return Err("Regression suite detecto fallos".into());
            }

            return Ok(());
        }

        NodeMode::CheckCode => {
            println!("╔══════════════════════════════════╗");
            println!("║     IaMine — Code Quality        ║");
            println!("╚══════════════════════════════════╝\n");

            let report = run_code_quality_checks()?;
            println!("✅ fmt passed: {}", report.format_passed);
            println!("✅ clippy passed: {}", report.clippy_passed);
            println!("⚠️  warnings: {}", report.warning_count);
            println!("🚦 minor gate pass: {}", report.passed_for_minor());

            for command in &report.commands {
                println!(
                    "- {} => success={} warnings={}",
                    command.name, command.success, command.warning_count
                );
                if !command.output_excerpt.is_empty() {
                    println!("{}", command.output_excerpt);
                }
            }

            if !report.passed_for_minor() {
                return Err("Code quality checks fallaron".into());
            }

            return Ok(());
        }

        NodeMode::CheckSecurity => {
            println!("╔══════════════════════════════════╗");
            println!("║     IaMine — Security Check      ║");
            println!("╚══════════════════════════════════╝\n");

            let report = run_security_checks()?;
            println!("✅ security passed: {}", report.passed);
            println!("⚠️  warnings: {}", report.warning_count);
            println!("❌ errors: {}", report.error_count);

            for finding in &report.findings {
                println!(
                    "- {:?} {}:{} {}",
                    finding.severity, finding.path, finding.line, finding.message
                );
            }

            if !report.passed {
                return Err("Security checks detectaron hallazgos bloqueantes".into());
            }

            return Ok(());
        }

        NodeMode::ValidateRelease => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Release Validation    ║");
            println!("╚══════════════════════════════════╝\n");

            let report = run_release_validation()?;
            println!("🏷️  Version: {}", current_release_version());
            println!("✅ tests_passed: {}", report.quality.tests_passed);
            println!("✅ regression_passed: {}", report.quality.regression_passed);
            println!("✅ security_passed: {}", report.quality.security_passed);
            println!("✅ code_clean_passed: {}", report.quality.code_clean_passed);
            println!("🚦 release_blocked: {}", report.quality.blocks_release());
            println!("⚠️  code warnings: {}", report.code_quality.warning_count);
            println!("⚠️  security warnings: {}", report.security.warning_count);

            if let Some(semantic_eval) = &report.semantic_eval {
                println!(
                    "🧠 semantic accuracy: {:.1}% (errors={})",
                    semantic_eval.accuracy * 100.0,
                    semantic_eval.error_cases.len()
                );
            }

            if report.quality.blocks_release() {
                return Err("Release bloqueada por quality gates".into());
            }

            return Ok(());
        }

        NodeMode::Daemon => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Inference Daemon      ║");
            println!("╚══════════════════════════════════╝\n");
            let node_identity = NodeIdentity::load_or_create();
            set_global_node_id(&node_identity.peer_id.to_string());
            let socket = daemon_socket_path();
            log_observability_event(
                LogLevel::Info,
                "daemon_started",
                "startup",
                None,
                None,
                None,
                {
                    let mut fields = Map::new();
                    fields.insert(
                        "peer_id".to_string(),
                        node_identity.peer_id.to_string().into(),
                    );
                    fields.insert(
                        "socket_path".to_string(),
                        socket.display().to_string().into(),
                    );
                    fields.insert("mode".to_string(), "daemon".into());
                    fields
                },
            );
            run_daemon(socket).await?;
            return Ok(());
        }

        NodeMode::TestInference { prompt } => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Test Inference       ║");
            println!("╚══════════════════════════════════╝\n");

            let _hw = HardwareAcceleration::detect();
            let registry = ModelRegistry::new();
            let storage = ModelStorage::new();

            // Elegir modelo disponible
            let model_id = ["tinyllama-1b", "llama3-3b", "mistral-7b"]
                .iter()
                .find(|&&id| storage.has_model(id))
                .copied()
                .unwrap_or("tinyllama-1b");

            println!("🤖 Modelo: {}", model_id);
            println!("💬 Prompt: {}\n", prompt);

            if !storage.has_model(model_id) {
                println!("❌ Modelo no instalado. Ejecuta primero:");
                println!("   iamine-node models download {}", model_id);
                return Ok(());
            }

            let model_desc = registry.get(model_id).unwrap();

            let resolution = resolve_policy_for_prompt(prompt, Some(model_id), &[]);
            let prompt_profile = resolution.profile.clone();
            let semantic_prompt = resolution.semantic_prompt.clone();
            let output_policy = resolve_output_policy(&prompt_profile, &semantic_prompt, None);
            println!(
                "[OutputPolicy] max_tokens: {} (reason: {})",
                output_policy.max_tokens, output_policy.reason
            );

            let req = RealInferenceRequest {
                task_id: "test-001".to_string(),
                model_id: model_id.to_string(),
                prompt: with_task_guard(prompt, prompt_profile.task_type, false),
                max_tokens: output_policy.max_tokens as u32,
                temperature: 0.7,
            };

            let runtime = if let Some(runtime) = choose_inference_runtime().await {
                runtime
            } else {
                let engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));
                engine.load_model(model_id, &model_desc.hash)?;
                InferenceRuntime::Engine(engine)
            };
            let result = run_local_inference_with_validation(
                runtime,
                req.task_id,
                req.model_id,
                semantic_prompt,
                prompt_profile.task_type,
                req.max_tokens,
                req.temperature,
            )
            .await?;
            record_semantic_feedback(prompt, &resolution.validation);
            println!("\n\n✅ Inference completada");
            println!("[Inference] tokens_generated: {}", result.tokens_generated);
            println!("[Inference] truncated: {}", result.truncated);
            println!(
                "[Inference] continuation_steps: {}",
                result.continuation_steps
            );
            if result.truncated {
                println!("[Warning] Output truncated at token budget");
            }
            return Ok(());
        }

        NodeMode::Infer {
            prompt,
            model_id,
            max_tokens_override,
            force_network,
            no_local,
            prefer_local,
        } => {
            println!("╔══════════════════════════════════╗");
            println!("║      IaMine — Inference          ║");
            println!("╚══════════════════════════════════╝\n");

            let registry = ModelRegistry::new();
            let storage = ModelStorage::new();
            let local_models = storage.list_local_models();
            let resolution = resolve_policy_for_prompt(prompt, model_id.as_deref(), &local_models);
            let profile = resolution.profile.clone();
            let candidate_models = resolution.candidate_models.clone();
            let selected_model = resolution.selected_model.clone();
            let semantic_prompt = resolution.semantic_prompt.clone();
            let output_policy =
                resolve_output_policy(&profile, &semantic_prompt, *max_tokens_override);
            let control_flags = InferenceControlFlags {
                force_network: *force_network,
                no_local: *no_local,
                prefer_local: *prefer_local,
            };
            println!(
                "[OutputPolicy] max_tokens: {} (reason: {})",
                output_policy.max_tokens, output_policy.reason
            );
            if control_flags.force_network || control_flags.no_local || control_flags.prefer_local {
                println!(
                    "[InferenceControl] force_network={} no_local={} prefer_local={}",
                    control_flags.force_network, control_flags.no_local, control_flags.prefer_local
                );
            }

            if control_flags.should_use_local(storage.has_model(&selected_model)) {
                println!("🤖 Modelo: {}", selected_model);
                println!("💬 Prompt: {}\n", prompt);

                let model_desc = registry.get(&selected_model).ok_or_else(|| {
                    format!("Modelo {} no encontrado en registry", selected_model)
                })?;
                let runtime = if let Some(runtime) = choose_inference_runtime().await {
                    runtime
                } else {
                    let engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));
                    engine.load_model(&selected_model, &model_desc.hash)?;
                    InferenceRuntime::Engine(engine)
                };
                let result = run_local_inference_with_validation(
                    runtime,
                    "infer-local-001".to_string(),
                    selected_model.clone(),
                    semantic_prompt,
                    profile.task_type,
                    output_policy.max_tokens as u32,
                    0.7,
                )
                .await?;
                record_semantic_feedback(prompt, &resolution.validation);
                println!("\n\n✅ Inference completada");
                println!("[Inference] tokens_generated: {}", result.tokens_generated);
                println!("[Inference] truncated: {}", result.truncated);
                println!(
                    "[Inference] continuation_steps: {}",
                    result.continuation_steps
                );
                if result.truncated {
                    println!("[Warning] Output truncated at token budget");
                }
                return Ok(());
            } else if control_flags.force_network {
                println!(
                    "🌐 Flag --force-network activo; omitiendo inferencia local para {}.",
                    selected_model
                );
                println!("   Candidates: {}", candidate_models.join(", "));
                println!("   Intentando ruta distribuida...\n");
            } else if control_flags.no_local {
                println!(
                    "🚫 Flag --no-local activo; omitiendo inferencia local para {}.",
                    selected_model
                );
                println!("   Candidates: {}", candidate_models.join(", "));
                println!("   Intentando ruta distribuida...\n");
            } else if model_id.is_some() {
                println!(
                    "⚠️  Override {} no esta instalado localmente, intentando ruta distribuida.",
                    selected_model
                );
            } else {
                println!("🤖 Modelo local preferido no disponible.");
                println!("   Candidates: {}", candidate_models.join(", "));
                println!("   Intentando ruta distribuida...\n");
            }
        }

        NodeMode::Capabilities => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Node Capabilities     ║");
            println!("╚══════════════════════════════════╝\n");

            let node_identity = NodeIdentity::load_or_create();
            let caps = ModelNodeCapabilities::detect(&node_identity.peer_id.to_string());
            caps.display();

            // Mostrar qué modelos puede ejecutar
            let runnable = iamine_models::runnable_models(&caps);
            println!("\n📋 Modelos ejecutables según hardware:");
            for mid in &runnable {
                let installed = caps.has_model(mid);
                let icon = if installed { "✅" } else { "⬜" };
                let req = ModelRequirements::for_model(mid).unwrap();
                println!(
                    "   {} {} (min {}GB RAM, {}GB storage{})",
                    icon,
                    mid,
                    req.min_ram_gb,
                    req.min_storage_gb,
                    if req.requires_gpu {
                        ", GPU requerida"
                    } else {
                        ""
                    }
                );
            }
            return Ok(());
        }

        NodeMode::Nodes => {
            // NO return aquí: este modo debe levantar red para descubrir peers/capabilities.
            println!("╔══════════════════════════════════╗");
            println!("║      IaMine — Nodes View         ║");
            println!("╚══════════════════════════════════╝\n");
        }

        NodeMode::Topology => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Network Topology     ║");
            println!("╚══════════════════════════════════╝\n");
            // This mode needs to discover peers, so don't return early
        }

        NodeMode::ModelsRecommend => {
            let node_identity = NodeIdentity::load_or_create();
            let caps = ModelNodeCapabilities::detect(&node_identity.peer_id.to_string());
            let benchmark = NodeBenchmark::run();

            let profile = AutoProvisionProfile {
                cpu_score: benchmark.cpu_score as u64,
                ram_gb: caps.ram_gb,
                gpu_available: benchmark.gpu_available,
                storage_available_gb: caps.storage_available_gb,
            };

            let provision = ModelAutoProvision::new(ModelRegistry::new(), ModelStorage::new());
            let recommended = provision.recommend_for_empty_node(&profile);

            println!("Compatible models for this node:");
            if recommended.is_empty() {
                println!("(none)");
            } else {
                for model in recommended {
                    println!("{}", model.id);
                }
            }
            return Ok(());
        }

        _ => {} // continuar con startup normal
    }

    if control_plane_only {
        unreachable!("control plane mode should have returned before runtime startup");
    }

    // ════════════════════════════════════════
    // WORKER STARTUP FLOW
    // ════════════════════════════════════════
    if matches!(mode, NodeMode::Worker) {
        println!("╔══════════════════════════════════╗");
        println!("║   IaMine Worker {:<14}║", current_release_version());
        println!("╚══════════════════════════════════╝\n");
    }

    // 1️⃣ Identidad de runtime (modo infer usa identidad efimera para evitar conflicto con worker local)
    let use_ephemeral_identity = matches!(mode, NodeMode::Infer { .. });
    let node_identity = if use_ephemeral_identity {
        NodeIdentity::ephemeral("infer_mode_peer_id_isolation")
    } else {
        NodeIdentity::load_or_create()
    };
    let peer_id = node_identity.peer_id;
    set_global_node_id(&peer_id.to_string());
    let id_keys = node_identity.keypair.clone();

    if use_ephemeral_identity {
        log_observability_event(
            LogLevel::Info,
            "identity_mode_selected",
            "startup",
            None,
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("mode".to_string(), "infer".into());
                fields.insert("identity_kind".to_string(), "ephemeral".into());
                fields.insert(
                    "reason".to_string(),
                    "avoid_peer_id_conflict_with_local_worker".into(),
                );
                fields.insert("peer_id".to_string(), peer_id.to_string().into());
                fields
            },
        );
    }

    // 2️⃣ Wallet
    let wallet = Wallet::load_or_create(&node_identity.wallet_address); // ← sin mut

    // 3️⃣ Benchmark (solo en modo worker)
    let benchmark = if matches!(mode, NodeMode::Worker) {
        Some(NodeBenchmark::run())
    } else {
        None
    };

    // 4️⃣ Resource policy desde CLI
    let resource_policy = ResourcePolicy::from_args(&args);
    if matches!(mode, NodeMode::Worker) {
        resource_policy.display();
    }

    // 5️⃣ Worker slots dinámicos
    let worker_slots = if let Some(ref b) = benchmark {
        b.calculate_slots(&resource_policy)
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
    };

    println!("\n═══════════════════════════════════");
    println!("  Peer ID:      {}", peer_id);
    println!("  Wallet:       {}", node_identity.wallet_address);
    if let Some(ref b) = benchmark {
        println!("  CPU Score:    {:.0}", b.cpu_score);
        println!("  RAM:          {} GB", b.ram_available_gb);
        println!(
            "  GPU:          {}",
            if b.gpu_available { "✅" } else { "❌" }
        );
    }
    println!("  Worker Slots: {}", worker_slots);
    if !matches!(mode, NodeMode::Topology) {
        println!("  Modo:         {:?}", mode);
    }
    println!("  Balance:      {} $MIND", wallet.balance);
    println!("  Tareas:       {}", wallet.tasks_completed);
    println!("  Reputación:   {:.1}/100", wallet.reputation);
    println!("═══════════════════════════════════\n");

    // 6️⃣ MODEL INFRASTRUCTURE v0.6
    let storage_config = StorageConfig::load();
    let model_registry = ModelRegistry::new();
    let model_storage = ModelStorage::new();
    let inference_engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));

    // v0.5.4: Detectar capabilities del nodo
    let node_caps = ModelNodeCapabilities::detect(&peer_id.to_string());
    let validated_advertised_models = if matches!(mode, NodeMode::Worker) {
        validate_models_for_advertising(&model_registry, &model_storage, &node_caps)
    } else {
        model_storage.list_local_models()
    };

    let worker_setup = if matches!(mode, NodeMode::Worker) {
        let detected = DetectedHardware {
            cpu_cores: std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(1),
            ram_gb: benchmark
                .as_ref()
                .map(|b| b.ram_available_gb as u32)
                .unwrap_or(node_caps.ram_gb),
            gpu_available: benchmark
                .as_ref()
                .map(|b| b.gpu_available)
                .unwrap_or(node_caps.gpu_type.is_some()),
            disk_available_gb: node_caps.storage_available_gb,
        };
        Some(NodeSetupConfig::load_or_run(&detected)?)
    } else {
        None
    };

    if let Some(cfg) = &worker_setup {
        cfg.display();
    }

    if matches!(mode, NodeMode::Worker) {
        node_caps.display();
    }

    if matches!(mode, NodeMode::Worker) {
        println!("💾 Storage limit: {} GB", storage_config.max_storage_gb);
        println!("🤖 Modelos disponibles localmente:");
        let local = model_storage.list_local_models();
        if local.is_empty() {
            println!("   (ninguno — usa --download-model <id>)");

            let provision = ModelAutoProvision::new(ModelRegistry::new(), ModelStorage::new());
            let profile = AutoProvisionProfile {
                cpu_score: benchmark.as_ref().map(|b| b.cpu_score as u64).unwrap_or(0),
                ram_gb: node_caps.ram_gb,
                gpu_available: benchmark.as_ref().map(|b| b.gpu_available).unwrap_or(false),
                storage_available_gb: worker_setup
                    .as_ref()
                    .map(|cfg| cfg.storage_limit_gb)
                    .unwrap_or(node_caps.storage_available_gb),
            };
            let recommended = provision.startup_recommendations(&profile);

            if !recommended.is_empty() {
                println!("\n⚠ No models installed\n");
                println!("Recommended:");
                for model in &recommended {
                    println!(
                        "{} ({:.0}MB)",
                        model.id,
                        model.size_bytes as f64 / 1_048_576.0
                    );
                }

                let auto_download_enabled = auto_model
                    || worker_setup
                        .as_ref()
                        .map(|cfg| cfg.auto_download_enabled())
                        .unwrap_or(false);

                if auto_download_enabled {
                    if let Some(model_id) = provision
                        .auto_download_recommended(&profile, None, false)
                        .await?
                    {
                        println!("\n⬇ Auto-downloaded: {}", model_id);
                    }
                } else {
                    print!("\nDownload now? [Y/n] ");
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                    let mut input = String::new();
                    let _ = std::io::stdin().read_line(&mut input);

                    if input.trim().is_empty() || matches!(input.trim(), "y" | "Y" | "yes" | "YES")
                    {
                        if let Some(model_id) = provision
                            .auto_download_recommended(&profile, None, false)
                            .await?
                        {
                            println!("✅ Modelo descargado: {}", model_id);
                        }
                    }
                }
            }
        } else {
            for m in &local {
                // Verificar espacio antes de mostrar
                let used = model_storage.total_size_bytes();
                println!(
                    "   ✅ {} (storage: {:.1}/{} GB)",
                    m,
                    used as f64 / 1_073_741_824.0,
                    storage_config.max_storage_gb
                );
            }
        }
        println!("📋 Modelos en registry:");
        for m in model_registry.list() {
            let available = if model_storage.has_model(&m.id) {
                "✅"
            } else {
                "⬜"
            };
            let fits = storage_config.has_space_for(m.size_bytes, model_storage.total_size_bytes());
            let fits_str = if fits { "" } else { " ⚠️ sin espacio" };
            println!(
                "   {} {} v{} ({}GB RAM){}",
                available, m.id, m.version, m.required_ram_gb, fits_str
            );
        }
    }

    // 7️⃣ Simulate workers — salida temprana
    if let NodeMode::SimulateWorkers { count } = &mode {
        println!("🔥 Simulando {} workers virtuales...", count);
        simulate_workers(*count, peer_id.to_string()).await;
        return Ok(());
    }

    let transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys)?)
        .multiplex(yamux::Config::default())
        .boxed();

    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(1))
        .validation_mode(gossipsub::ValidationMode::Permissive)
        .build()
        .map_err(|e| format!("Gossipsub config error: {}", e))?;

    let mut gossipsub_behaviour = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(id_keys.clone()),
        gossipsub_config,
    )
    .map_err(|e| format!("Gossipsub error: {}", e))?;

    let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC);
    gossipsub_behaviour.subscribe(&task_topic)?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-bids"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-assign"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-heartbeat"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(RESULTS_TOPIC))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(CAP_TOPIC))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(DIRECT_INF_TOPIC))?;
    let mut pubsub_topics = PubsubTopicTracker::default();
    pubsub_topics.register_local_subscription(TASK_TOPIC);
    pubsub_topics.register_local_subscription(DIRECT_INF_TOPIC);
    pubsub_topics.register_local_subscription(RESULTS_TOPIC);
    for topic_name in [TASK_TOPIC, DIRECT_INF_TOPIC, RESULTS_TOPIC] {
        log_observability_event(
            LogLevel::Info,
            "pubsub_topic_joined",
            "startup",
            None,
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("topic".to_string(), topic_name.into());
                fields.insert("scope".to_string(), "local".into());
                fields
            },
        );
    }

    let mut kad_cfg = kad::Config::default();
    kad_cfg.set_query_timeout(Duration::from_secs(30));
    let kademlia =
        kad::Behaviour::with_config(peer_id, kad::store::MemoryStore::new(peer_id), kad_cfg);

    let behaviour = IamineBehaviour {
        ping: ping::Behaviour::default(),
        identify: identify::Behaviour::new(identify::Config::new(
            "/iamine/1.0".to_string(),
            id_keys.public(),
        )),
        request_response: cbor::Behaviour::<TaskRequest, TaskResponse>::new(
            [(
                StreamProtocol::new("/iamine/task/1.0"),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        ),
        // ← Protocolo directo para resultados
        result_response: cbor::Behaviour::<TaskResultRequest, TaskResultResponse>::new(
            [(
                StreamProtocol::new("/iamine/result/1.0"),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        ),
        kademlia,
        mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?,
        gossipsub: gossipsub_behaviour,
    };

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(60)),
    );

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

    let args: Vec<String> = std::env::args().collect();
    for arg in &args {
        if arg.starts_with("--bootnode=") {
            let addr_str = arg.replace("--bootnode=", "");
            if let Ok(addr) = addr_str.parse::<Multiaddr>() {
                println!("🔗 Conectando a bootnode: {}", addr);
                let _ = swarm.dial(addr);
            }
        }
    }

    let worker_port: u16 = {
        if let Some(port_arg) = args.iter().find(|a| a.starts_with("--port=")) {
            port_arg.replace("--port=", "").parse().unwrap_or(9000)
        } else {
            9000
        }
    };
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

    let listen_addr: Multiaddr = if matches!(mode, NodeMode::Worker) {
        format!("/ip4/0.0.0.0/tcp/{}", worker_port).parse()?
    } else if matches!(mode, NodeMode::Relay) {
        "/ip4/0.0.0.0/tcp/9999".parse()?
    } else {
        "/ip4/0.0.0.0/tcp/0".parse()?
    };
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
                if matches!(mode, NodeMode::Worker) {
                    let rep = queue.reputation().await;
                    let uptime = heartbeat.uptime_secs();
                    let available_slots = pool.available_slots();
                    let active_tasks = match compute_active_tasks(pool.max_concurrent, available_slots) {
                        Ok(value) => value,
                        Err(error) => {
                            emit_worker_startup_overflow_event(WorkerStartupOverflowContext {
                                trace_id: "startup",
                                node_id: &node_identity.node_id,
                                peer_id: &peer_id.to_string(),
                                port: worker_port,
                                resource_policy: &resource_policy,
                                worker_slots,
                                max_concurrent: pool.max_concurrent,
                                available_slots,
                                error: &error,
                                fallback_behavior: "exit_cleanly_invalid_startup_state",
                            });
                            return Err(format!(
                                "Worker startup math invalid: {} [{}]",
                                error.describe(),
                                WORKER_STARTUP_OVERFLOW_001
                            )
                            .into());
                        }
                    };
                    {
                        let mut m = metrics.write().await;
                        m.uptime_secs = uptime;
                        m.reputation_score = rep.reputation_score;
                        m.active_workers = active_tasks;
                        m.network_peers = peer_tracker.peer_count();
                        m.direct_peers = client_state.known_workers.len();
                        m.avg_latency_ms = peer_tracker.avg_latency();
                    }

                    if rate_limiter.allow("Heartbeat") {
                        let hb = serde_json::json!({
                            "type": "Heartbeat",
                            "peer_id": peer_id.to_string(),
                            "available_slots": available_slots,
                            "reputation_score": rep.reputation_score,
                            "uptime_secs": uptime,
                            "avg_latency_ms": rep.avg_execution_ms,
                            "capabilities": {
                                "cpu_cores": capabilities.cpu_cores,
                                "ram_gb": capabilities.ram_gb,
                                "gpu_available": capabilities.gpu_available,
                                "supported_tasks": capabilities.supported_tasks,
                            }
                        });
                        let hb_topic = gossipsub::IdentTopic::new("iamine-heartbeat");
                        let _ = swarm.behaviour_mut().gossipsub.publish(
                            hb_topic, serde_json::to_vec(&hb).unwrap(),
                        );
                    }

                    // ─── Broadcast NodeModelsBroadcast
                    let installer = ModelInstaller::new();
                    let allowed_model_ids: HashSet<_> =
                        validated_advertised_models.iter().cloned().collect();
                    let mut nm = installer.build_node_models(&peer_id.to_string());
                    nm.models
                        .retain(|model| allowed_model_ids.contains(&model.id));
                    if !nm.models.is_empty() {
                        let payload = serde_json::json!({
                            "type": "NodeModelsBroadcast",
                            "node_id": peer_id.to_string(),
                            "models": nm.models,
                        });
                        let _ = swarm.behaviour_mut().gossipsub.publish(
                            gossipsub::IdentTopic::new("iamine-heartbeat"),
                            serde_json::to_vec(&payload).unwrap(),
                        );
                    }

                    // Broadcast capabilities
                    let cap_hb = NodeCapabilityHeartbeat {
                        peer_id: peer_id.to_string(),
                        cpu_score: benchmark.as_ref().map(|b| b.cpu_score as u64).unwrap_or(0),
                        ram_gb: benchmark.as_ref().map(|b| b.ram_available_gb as u32).unwrap_or(8),
                        gpu_available: benchmark.as_ref().map(|b| b.gpu_available).unwrap_or(false),
                        storage_available_gb: node_caps.storage_available_gb,
                        accelerator: node_caps.accelerator.clone(),
                        models: validated_advertised_models.clone(),
                        worker_slots: worker_slots as u32,
                        active_tasks: active_tasks as u32,
                        latency_ms: peer_tracker.avg_latency().max(1.0) as u32,
                    };
                    let _ = swarm.behaviour_mut().gossipsub.publish(
                        gossipsub::IdentTopic::new(CAP_TOPIC),
                        serde_json::to_vec(&cap_hb).unwrap(),
                    );

                    // Sync topology clusters to registry
                    {
                        let mut topo = topology.write().await;
                        topo.assign_clusters(&peer_id.to_string());
                        let mut reg = registry.write().await;
                        for cluster in topo.all_clusters() {
                            for node_id in &cluster.nodes {
                                reg.set_cluster(&node_id.to_string(), &cluster.id);
                            }
                        }
                    }
                }

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
