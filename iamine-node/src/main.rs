mod benchmark;
mod code_quality;
mod daemon_runtime;
mod executor;
mod heartbeat;
mod metrics;
mod model_selector_cli;
mod network;
mod node_identity;
mod peer_tracker;
mod protocol;
mod quality_gate;
mod rate_limiter;
mod regression_runner;
mod resource_policy;
mod result_protocol;
mod security_checks;
mod setup_wizard;
mod task_cache;
mod task_codec;
mod task_protocol;
mod task_queue;
mod task_scheduler;
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
    record_distributed_task_fallback, record_distributed_task_latency,
    record_distributed_task_retry, record_distributed_task_started, record_model_metrics,
    record_task_attempt, record_task_latency, select_retry_target, set_global_node_id,
    set_global_runtime_context, task_trace, validate_result, validate_semantic_decision,
    Complexity as PromptComplexityLevel, DeterministicLevel as PromptDeterministicLevel,
    DistributedTaskMetrics, DistributedTaskResult, Domain as PromptDomain,
    ExactSubtype as PromptExactSubtype, FailureKind, IntelligentScheduler,
    Language as PromptLanguage, LogLevel, ModelKarma, ModelMetrics, ModelPolicyEngine,
    NetworkTopology, NodeCapabilityHeartbeat, NodeHealth, NodeRegistry, OutputPolicyDecision,
    OutputStyle as PromptOutputStyle, PromptProfile, ResultStatus, RetryPolicy, RetryState,
    SemanticFeedbackEngine, SemanticRoutingDecision, SharedNetworkTopology, SharedNodeRegistry,
    StructuredLogEntry, TaskClaim, TaskManager, TaskTrace, TaskType as PromptTaskType,
    ValidationResult as SemanticValidationResult, MODEL_LOAD_FAILED_001, MODEL_UNSUPPORTED_HW_002,
    NETWORK_NO_PUBSUB_PEERS_001, NET_PEER_DISCONNECTED_002, NODE_BLACKLISTED_001,
    NODE_UNHEALTHY_002, PUBSUB_TOPIC_NOT_READY_001, SCH_NO_NODE_001, TASK_DISPATCH_UNCONFIRMED_001,
    TASK_EMPTY_RESULT_003, TASK_FAILED_002, TASK_TIMEOUT_001, WORKER_STARTUP_OVERFLOW_001,
};

#[cfg(test)]
use iamine_network::analyze_prompt;

use benchmark::NodeBenchmark;
use code_quality::run_code_quality_checks;
use daemon_runtime::{daemon_is_available, daemon_socket_path, infer_via_daemon, run_daemon};
use heartbeat::HeartbeatService;
use metrics::{start_metrics_server, NodeMetrics};
use model_selector_cli::ModelSelectorCLI;
use node_identity::NodeIdentity; // ← actualizado
use peer_tracker::PeerTracker;
use quality_gate::{current_release_version, run_release_validation};
use rate_limiter::RateLimiter;
use regression_runner::run_default_regression_suite;
use resource_policy::ResourcePolicy;
use result_protocol::{TaskResultRequest, TaskResultResponse};
use security_checks::run_security_checks;
use serde_json::{Map, Value};
use setup_wizard::{DetectedHardware, NodeSetupConfig};
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
use std::str::FromStr;
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

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum NodeMode {
    Daemon,
    Worker,
    Relay,
    Client {
        peer: Option<Multiaddr>,
        task_type: String,
        data: String,
    },
    Stress {
        peer: Option<Multiaddr>,
        count: usize,
    },
    Broadcast {
        task_type: String,
        data: String,
    },
    SimulateWorkers {
        count: usize,
    },
    ModelsList,
    ModelsStats,
    ModelsDownload {
        model_id: String,
    },
    ModelsRemove {
        model_id: String,
    },
    ModelsMenu, // ← nuevo
    ModelsSearch {
        query: String,
    }, // ← nuevo
    TestInference {
        prompt: String,
    },
    Infer {
        prompt: String,
        model_id: Option<String>,
        max_tokens_override: Option<u32>,
        force_network: bool,
        no_local: bool,
        prefer_local: bool,
    },
    SemanticEval,
    RegressionRun,
    CheckCode,
    CheckSecurity,
    ValidateRelease,
    TasksStats,
    TasksTrace {
        task_id: String,
    },
    Capabilities,
    Nodes,
    Topology, // ← NEW
    ModelsRecommend,
}

#[derive(Debug, Clone, Copy, Default)]
struct DebugFlags {
    network: bool,
    scheduler: bool,
    tasks: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct InferenceControlFlags {
    force_network: bool,
    no_local: bool,
    prefer_local: bool,
}

impl InferenceControlFlags {
    fn from_args(args: &[String]) -> Self {
        Self {
            force_network: args.iter().any(|arg| arg == "--force-network"),
            no_local: args.iter().any(|arg| arg == "--no-local"),
            prefer_local: args.iter().any(|arg| arg == "--prefer-local"),
        }
    }

    fn should_use_local(self, has_local_model: bool) -> bool {
        if self.force_network || self.no_local {
            return false;
        }
        if self.prefer_local {
            return has_local_model;
        }
        has_local_model
    }
}

impl DebugFlags {
    fn from_args(args: &[String]) -> Self {
        Self {
            network: args.iter().any(|arg| arg == "--debug-network"),
            scheduler: args.iter().any(|arg| arg == "--debug-scheduler"),
            tasks: args.iter().any(|arg| arg == "--debug-tasks"),
        }
    }
}

fn mode_label(mode: &NodeMode) -> &'static str {
    match mode {
        NodeMode::Daemon => "daemon",
        NodeMode::Worker => "worker",
        NodeMode::Relay => "relay",
        NodeMode::Client { .. } => "client",
        NodeMode::Stress { .. } => "stress",
        NodeMode::Broadcast { .. } => "broadcast",
        NodeMode::SimulateWorkers { .. } => "simulate-workers",
        NodeMode::ModelsList => "models-list",
        NodeMode::ModelsStats => "models-stats",
        NodeMode::ModelsDownload { .. } => "models-download",
        NodeMode::ModelsRemove { .. } => "models-remove",
        NodeMode::ModelsMenu => "models-menu",
        NodeMode::ModelsSearch { .. } => "models-search",
        NodeMode::TestInference { .. } => "test-inference",
        NodeMode::Infer { .. } => "infer",
        NodeMode::SemanticEval => "semantic-eval",
        NodeMode::RegressionRun => "regression-run",
        NodeMode::CheckCode => "check-code",
        NodeMode::CheckSecurity => "check-security",
        NodeMode::ValidateRelease => "validate-release",
        NodeMode::TasksStats => "tasks-stats",
        NodeMode::TasksTrace { .. } => "tasks-trace",
        NodeMode::Capabilities => "capabilities",
        NodeMode::Nodes => "nodes",
        NodeMode::Topology => "topology",
        NodeMode::ModelsRecommend => "models-recommend",
    }
}

fn parse_args() -> Result<NodeMode, String> {
    let raw_args: Vec<String> = std::env::args().collect();
    let args: Vec<String> = raw_args
        .into_iter()
        .filter(|arg| {
            !matches!(
                arg.as_str(),
                "--debug-network" | "--debug-scheduler" | "--debug-tasks"
            )
        })
        .collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("--daemon") => Ok(NodeMode::Daemon),
        Some("--worker") | None => Ok(NodeMode::Worker),
        Some("--relay") => Ok(NodeMode::Relay),
        Some("models") => match args.get(2).map(|s| s.as_str()) {
            Some("list") => Ok(NodeMode::ModelsList),
            Some("stats") => Ok(NodeMode::ModelsStats),
            Some("recommend") => Ok(NodeMode::ModelsRecommend),
            Some("menu") => Ok(NodeMode::ModelsMenu),
            Some("search") => {
                let query = args.get(3).ok_or("Falta <query>")?.clone();
                Ok(NodeMode::ModelsSearch { query })
            }
            Some("download") => {
                let id = args.get(3).ok_or("Falta <model_id>")?.clone();
                Ok(NodeMode::ModelsDownload { model_id: id })
            }
            Some("remove") => {
                let id = args.get(3).ok_or("Falta <model_id>")?.clone();
                Ok(NodeMode::ModelsRemove { model_id: id })
            }
            _ => Err(
                "Uso: iamine models [list|stats|recommend|menu|search <q>|download <id>|remove <id>]"
                    .to_string(),
            ),
        },
        Some("--client") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (
                    Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?),
                    3,
                )
            } else {
                (None, 2)
            };
            let task_type = args.get(offset).ok_or("Falta <task_type>")?.clone();
            let data = args.get(offset + 1).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Client {
                peer,
                task_type,
                data,
            })
        }

        Some("--stress") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (
                    Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?),
                    3,
                )
            } else {
                (None, 2)
            };
            let count = args
                .get(offset)
                .unwrap_or(&"10".to_string())
                .parse::<usize>()
                .unwrap_or(10);
            Ok(NodeMode::Stress { peer, count })
        }

        Some("--broadcast") => {
            let task_type = args.get(2).ok_or("Falta <task_type>")?.clone();
            let data = args.get(3).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Broadcast { task_type, data })
        }

        Some("--simulate-workers") => {
            let count = args
                .get(2)
                .unwrap_or(&"10".to_string())
                .parse::<usize>()
                .unwrap_or(10);
            Ok(NodeMode::SimulateWorkers { count })
        }

        Some("test-inference") => {
            let prompt = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "What is 2+2?".to_string());
            Ok(NodeMode::TestInference { prompt })
        }

        Some("capabilities") => Ok(NodeMode::Capabilities),

        Some("infer") => {
            let prompt = args.get(2).ok_or("Falta <prompt>")?.clone();
            let model_id = args
                .iter()
                .position(|a| a == "--model")
                .and_then(|i| args.get(i + 1).cloned());
            let max_tokens_override = parse_optional_u32_flag(&args, "--max-tokens")?;
            let control_flags = InferenceControlFlags::from_args(&args);
            Ok(NodeMode::Infer {
                prompt,
                model_id,
                max_tokens_override,
                force_network: control_flags.force_network,
                no_local: control_flags.no_local,
                prefer_local: control_flags.prefer_local,
            })
        }

        Some("semantic-eval") => Ok(NodeMode::SemanticEval),
        Some("regression-run") => Ok(NodeMode::RegressionRun),
        Some("check-code") => Ok(NodeMode::CheckCode),
        Some("check-security") => Ok(NodeMode::CheckSecurity),
        Some("validate-release") => Ok(NodeMode::ValidateRelease),
        Some("tasks") => match args.get(2).map(|s| s.as_str()) {
            Some("stats") => Ok(NodeMode::TasksStats),
            Some("trace") => {
                let task_id = args.get(3).ok_or("Falta <task_id>")?.clone();
                Ok(NodeMode::TasksTrace { task_id })
            }
            _ => Err("Uso: iamine-node tasks [stats|trace <task_id>]".to_string()),
        },

        Some("nodes") => Ok(NodeMode::Nodes),

        Some("topology") => Ok(NodeMode::Topology), // ← NEW

        Some(unknown) => Err(format!("Modo desconocido: {}", unknown)),
    }
}

fn parse_optional_u32_flag(args: &[String], flag: &str) -> Result<Option<u32>, String> {
    let Some(index) = args.iter().position(|arg| arg == flag) else {
        return Ok(None);
    };

    let Some(raw) = args.get(index + 1) else {
        return Err(format!("Falta valor para {}", flag));
    };

    raw.parse::<u32>()
        .map(Some)
        .map_err(|_| format!("Valor invalido para {}: {}", flag, raw))
}

fn prompt_language_label(language: PromptLanguage) -> &'static str {
    match language {
        PromptLanguage::English => "English",
        PromptLanguage::Spanish => "Spanish",
        PromptLanguage::Unknown => "Unknown",
    }
}

fn prompt_complexity_label(complexity: PromptComplexityLevel) -> &'static str {
    match complexity {
        PromptComplexityLevel::Low => "Low",
        PromptComplexityLevel::Medium => "Medium",
        PromptComplexityLevel::High => "High",
    }
}

fn prompt_task_label(task_type: PromptTaskType) -> &'static str {
    match task_type {
        PromptTaskType::Math => "Math",
        PromptTaskType::ExactMath => "ExactMath",
        PromptTaskType::SymbolicMath => "SymbolicMath",
        PromptTaskType::Generative => "Generative",
        PromptTaskType::StructuredList => "StructuredList",
        PromptTaskType::Deterministic => "Deterministic",
        PromptTaskType::Code => "Code",
        PromptTaskType::Conceptual => "Conceptual",
        PromptTaskType::Reasoning => "Reasoning",
        PromptTaskType::Summarization => "Summarization",
        PromptTaskType::General => "General",
    }
}

fn prompt_output_style_label(output_style: PromptOutputStyle) -> &'static str {
    match output_style {
        PromptOutputStyle::Exact => "Exact",
        PromptOutputStyle::Explanatory => "Explanatory",
        PromptOutputStyle::Structured => "Structured",
        PromptOutputStyle::Generative => "Generative",
        PromptOutputStyle::Hybrid => "Hybrid",
    }
}

fn prompt_deterministic_level_label(level: PromptDeterministicLevel) -> &'static str {
    match level {
        PromptDeterministicLevel::High => "High",
        PromptDeterministicLevel::Medium => "Medium",
        PromptDeterministicLevel::Low => "Low",
    }
}

fn prompt_domain_label(domain: Option<PromptDomain>) -> &'static str {
    match domain {
        Some(PromptDomain::Math) => "Math",
        Some(PromptDomain::Physics) => "Physics",
        Some(PromptDomain::Business) => "Business",
        Some(PromptDomain::Philosophy) => "Philosophy",
        Some(PromptDomain::Code) => "Code",
        Some(PromptDomain::General) | None => "General",
    }
}

fn exact_subtype_label(exact_subtype: PromptExactSubtype) -> &'static str {
    match exact_subtype {
        PromptExactSubtype::Integer => "Integer",
        PromptExactSubtype::DecimalSequence => "DecimalSequence",
        PromptExactSubtype::Sequence => "Sequence",
    }
}

fn log_semantic_decision(decision: &SemanticRoutingDecision) {
    let secondary = if decision.profile.semantic.secondary_tasks.is_empty() {
        "[]".to_string()
    } else {
        format!(
            "[{}]",
            decision
                .profile
                .semantic
                .secondary_tasks
                .iter()
                .map(|task| prompt_task_label(*task))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    println!(
        "[Semantic] Primary: {}",
        prompt_task_label(decision.profile.semantic.primary_task)
    );
    println!("[Semantic] Secondary: {}", secondary);
    println!(
        "[Semantic] Style: {}",
        prompt_output_style_label(decision.profile.semantic.output_style)
    );
    println!(
        "[Semantic] Context: {}",
        decision.profile.semantic.requires_context
    );
    println!(
        "[Semantic] Domain: {}",
        prompt_domain_label(decision.profile.semantic.domain)
    );
    println!(
        "[Semantic] Deterministic: {}",
        prompt_deterministic_level_label(decision.profile.semantic.deterministic_level)
    );
    println!("[Semantic] Confidence: {:.2}", decision.profile.confidence);
    println!("[Semantic] Fallback: {}", decision.fallback_applied);
    if decision.fallback_applied {
        println!(
            "[Semantic] Original task: {}",
            prompt_task_label(decision.original_task_type)
        );
    }
}

fn log_semantic_validation(validation: &SemanticValidationResult) {
    println!(
        "[SemanticValidator] Confidence: {:.2} -> {:.2}",
        validation.confidence_before, validation.confidence_after
    );
    println!(
        "[SemanticValidator] Model validation: {}",
        validation.model_validation_used
    );
    println!(
        "[SemanticValidator] Correction applied: {}",
        validation.correction_applied
    );
    if !validation.conflicts.is_empty() {
        println!(
            "[SemanticValidator] Conflicts: {}",
            validation.conflicts.join(", ")
        );
    }
}

fn record_semantic_feedback(prompt: &str, validation: &SemanticValidationResult) {
    let engine = SemanticFeedbackEngine::default();
    if let Err(error) = engine.append_from_validation(prompt, validation) {
        eprintln!("[Feedback] Logging failed: {}", error);
    } else {
        println!(
            "[Feedback] Logged: {}",
            default_semantic_log_path().display()
        );
    }
}

fn task_requires_validation(task_type: PromptTaskType) -> bool {
    matches!(
        task_type,
        PromptTaskType::ExactMath | PromptTaskType::StructuredList | PromptTaskType::Deterministic
    )
}

#[derive(Clone)]
enum InferenceRuntime {
    Engine(Arc<RealInferenceEngine>),
    Daemon(PathBuf),
}

fn prompt_requests_decimal_sequence(prompt: &str) -> bool {
    let lower = prompt.to_lowercase();
    ["pi", "π", "digit", "digits", "digito", "digitos", "dígitos"]
        .iter()
        .any(|needle| lower.contains(needle))
}

fn with_task_guard(prompt: &str, task_type: PromptTaskType, retry: bool) -> String {
    match task_type {
        PromptTaskType::ExactMath => {
            if !retry {
                return prompt.to_string();
            }

            if prompt_requests_decimal_sequence(prompt) {
                format!(
                    "Return only the requested numeric sequence in plain text. Do not add words. Keep the decimal point attached to the digits.\n\n{}",
                    prompt
                )
            } else {
                format!(
                    "Return only the final numeric answer in plain text. Do not add words or extra symbols.\n\n{}",
                    prompt
                )
            }
        }
        PromptTaskType::StructuredList => {
            if !retry {
                return prompt.to_string();
            }

            let retry_guard = if retry {
                "Be stricter: if the task is the alphabet, return exactly: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z"
            } else {
                "Return only the full ordered list in plain text, with no missing items and no duplicates."
            };
            format!("{}\n\n{}", retry_guard, prompt)
        }
        PromptTaskType::Deterministic => {
            if !retry {
                return prompt.to_string();
            }

            let retry_guard = if retry {
                "Be stricter: return only the requested data, exactly and in order, in plain text."
            } else {
                "Return only the requested data, exactly and in order, in plain text."
            };
            format!("{}\n\n{}", retry_guard, prompt)
        }
        _ => prompt.to_string(),
    }
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

async fn choose_inference_runtime() -> Option<InferenceRuntime> {
    let socket = daemon_socket_path();
    if daemon_is_available(&socket).await {
        println!(
            "[Daemon] Connected to persistent runtime: {}",
            socket.display()
        );
        Some(InferenceRuntime::Daemon(socket))
    } else {
        None
    }
}

#[derive(Clone)]
struct PromptResolution {
    profile: PromptProfile,
    candidate_models: Vec<String>,
    selected_model: String,
    semantic_prompt: String,
    validation: SemanticValidationResult,
}

struct DistributedInferState {
    trace_task_id: String,
    prompt: String,
    model_override: Option<String>,
    max_tokens_override: Option<u32>,
    current_request_id: String,
    current_task_type: Option<PromptTaskType>,
    current_semantic_prompt: Option<String>,
    current_peer: Option<String>,
    current_model: Option<String>,
    candidate_models: Vec<String>,
    local_cluster_id: Option<String>,
    retry_policy: RetryPolicy,
    retry_state: RetryState,
}

impl DistributedInferState {
    fn new(
        prompt: String,
        model_override: Option<String>,
        max_tokens_override: Option<u32>,
    ) -> Self {
        let trace_task_id = uuid_simple();
        Self {
            trace_task_id: trace_task_id.clone(),
            prompt,
            model_override,
            max_tokens_override,
            current_request_id: trace_task_id,
            current_task_type: None,
            current_semantic_prompt: None,
            current_peer: None,
            current_model: None,
            candidate_models: Vec::new(),
            local_cluster_id: None,
            retry_policy: RetryPolicy {
                max_retries: MAX_DISTRIBUTED_RETRIES,
                timeout_ms: INFER_TIMEOUT_MS,
            },
            retry_state: RetryState::default(),
        }
    }

    fn record_attempt(
        &mut self,
        peer_id: String,
        model_id: String,
        task_type: PromptTaskType,
        semantic_prompt: String,
        local_cluster_id: Option<String>,
        candidate_models: Vec<String>,
    ) {
        self.current_peer = Some(peer_id);
        self.current_model = Some(model_id);
        self.current_task_type = Some(task_type);
        self.current_semantic_prompt = Some(semantic_prompt);
        self.local_cluster_id = local_cluster_id;
        self.candidate_models = candidate_models;
    }

    fn schedule_retry(&mut self, failed_peer: Option<&str>, failed_model: Option<&str>) -> bool {
        self.retry_state.record_failure(failed_peer, failed_model);
        if !self.retry_state.can_retry(&self.retry_policy) {
            return false;
        }

        self.retry_state.advance_retry();
        self.current_request_id = uuid_simple();
        self.current_task_type = None;
        self.current_semantic_prompt = None;
        self.current_peer = None;
        self.current_model = None;
        true
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DispatchReadinessError {
    code: &'static str,
    reason: &'static str,
}

impl DispatchReadinessError {
    fn new(code: &'static str, reason: &'static str) -> Self {
        Self { code, reason }
    }
}

#[derive(Debug, Clone)]
struct DispatchReadinessSnapshot {
    connected_peer_count: usize,
    mesh_peer_count: usize,
    topic_peer_count: usize,
    joined_task_topic: bool,
    joined_direct_topic: bool,
    joined_results_topic: bool,
    selected_topic: &'static str,
}

fn topic_hash_string(topic_name: &str) -> String {
    gossipsub::IdentTopic::new(topic_name).hash().to_string()
}

fn evaluate_dispatch_readiness(
    snapshot: &DispatchReadinessSnapshot,
) -> Result<(), DispatchReadinessError> {
    if snapshot.connected_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            NETWORK_NO_PUBSUB_PEERS_001,
            "no_connected_peers",
        ));
    }

    if !snapshot.joined_task_topic
        || !snapshot.joined_direct_topic
        || !snapshot.joined_results_topic
    {
        return Err(DispatchReadinessError::new(
            PUBSUB_TOPIC_NOT_READY_001,
            "required_topics_not_joined",
        ));
    }

    if snapshot.mesh_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            NETWORK_NO_PUBSUB_PEERS_001,
            "no_pubsub_mesh_peers",
        ));
    }

    if snapshot.topic_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            PUBSUB_TOPIC_NOT_READY_001,
            "destination_topic_has_zero_peers",
        ));
    }

    Ok(())
}

fn should_print_result_output(output: &str) -> bool {
    !output.trim().is_empty()
}

#[cfg(test)]
fn apply_retry_fallback_metrics(
    metrics: &mut DistributedTaskMetrics,
    retry_taken: bool,
    fallback_broadcast: bool,
) {
    if retry_taken {
        metrics.retry_recorded();
    }
    if fallback_broadcast {
        metrics.fallback_recorded();
    }
}

fn emit_dispatch_context_event(
    trace_task_id: &str,
    attempt_id: &str,
    selected_model: &str,
    candidates: &[String],
    connected_peer_count: usize,
    topic_peer_count: usize,
    selected_topic: &str,
    target_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "task_dispatch_context",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert(
                "selected_model".to_string(),
                selected_model.to_string().into(),
            );
            fields.insert("candidates".to_string(), serde_json::json!(candidates));
            fields.insert(
                "connected_peer_count".to_string(),
                (connected_peer_count as u64).into(),
            );
            fields.insert(
                "topic_peer_count".to_string(),
                (topic_peer_count as u64).into(),
            );
            fields.insert(
                "selected_topic".to_string(),
                selected_topic.to_string().into(),
            );
            if let Some(target_peer_id) = target_peer_id {
                fields.insert("target_peer_id".to_string(), target_peer_id.into());
                fields.insert("selected_peer_id".to_string(), target_peer_id.into());
            }
            fields
        },
    );
}

fn emit_dispatch_readiness_failure_event(
    trace_task_id: &str,
    attempt_id: &str,
    selected_model: &str,
    candidates: &[String],
    target_peer_id: Option<&str>,
    snapshot: &DispatchReadinessSnapshot,
    error: &DispatchReadinessError,
) {
    log_observability_event(
        LogLevel::Error,
        "task_dispatch_readiness_failed",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        Some(error.code),
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("reason".to_string(), error.reason.into());
            fields.insert("error_kind".to_string(), "dispatch_readiness".into());
            fields.insert("recoverable".to_string(), true.into());
            fields.insert("candidates".to_string(), serde_json::json!(candidates));
            fields.insert(
                "connected_peer_count".to_string(),
                (snapshot.connected_peer_count as u64).into(),
            );
            fields.insert(
                "mesh_peer_count".to_string(),
                (snapshot.mesh_peer_count as u64).into(),
            );
            fields.insert(
                "topic_peer_count".to_string(),
                (snapshot.topic_peer_count as u64).into(),
            );
            fields.insert(
                "joined_task_topic".to_string(),
                snapshot.joined_task_topic.into(),
            );
            fields.insert(
                "joined_direct_topic".to_string(),
                snapshot.joined_direct_topic.into(),
            );
            fields.insert(
                "joined_results_topic".to_string(),
                snapshot.joined_results_topic.into(),
            );
            fields.insert("selected_topic".to_string(), snapshot.selected_topic.into());
            if let Some(target_peer_id) = target_peer_id {
                fields.insert("target_peer_id".to_string(), target_peer_id.into());
                fields.insert("selected_peer_id".to_string(), target_peer_id.into());
            }
            fields
        },
    );
}

fn emit_task_publish_attempt_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    publish_peer_count: usize,
    payload_size: usize,
    selected_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "task_publish_attempt",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert(
                "publish_peer_count".to_string(),
                (publish_peer_count as u64).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
}

fn emit_task_published_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    publish_peer_count: usize,
    payload_size: usize,
    message_id: &str,
    selected_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "task_published",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert(
                "publish_peer_count".to_string(),
                (publish_peer_count as u64).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            fields.insert("message_id".to_string(), message_id.into());
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
}

fn emit_task_publish_failed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    publish_peer_count: usize,
    payload_size: usize,
    error: &str,
    selected_peer_id: Option<&str>,
) {
    log_observability_event(
        LogLevel::Error,
        "task_publish_failed",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert(
                "publish_peer_count".to_string(),
                (publish_peer_count as u64).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            fields.insert("error".to_string(), error.into());
            fields.insert("error_kind".to_string(), "publish_error".into());
            fields.insert("recoverable".to_string(), true.into());
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
}

fn emit_worker_task_message_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    topic: &str,
    from_peer: &str,
    payload_parse_result: &str,
    selected_model: &str,
    local_model_available: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "task_message_received",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("from_peer".to_string(), from_peer.into());
            fields.insert(
                "payload_parse_result".to_string(),
                payload_parse_result.into(),
            );
            fields.insert(
                "local_model_available".to_string(),
                local_model_available.into(),
            );
            fields
        },
    );
}

fn emit_direct_inference_request_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    selected_model: &str,
    from_peer: &str,
    worker_peer_id: &str,
    local_model_available: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "direct_inference_request_received",
        trace_task_id,
        Some(trace_task_id),
        Some(selected_model),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("from_peer".to_string(), from_peer.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert(
                "local_model_available".to_string(),
                local_model_available.into(),
            );
            fields
        },
    );
}

fn emit_retry_scheduled_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    failed_peer_id: Option<&str>,
    selected_peer_id: Option<&str>,
    error_kind: &str,
) {
    log_observability_event(
        LogLevel::Warn,
        "retry_scheduled",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("error_kind".to_string(), error_kind.into());
            fields.insert("recoverable".to_string(), true.into());
            if let Some(failed_peer_id) = failed_peer_id {
                fields.insert("failed_peer_id".to_string(), failed_peer_id.into());
            }
            if let Some(selected_peer_id) = selected_peer_id {
                fields.insert("selected_peer_id".to_string(), selected_peer_id.into());
            }
            fields
        },
    );
}

fn emit_worker_task_completed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    result_size: usize,
    success: bool,
    worker_peer_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "task_completed",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("result_size".to_string(), (result_size as u64).into());
            fields.insert("success".to_string(), success.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields
        },
    );
}

fn emit_worker_result_published_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    result_size: usize,
    message_id: Option<&str>,
    worker_peer_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "result_published",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("result_size".to_string(), (result_size as u64).into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            if let Some(message_id) = message_id {
                fields.insert("message_id".to_string(), message_id.into());
            }
            fields
        },
    );
}

fn emit_result_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    from_peer: &str,
    latency_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "result_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("from_peer".to_string(), from_peer.into());
            fields.insert("worker_peer_id".to_string(), from_peer.into());
            fields.insert("latency_ms".to_string(), latency_ms.into());
            fields
        },
    );
}

fn clear_stream_state(
    token_buffer: &mut HashMap<u32, String>,
    next_token_idx: &mut u32,
    rendered_output: &mut String,
) {
    token_buffer.clear();
    *next_token_idx = 0;
    rendered_output.clear();
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
    match tokio::time::timeout(
        Duration::from_millis(INFER_TIMEOUT_MS),
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
        Err(_) => Err(format!(
            "Inference exceeded timeout of {} ms",
            INFER_TIMEOUT_MS
        )),
    }
}

fn resolve_policy_for_prompt(
    prompt: &str,
    model_override: Option<&str>,
    available_models: &[String],
) -> PromptResolution {
    let semantic_prompt = normalize_expression(prompt).unwrap_or_else(|| prompt.to_string());
    if semantic_prompt != prompt {
        println!(
            "[Parser] Normalized expression: {} → {}",
            prompt, semantic_prompt
        );
    }

    let semantic_decision = analyze_prompt_semantics(&semantic_prompt);
    let validated = validate_semantic_decision(&semantic_prompt, semantic_decision);
    let profile = validated.decision.profile.clone();
    println!(
        "[Analyzer] Language: {}, Complexity: {}, Task: {}",
        prompt_language_label(profile.language),
        prompt_complexity_label(profile.complexity),
        prompt_task_label(profile.task_type),
    );
    log_semantic_decision(&validated.decision);
    log_semantic_validation(&validated.validation);

    if let Some(model_id) = model_override {
        println!("[Policy] Manual override: {}", model_id);
        return PromptResolution {
            profile,
            candidate_models: vec![model_id.to_string()],
            selected_model: model_id.to_string(),
            semantic_prompt,
            validation: validated.validation,
        };
    }

    let policy = ModelPolicyEngine::default();
    let candidates = policy.candidate_models(&profile);
    let decision = if available_models.is_empty() {
        policy.select_model_decision(&profile)
    } else {
        policy.select_model_decision_from_available(&profile, available_models)
    };
    println!(
        "[Policy] Selected model: {} (reason: {})",
        decision.model, decision.reason
    );
    PromptResolution {
        profile,
        candidate_models: candidates,
        selected_model: decision.model,
        semantic_prompt,
        validation: validated.validation,
    }
}

fn resolve_output_policy(
    profile: &PromptProfile,
    prompt: &str,
    max_tokens_override: Option<u32>,
) -> OutputPolicyDecision {
    if let Some(override_value) = max_tokens_override {
        let clamped = override_value.clamp(64, 2048) as usize;
        return OutputPolicyDecision {
            max_tokens: clamped,
            reason: "cli override".to_string(),
        };
    }

    describe_output_policy(profile, prompt)
}

fn print_model_karma_stats() {
    let registry = ModelRegistry::new();
    let mut stats = ranked_models();

    for descriptor in registry.list() {
        if !stats.iter().any(|entry| entry.model_id == descriptor.id) {
            stats.push(ModelKarma::new(descriptor.id.clone()));
        }
    }

    stats.sort_by(|left, right| {
        right
            .karma_score()
            .partial_cmp(&left.karma_score())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.model_id.cmp(&right.model_id))
    });

    println!("model | score | latency | success rate | semantic | runs");
    for entry in stats {
        println!(
            "{} | {:.3} | {:.3} | {:.1}% | {:.1}% | {}",
            entry.model_id,
            entry.karma_score(),
            entry.latency_score,
            entry.accuracy_score * 100.0,
            entry.semantic_success_rate * 100.0,
            entry.total_runs
        );
    }
}

fn print_distributed_task_stats(metrics: &DistributedTaskMetrics) {
    println!("metric | value");
    println!("total_tasks | {}", metrics.total_tasks);
    println!("failed_tasks | {}", metrics.failed_tasks);
    println!("retries_count | {}", metrics.retries_count);
    println!("fallback_count | {}", metrics.fallback_count);
    println!("avg_latency_ms | {:.1}", metrics.avg_latency_ms);
}

fn print_task_trace_entry(trace: &TaskTrace) {
    println!("task_id: {}", trace.task_id);
    println!(
        "node_history: {}",
        if trace.node_history.is_empty() {
            "-".to_string()
        } else {
            trace.node_history.join(" -> ")
        }
    );
    println!(
        "model_history: {}",
        if trace.model_history.is_empty() {
            "-".to_string()
        } else {
            trace.model_history.join(" -> ")
        }
    );
    println!("retries: {}", trace.retries);
    println!("fallbacks: {}", trace.fallbacks);
    println!("total_latency_ms: {}", trace.total_latency_ms);
}

fn debug_task_log(
    flags: DebugFlags,
    peer_id: &str,
    cluster_id: Option<&str>,
    model_id: &str,
    task_id: &str,
    attempt_id: &str,
    message: &str,
) {
    if flags.tasks {
        println!(
            "[DebugTasks] peer_id={} cluster_id={} model_id={} task_id={} attempt_id={} {}",
            peer_id,
            cluster_id.unwrap_or("-"),
            model_id,
            task_id,
            attempt_id,
            message
        );
    }
}

fn debug_scheduler_log(flags: DebugFlags, message: impl AsRef<str>) {
    if flags.scheduler {
        println!("[DebugScheduler] {}", message.as_ref());
    }
}

fn debug_network_log(flags: DebugFlags, message: impl AsRef<str>) {
    if flags.network {
        println!("[DebugNetwork] {}", message.as_ref());
    }
}

fn finalize_distributed_task_observability(
    trace_task_id: &str,
    started_at: Option<tokio::time::Instant>,
    failed: bool,
) -> (Option<TaskTrace>, DistributedTaskMetrics) {
    if let Some(started_at) = started_at {
        let latency_ms = started_at.elapsed().as_millis() as u64;
        let _ = record_task_latency(trace_task_id, latency_ms);
        let _ = record_distributed_task_latency(latency_ms);
    }

    if failed {
        let _ = record_distributed_task_failed();
    }

    let metrics = distributed_task_metrics();
    log_observability_event(
        LogLevel::Info,
        "metrics_snapshot",
        trace_task_id,
        Some(trace_task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("total_tasks".to_string(), metrics.total_tasks.into());
            fields.insert("failed_tasks".to_string(), metrics.failed_tasks.into());
            fields.insert("retries_count".to_string(), metrics.retries_count.into());
            fields.insert("fallback_count".to_string(), metrics.fallback_count.into());
            fields.insert("avg_latency_ms".to_string(), metrics.avg_latency_ms.into());
            fields.insert("failed".to_string(), failed.into());
            fields
        },
    );

    (task_trace(trace_task_id), metrics)
}

fn is_control_plane_mode(mode: &NodeMode) -> bool {
    matches!(
        mode,
        NodeMode::ModelsList
            | NodeMode::ModelsStats
            | NodeMode::ModelsDownload { .. }
            | NodeMode::ModelsRemove { .. }
            | NodeMode::TasksStats
            | NodeMode::TasksTrace { .. }
    )
}

fn validate_models_for_advertising(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
) -> Vec<String> {
    let mut validated = Vec::new();

    for model_id in storage.list_local_models() {
        if let Some(requirements) = ModelRequirements::for_model(&model_id) {
            if !can_node_run_model(node_caps, &requirements) {
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
                continue;
            }
        }

        let Some(model_desc) = registry.get(&model_id) else {
            println!(
                "[Health] Skipping advertisement for {}: missing registry descriptor",
                model_id
            );
            continue;
        };

        let engine = RealInferenceEngine::new(ModelStorage::new());
        match engine.load_model(&model_id, &model_desc.hash) {
            Ok(()) => {
                log_observability_event(
                    LogLevel::Info,
                    "model_validated",
                    "startup",
                    None,
                    Some(&model_id),
                    None,
                    Map::new(),
                );
                validated.push(model_id)
            }
            Err(error) => {
                println!(
                    "[Health] Skipping advertisement for {}: backend validation failed ({})",
                    model_id, error
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
                        fields.insert("reason".to_string(), error.into());
                        fields
                    },
                );
            }
        }
    }

    validated
}

fn log_observability_event(
    level: LogLevel,
    event: &str,
    trace_id: &str,
    task_id: Option<&str>,
    model_id: Option<&str>,
    error_code: Option<&str>,
    fields: Map<String, Value>,
) {
    let mut entry = StructuredLogEntry::new(level, event, trace_id, "-");
    if let Some(task_id) = task_id {
        entry = entry.with_task_id(task_id.to_string());
    }
    if let Some(model_id) = model_id {
        entry = entry.with_model_id(model_id.to_string());
    }
    if let Some(error_code) = error_code {
        entry = entry.with_error_code(error_code.to_string());
    }
    entry.fields = fields;
    let _ = log_structured(entry);
}

fn health_fields(peer_id: &str, health: &NodeHealth) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("peer_id".to_string(), peer_id.into());
    fields.insert(
        "success_rate".to_string(),
        (health.success_rate as f64).into(),
    );
    fields.insert(
        "avg_latency_ms".to_string(),
        (health.avg_latency_ms as f64).into(),
    );
    fields.insert("failure_count".to_string(), health.failure_count.into());
    fields.insert("timeout_count".to_string(), health.timeout_count.into());
    fields.insert("blacklisted".to_string(), health.is_blacklisted().into());
    if let Some(last_success) = health.last_success_timestamp {
        fields.insert("last_success_timestamp".to_string(), last_success.into());
    }
    fields
}

fn log_health_update(
    trace_id: &str,
    peer_id: &str,
    model_id: Option<&str>,
    health: &NodeHealth,
    error_code: Option<&str>,
) {
    log_observability_event(
        LogLevel::Info,
        "health_update",
        trace_id,
        None,
        model_id,
        error_code,
        health_fields(peer_id, health),
    );

    if health.is_blacklisted() {
        log_observability_event(
            LogLevel::Warn,
            "node_blacklisted",
            trace_id,
            None,
            model_id,
            Some(NODE_BLACKLISTED_001),
            health_fields(peer_id, health),
        );
    } else if health.last_success_timestamp.is_some() {
        log_observability_event(
            LogLevel::Info,
            "node_recovered",
            trace_id,
            None,
            model_id,
            None,
            health_fields(peer_id, health),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StartupMathError {
    operation: &'static str,
    operand_a: u64,
    operand_b: u64,
    reason: &'static str,
}

impl StartupMathError {
    fn new(operation: &'static str, operand_a: u64, operand_b: u64, reason: &'static str) -> Self {
        Self {
            operation,
            operand_a,
            operand_b,
            reason,
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} failed (a={}, b={}, reason={})",
            self.operation, self.operand_a, self.operand_b, self.reason
        )
    }
}

fn checked_sub_u16(
    operation: &'static str,
    a: u16,
    b: u16,
    reason: &'static str,
) -> Result<u16, StartupMathError> {
    a.checked_sub(b)
        .ok_or_else(|| StartupMathError::new(operation, a as u64, b as u64, reason))
}

fn checked_sub_usize(
    operation: &'static str,
    a: usize,
    b: usize,
    reason: &'static str,
) -> Result<usize, StartupMathError> {
    a.checked_sub(b)
        .ok_or_else(|| StartupMathError::new(operation, a as u64, b as u64, reason))
}

fn compute_metrics_port(worker_port: u16) -> Result<u16, StartupMathError> {
    let offset = checked_sub_u16(
        "worker_port_minus_base",
        worker_port,
        9000,
        "worker_port_below_metrics_base",
    )?;

    9090u16.checked_add(offset).ok_or_else(|| {
        StartupMathError::new(
            "metrics_port_plus_offset",
            9090,
            offset as u64,
            "metrics_port_out_of_range",
        )
    })
}

fn compute_active_tasks(
    max_concurrent: usize,
    available_slots: usize,
) -> Result<usize, StartupMathError> {
    checked_sub_usize(
        "max_concurrent_minus_available_slots",
        max_concurrent,
        available_slots,
        "available_slots_exceeds_max_concurrent",
    )
}

fn resource_policy_value(resource_policy: &ResourcePolicy) -> Value {
    serde_json::json!({
        "cpu_cores": resource_policy.cpu_cores,
        "max_cpu_load": resource_policy.max_cpu_load,
        "ram_limit_gb": resource_policy.ram_limit_gb,
        "gpu_enabled": resource_policy.gpu_enabled,
        "disk_limit_gb": resource_policy.disk_limit_gb,
        "disk_path": resource_policy.disk_path,
    })
}

fn emit_worker_startup_overflow_event(
    trace_id: &str,
    node_id: &str,
    peer_id: &str,
    port: u16,
    resource_policy: &ResourcePolicy,
    worker_slots: usize,
    max_concurrent: usize,
    available_slots: usize,
    error: &StartupMathError,
    fallback_behavior: &str,
) {
    let mut fields = Map::new();
    fields.insert("node_id".to_string(), node_id.into());
    fields.insert("peer_id".to_string(), peer_id.into());
    fields.insert("port".to_string(), (port as u64).into());
    fields.insert(
        "resource_policy".to_string(),
        resource_policy_value(resource_policy),
    );
    fields.insert(
        "slots".to_string(),
        serde_json::json!({
            "worker_slots": worker_slots,
            "max_concurrent": max_concurrent,
            "available_slots": available_slots,
        }),
    );
    fields.insert("operation".to_string(), error.operation.into());
    fields.insert("operand_a".to_string(), error.operand_a.into());
    fields.insert("operand_b".to_string(), error.operand_b.into());
    fields.insert("reason".to_string(), error.reason.into());
    fields.insert("fallback_behavior".to_string(), fallback_behavior.into());

    log_observability_event(
        LogLevel::Error,
        "worker_startup_invalid_math",
        trace_id,
        None,
        None,
        Some(WORKER_STARTUP_OVERFLOW_001),
        fields,
    );
}

fn cluster_label_for_latency(latency_ms: f64) -> &'static str {
    if latency_ms < 10.0 {
        "LOCAL"
    } else if latency_ms < 50.0 {
        "NEARBY"
    } else {
        "REMOTE"
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
        println!("║       IaMine Worker v0.6         ║");
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
                emit_worker_startup_overflow_event(
                    "startup",
                    &node_identity.node_id,
                    &peer_id.to_string(),
                    worker_port,
                    &resource_policy,
                    worker_slots,
                    pool.max_concurrent,
                    pool.available_slots(),
                    &error,
                    "continue_without_metrics_server",
                );
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

    // Estado
    let mut pending_tasks: Vec<TaskRequest> = Vec::new();
    let mut waiting_for_response = false;
    let mut completed = 0usize;
    let mut total_tasks = 0usize;
    let mut connected_peer: Option<PeerId> = None;
    let mut known_workers: HashSet<PeerId> = HashSet::new();
    // Map de origin_peer_id → PeerId para direct result routing
    let mut origin_peer_map: std::collections::HashMap<String, PeerId> =
        std::collections::HashMap::new();
    let mut tasks_sent = false;

    match &mode {
        NodeMode::Client {
            task_type, data, ..
        } => {
            pending_tasks.push(TaskRequest::legacy(
                uuid_simple(),
                task_type.clone(),
                data.clone(),
            ));
            total_tasks = 1;
        }
        NodeMode::Stress { count, .. } => {
            total_tasks = *count;
            for i in 0..*count {
                pending_tasks.push(TaskRequest::legacy(
                    format!("stress_{:03}", i + 1),
                    "reverse_string".to_string(),
                    format!("iamine_{}", i + 1),
                ));
            }
            println!("🔥 Stress test: {} tareas preparadas\n", count);
        }
        _ => {}
    }

    // ← v0.6: Si es modo Infer, preparar solicitud de inferencia
    let mut infer_request_id: Option<String> = None;
    let mut infer_broadcast_sent = false;
    let mut pending_inference: std::collections::HashMap<String, tokio::time::Instant> =
        std::collections::HashMap::new();
    let mut infer_started_at: Option<tokio::time::Instant> = None;
    let mut distributed_infer_state: Option<DistributedInferState> = None;

    // v0.6.2: estado de streaming ordenado
    let mut token_buffer: HashMap<u32, String> = HashMap::new();
    let mut next_token_idx: u32 = 0;
    let mut rendered_output = String::new();

    if let NodeMode::Infer {
        prompt,
        model_id,
        max_tokens_override,
        ..
    } = &mode
    {
        let local_available = model_storage.list_local_models();
        let resolution = resolve_policy_for_prompt(prompt, model_id.as_deref(), &local_available);
        let profile = resolution.profile;
        let candidates = resolution.candidate_models;
        let selected = resolution.selected_model;
        let semantic_prompt = resolution.semantic_prompt;
        let output_policy = resolve_output_policy(&profile, &semantic_prompt, *max_tokens_override);
        println!(
            "🧠 Distributing inference: model={} prompt='{}'",
            selected, semantic_prompt
        );
        println!("   Candidates: {}", candidates.join(", "));
        println!(
            "   [OutputPolicy] max_tokens: {} (reason: {})",
            output_policy.max_tokens, output_policy.reason
        );
        let infer_state =
            DistributedInferState::new(prompt.clone(), model_id.clone(), *max_tokens_override);
        let _ = log_structured(prompt_log_entry(
            &infer_state.trace_task_id,
            &semantic_prompt,
            &format!("{:?}", profile.language),
            &format!("{:?}", profile.task_type),
            profile.confidence,
        ));
        println!("   Task ID: {}", infer_state.trace_task_id);
        println!("   Request ID: {}", infer_state.current_request_id);
        infer_request_id = Some(infer_state.current_request_id.clone());
        infer_started_at = Some(tokio::time::Instant::now());
        let _ = record_distributed_task_started();
        distributed_infer_state = Some(infer_state);
    }

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
                            emit_worker_startup_overflow_event(
                                "startup",
                                &node_identity.node_id,
                                &peer_id.to_string(),
                                worker_port,
                                &resource_policy,
                                worker_slots,
                                pool.max_concurrent,
                                available_slots,
                                &error,
                                "exit_cleanly_invalid_startup_state",
                            );
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
                        m.direct_peers = known_workers.len();
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
                if matches!(mode, NodeMode::Infer { .. }) && !infer_broadcast_sent {
                    if let Some(infer_state) = distributed_infer_state.as_mut() {
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
                                let selected_cluster_id = reg
                                    .all_nodes()
                                    .into_iter()
                                    .find(|(peer_id, _)| peer_id == &decision.peer_id)
                                    .and_then(|(_, capability)| capability.cluster_id);
                                (
                                    decision.peer_id,
                                    decision.model_id,
                                    decision.score,
                                    selected_cluster_id,
                                )
                            });
                        drop(reg);

                        if let Some((best_peer, routed_model, node_score, selected_cluster_id)) =
                            selected
                        {
                            let rid = infer_state.current_request_id.clone();
                            let trace_task_id = infer_state.trace_task_id.clone();
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
                                connected_peer_count: known_workers.len(),
                                mesh_peer_count: pubsub_topics.mesh_peer_count(),
                                topic_peer_count,
                                joined_task_topic: pubsub_topics.joined(TASK_TOPIC),
                                joined_direct_topic: pubsub_topics.joined(DIRECT_INF_TOPIC),
                                joined_results_topic: pubsub_topics.joined(RESULTS_TOPIC),
                                selected_topic: DIRECT_INF_TOPIC,
                            };
                            emit_dispatch_context_event(
                                &trace_task_id,
                                &rid,
                                &routed_model,
                                &candidates,
                                readiness_snapshot.connected_peer_count,
                                readiness_snapshot.topic_peer_count,
                                DIRECT_INF_TOPIC,
                                Some(&best_peer),
                            );
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
                                let elapsed_ms = infer_started_at
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
                                        &trace_task_id,
                                        &rid,
                                        &routed_model,
                                        DIRECT_INF_TOPIC,
                                        topic_peer_count,
                                        payload_size,
                                        &message_id,
                                        Some(&best_peer),
                                    );
                                    metrics
                                        .write()
                                        .await
                                        .routing_decision(start.elapsed().as_millis() as u64);
                                    infer_broadcast_sent = true;
                                    waiting_for_response = true;
                                    pending_inference.insert(
                                        rid.clone(),
                                        tokio::time::Instant::now(),
                                    );
                                    infer_request_id = Some(rid.clone());
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
                                        &trace_task_id,
                                        &rid,
                                        &routed_model,
                                        DIRECT_INF_TOPIC,
                                        topic_peer_count,
                                        payload_size,
                                        &error_text,
                                        Some(&best_peer),
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
                        } else if infer_started_at
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
                                connected_peer_count: known_workers.len(),
                                mesh_peer_count: pubsub_topics.mesh_peer_count(),
                                topic_peer_count,
                                joined_task_topic: pubsub_topics.joined(TASK_TOPIC),
                                joined_direct_topic: pubsub_topics.joined(DIRECT_INF_TOPIC),
                                joined_results_topic: pubsub_topics.joined(RESULTS_TOPIC),
                                selected_topic: TASK_TOPIC,
                            };
                            emit_dispatch_context_event(
                                &trace_task_id,
                                &rid,
                                &selected_model,
                                &candidates,
                                readiness_snapshot.connected_peer_count,
                                readiness_snapshot.topic_peer_count,
                                TASK_TOPIC,
                                None,
                            );
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
                                let elapsed_ms = infer_started_at
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
                                        &trace_task_id,
                                        &rid,
                                        &selected_model,
                                        TASK_TOPIC,
                                        topic_peer_count,
                                        payload_size,
                                        &message_id,
                                        None,
                                    );
                                    let _ = record_distributed_task_fallback();
                                    infer_broadcast_sent = true;
                                    waiting_for_response = true;
                                    pending_inference.insert(rid.clone(), tokio::time::Instant::now());
                                    infer_request_id = Some(rid.clone());
                                    println!(
                                        "↪️  Fallback broadcast enviado: task_id={} attempt_id={} message_id={}",
                                        trace_task_id, rid, message_id
                                    );
                                }
                                Err(error) => {
                                    let error_text = error.to_string();
                                    emit_task_publish_failed_event(
                                        &trace_task_id,
                                        &rid,
                                        &selected_model,
                                        TASK_TOPIC,
                                        topic_peer_count,
                                        payload_size,
                                        &error_text,
                                        None,
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
                    if let Some(rid) = infer_request_id.clone() {
                        if let Some(t0) = pending_inference.get(&rid) {
                            if t0.elapsed().as_millis() as u64 >= INFER_TIMEOUT_MS {
                                let failure_kind = FailureKind::Timeout;
                                eprintln!(
                                    "\n[Fault] task_id={} attempt_id={} kind={:?} timeout_ms={}",
                                    distributed_infer_state
                                        .as_ref()
                                        .map(|state| state.trace_task_id.as_str())
                                        .unwrap_or("-"),
                                    rid,
                                    failure_kind,
                                    INFER_TIMEOUT_MS
                                );
                                if let Some(infer_state) = distributed_infer_state.as_mut() {
                                    let trace_task_id = infer_state.trace_task_id.clone();
                                    if let Some(current_peer) = infer_state.current_peer.as_deref() {
                                        if let Some(health) =
                                            registry.write().await.record_timeout(current_peer)
                                        {
                                            log_health_update(
                                                &trace_task_id,
                                                current_peer,
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
                                        infer_state.current_model.as_deref(),
                                        Some(TASK_TIMEOUT_001),
                                        {
                                            let mut fields = Map::new();
                                            fields.insert("attempt_id".to_string(), rid.clone().into());
                                            fields.insert("latency_ms".to_string(), INFER_TIMEOUT_MS.into());
                                            fields.insert("retry".to_string(), true.into());
                                            fields.insert("error_kind".to_string(), "timeout".into());
                                            fields.insert("recoverable".to_string(), true.into());
                                            if let Some(current_peer) = infer_state.current_peer.as_deref() {
                                                fields.insert("peer_id".to_string(), current_peer.into());
                                            }
                                            fields
                                        },
                                    );
                                    let mut preview_retry = infer_state.retry_state.clone();
                                    preview_retry.record_failure(
                                        infer_state.current_peer.as_deref(),
                                        infer_state.current_model.as_deref(),
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
                                    pending_inference.remove(&rid);
                                    clear_stream_state(
                                        &mut token_buffer,
                                        &mut next_token_idx,
                                        &mut rendered_output,
                                    );
                                    if retried {
                                        emit_retry_scheduled_event(
                                            &trace_task_id,
                                            &infer_state.current_request_id,
                                            failed_model.as_deref(),
                                            failed_peer.as_deref(),
                                            retry_target.as_ref().map(|target| target.peer_id.as_str()),
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
                                        infer_request_id =
                                            Some(infer_state.current_request_id.clone());
                                        infer_broadcast_sent = false;
                                        waiting_for_response = false;
                                        continue;
                                    }

                                    let (trace, aggregate_metrics) =
                                        finalize_distributed_task_observability(
                                            &trace_task_id,
                                            infer_started_at,
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

            event = swarm.select_next_some() => match event {

                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("🌐 Escuchando en: {}", address);
                    swarm.behaviour_mut().kademlia.add_address(&peer_id, address);
                }

                SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Discovered(peers))) => {
                    for (pid, addr) in peers {
                        if pid != peer_id {
                            println!("🔍 mDNS descubrió: {} @ {}", pid, addr);
                            log_observability_event(
                                LogLevel::Info,
                                "peer_discovered",
                                "network",
                                None,
                                None,
                                None,
                                {
                                    let mut fields = Map::new();
                                    fields.insert("peer_id".to_string(), pid.to_string().into());
                                    fields.insert("address".to_string(), addr.to_string().into());
                                    fields
                                },
                            );
                            debug_network_log(
                                debug_flags,
                                format!("mdns discovered peer_id={} addr={}", pid, addr),
                            );
                            swarm.behaviour_mut().kademlia.add_address(&pid, addr.clone());
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&pid);
                            known_workers.insert(pid);
                            if is_client && connected_peer.is_none() && !tasks_sent {
                                let _ = swarm.dial(addr);
                            }
                        }
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Expired(peers))) => {
                    for (pid, _) in peers {
                        log_observability_event(
                            LogLevel::Warn,
                            "peer_disconnected",
                            "network",
                            None,
                            None,
                            Some(NET_PEER_DISCONNECTED_002),
                            {
                                let mut fields = Map::new();
                                fields.insert("peer_id".to_string(), pid.to_string().into());
                                fields
                            },
                        );
                        known_workers.remove(&pid);
                        pubsub_topics.unregister_peer(&pid);
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&pid);
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source,
                    message,
                    ..
                })) => {
                    let from_peer = propagation_source.to_string();
                    let message_topic = if message.topic == gossipsub::IdentTopic::new(TASK_TOPIC).hash() {
                        TASK_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(DIRECT_INF_TOPIC).hash() {
                        DIRECT_INF_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(RESULTS_TOPIC).hash() {
                        RESULTS_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(CAP_TOPIC).hash() {
                        CAP_TOPIC
                    } else {
                        "unknown"
                    };

                    // 1) Procesar capabilities por TOPIC (no por msg_type)
                    if message.topic == gossipsub::IdentTopic::new(CAP_TOPIC).hash() {
                        if let Ok(hb) = serde_json::from_slice::<NodeCapabilityHeartbeat>(&message.data) {
                            let _ = registry.write().await.update_from_heartbeat(hb);
                        }
                        continue;
                    }

                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&message.data) {
                        let msg_type = msg["type"].as_str().unwrap_or("");

                        // ...existing rate limiter...

                        match msg_type {
                            "TaskOffer" if matches!(mode, NodeMode::Worker) => {
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let task_type = msg["task_type"].as_str().unwrap_or("").to_string();
                                let origin_peer = msg["origin_peer"].as_str().unwrap_or("").to_string();

                                if task_cache.is_duplicate(&task_id) {
                                    println!("⚡ [Cache] Tarea {} ya vista", &task_id[..8.min(task_id.len())]);
                                } else if !capabilities.supports(&task_type) {
                                    println!("🚫 [Worker] No soporta '{}'", task_type);
                                } else {
                                    let available = pool.available_slots();
                                    if available > 0 {
                                        println!("📋 [Worker] Bid para tarea {} ({} slots)", task_id, available);
                                        let bid = serde_json::json!({
                                            "type": "TaskBid",
                                            "task_id": task_id,
                                            "worker_id": peer_id.to_string(),
                                            "origin_peer": origin_peer,
                                            "reputation_score": queue.reputation().await.reputation_score,
                                            "available_slots": available,
                                            "estimated_ms": 10,
                                        });
                                        let _ = swarm.behaviour_mut().gossipsub.publish(
                                            gossipsub::IdentTopic::new("iamine-bids"),
                                            serde_json::to_vec(&bid).unwrap(),
                                        );
                                    }
                                }
                            }

                            "TaskBid" if matches!(mode, NodeMode::Broadcast { .. }) => {
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let worker_id = msg["worker_id"].as_str().unwrap_or("").to_string();
                                let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
                                let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
                                let est_ms = msg["estimated_ms"].as_u64().unwrap_or(1000);
                                let latency = peer_tracker.get_latency(&worker_id);

                                println!("📨 [Scheduler] Bid: worker={}... rep={} slots={} latency={:.1}ms",
                                    &worker_id[..8.min(worker_id.len())], rep, slots, latency);

                                if let Some(winner) = scheduler.receive_bid(&task_id, worker_id.clone(), rep, slots, est_ms).await {
                                    println!("🏆 Asignando {} a {}...", &task_id[..8], &winner[..8.min(winner.len())]);

                                    let deadline_ms = 30_000u64;

                                    // ← Guardar peer_id del winner para direct result routing
                                    if let Some(winner_peer) = known_workers.iter()
                                        .find(|p| p.to_string().starts_with(&winner[..8.min(winner.len())]))
                                        .copied()
                                    {
                                        origin_peer_map.insert(task_id.clone(), winner_peer);
                                    }

                                    let assign = serde_json::json!({
                                        "type": "TaskAssign",
                                        "task_id": task_id,
                                        "assigned_worker": winner,
                                        "origin_peer": peer_id.to_string(),
                                        "deadline_ms": deadline_ms,
                                        "task_type": if let NodeMode::Broadcast { task_type, .. } = &mode { task_type } else { "" },
                                        "data": if let NodeMode::Broadcast { data, .. } = &mode { data } else { "" },
                                    });
                                    let _ = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new("iamine-assign"),
                                        serde_json::to_vec(&assign).unwrap(),
                                    );

                                    // ← Timeout recovery COMPLETO: rebroadcast si expira
                                    let rebroadcast_task_id = task_id.clone();
                                    let scheduler_ref = Arc::clone(&scheduler);
                                    let task_type_clone = if let NodeMode::Broadcast { task_type, .. } = &mode {
                                        task_type.clone()
                                    } else { String::new() };
                                    let data_clone = if let NodeMode::Broadcast { data, .. } = &mode {
                                        data.clone()
                                    } else { String::new() };
                                    let origin_str = peer_id.to_string();

                                    // Clonar el gossipsub handle para el spawn
                                    // Usamos un canal para señalizar rebroadcast
                                    let (rebroadcast_tx, mut rebroadcast_rx) = tokio::sync::mpsc::channel::<serde_json::Value>(1);

                                    tokio::spawn(async move {
                                        tokio::time::sleep(Duration::from_millis(deadline_ms)).await;
                                        let stats = scheduler_ref.stats().await;
                                        // Si aún hay tareas asignadas (no completadas)
                                        if stats.1 > 0 {
                                            println!("⏱️ [Recovery] Tarea {} expiró — rebroadcasting...", rebroadcast_task_id);
                                            let offer = serde_json::json!({
                                                "type": "TaskOffer",
                                                "task_id": format!("{}r", rebroadcast_task_id), // nueva ID
                                                "task_type": task_type_clone,
                                                "data": data_clone,
                                                "requester_id": origin_str,
                                                "origin_peer": origin_str,
                                                "is_retry": true,
                                            });
                                            let _ = rebroadcast_tx.send(offer).await;
                                        }
                                    });

                                    // Procesar rebroadcast en el loop principal
                                    if let Ok(offer) = rebroadcast_rx.try_recv() {
                                        let _ = swarm.behaviour_mut().gossipsub.publish(
                                            task_topic.clone(),
                                            serde_json::to_vec(&offer).unwrap(),
                                        );
                                    }
                                }
                            }

                            "TaskAssign" if matches!(mode, NodeMode::Worker) => {
                                let assigned = msg["assigned_worker"].as_str().unwrap_or("");
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let task_type = msg["task_type"].as_str().unwrap_or("").to_string();
                                let data = msg["data"].as_str().unwrap_or("").to_string();
                                let origin_peer_str = msg["origin_peer"].as_str().unwrap_or("").to_string();
                                let deadline_ms = msg["deadline_ms"].as_u64().unwrap_or(30_000);

                                if assigned == peer_id.to_string() {
                                    println!("🎯 [Worker] ¡Asignado! {} (deadline: {}ms)", task_id, deadline_ms);

                                    let _origin_pid = known_workers.iter()  // ← _ prefix
                                        .find(|p| p.to_string() == origin_peer_str)
                                        .copied();

                                    let queue_ref = Arc::clone(&queue);
                                    let metrics_ref = Arc::clone(&metrics);
                                    let _worker_id_str = peer_id.to_string(); // ← _ prefix

                                    tokio::spawn(async move {
                                        let exec = async {
                                            let _ = queue_ref.push(task_id.clone(), task_type, data).await;
                                            queue_ref.outcome_rx.lock().await.recv().await
                                        };

                                        match tokio::time::timeout(Duration::from_millis(deadline_ms), exec).await {
                                            Ok(Some(outcome)) => {
                                                let success = matches!(outcome.status, task_queue::OutcomeStatus::Success);
                                                {
                                                    let mut m = metrics_ref.write().await;
                                                    if success { m.task_success(0); } else { m.task_failed(); }
                                                }
                                                println!("✅ [Worker] Tarea {} completada (success={})", outcome.task_id, success);
                                                let rep = queue_ref.reputation().await;
                                                println!("⭐ Reputación: {}/100", rep.reputation_score);

                                                // ← TaskResult directo al origin (no gossip)
                                                // El PeerId lo resolvemos en el evento ResultResponse
                                                println!("📤 [Worker] Resultado listo → origin {}", &origin_peer_str[..8.min(origin_peer_str.len())]);
                                            }
                                            Ok(None) => eprintln!("❌ [Worker] Canal cerrado"),
                                            Err(_) => {
                                                eprintln!("⏱️ [Worker] Deadline expirado {}ms", deadline_ms);
                                                metrics_ref.write().await.task_timed_out();
                                            }
                                        }
                                    });
                                } else {
                                    println!("⏭️  [Worker] Tarea {} → otro worker ganó", task_id);
                                }
                            }

                            "TaskCancel" if matches!(mode, NodeMode::Worker) => {
                                let task_id = msg["task_id"].as_str().unwrap_or("");
                                let reason = msg["reason"].as_str().unwrap_or("sin razón");
                                println!("🚫 [Worker] Tarea {} cancelada: {}", task_id, reason);
                            }

                            "Heartbeat" => {
                                let worker_id = msg["peer_id"].as_str().unwrap_or("");
                                let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
                                let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
                                let uptime = msg["uptime_secs"].as_u64().unwrap_or(0);
                                peer_tracker.update_heartbeat(worker_id, slots, rep);
                                println!("💓 Heartbeat {}... slots={} rep={} uptime={}s peers={}",
                                    &worker_id[..8.min(worker_id.len())], slots, rep, uptime,
                                    peer_tracker.peer_count());
                                let mut m = metrics.write().await;
                                m.network_peers = peer_tracker.peer_count();
                                m.mesh_peers = peer_tracker.peer_count();
                            }

                            // ═══════════════════════════════════════════
                            // v0.6: DISTRIBUTED INFERENCE
                            // ═══════════════════════════════════════════

                            "InferenceRequest" if matches!(mode, NodeMode::Worker) => {
                                let request_id = msg["request_id"].as_str().unwrap_or("").to_string();
                                let task_id = msg["task_id"]
                                    .as_str()
                                    .filter(|value| !value.trim().is_empty())
                                    .unwrap_or(request_id.as_str())
                                    .to_string();
                                let attempt_id = msg["attempt_id"]
                                    .as_str()
                                    .filter(|value| !value.trim().is_empty())
                                    .unwrap_or(request_id.as_str())
                                    .to_string();
                                let model_id = msg["model_id"].as_str().unwrap_or("").to_string();
                                let prompt = msg["prompt"].as_str().unwrap_or("").to_string();
                                let max_tokens = msg["max_tokens"].as_u64().unwrap_or(200) as u32;
                                let temperature = msg["temperature"].as_f64().unwrap_or(0.7) as f32;
                                let _requester = msg["requester_peer"].as_str().unwrap_or("").to_string();
                                let local_model_available = model_storage.has_model(&model_id);
                                emit_worker_task_message_received_event(
                                    &task_id,
                                    &attempt_id,
                                    message_topic,
                                    &from_peer,
                                    "ok",
                                    &model_id,
                                    local_model_available,
                                );
                                emit_direct_inference_request_received_event(
                                    &task_id,
                                    &attempt_id,
                                    &model_id,
                                    &from_peer,
                                    &peer_id.to_string(),
                                    local_model_available,
                                );

                                println!("🧠 [Worker] InferenceRequest: model={} prompt='{}'",
                                    model_id, &prompt[..prompt.len().min(40)]);

                                // Check model installed
                                if !local_model_available {
                                    println!("   ⚠️ Modelo {} no instalado — ignorando", model_id);
                                    continue;
                                }

                                // v0.5.4: Validar requisitos de hardware
                                if let Some(req) = ModelRequirements::for_model(&model_id) {
                                    if !can_node_run_model(&node_caps, &req) {
                                        println!("   ⚠️ Hardware insuficiente para {} — ignorando", model_id);
                                        continue;
                                    }
                                }

                                let engine_ref = Arc::clone(&inference_engine);
                                let metrics_ref = Arc::clone(&metrics);
                                let registry_clone = ModelRegistry::new();
                                let peer_id_str = peer_id.to_string();
                                let request_id_clone = request_id.clone();
                                let model_id_for_inference = model_id.clone();

                                // Crear canal para tokens streaming
                                let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);

                                // Spawn inference execution
                                let inference_handle = tokio::spawn(async move {
                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id_for_inference.clone(),
                                        prompt,
                                        max_tokens,
                                        temperature,
                                    };
                                    let daemon_socket = daemon_socket_path();
                                    let result = if daemon_is_available(&daemon_socket).await {
                                        match infer_via_daemon(&daemon_socket, req, |token| {
                                            let _ = token_tx.try_send(token);
                                        }).await {
                                            Ok(response) => response.result,
                                            Err(e) => {
                                                return InferenceTaskResult::failure(
                                                    request_id_clone.clone(),
                                                    model_id_for_inference.clone(),
                                                    peer_id_str.clone(),
                                                    e,
                                                );
                                            }
                                        }
                                    } else {
                                        let eng = Arc::clone(&engine_ref);
                                        let hash = registry_clone.get(&model_id_for_inference)
                                            .map(|m| m.hash.clone())
                                            .unwrap_or_default();
                                        if let Err(e) = eng.load_model(&model_id_for_inference, &hash) {
                                            return InferenceTaskResult::failure(
                                                request_id_clone.clone(),
                                                model_id_for_inference.clone(),
                                                peer_id_str.clone(),
                                                e,
                                            );
                                        }

                                        eng.run_inference(req, Some(token_tx)).await
                                    };

                                    InferenceTaskResult::success(
                                        request_id_clone,
                                        model_id_for_inference,
                                        result.output,
                                        result.tokens_generated,
                                        result.truncated,
                                        result.continuation_steps,
                                        result.execution_ms,
                                        peer_id_str,
                                        result.accelerator_used,
                                    )
                                });

                                // Stream tokens via gossipsub while inference runs
                                let mut token_idx = 0u32;
                                while let Some(token) = token_rx.recv().await {
                                    let st = StreamedToken {
                                        request_id: request_id.clone(),
                                        token,
                                        index: token_idx,
                                        is_final: false,
                                    };
                                    let _ = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new(RESULTS_TOPIC),
                                        serde_json::to_vec(&st.to_gossip_json()).unwrap(),
                                    );
                                    token_idx += 1;
                                }

                                if let Ok(result) = inference_handle.await {
                                    {
                                        let mut m = metrics_ref.write().await;
                                        if result.success {
                                            m.inference_success(result.execution_ms, result.tokens_generated as u64);
                                        } else {
                                            m.inference_failed();
                                        }
                                    }
                                    let result_size = result.output.len();
                                    emit_worker_task_completed_event(
                                        &task_id,
                                        &attempt_id,
                                        &model_id,
                                        result_size,
                                        result.success,
                                        &peer_id.to_string(),
                                    );
                                    let mut result_payload = result.to_gossip_json();
                                    if let Some(map) = result_payload.as_object_mut() {
                                        map.insert("task_id".to_string(), task_id.clone().into());
                                        map.insert("attempt_id".to_string(), attempt_id.clone().into());
                                    }
                                    let payload_size = result_payload.to_string().len();
                                    let publish_result = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new(RESULTS_TOPIC),
                                        serde_json::to_vec(&result_payload).unwrap(),
                                    );
                                    match publish_result {
                                        Ok(message_id) => emit_worker_result_published_event(
                                            &task_id,
                                            &attempt_id,
                                            &model_id,
                                            RESULTS_TOPIC,
                                            payload_size,
                                            Some(&message_id.to_string()),
                                            &peer_id.to_string(),
                                        ),
                                        Err(error) => log_observability_event(
                                            LogLevel::Error,
                                            "result_publish_failed",
                                            &task_id,
                                            Some(&task_id),
                                            Some(&model_id),
                                            Some(TASK_DISPATCH_UNCONFIRMED_001),
                                            {
                                                let mut fields = Map::new();
                                                fields.insert("attempt_id".to_string(), attempt_id.clone().into());
                                                fields.insert("topic".to_string(), RESULTS_TOPIC.into());
                                                fields.insert("error".to_string(), error.to_string().into());
                                                fields
                                            },
                                        ),
                                    }
                                    println!("✅ [Worker] Inference completada: {} tokens en {}ms",
                                        result.tokens_generated, result.execution_ms);
                                }
                            }

                            "InferenceToken" if matches!(mode, NodeMode::Infer { .. }) => {
                                let rid = msg["request_id"].as_str().unwrap_or("");
                                if infer_request_id.as_deref() == Some(rid) {
                                    let idx = msg["index"].as_u64().unwrap_or(0) as u32;
                                    let token = msg["token"].as_str().unwrap_or("").to_string();

                                    token_buffer.insert(idx, token);

                                    // flush en orden
                                    while let Some(next) = token_buffer.remove(&next_token_idx) {
                                        print!("{}", next);
                                        rendered_output.push_str(&next); // <- nuevo
                                        let _ = std::io::Write::flush(&mut std::io::stdout());
                                        next_token_idx += 1;
                                    }
                                }
                            }

                            "InferenceResult" if matches!(mode, NodeMode::Infer { .. }) => {
                                let rid = msg["request_id"].as_str().unwrap_or("");
                                if infer_request_id.as_deref() == Some(rid) {
                                    let task_id = msg["task_id"]
                                        .as_str()
                                        .filter(|value| !value.trim().is_empty())
                                        .or_else(|| {
                                            distributed_infer_state
                                                .as_ref()
                                                .map(|state| state.trace_task_id.as_str())
                                        })
                                        .unwrap_or(rid)
                                        .to_string();
                                    let attempt_id = msg["attempt_id"]
                                        .as_str()
                                        .filter(|value| !value.trim().is_empty())
                                        .unwrap_or(rid)
                                        .to_string();
                                    let success = msg["success"].as_bool().unwrap_or(false);
                                    let tokens = msg["tokens_generated"].as_u64().unwrap_or(0);
                                    let truncated = msg["truncated"].as_bool().unwrap_or(false);
                                    let continuation_steps = msg["continuation_steps"].as_u64().unwrap_or(0);
                                    let ms = msg["execution_ms"].as_u64().unwrap_or(0);
                                    let worker = msg["worker_peer"].as_str().unwrap_or("unknown");
                                    let accel = msg["accelerator"].as_str().unwrap_or("unknown");
                                    let full_output = msg["output"].as_str().unwrap_or("").to_string();
                                    let latency_ms = infer_started_at
                                        .as_ref()
                                        .map(|started| started.elapsed().as_millis() as u64)
                                        .unwrap_or(ms);
                                    emit_result_received_event(
                                        &task_id,
                                        &attempt_id,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        worker,
                                        latency_ms,
                                    );
                                    let validation_status = if success {
                                        if !should_print_result_output(&full_output) {
                                            ResultStatus::Retryable(
                                                "empty distributed inference output".to_string(),
                                            )
                                        } else if let Some(infer_state) = distributed_infer_state.as_ref() {
                                            validate_result(
                                                infer_state
                                                    .current_semantic_prompt
                                                    .as_deref()
                                                    .unwrap_or(""),
                                                infer_state
                                                    .current_task_type
                                                    .unwrap_or(PromptTaskType::General),
                                                &full_output,
                                            )
                                        } else {
                                            ResultStatus::Valid
                                        }
                                    } else {
                                        ResultStatus::Retryable(
                                            msg["error"]
                                                .as_str()
                                                .unwrap_or("unknown inference failure")
                                                .to_string(),
                                        )
                                    };

                                    // v0.6.2: solo imprimir parte faltante, sin duplicar
                                    if success && should_print_result_output(&full_output) {
                                        if rendered_output.is_empty() {
                                            print!("{}", full_output);
                                        } else if full_output.starts_with(&rendered_output) {
                                            let suffix = &full_output[rendered_output.len()..];
                                            if !suffix.is_empty() {
                                                print!("{}", suffix);
                                            }
                                        }
                                        let _ = std::io::Write::flush(&mut std::io::stdout());
                                    }

                                    let retry_reason = match validation_status {
                                        ResultStatus::Valid => None,
                                        ResultStatus::Invalid(reason)
                                        | ResultStatus::Retryable(reason) => Some(reason),
                                    };

                                    if let Some(reason) = retry_reason {
                                        if let Some(health) =
                                            registry.write().await.record_failure(worker)
                                        {
                                            log_health_update(
                                                distributed_infer_state
                                                    .as_ref()
                                                    .map(|state| state.trace_task_id.as_str())
                                                    .unwrap_or("-"),
                                                worker,
                                                distributed_infer_state
                                                    .as_ref()
                                                    .and_then(|state| state.current_model.as_deref()),
                                                &health,
                                                Some(NODE_UNHEALTHY_002),
                                            );
                                        }
                                        let error_code = if full_output.trim().is_empty() {
                                            TASK_EMPTY_RESULT_003
                                        } else {
                                            TASK_FAILED_002
                                        };
                                        log_observability_event(
                                            LogLevel::Error,
                                            "task_failed",
                                            distributed_infer_state
                                                .as_ref()
                                                .map(|state| state.trace_task_id.as_str())
                                                .unwrap_or("-"),
                                            distributed_infer_state
                                                .as_ref()
                                                .map(|state| state.trace_task_id.as_str()),
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref()),
                                            Some(error_code),
                                            {
                                                let mut fields = Map::new();
                                                fields.insert(
                                                    "worker_peer_id".to_string(),
                                                    worker.to_string().into(),
                                                );
                                                fields.insert("attempt_id".to_string(), attempt_id.clone().into());
                                                fields.insert("reason".to_string(), reason.clone().into());
                                                fields.insert("retry".to_string(), true.into());
                                                fields.insert(
                                                    "error_kind".to_string(),
                                                    "validation_failure".into(),
                                                );
                                                fields.insert("recoverable".to_string(), true.into());
                                                fields
                                            },
                                        );
                                        println!(
                                            "\n[Fault] task_id={} attempt_id={} peer_id={} model_id={} reason={}",
                                            task_id,
                                            attempt_id,
                                            worker,
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref())
                                                .unwrap_or("-"),
                                            reason
                                        );
                                        let retry_target = if let Some(infer_state) =
                                            distributed_infer_state.as_ref()
                                        {
                                            let mut preview_retry = infer_state.retry_state.clone();
                                            preview_retry.record_failure(
                                                Some(worker),
                                                infer_state.current_model.as_deref(),
                                            );
                                            let reg = registry.read().await;
                                            select_retry_target(
                                                &reg,
                                                &IntelligentScheduler::new(),
                                                &infer_state.candidate_models,
                                                infer_state.local_cluster_id.as_deref(),
                                                &preview_retry,
                                            )
                                        } else {
                                            None
                                        };

                                        if let Some(infer_state) = distributed_infer_state.as_ref() {
                                            task_manager.fail(&infer_state.trace_task_id, &reason).await;
                                        }
                                        pending_inference.remove(rid);
                                        clear_stream_state(
                                            &mut token_buffer,
                                            &mut next_token_idx,
                                            &mut rendered_output,
                                        );

                                        if let Some(infer_state) = distributed_infer_state.as_mut() {
                                            let trace_task_id = infer_state.trace_task_id.clone();
                                            let failure_kind = if success {
                                                FailureKind::InvalidOutput
                                            } else {
                                                FailureKind::TaskFailure
                                            };
                                            let failed_model = infer_state.current_model.clone();
                                            if infer_state.schedule_retry(
                                                Some(worker),
                                                failed_model.as_deref(),
                                            ) && retry_target.is_some()
                                            {
                                                emit_retry_scheduled_event(
                                                    &trace_task_id,
                                                    &infer_state.current_request_id,
                                                    failed_model.as_deref(),
                                                    Some(worker),
                                                    retry_target
                                                        .as_ref()
                                                        .map(|target| target.peer_id.as_str()),
                                                    "validation_failure",
                                                );
                                                let target = retry_target.unwrap();
                                                println!(
                                                    "[Retry] task_id={} attempt_id={} kind={:?} peer_id={} model_id={}",
                                                    trace_task_id,
                                                    infer_state.current_request_id,
                                                    failure_kind,
                                                    target.peer_id,
                                                    target.model_id
                                                );
                                                infer_request_id =
                                                    Some(infer_state.current_request_id.clone());
                                                infer_broadcast_sent = false;
                                                waiting_for_response = false;
                                                continue;
                                            }

                                            let (trace, aggregate_metrics) =
                                                finalize_distributed_task_observability(
                                                    &trace_task_id,
                                                    infer_started_at,
                                                    true,
                                                );
                                            if let Some(trace) = trace {
                                                println!(
                                                    "[Metrics] latency={} retries={} fallbacks={}",
                                                    trace.total_latency_ms,
                                                    trace.retries,
                                                    trace.fallbacks
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

                                    if let Some(health) = registry.write().await.record_success(worker, ms) {
                                        log_health_update(
                                            distributed_infer_state
                                                .as_ref()
                                                .map(|state| state.trace_task_id.as_str())
                                                .unwrap_or("-"),
                                            worker,
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref()),
                                            &health,
                                            None,
                                        );
                                    }
                                    log_observability_event(
                                        LogLevel::Info,
                                        "task_completed",
                                        &task_id,
                                        Some(&task_id),
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        None,
                                        {
                                            let mut fields = Map::new();
                                            fields.insert(
                                                "worker_peer_id".to_string(),
                                                worker.to_string().into(),
                                            );
                                            fields.insert("attempt_id".to_string(), attempt_id.clone().into());
                                            fields.insert("latency_ms".to_string(), latency_ms.into());
                                            fields.insert("success".to_string(), true.into());
                                            fields
                                        },
                                    );
                                    println!("\n\n═══════════════════════════════════");
                                    if success && should_print_result_output(&full_output) {
                                        println!(
                                            "✅ Distributed inference completada: task_id={} attempt_id={} peer_id={} model_id={}",
                                            task_id,
                                            attempt_id,
                                            worker,
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref())
                                                .unwrap_or("-")
                                        );
                                        println!("   Worker:  {}...", &worker[..12.min(worker.len())]);
                                        println!("   Tokens:  {}", tokens);
                                        println!("   Truncated: {}", truncated);
                                        println!("   Continuations: {}", continuation_steps);
                                        println!("   Tiempo:  {}ms", ms);
                                        println!("   Accel:   {}", accel);
                                        if truncated {
                                            println!("[Warning] Output truncated at token budget");
                                        }
                                    } else {
                                        let err = msg["error"].as_str().unwrap_or("unknown");
                                        println!(
                                            "❌ Inference falló: task_id={} attempt_id={} peer_id={} error={}",
                                            task_id,
                                            attempt_id,
                                            worker,
                                            err
                                        );
                                    }
                                    println!("═══════════════════════════════════");
                                    clear_stream_state(
                                        &mut token_buffer,
                                        &mut next_token_idx,
                                        &mut rendered_output,
                                    );
                                    if let Some(infer_state) = distributed_infer_state.as_ref() {
                                        let (trace, aggregate_metrics) =
                                            finalize_distributed_task_observability(
                                                &infer_state.trace_task_id,
                                                infer_started_at,
                                                false,
                                            );
                                        if let Some(trace) = trace {
                                            println!(
                                                "[Trace] nodes={} models={}",
                                                trace.node_history.join(" -> "),
                                                trace.model_history.join(" -> ")
                                            );
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

                            "NodeCapabilities" => {
                                if let Ok(hb) = serde_json::from_value::<NodeCapabilityHeartbeat>(msg.clone()) {
                                    let _ = registry.write().await.update_from_heartbeat(hb);
                                }
                            }

                            "DirectInferenceRequest" if matches!(mode, NodeMode::Worker) => {
                                let target = msg["target_peer"].as_str().unwrap_or("");
                                if target != peer_id.to_string() {
                                    continue;
                                }

                                let request_id = msg["request_id"].as_str().unwrap_or("").to_string();
                                let task_id = msg["task_id"]
                                    .as_str()
                                    .filter(|value| !value.trim().is_empty())
                                    .unwrap_or(request_id.as_str())
                                    .to_string();
                                let attempt_id = msg["attempt_id"]
                                    .as_str()
                                    .filter(|value| !value.trim().is_empty())
                                    .unwrap_or(request_id.as_str())
                                    .to_string();
                                let model_id = msg["model"].as_str().unwrap_or("tinyllama-1b").to_string();
                                let prompt = msg["prompt"].as_str().unwrap_or("").to_string();
                                let max_tokens = msg["max_tokens"].as_u64().unwrap_or(200) as u32;
                                let local_model_available = model_storage.has_model(&model_id);
                                emit_worker_task_message_received_event(
                                    &task_id,
                                    &attempt_id,
                                    message_topic,
                                    &from_peer,
                                    "ok",
                                    &model_id,
                                    local_model_available,
                                );

                                println!("🧠 [Worker] DirectInferenceRequest: model={} prompt='{}'",
                                    model_id, &prompt[..prompt.len().min(40)]);

                                if !local_model_available {
                                    println!("   ⚠️ Modelo {} no instalado — ignorando", model_id);
                                    continue;
                                }

                                if let Some(req) = ModelRequirements::for_model(&model_id) {
                                    if !can_node_run_model(&node_caps, &req) {
                                        println!("   ⚠️ Hardware insuficiente para {} — ignorando", model_id);
                                        continue;
                                    }
                                }

                                let engine_ref = Arc::clone(&inference_engine);
                                let metrics_ref = Arc::clone(&metrics);
                                let registry_clone = ModelRegistry::new();
                                let peer_id_str = peer_id.to_string();
                                let request_id_clone = request_id.clone();
                                let model_id_for_inference = model_id.clone();

                                let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);

                                let inference_handle = tokio::spawn(async move {
                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id_for_inference.clone(),
                                        prompt,
                                        max_tokens,
                                        temperature: 0.7,
                                    };
                                    let daemon_socket = daemon_socket_path();
                                    let result = if daemon_is_available(&daemon_socket).await {
                                        match infer_via_daemon(&daemon_socket, req, |token| {
                                            let _ = token_tx.try_send(token);
                                        }).await {
                                            Ok(response) => response.result,
                                            Err(e) => {
                                                return InferenceTaskResult::failure(
                                                    request_id_clone.clone(),
                                                    model_id_for_inference.clone(),
                                                    peer_id_str.clone(),
                                                    e,
                                                );
                                            }
                                        }
                                    } else {
                                        let eng = Arc::clone(&engine_ref);
                                        let hash = registry_clone.get(&model_id_for_inference)
                                            .map(|m| m.hash.clone())
                                            .unwrap_or_default();

                                        if let Err(e) = eng.load_model(&model_id_for_inference, &hash) {
                                            return InferenceTaskResult::failure(
                                                request_id_clone.clone(),
                                                model_id_for_inference.clone(),
                                                peer_id_str.clone(),
                                                e,
                                            );
                                        }

                                        eng.run_inference(req, Some(token_tx)).await
                                    };

                                    InferenceTaskResult::success(
                                        request_id_clone,
                                        model_id_for_inference,
                                        result.output,
                                        result.tokens_generated,
                                        result.truncated,
                                        result.continuation_steps,
                                        result.execution_ms,
                                        peer_id_str,
                                        result.accelerator_used,
                                    )
                                });

                                let mut token_idx = 0u32;
                                while let Some(token) = token_rx.recv().await {
                                    let st = StreamedToken {
                                        request_id: request_id.clone(),
                                        token,
                                        index: token_idx,
                                        is_final: false,
                                    };
                                    let _ = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new(RESULTS_TOPIC),
                                        serde_json::to_vec(&st.to_gossip_json()).unwrap(),
                                    );
                                    token_idx += 1;
                                }

                                if let Ok(result) = inference_handle.await {
                                    {
                                        let mut m = metrics_ref.write().await;
                                        if result.success {
                                            m.inference_success(result.execution_ms, result.tokens_generated as u64);
                                        } else {
                                            m.inference_failed();
                                        }
                                    }
                                    let result_size = result.output.len();
                                    emit_worker_task_completed_event(
                                        &task_id,
                                        &attempt_id,
                                        &model_id,
                                        result_size,
                                        result.success,
                                        &peer_id.to_string(),
                                    );
                                    let mut result_payload = result.to_gossip_json();
                                    if let Some(map) = result_payload.as_object_mut() {
                                        map.insert("task_id".to_string(), task_id.clone().into());
                                        map.insert("attempt_id".to_string(), attempt_id.clone().into());
                                    }
                                    let payload_size = result_payload.to_string().len();
                                    let publish_result = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new(RESULTS_TOPIC),
                                        serde_json::to_vec(&result_payload).unwrap(),
                                    );
                                    match publish_result {
                                        Ok(message_id) => emit_worker_result_published_event(
                                            &task_id,
                                            &attempt_id,
                                            &model_id,
                                            RESULTS_TOPIC,
                                            payload_size,
                                            Some(&message_id.to_string()),
                                            &peer_id.to_string(),
                                        ),
                                        Err(error) => log_observability_event(
                                            LogLevel::Error,
                                            "result_publish_failed",
                                            &task_id,
                                            Some(&task_id),
                                            Some(&model_id),
                                            Some(TASK_DISPATCH_UNCONFIRMED_001),
                                            {
                                                let mut fields = Map::new();
                                                fields.insert("attempt_id".to_string(), attempt_id.clone().into());
                                                fields.insert("topic".to_string(), RESULTS_TOPIC.into());
                                                fields.insert("error".to_string(), error.to_string().into());
                                                fields
                                            },
                                        ),
                                    }

                                    println!("✅ [Worker] Direct inference completada: {} tokens en {}ms",
                                        result.tokens_generated, result.execution_ms);
                                }
                            }

                            _ => {}
                        }
                    } else if matches!(mode, NodeMode::Worker)
                        && (message_topic == TASK_TOPIC || message_topic == DIRECT_INF_TOPIC)
                    {
                        log_observability_event(
                            LogLevel::Warn,
                            "task_message_received",
                            "dispatch",
                            None,
                            None,
                            Some(TASK_DISPATCH_UNCONFIRMED_001),
                            {
                                let mut fields = Map::new();
                                fields.insert("topic".to_string(), message_topic.into());
                                fields.insert("from_peer".to_string(), from_peer.into());
                                fields.insert("payload_parse_result".to_string(), "invalid_json".into());
                                fields.insert(
                                    "payload_size".to_string(),
                                    (message.data.len() as u64).into(),
                                );
                                fields
                            },
                        );
                    }
                }

                // ← Direct result routing — origin recibe resultado
                SwarmEvent::Behaviour(IaMineEvent::ResultResponse(RREvent::Message {
                    peer,
                    message: Message::Request { request, channel, .. },
                })) => {
                    println!("🎉 [Origin] Resultado directo de {}:",
                        &peer.to_string()[..8]);
                    println!("   task_id={} success={} ms={}",
                        request.task_id, request.success, request.execution_ms);

                    let _ = swarm.behaviour_mut().result_response.send_response(
                        channel,
                        TaskResultResponse { acknowledged: true },
                    );
                }

                SwarmEvent::Behaviour(IaMineEvent::ResultResponse(RREvent::Message {
                    peer,
                    message: Message::Response { response, .. },
                })) => {
                    if response.acknowledged {
                        println!("✅ [Worker] Origin {} confirmó resultado", &peer.to_string()[..8]);
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id: pid, topic })) => {
                    println!("📢 Peer {} suscrito a: {}", pid, topic);
                    pubsub_topics.register_peer_subscription(&pid, &topic);
                    log_observability_event(
                        LogLevel::Info,
                        "pubsub_topic_joined",
                        "network",
                        None,
                        None,
                        None,
                        {
                            let mut fields = Map::new();
                            fields.insert("peer_id".to_string(), pid.to_string().into());
                            fields.insert("topic".to_string(), topic.to_string().into());
                            fields.insert("scope".to_string(), "remote".into());
                            fields
                        },
                    );

                    // Desactivar ruta vieja: no enviar InferenceRequest broadcast aquí.
                    // (Se envía por smart routing en heartbeat tick cuando registry tenga candidatos)

                    // ...existing code...
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id: pid, topic })) => {
                    println!("🔕 Peer {} desuscrito de: {}", pid, topic);
                    pubsub_topics.unregister_peer_subscription(&pid, &topic);
                }

                SwarmEvent::ConnectionEstablished { peer_id: pid, endpoint, .. } => {
                    println!("✅ Conectado a: {} ({})", pid, endpoint.get_remote_address());
                    log_observability_event(
                        LogLevel::Info,
                        "peer_connected",
                        "network",
                        None,
                        None,
                        None,
                        {
                            let mut fields = Map::new();
                            fields.insert("peer_id".to_string(), pid.to_string().into());
                            fields.insert(
                                "address".to_string(),
                                endpoint.get_remote_address().to_string().into(),
                            );
                            fields
                        },
                    );
                    connected_peer = Some(pid);
                    known_workers.insert(pid);
                    if is_client && !tasks_sent && !matches!(mode, NodeMode::Broadcast { .. }) {
                        if let Some(task) = pending_tasks.first().cloned() {
                            println!("📤 [{}/{}] Enviando: {} → '{}'",
                                1, total_tasks, task.task_type, task.data);
                            swarm.behaviour_mut().request_response.send_request(&pid, task);
                            waiting_for_response = true;
                            tasks_sent = true;
                        }
                    }
                }

                SwarmEvent::ConnectionClosed { peer_id: pid, .. } => {
                    log_observability_event(
                        LogLevel::Warn,
                        "peer_disconnected",
                        "network",
                        None,
                        None,
                        Some(NET_PEER_DISCONNECTED_002),
                        {
                            let mut fields = Map::new();
                            fields.insert("peer_id".to_string(), pid.to_string().into());
                            fields
                        },
                    );
                    known_workers.remove(&pid);
                    pubsub_topics.unregister_peer(&pid);
                    if is_client && !waiting_for_response && !matches!(mode, NodeMode::Broadcast { .. }) {
                        println!("\n📊 Completado: {}/{} tareas", completed, total_tasks);
                        break;
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
                    if let Ok(rtt) = result {
                        let rtt_ms = rtt.as_micros() as f64 / 1000.0;
                        println!(
                            "[Latency] RTT to peer {} = {:.1} ms",
                            &peer.to_string()[..12.min(peer.to_string().len())],
                            rtt_ms
                        );
                        peer_tracker.update_latency(&peer.to_string(), rtt_ms);
                        topology.write().await.update_latency(&peer.to_string(), rtt_ms);
                        log_observability_event(
                            LogLevel::Info,
                            "peer_latency",
                            "network",
                            None,
                            None,
                            None,
                            {
                                let mut fields = Map::new();
                                fields.insert("peer_id".to_string(), peer.to_string().into());
                                fields.insert("latency_ms".to_string(), rtt_ms.into());
                                fields.insert(
                                    "cluster".to_string(),
                                    cluster_label_for_latency(rtt_ms).into(),
                                );
                                fields
                            },
                        );
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Kademlia(kad::Event::RoutingUpdated { peer, .. })) => {
                    println!("📡 Kademlia: nodo añadido {}", peer);
                    debug_network_log(
                        debug_flags,
                        format!("kademlia routing updated peer_id={}", peer),
                    );
                }

                SwarmEvent::Behaviour(IaMineEvent::RequestResponse(event)) => {
                    match event {
                        RREvent::Message { peer, message: Message::Request { request, channel, .. } } => {
                            if let Some(task) = request.distributed_task.clone() {
                                let local_cluster_id = {
                                    let topo = topology.read().await;
                                    topo.cluster_for_peer(&peer_id.to_string()).map(|s| s.to_string())
                                };
                                println!(
                                    "[Task] Received task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                                    task.id,
                                    task.attempt_id,
                                    peer,
                                    local_cluster_id.as_deref().unwrap_or("-"),
                                    task.model
                                );
                                emit_worker_task_message_received_event(
                                    &task.id,
                                    &task.attempt_id,
                                    "request_response",
                                    &peer.to_string(),
                                    "ok",
                                    &task.model,
                                    model_storage.has_model(&task.model),
                                );
                                log_observability_event(
                                    LogLevel::Info,
                                    "task_received",
                                    &task.id,
                                    Some(&task.id),
                                    Some(&task.model),
                                    None,
                                    {
                                        let mut fields = Map::new();
                                        fields.insert("peer_id".to_string(), peer.to_string().into());
                                        fields.insert("attempt_id".to_string(), task.attempt_id.clone().into());
                                        fields.insert(
                                            "cluster_id".to_string(),
                                            local_cluster_id
                                                .clone()
                                                .unwrap_or_else(|| "-".to_string())
                                                .into(),
                                        );
                                        fields
                                    },
                                );
                                debug_network_log(
                                    debug_flags,
                                    format!(
                                        "received distributed request task_id={} attempt_id={} from_peer={} cluster_id={} model_id={}",
                                        task.id,
                                        task.attempt_id,
                                        peer,
                                        local_cluster_id.as_deref().unwrap_or("-"),
                                        task.model
                                    ),
                                );
                                let task_manager_ref = Arc::clone(&task_manager);
                                let task_response_tx_ref = task_response_tx.clone();
                                let prompt = task.prompt.clone();
                                let model = task.model.clone();
                                let task_id = task.id.clone();
                                let attempt_id = task.attempt_id.clone();
                                let peer_string = peer.to_string();
                                let local_cluster_for_task = local_cluster_id.clone();

                                tokio::spawn(async move {
                                    match task_manager_ref.claim_task(task.clone()).await {
                                        TaskClaim::Started => {
                                            println!(
                                                "[Task] Started task_id={} attempt_id={} peer_id={} cluster_id={} model_id={}",
                                                task_id,
                                                attempt_id,
                                                peer_string,
                                                local_cluster_for_task.as_deref().unwrap_or("-"),
                                                model
                                            );
                                        }
                                        TaskClaim::InProgress => {
                                            let duplicate_message = format!(
                                                "duplicate task already running on node for task_id={}",
                                                task_id
                                            );
                                            let _ = task_response_tx_ref
                                                .send(PendingTaskResponse {
                                                    channel,
                                                    response: TaskResponse::distributed(
                                                        DistributedTaskResult::failure_for_attempt(
                                                            task_id.clone(),
                                                            attempt_id.clone(),
                                                            duplicate_message,
                                                        ),
                                                    ),
                                                })
                                                .await;
                                            return;
                                        }
                                        TaskClaim::Finished(existing_result) => {
                                            let cached = if existing_result.success {
                                                DistributedTaskResult::success_for_attempt(
                                                    existing_result.task_id,
                                                    attempt_id.clone(),
                                                    existing_result.output,
                                                )
                                            } else {
                                                DistributedTaskResult::failure_for_attempt(
                                                    existing_result.task_id,
                                                    attempt_id.clone(),
                                                    existing_result.output,
                                                )
                                            };
                                            let _ = task_response_tx_ref
                                                .send(PendingTaskResponse {
                                                    channel,
                                                    response: TaskResponse::distributed(cached),
                                                })
                                                .await;
                                            return;
                                        }
                                    }

                                    let resolution =
                                        resolve_policy_for_prompt(&prompt, Some(&model), &[]);
                                    let profile = resolution.profile.clone();
                                    let semantic_prompt = resolution.semantic_prompt.clone();
                                    let output_policy =
                                        resolve_output_policy(&profile, &semantic_prompt, None);

                                    let result = if let Some(runtime) = choose_inference_runtime().await {
                                        run_local_inference_with_timeout(
                                            runtime,
                                            task_id.clone(),
                                            model.clone(),
                                            semantic_prompt,
                                            profile.task_type,
                                            output_policy.max_tokens as u32,
                                            0.7,
                                        )
                                        .await
                                    } else {
                                        let registry = ModelRegistry::new();
                                        let model_desc = match registry.get(&model) {
                                            Some(desc) => desc,
                                            None => {
                                                let error = format!(
                                                    "Modelo {} no encontrado en registry",
                                                    model
                                                );
                                                task_manager_ref.fail(&task_id, &error).await;
                                                let _ = task_response_tx_ref
                                                    .send(PendingTaskResponse {
                                                        channel,
                                                        response: TaskResponse::distributed(
                                                            DistributedTaskResult::failure_for_attempt(
                                                                task_id.clone(),
                                                                attempt_id.clone(),
                                                                error,
                                                            ),
                                                        ),
                                                    })
                                                    .await;
                                                return;
                                            }
                                        };
                                        let engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));
                                        if let Err(error) = engine.load_model(&model, &model_desc.hash) {
                                            task_manager_ref.fail(&task_id, &error).await;
                                            let _ = task_response_tx_ref
                                                .send(PendingTaskResponse {
                                                    channel,
                                                    response: TaskResponse::distributed(
                                                        DistributedTaskResult::failure_for_attempt(
                                                            task_id.clone(),
                                                            attempt_id.clone(),
                                                            error,
                                                        ),
                                                    ),
                                                })
                                                .await;
                                            return;
                                        }

                                        run_local_inference_with_timeout(
                                            InferenceRuntime::Engine(engine),
                                            task_id.clone(),
                                            model.clone(),
                                            semantic_prompt,
                                            profile.task_type,
                                            output_policy.max_tokens as u32,
                                            0.7,
                                        )
                                        .await
                                    };

                                    match result {
                                        Ok(result) => {
                                            record_semantic_feedback(&prompt, &resolution.validation);
                                            match validate_result(
                                                &resolution.semantic_prompt,
                                                profile.task_type,
                                                &result.output,
                                            ) {
                                                ResultStatus::Valid => {}
                                                ResultStatus::Invalid(reason)
                                                | ResultStatus::Retryable(reason) => {
                                                    println!("[Fault] {}", reason);
                                                    task_manager_ref.fail(&task_id, &reason).await;
                                                    let _ = task_response_tx_ref
                                                        .send(PendingTaskResponse {
                                                            channel,
                                                            response: TaskResponse::distributed(
                                                                DistributedTaskResult::failure_for_attempt(
                                                                    task_id.clone(),
                                                                    attempt_id.clone(),
                                                                    reason,
                                                                ),
                                                            ),
                                                        })
                                                        .await;
                                                    return;
                                                }
                                            }
                                            let distributed_result = DistributedTaskResult::success(
                                                task_id.clone(),
                                                result.output,
                                            );
                                            let distributed_result =
                                                DistributedTaskResult::success_for_attempt(
                                                    distributed_result.task_id,
                                                    attempt_id.clone(),
                                                    distributed_result.output,
                                                );
                                            task_manager_ref.complete(distributed_result.clone()).await;
                                            let _ = task_response_tx_ref
                                                .send(PendingTaskResponse {
                                                    channel,
                                                    response: TaskResponse::distributed(
                                                        distributed_result,
                                                    ),
                                                })
                                                .await;
                                        }
                                        Err(error) => {
                                            task_manager_ref.fail(&task_id, &error).await;
                                            let _ = task_response_tx_ref
                                                .send(PendingTaskResponse {
                                                    channel,
                                                    response: TaskResponse::distributed(
                                                        DistributedTaskResult::failure_for_attempt(
                                                            task_id.clone(),
                                                            attempt_id.clone(),
                                                            error,
                                                        ),
                                                    ),
                                                })
                                                .await;
                                        }
                                    }
                                });
                            } else {
                                println!("📨 Tarea P2P de {}: {} → '{}'", peer, request.task_type, request.data);
                                let queue_ref = Arc::clone(&queue);
                                let t_id = request.task_id.clone();
                                let t_type = request.task_type.clone();
                                let t_data = request.data.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = queue_ref.push(t_id, t_type, t_data).await {
                                        eprintln!("❌ Error encolando: {}", e);
                                    }
                                    if let Some(outcome) = queue_ref.outcome_rx.lock().await.recv().await {
                                        println!("🏁 {} → {:?}", outcome.task_id, outcome.status);
                                    }
                                });
                                let response = TaskExecutor::execute_task(
                                    request.task_id, request.task_type, request.data,
                                );
                                let _ = swarm.behaviour_mut().request_response.send_response(channel, response);
                            }
                        }
                        RREvent::Message { peer, message: Message::Response { response, .. } } => {
                            if let Some(distributed_result) = response.distributed_result {
                                let expected_task_id = distributed_infer_state
                                    .as_ref()
                                    .map(|infer_state| infer_state.trace_task_id.as_str());
                                if infer_request_id.as_deref()
                                    != Some(distributed_result.attempt_id.as_str())
                                    || expected_task_id != Some(distributed_result.task_id.as_str())
                                {
                                    println!(
                                        "[Task] Ignoring stale response task_id={} attempt_id={} from peer_id={}",
                                        distributed_result.task_id, distributed_result.attempt_id, peer
                                    );
                                    continue;
                                }

                                pending_inference.remove(&distributed_result.attempt_id);
                                let peer_id = peer.to_string();
                                let latency_ms = infer_started_at
                                    .as_ref()
                                    .map(|started| started.elapsed().as_millis() as u64)
                                    .unwrap_or_default();
                                emit_result_received_event(
                                    &distributed_result.task_id,
                                    &distributed_result.attempt_id,
                                    distributed_infer_state
                                        .as_ref()
                                        .and_then(|state| state.current_model.as_deref()),
                                    &peer_id,
                                    latency_ms,
                                );
                                let validation_status = if distributed_result.success {
                                    if !should_print_result_output(&distributed_result.output) {
                                        ResultStatus::Retryable(
                                            "empty distributed response output".to_string(),
                                        )
                                    } else if let Some(infer_state) = distributed_infer_state.as_ref() {
                                        validate_result(
                                            infer_state
                                                .current_semantic_prompt
                                                .as_deref()
                                                .unwrap_or(""),
                                            infer_state
                                                .current_task_type
                                                .unwrap_or(PromptTaskType::General),
                                            &distributed_result.output,
                                        )
                                    } else {
                                        ResultStatus::Valid
                                    }
                                } else {
                                    ResultStatus::Retryable(distributed_result.output.clone())
                                };

                                let retry_reason = match validation_status {
                                    ResultStatus::Valid => None,
                                    ResultStatus::Invalid(reason)
                                    | ResultStatus::Retryable(reason) => Some(reason),
                                };

                                if let Some(reason) = retry_reason {
                                    if let Some(health) = registry.write().await.record_failure(&peer_id) {
                                        log_health_update(
                                            &distributed_result.task_id,
                                            &peer_id,
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref()),
                                            &health,
                                            Some(NODE_UNHEALTHY_002),
                                        );
                                    }
                                    let error_code = if distributed_result.output.trim().is_empty() {
                                        TASK_EMPTY_RESULT_003
                                    } else {
                                        TASK_FAILED_002
                                    };
                                    log_observability_event(
                                        LogLevel::Error,
                                        "task_failed",
                                        &distributed_result.task_id,
                                        Some(&distributed_result.task_id),
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        Some(error_code),
                                        {
                                            let mut fields = Map::new();
                                            fields.insert(
                                                "worker_peer_id".to_string(),
                                                peer_id.clone().into(),
                                            );
                                            fields.insert(
                                                "attempt_id".to_string(),
                                                distributed_result.attempt_id.clone().into(),
                                            );
                                            fields.insert("reason".to_string(), reason.clone().into());
                                            fields.insert("retry".to_string(), true.into());
                                            fields.insert("error_kind".to_string(), "task_failure".into());
                                            fields.insert("recoverable".to_string(), true.into());
                                            fields
                                        },
                                    );
                                    let retry_target =
                                        if let Some(infer_state) = distributed_infer_state.as_ref() {
                                            let mut preview_retry = infer_state.retry_state.clone();
                                            preview_retry.record_failure(
                                                Some(&peer.to_string()),
                                                infer_state.current_model.as_deref(),
                                            );
                                            let reg = registry.read().await;
                                            select_retry_target(
                                                &reg,
                                                &IntelligentScheduler::new(),
                                                &infer_state.candidate_models,
                                                infer_state.local_cluster_id.as_deref(),
                                                &preview_retry,
                                            )
                                        } else {
                                            None
                                        };

                                    task_manager.fail(&distributed_result.task_id, &reason).await;
                                    eprintln!(
                                        "[Fault] task_id={} attempt_id={} peer_id={} model_id={} reason={}",
                                        distributed_result.task_id,
                                        distributed_result.attempt_id,
                                        peer,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref())
                                            .unwrap_or("-"),
                                        reason
                                    );

                                    if let Some(infer_state) = distributed_infer_state.as_mut() {
                                        let trace_task_id = infer_state.trace_task_id.clone();
                                        let failure_kind = if distributed_result.success {
                                            FailureKind::InvalidOutput
                                        } else {
                                            FailureKind::TaskFailure
                                        };
                                        let failed_model = infer_state.current_model.clone();
                                        if infer_state.schedule_retry(
                                            Some(&peer.to_string()),
                                            failed_model.as_deref(),
                                        ) && retry_target.is_some()
                                        {
                                            let failed_peer_id = peer.to_string();
                                            emit_retry_scheduled_event(
                                                &trace_task_id,
                                                &infer_state.current_request_id,
                                                failed_model.as_deref(),
                                                Some(&failed_peer_id),
                                                retry_target
                                                    .as_ref()
                                                    .map(|target| target.peer_id.as_str()),
                                                "task_failure",
                                            );
                                            let target = retry_target.unwrap();
                                            println!(
                                                "[Retry] task_id={} attempt_id={} kind={:?} peer_id={} model_id={}",
                                                trace_task_id,
                                                infer_state.current_request_id,
                                                failure_kind,
                                                target.peer_id,
                                                target.model_id
                                            );
                                            clear_stream_state(
                                                &mut token_buffer,
                                                &mut next_token_idx,
                                                &mut rendered_output,
                                            );
                                            waiting_for_response = false;
                                            infer_broadcast_sent = false;
                                            infer_request_id =
                                                Some(infer_state.current_request_id.clone());
                                            continue;
                                        }

                                        let (trace, aggregate_metrics) =
                                            finalize_distributed_task_observability(
                                                &trace_task_id,
                                                infer_started_at,
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

                                if let Some(health) =
                                    registry.write().await.record_success(&peer_id, latency_ms)
                                {
                                    log_health_update(
                                        &distributed_result.task_id,
                                        &peer_id,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        &health,
                                        None,
                                    );
                                }
                                log_observability_event(
                                    LogLevel::Info,
                                    "task_completed",
                                    &distributed_result.task_id,
                                    Some(&distributed_result.task_id),
                                    distributed_infer_state
                                        .as_ref()
                                        .and_then(|state| state.current_model.as_deref()),
                                    None,
                                    {
                                        let mut fields = Map::new();
                                        fields.insert(
                                            "worker_peer_id".to_string(),
                                            peer_id.clone().into(),
                                        );
                                        fields.insert(
                                            "attempt_id".to_string(),
                                            distributed_result.attempt_id.clone().into(),
                                        );
                                        fields.insert("latency_ms".to_string(), latency_ms.into());
                                        fields.insert("success".to_string(), true.into());
                                        fields
                                    },
                                );
                                task_manager.complete(distributed_result.clone()).await;
                                if should_print_result_output(&distributed_result.output) {
                                    println!("{}", distributed_result.output);
                                }
                                println!(
                                    "\n\n✅ Inference completada: task_id={} attempt_id={} peer_id={} model_id={}",
                                    distributed_result.task_id,
                                    distributed_result.attempt_id,
                                    peer,
                                    distributed_infer_state
                                        .as_ref()
                                        .and_then(|state| state.current_model.as_deref())
                                        .unwrap_or("-")
                                );
                                if let Some(infer_state) = distributed_infer_state.as_ref() {
                                    let (trace, aggregate_metrics) =
                                        finalize_distributed_task_observability(
                                            &infer_state.trace_task_id,
                                            infer_started_at,
                                            false,
                                        );
                                    if let Some(trace) = trace {
                                        println!(
                                            "[Trace] nodes={} models={}",
                                            trace.node_history.join(" -> "),
                                            trace.model_history.join(" -> ")
                                        );
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
                            } else {
                                waiting_for_response = false;
                                completed += 1;
                                if !pending_tasks.is_empty() { pending_tasks.remove(0); }
                                println!("📩 [{}/{}] {} de {}: '{}'",
                                    completed, total_tasks,
                                    if response.success { "✅" } else { "❌" },
                                    peer, response.result);
                                if let Some(next_task) = pending_tasks.first().cloned() {
                                    if let Some(pid) = connected_peer {
                                        swarm.behaviour_mut().request_response.send_request(&pid, next_task);
                                        waiting_for_response = true;
                                    }
                                } else {
                                    println!("\n🎉 Completadas: {}/{}", completed, total_tasks);
                                    break;
                                }
                            }
                        }
                        RREvent::OutboundFailure { peer, error, .. } => {
                            eprintln!("❌ Outbound {}: {:?}", peer, error);
                            if is_client && !waiting_for_response { break; }
                        }
                        _ => {}
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
                if tasks_done % 10 == 0 {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_tokens_override() {
        let profile = analyze_prompt("explica la teoria de la relatividad");
        let decision =
            resolve_output_policy(&profile, "explica la teoria de la relatividad", Some(100));

        assert_eq!(decision.max_tokens, 100);
        assert_eq!(decision.reason, "cli override");
    }

    #[test]
    fn test_max_tokens_override_is_clamped() {
        let profile = analyze_prompt("What is 2+2?");
        let decision = resolve_output_policy(&profile, "What is 2+2?", Some(10));

        assert_eq!(decision.max_tokens, 64);
    }

    #[test]
    fn test_parse_optional_u32_flag() {
        let args = vec![
            "iamine-node".to_string(),
            "infer".to_string(),
            "explica".to_string(),
            "--max-tokens".to_string(),
            "1024".to_string(),
        ];

        assert_eq!(
            parse_optional_u32_flag(&args, "--max-tokens").unwrap(),
            Some(1024)
        );
    }

    #[test]
    fn test_exact_math_uses_natural_prompt_on_first_attempt() {
        assert_eq!(
            with_task_guard("What is 2+2?", PromptTaskType::ExactMath, false),
            "What is 2+2?"
        );
    }

    #[test]
    fn test_exact_math_retry_guard_is_simple() {
        let guarded = with_task_guard("6x9", PromptTaskType::ExactMath, true);
        assert!(guarded.contains("Return only the final numeric answer"));
        assert!(guarded.ends_with("\n\n6x9"));
    }

    #[test]
    fn test_decimal_sequence_retry_guard_mentions_decimal_point() {
        let guarded = with_task_guard(
            "dame los primeros 100 digitos de pi",
            PromptTaskType::ExactMath,
            true,
        );
        assert!(guarded.contains("Keep the decimal point attached"));
    }

    #[test]
    fn test_cli_no_runtime_start() {
        assert!(is_control_plane_mode(&NodeMode::ModelsList));
        assert!(is_control_plane_mode(&NodeMode::ModelsStats));
        assert!(is_control_plane_mode(&NodeMode::ModelsDownload {
            model_id: "llama3-3b".to_string()
        }));
        assert!(is_control_plane_mode(&NodeMode::ModelsRemove {
            model_id: "llama3-3b".to_string()
        }));
        assert!(is_control_plane_mode(&NodeMode::TasksStats));
        assert!(is_control_plane_mode(&NodeMode::TasksTrace {
            task_id: "task-1".to_string()
        }));
        assert!(!is_control_plane_mode(&NodeMode::Worker));
    }

    #[test]
    fn test_force_network_routing() {
        let flags = InferenceControlFlags {
            force_network: true,
            no_local: false,
            prefer_local: true,
        };
        assert!(!flags.should_use_local(true));

        let prefer_local = InferenceControlFlags {
            force_network: false,
            no_local: false,
            prefer_local: true,
        };
        assert!(prefer_local.should_use_local(true));
        assert!(!prefer_local.should_use_local(false));
    }

    #[test]
    fn test_checked_sub_usize_b_greater_than_a() {
        let error = checked_sub_usize(
            "max_concurrent_minus_available_slots",
            1,
            2,
            "b_greater_than_a",
        )
        .unwrap_err();

        assert_eq!(error.operation, "max_concurrent_minus_available_slots");
        assert_eq!(error.operand_a, 1);
        assert_eq!(error.operand_b, 2);
        assert_eq!(error.reason, "b_greater_than_a");
    }

    #[test]
    fn test_invalid_startup_resource_calculation_path() {
        let error = compute_metrics_port(7001).unwrap_err();

        assert_eq!(error.operation, "worker_port_minus_base");
        assert_eq!(error.operand_a, 7001);
        assert_eq!(error.operand_b, 9000);
        assert_eq!(error.reason, "worker_port_below_metrics_base");
    }

    #[test]
    fn test_worker_startup_overflow_emits_structured_error_code() {
        let trace_id = format!("startup-overflow-test-{}", uuid_simple());
        let error = StartupMathError::new(
            "worker_port_minus_base",
            7001,
            9000,
            "worker_port_below_metrics_base",
        );
        let policy = ResourcePolicy {
            cpu_cores: 4,
            max_cpu_load: 80,
            ram_limit_gb: 4,
            gpu_enabled: false,
            disk_limit_gb: 10,
            disk_path: "/tmp/iamine".to_string(),
        };

        emit_worker_startup_overflow_event(
            &trace_id,
            "node-test",
            "peer-test",
            7001,
            &policy,
            4,
            4,
            4,
            &error,
            "continue_without_metrics_server",
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| {
                entry.trace_id == trace_id && entry.event == "worker_startup_invalid_math"
            })
            .expect("startup overflow entry not found");

        assert_eq!(
            entry.error_code.as_deref(),
            Some(WORKER_STARTUP_OVERFLOW_001)
        );
        assert_eq!(
            entry
                .fields
                .get("fallback_behavior")
                .and_then(|value| value.as_str()),
            Some("continue_without_metrics_server")
        );
    }

    #[test]
    fn test_no_publish_before_readiness() {
        let snapshot = DispatchReadinessSnapshot {
            connected_peer_count: 0,
            mesh_peer_count: 0,
            topic_peer_count: 0,
            joined_task_topic: true,
            joined_direct_topic: true,
            joined_results_topic: true,
            selected_topic: DIRECT_INF_TOPIC,
        };

        let error = evaluate_dispatch_readiness(&snapshot).unwrap_err();
        assert_eq!(error.code, NETWORK_NO_PUBSUB_PEERS_001);
        assert_eq!(error.reason, "no_connected_peers");
    }

    #[test]
    fn test_structured_error_when_topic_has_zero_peers() {
        let snapshot = DispatchReadinessSnapshot {
            connected_peer_count: 2,
            mesh_peer_count: 2,
            topic_peer_count: 0,
            joined_task_topic: true,
            joined_direct_topic: true,
            joined_results_topic: true,
            selected_topic: TASK_TOPIC,
        };

        let error = evaluate_dispatch_readiness(&snapshot).unwrap_err();
        assert_eq!(error.code, PUBSUB_TOPIC_NOT_READY_001);
        assert_eq!(error.reason, "destination_topic_has_zero_peers");
    }

    #[test]
    fn test_dispatch_observability_events_emitted() {
        let trace_id = format!("dispatch-observability-{}", uuid_simple());
        let attempt_id = "attempt-observability-1";
        let model_id = "tinyllama-1b";
        let candidates = vec![model_id.to_string(), "llama3-3b".to_string()];

        emit_dispatch_context_event(
            &trace_id,
            attempt_id,
            model_id,
            &candidates,
            3,
            2,
            DIRECT_INF_TOPIC,
            Some("peer-target-1"),
        );
        emit_task_publish_attempt_event(
            &trace_id,
            attempt_id,
            model_id,
            DIRECT_INF_TOPIC,
            2,
            128,
            Some("peer-target-1"),
        );
        emit_task_published_event(
            &trace_id,
            attempt_id,
            model_id,
            DIRECT_INF_TOPIC,
            2,
            128,
            "message-1",
            Some("peer-target-1"),
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();

        assert!(entries
            .iter()
            .any(|entry| { entry.trace_id == trace_id && entry.event == "task_dispatch_context" }));
        let dispatch_entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "task_dispatch_context")
            .expect("task_dispatch_context entry not found");
        assert_eq!(dispatch_entry.task_id.as_deref(), Some(trace_id.as_str()));
        assert_eq!(dispatch_entry.model_id.as_deref(), Some(model_id));
        assert_eq!(
            dispatch_entry
                .fields
                .get("attempt_id")
                .and_then(|value| value.as_str()),
            Some(attempt_id)
        );
        assert_eq!(
            dispatch_entry
                .fields
                .get("selected_peer_id")
                .and_then(|value| value.as_str()),
            Some("peer-target-1")
        );
        assert!(entries
            .iter()
            .any(|entry| { entry.trace_id == trace_id && entry.event == "task_publish_attempt" }));
        assert!(entries.iter().any(|entry| {
            entry.trace_id == trace_id
                && entry.event == "task_published"
                && entry
                    .fields
                    .get("message_id")
                    .and_then(|value| value.as_str())
                    == Some("message-1")
        }));
    }

    #[test]
    fn test_error_events_include_required_error_fields() {
        let trace_id = format!("dispatch-error-{}", uuid_simple());
        emit_task_publish_failed_event(
            &trace_id,
            "attempt-error-1",
            "tinyllama-1b",
            DIRECT_INF_TOPIC,
            0,
            256,
            "insufficient peers",
            Some("peer-target-err"),
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "task_publish_failed")
            .expect("task_publish_failed entry not found");

        assert_eq!(
            entry.error_code.as_deref(),
            Some(TASK_DISPATCH_UNCONFIRMED_001)
        );
        assert_eq!(
            entry
                .fields
                .get("error_kind")
                .and_then(|value| value.as_str()),
            Some("publish_error")
        );
        assert_eq!(
            entry
                .fields
                .get("recoverable")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn test_worker_receipt_logs_emitted() {
        let trace_id = format!("worker-receipt-{}", uuid_simple());
        emit_worker_task_message_received_event(
            &trace_id,
            "attempt-worker-1",
            DIRECT_INF_TOPIC,
            "peer-source-1",
            "ok",
            "tinyllama-1b",
            true,
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "task_message_received")
            .expect("task_message_received entry not found");

        assert_eq!(
            entry
                .fields
                .get("payload_parse_result")
                .and_then(|value| value.as_str()),
            Some("ok")
        );
        assert_eq!(
            entry
                .fields
                .get("local_model_available")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn test_result_received_logs_emitted() {
        let trace_id = format!("result-received-{}", uuid_simple());
        emit_result_received_event(
            &trace_id,
            "attempt-result-1",
            Some("tinyllama-1b"),
            "peer-worker-1",
            42,
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "result_received")
            .expect("result_received entry not found");

        assert_eq!(
            entry
                .fields
                .get("attempt_id")
                .and_then(|value| value.as_str()),
            Some("attempt-result-1")
        );
        assert_eq!(
            entry
                .fields
                .get("latency_ms")
                .and_then(|value| value.as_u64()),
            Some(42)
        );
    }

    #[test]
    fn test_metrics_increments_for_retry_and_fallback() {
        let mut metrics = DistributedTaskMetrics::default();
        apply_retry_fallback_metrics(&mut metrics, true, false);
        apply_retry_fallback_metrics(&mut metrics, false, true);

        assert_eq!(metrics.retries_count, 1);
        assert_eq!(metrics.fallback_count, 1);
        assert_eq!(metrics.failed_tasks, 0);
    }

    #[test]
    fn test_empty_result_does_not_print_as_success_response() {
        assert!(!should_print_result_output(""));
        assert!(!should_print_result_output("   "));
        assert!(should_print_result_output("ok"));
    }

    #[test]
    fn test_identity_conflict_path_uses_ephemeral_identity_strategy() {
        let identity_a = NodeIdentity::ephemeral("test-identity-safety");
        let identity_b = NodeIdentity::ephemeral("test-identity-safety");

        assert_ne!(identity_a.peer_id, identity_b.peer_id);
        assert_ne!(identity_a.node_id, identity_b.node_id);
    }

    #[test]
    fn test_human_logs_remain_unaffected() {
        let human_line = "✅ Conectado a: 12D3KooWorker (/ip4/127.0.0.1/tcp/7001)";
        assert!(serde_json::from_str::<serde_json::Value>(human_line).is_err());
        assert!(human_line.contains("Conectado a"));
    }

    #[test]
    fn test_no_panic_for_invalid_startup_math_inputs() {
        let result = std::panic::catch_unwind(|| {
            let _ = compute_metrics_port(7001);
            let _ = compute_active_tasks(1, 2);
        });

        assert!(result.is_ok());
    }
}
