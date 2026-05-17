mod backend_policy;
mod benchmark;
mod broadcast_protocol;
mod broadcast_runtime;
mod broadcast_worker;
mod capability_display;
mod cli;
mod cluster_cli;
mod cluster_events;
mod cluster_health;
mod cluster_readiness;
mod cluster_registry;
mod cluster_status;
mod code_quality;
mod cpu_feature_guard;
mod daemon_runtime;
mod executor;
mod final_outcome;
mod heartbeat;
mod metrics;
mod metrics_policy;
mod metrics_server;
mod mode_dispatch;
mod model_display_policy;
mod model_executability;
mod model_selector_cli;
mod network;
mod node_identity;
mod node_modes;
mod peer_tracker;
mod protocol;
mod pubsub_observability;
mod pubsub_readiness;
mod pubsub_topic_tracker;
mod pubsub_topics;
mod quality_gate;
mod rate_limiter;
mod regression_runner;
mod resource_policy;
mod result_acceptance;
mod result_observability;
mod result_protocol;
mod security_checks;
mod setup_wizard;
mod task_cache;
mod task_codec;
mod task_events;
mod task_lifecycle;
mod task_protocol;
mod task_queue;
mod task_scheduler;
mod task_trace;
mod tasks_cli;
mod usage;
mod wallet;
mod worker_capabilities;
mod worker_capability_advertisement;
mod worker_pool;
mod worker_startup_policy;

use iamine_models::{
    normalize_output, DirectInferenceRequest, HardwareAcceleration, InferenceTask,
    InferenceTaskResult, ModelRegistry, ModelStorage, RealInferenceEngine, RealInferenceRequest,
    RealInferenceResult, StreamedToken,
};

use iamine_network::{
    analyze_prompt_semantics, claim_task_attempt_peer, default_semantic_log_path,
    describe_output_policy, detect_exact_subtype, health_policy_thresholds, log_structured,
    normalize_expression, prompt_log_entry, record_distributed_task_fallback,
    record_distributed_task_late_result, record_distributed_task_retry,
    record_distributed_task_started, record_model_metrics, record_task_attempt,
    select_retry_target, set_global_node_id, set_global_runtime_context, validate_result,
    validate_semantic_decision, DistributedTaskResult, FailureKind, IntelligentScheduler, LogLevel,
    ModelMetrics, ModelPolicyEngine, NetworkTopology, NodeCapability, NodeCapabilityHeartbeat,
    NodeHealth, NodeRegistry, OutputPolicyDecision, PromptProfile, ResultStatus, RetryPolicy,
    RetryState, SemanticFeedbackEngine, SharedNetworkTopology, SharedNodeRegistry,
    StructuredLogEntry, TaskClaim, TaskManager, TaskType as PromptTaskType,
    ValidationResult as SemanticValidationResult, NET_PEER_DISCONNECTED_002, NODE_BLACKLISTED_001,
    NODE_UNHEALTHY_002, SCH_NO_NODE_001, TASK_DISPATCH_UNCONFIRMED_001, TASK_EMPTY_RESULT_003,
    TASK_FAILED_002, TASK_TIMEOUT_001, WORKER_STARTUP_OVERFLOW_001,
};

#[cfg(test)]
use iamine_network::analyze_prompt;
#[cfg(test)]
use iamine_network::DistributedTaskMetrics;

use benchmark::NodeBenchmark;
use daemon_runtime::{daemon_is_available, daemon_socket_path, infer_via_daemon, run_daemon};
use heartbeat::HeartbeatService;
use metrics::NodeMetrics;
use metrics_server::*;
use node_identity::NodeIdentity; // ← actualizado
use peer_tracker::PeerTracker;
use quality_gate::runtime_version_metadata;
use rate_limiter::RateLimiter;
use resource_policy::ResourcePolicy;
use result_observability::*;
use result_protocol::{
    publish_worker_result_payload, send_worker_result_direct_response, BroadcastTaskResultMessage,
    TaskResultRequest, TaskResultResponse,
};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use task_cache::TaskCache;
use task_events::*;
use task_lifecycle::{TaskLifecycleErrorCode, TaskSelectionReason};
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

use broadcast_protocol::*;
use broadcast_runtime::*;
use broadcast_worker::*;
use capability_display::*;
use cli::{parse_args, parse_worker_port};
use cluster_events::*;
use cluster_registry::*;
use cluster_status::*;
use executor::TaskExecutor;
use final_outcome::*;
use mode_dispatch::{handle_pre_network_mode, is_control_plane_mode};
use model_display_policy::*;
use model_executability::*;
use node_modes::{mode_label, DebugFlags, InferenceControlFlags, NodeMode};
use task_protocol::{TaskRequest, TaskResponse};
use usage::print_usage;
use worker_capability_advertisement::*;
use worker_startup_policy::*;

pub(crate) use pubsub_observability::*;
pub(crate) use pubsub_readiness::*;
pub(crate) use pubsub_topic_tracker::*;
pub(crate) use pubsub_topics::*;

const INFER_TIMEOUT_MS: u64 = 5_000;
const INFER_FALLBACK_AFTER_MS: u64 = 2_000;
const MAX_DISTRIBUTED_RETRIES: u8 = 2;
const MIN_ADAPTIVE_TIMEOUT_MS: u64 = 7_500;
const MAX_ADAPTIVE_TIMEOUT_MS: u64 = 45_000;
const WATCHDOG_EXTENSION_STEP_MS: u64 = 4_000;
const WATCHDOG_STALL_FACTOR_NUM: u64 = 3;
const WATCHDOG_STALL_FACTOR_DEN: u64 = 5;
const WATCHDOG_MIN_STALL_MS: u64 = 6_000;
const LATE_RESULT_ACCEPTANCE_WINDOW_MS: u64 = 15_000;
const MIN_REMOTE_EXECUTION_TIMEOUT_MS: u64 = 120_000;
const MAX_REMOTE_EXECUTION_TIMEOUT_MS: u64 = 900_000;
const UNCLAIMED_WORKER_PEER_ID: &str = "-";

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

#[derive(Default)]
struct HumanLogThrottle {
    last_log_ms: HashMap<String, u64>,
    last_values: HashMap<String, String>,
}

impl HumanLogThrottle {
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn should_log(&mut self, key: &str, interval_ms: u64, value: Option<&str>) -> bool {
        let now_ms = Self::now_ms();
        let previous_ms = self.last_log_ms.get(key).copied().unwrap_or(0);
        let unchanged = value
            .and_then(|val| self.last_values.get(key).map(|previous| previous == val))
            .unwrap_or(false);

        if unchanged && now_ms.saturating_sub(previous_ms) < interval_ms {
            return false;
        }

        self.last_log_ms.insert(key.to_string(), now_ms);
        if let Some(value) = value {
            self.last_values.insert(key.to_string(), value.to_string());
        }
        true
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptLifecycleState {
    Queued,
    Starting,
    LoadingModel,
    InferenceWarmup,
    Running,
    ProducingTokens,
    Stalled,
    TimedOut,
    Completed,
    LateCompleted,
    Failed,
}

impl AttemptLifecycleState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::LoadingModel => "loading_model",
            Self::InferenceWarmup => "inference_warmup",
            Self::Running => "running",
            Self::ProducingTokens => "producing_tokens",
            Self::Stalled => "stalled",
            Self::TimedOut => "timed_out",
            Self::Completed => "completed",
            Self::LateCompleted => "late_completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptDispatchType {
    Direct,
    FallbackBroadcast,
}

impl AttemptDispatchType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::FallbackBroadcast => "fallback_broadcast",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptClaimSource {
    TaskMessageReceived,
    AttemptProgress,
    ResultReceived,
}

impl AttemptClaimSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::TaskMessageReceived => "task_message_received",
            Self::AttemptProgress => "attempt_progress",
            Self::ResultReceived => "result_received",
        }
    }
}

fn is_unclaimed_worker_peer_id(peer_id: &str) -> bool {
    peer_id.trim().is_empty() || matches!(peer_id, UNCLAIMED_WORKER_PEER_ID | "unclaimed")
}

fn is_valid_claiming_worker(peer_id: &str) -> bool {
    !is_unclaimed_worker_peer_id(peer_id) && peer_id != "unknown"
}

fn is_meaningfully_in_flight(state: AttemptLifecycleState) -> bool {
    matches!(
        state,
        AttemptLifecycleState::Queued
            | AttemptLifecycleState::Starting
            | AttemptLifecycleState::LoadingModel
            | AttemptLifecycleState::InferenceWarmup
            | AttemptLifecycleState::Running
            | AttemptLifecycleState::ProducingTokens
    )
}

fn lifecycle_state_from_progress_stage(stage: &str) -> Option<AttemptLifecycleState> {
    match stage {
        "task_received" | "task_message_received" | "worker_claimed" => {
            Some(AttemptLifecycleState::Starting)
        }
        "attempt_started" => Some(AttemptLifecycleState::Starting),
        "model_loading" | "model_load_started" => Some(AttemptLifecycleState::LoadingModel),
        "model_loaded" | "model_load_completed" => Some(AttemptLifecycleState::InferenceWarmup),
        "inference_warmup" => Some(AttemptLifecycleState::InferenceWarmup),
        "inference_started" => Some(AttemptLifecycleState::Running),
        "first_token_generated" | "tokens_generated_count" => {
            Some(AttemptLifecycleState::ProducingTokens)
        }
        "inference_finished" | "result_serialized" | "result_published" => {
            Some(AttemptLifecycleState::Running)
        }
        _ => None,
    }
}

fn claim_source_from_progress_stage(stage: &str) -> AttemptClaimSource {
    match stage {
        "task_received" | "task_message_received" | "worker_claimed" => {
            AttemptClaimSource::TaskMessageReceived
        }
        _ => AttemptClaimSource::AttemptProgress,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttemptTimeoutPolicy {
    timeout_ms: u64,
    claim_timeout_ms: u64,
    first_progress_timeout_ms: u64,
    model_load_timeout_ms: u64,
    first_token_timeout_ms: u64,
    stall_timeout_ms: u64,
    extension_step_ms: u64,
    max_wait_ms: u64,
    max_execution_timeout_ms: u64,
    latency_class: &'static str,
}

impl AttemptTimeoutPolicy {
    fn from_model_and_node(model_id: &str, worker: Option<&NodeCapability>) -> Self {
        let lower_model = model_id.to_ascii_lowercase();
        let mut timeout_ms: u64 = if lower_model.contains("mistral-7b") {
            16_000
        } else if lower_model.contains("llama3-3b") {
            11_000
        } else if lower_model.contains("tinyllama") {
            7_500
        } else {
            9_000
        };

        let mut latency_class = if timeout_ms >= 14_000 {
            "high"
        } else if timeout_ms >= 9_500 {
            "medium"
        } else {
            "low"
        };

        if let Some(worker) = worker {
            if worker.gpu_available || worker.accelerator.to_ascii_lowercase().contains("gpu") {
                timeout_ms = timeout_ms.saturating_sub(2_500);
                latency_class = "low";
            } else if worker.cpu_score <= 45_000 {
                timeout_ms = timeout_ms.saturating_add(6_000);
                latency_class = "high";
            } else if worker.cpu_score <= 90_000 {
                timeout_ms = timeout_ms.saturating_add(3_500);
                if latency_class == "low" {
                    latency_class = "medium";
                }
            }
            timeout_ms = timeout_ms.saturating_add((worker.latency_ms as u64).min(2_000));
        }

        timeout_ms = timeout_ms.clamp(MIN_ADAPTIVE_TIMEOUT_MS, MAX_ADAPTIVE_TIMEOUT_MS);
        let stall_timeout_ms = ((timeout_ms.saturating_mul(WATCHDOG_STALL_FACTOR_NUM))
            / WATCHDOG_STALL_FACTOR_DEN)
            .max(WATCHDOG_MIN_STALL_MS);
        let mut claim_timeout_ms = (timeout_ms / 2).max(INFER_FALLBACK_AFTER_MS);
        let mut first_progress_timeout_ms = timeout_ms;
        if lower_model.contains("mistral-7b") {
            claim_timeout_ms = claim_timeout_ms.max(120_000);
            first_progress_timeout_ms = first_progress_timeout_ms.max(120_000);
        }
        let model_load_timeout_ms = timeout_ms
            .saturating_mul(10)
            .clamp(60_000, MAX_REMOTE_EXECUTION_TIMEOUT_MS / 2);
        let first_token_timeout_ms = timeout_ms
            .saturating_mul(12)
            .clamp(90_000, MAX_REMOTE_EXECUTION_TIMEOUT_MS / 2);
        let max_wait_ms = timeout_ms
            .saturating_add(timeout_ms / 2)
            .min(MAX_ADAPTIVE_TIMEOUT_MS.saturating_add(10_000));
        let max_execution_timeout_ms = timeout_ms.saturating_mul(24).clamp(
            MIN_REMOTE_EXECUTION_TIMEOUT_MS,
            MAX_REMOTE_EXECUTION_TIMEOUT_MS,
        );

        Self {
            timeout_ms,
            claim_timeout_ms,
            first_progress_timeout_ms,
            model_load_timeout_ms,
            first_token_timeout_ms,
            stall_timeout_ms,
            extension_step_ms: WATCHDOG_EXTENSION_STEP_MS,
            max_wait_ms,
            max_execution_timeout_ms,
            latency_class,
        }
    }
}

#[derive(Debug, Clone)]
struct AttemptWatchdog {
    task_id: String,
    attempt_id: String,
    worker_peer_id: String,
    model_id: String,
    attempt_type: AttemptDispatchType,
    claimable: bool,
    state: AttemptLifecycleState,
    policy: AttemptTimeoutPolicy,
    started_at: tokio::time::Instant,
    last_progress_at: tokio::time::Instant,
    deadline_at: tokio::time::Instant,
    max_deadline_at: tokio::time::Instant,
    last_progress_stage: String,
    last_tokens_generated: u64,
    progress_observed: bool,
    no_progress_warning_emitted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchdogCheck {
    Healthy,
    Extended,
    Stalled,
    TimedOut,
}

impl AttemptWatchdog {
    fn new(
        task_id: String,
        attempt_id: String,
        worker_peer_id: String,
        model_id: String,
        policy: AttemptTimeoutPolicy,
    ) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            task_id,
            attempt_id,
            worker_peer_id,
            model_id,
            attempt_type: AttemptDispatchType::Direct,
            claimable: false,
            state: AttemptLifecycleState::Queued,
            started_at: now,
            last_progress_at: now,
            deadline_at: now + Duration::from_millis(policy.timeout_ms),
            max_deadline_at: now + Duration::from_millis(policy.max_execution_timeout_ms),
            last_progress_stage: "queued".to_string(),
            last_tokens_generated: 0,
            progress_observed: false,
            no_progress_warning_emitted: false,
            policy,
        }
    }

    fn new_fallback_broadcast(
        task_id: String,
        attempt_id: String,
        model_id: String,
        policy: AttemptTimeoutPolicy,
    ) -> Self {
        let mut watchdog = Self::new(
            task_id,
            attempt_id,
            UNCLAIMED_WORKER_PEER_ID.to_string(),
            model_id,
            policy,
        );
        watchdog.attempt_type = AttemptDispatchType::FallbackBroadcast;
        watchdog.claimable = true;
        watchdog
    }

    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    fn elapsed_since_last_progress_ms(&self) -> u64 {
        self.last_progress_at.elapsed().as_millis() as u64
    }

    fn transition_state(&mut self, next: AttemptLifecycleState) -> bool {
        if self.state == next {
            return false;
        }
        self.state = next;
        true
    }

    fn is_fallback_broadcast(&self) -> bool {
        self.attempt_type == AttemptDispatchType::FallbackBroadcast
    }

    fn is_unclaimed(&self) -> bool {
        self.claimable && is_unclaimed_worker_peer_id(&self.worker_peer_id)
    }

    fn accepts_worker(&self, worker_peer_id: &str) -> bool {
        is_valid_claiming_worker(worker_peer_id)
            && (self.is_unclaimed() || self.worker_peer_id == worker_peer_id)
    }

    fn first_token_observed(&self) -> bool {
        self.last_tokens_generated > 0 || self.state == AttemptLifecycleState::ProducingTokens
    }

    fn claim_worker(
        &mut self,
        worker_peer_id: &str,
        _source: AttemptClaimSource,
    ) -> Option<String> {
        if !self.is_fallback_broadcast()
            || !self.is_unclaimed()
            || !is_valid_claiming_worker(worker_peer_id)
        {
            return None;
        }

        let previous = self.worker_peer_id.clone();
        self.worker_peer_id = worker_peer_id.to_string();
        self.claimable = false;
        Some(previous)
    }

    fn late_acceptance_deadline_ms(&self) -> u64 {
        self.policy
            .max_execution_timeout_ms
            .saturating_add(LATE_RESULT_ACCEPTANCE_WINDOW_MS)
    }

    fn record_progress(&mut self, stage: &str, tokens_generated_count: Option<u64>) -> bool {
        let stage_changed = self.last_progress_stage != stage;
        let token_growth = tokens_generated_count
            .map(|tokens| tokens > self.last_tokens_generated)
            .unwrap_or(false);
        let startup_heartbeat = stage == "still_running"
            && matches!(
                self.state,
                AttemptLifecycleState::LoadingModel
                    | AttemptLifecycleState::InferenceWarmup
                    | AttemptLifecycleState::Running
            )
            && !self.first_token_observed();
        let meaningful = stage_changed || token_growth || startup_heartbeat;
        if !meaningful {
            return false;
        }

        let now = tokio::time::Instant::now();
        self.last_progress_at = now;
        self.last_progress_stage = stage.to_string();
        self.progress_observed = true;
        if let Some(tokens) = tokens_generated_count {
            self.last_tokens_generated = tokens.max(self.last_tokens_generated);
        }
        if let Some(next_state) = lifecycle_state_from_progress_stage(stage) {
            let _ = self.transition_state(next_state);
        }
        if now < self.max_deadline_at {
            let proposed_deadline = now
                + Duration::from_millis(
                    self.policy
                        .stall_timeout_ms
                        .max(self.policy.extension_step_ms),
                );
            self.deadline_at = std::cmp::max(
                self.deadline_at,
                std::cmp::min(proposed_deadline, self.max_deadline_at),
            );
        }
        true
    }

    fn check(&mut self) -> WatchdogCheck {
        let now = tokio::time::Instant::now();
        if now >= self.max_deadline_at {
            return WatchdogCheck::TimedOut;
        }

        if !self.progress_observed {
            let first_evidence_timeout_ms = if self.is_unclaimed() {
                self.policy.claim_timeout_ms
            } else {
                self.policy.first_progress_timeout_ms
            };
            return if self.elapsed_ms() >= first_evidence_timeout_ms {
                WatchdogCheck::Stalled
            } else {
                WatchdogCheck::Healthy
            };
        }

        if !self.first_token_observed() {
            let phase_timeout_ms = match self.state {
                AttemptLifecycleState::LoadingModel => self.policy.model_load_timeout_ms,
                AttemptLifecycleState::InferenceWarmup | AttemptLifecycleState::Running => {
                    self.policy.first_token_timeout_ms
                }
                _ => 0,
            };
            if phase_timeout_ms > 0 && self.elapsed_ms() >= phase_timeout_ms {
                return WatchdogCheck::Stalled;
            }
        }

        if self.elapsed_since_last_progress_ms() >= self.policy.stall_timeout_ms {
            return WatchdogCheck::Stalled;
        }

        if now < self.deadline_at {
            return WatchdogCheck::Healthy;
        }

        let grace_deadline =
            self.last_progress_at + Duration::from_millis(self.policy.stall_timeout_ms);
        if grace_deadline > self.deadline_at {
            self.deadline_at = std::cmp::min(grace_deadline, self.max_deadline_at);
            if now < self.deadline_at {
                return WatchdogCheck::Extended;
            }
        }

        WatchdogCheck::Healthy
    }
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
    task_retry_count: u32,
    task_fallback_count: u32,
    attempt_order: Vec<String>,
    attempt_records: HashMap<String, HumanAttemptRecord>,
}

#[derive(Debug, Clone)]
struct HumanAttemptRecord {
    attempt_id: String,
    peer_id: String,
    model_id: String,
    state: String,
    outcome: String,
    reason: Option<String>,
}

impl HumanAttemptRecord {
    fn new(attempt_id: String, peer_id: String, model_id: String) -> Self {
        Self {
            attempt_id,
            peer_id,
            model_id,
            state: "starting".to_string(),
            outcome: "in_progress".to_string(),
            reason: None,
        }
    }
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
            task_retry_count: 0,
            task_fallback_count: 0,
            attempt_order: Vec::new(),
            attempt_records: HashMap::new(),
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

    fn register_attempt_record(
        &mut self,
        attempt_id: &str,
        peer_id: &str,
        model_id: &str,
        is_fallback: bool,
    ) {
        if !self.attempt_order.iter().any(|id| id == attempt_id) {
            self.attempt_order.push(attempt_id.to_string());
        }
        let entry = self
            .attempt_records
            .entry(attempt_id.to_string())
            .or_insert_with(|| {
                HumanAttemptRecord::new(
                    attempt_id.to_string(),
                    peer_id.to_string(),
                    model_id.to_string(),
                )
            });
        entry.peer_id = peer_id.to_string();
        entry.model_id = model_id.to_string();
        entry.state = if is_fallback {
            "queued_fallback".to_string()
        } else {
            "starting".to_string()
        };
        entry.outcome = "in_progress".to_string();
        entry.reason = None;
    }

    fn update_attempt_state(
        &mut self,
        attempt_id: &str,
        state: impl Into<String>,
        outcome: Option<&str>,
        reason: Option<&str>,
    ) {
        if let Some(record) = self.attempt_records.get_mut(attempt_id) {
            record.state = state.into();
            if let Some(outcome) = outcome {
                record.outcome = outcome.to_string();
            }
            if let Some(reason) = reason {
                record.reason = Some(reason.to_string());
            }
        }
    }

    fn claim_attempt_record(&mut self, attempt_id: &str, worker_peer_id: &str) -> bool {
        if !is_valid_claiming_worker(worker_peer_id) {
            return false;
        }

        if let Some(record) = self.attempt_records.get_mut(attempt_id) {
            if is_unclaimed_worker_peer_id(&record.peer_id) {
                record.peer_id = worker_peer_id.to_string();
                self.current_peer = Some(worker_peer_id.to_string());
                return true;
            }
        }

        false
    }

    fn increment_task_retry_count(&mut self) {
        self.task_retry_count = self.task_retry_count.saturating_add(1);
    }

    fn increment_task_fallback_count(&mut self) {
        self.task_fallback_count = self.task_fallback_count.saturating_add(1);
    }

    fn attempt_summary_lines(&self) -> Vec<String> {
        self.attempt_order
            .iter()
            .enumerate()
            .filter_map(|(index, attempt_id)| {
                self.attempt_records.get(attempt_id).map(|record| {
                    let reason = record
                        .reason
                        .as_deref()
                        .map(|text| format!(" reason={}", text))
                        .unwrap_or_default();
                    format!(
                        "  {}. attempt={} peer={} model={} state={} outcome={}{}",
                        index + 1,
                        record.attempt_id,
                        record.peer_id,
                        record.model_id,
                        record.state,
                        record.outcome,
                        reason
                    )
                })
            })
            .collect()
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AttemptKey {
    task_id: String,
    attempt_id: String,
}

impl AttemptKey {
    fn new(task_id: impl Into<String>, attempt_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            attempt_id: attempt_id.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveAttempt {
    model_id: String,
    worker_peer_id: String,
    attempt_type: AttemptDispatchType,
}

impl ActiveAttempt {
    fn new(
        model_id: impl Into<String>,
        worker_peer_id: impl Into<String>,
        attempt_type: AttemptDispatchType,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            worker_peer_id: worker_peer_id.into(),
            attempt_type,
        }
    }

    fn is_fallback_broadcast(&self) -> bool {
        self.attempt_type == AttemptDispatchType::FallbackBroadcast
    }

    fn accepts_worker(&self, worker_peer_id: &str) -> bool {
        is_valid_claiming_worker(worker_peer_id)
            && (is_unclaimed_worker_peer_id(&self.worker_peer_id)
                || self.worker_peer_id == worker_peer_id)
    }

    fn claim_worker(&mut self, worker_peer_id: &str) -> Option<String> {
        if !self.is_fallback_broadcast()
            || !is_unclaimed_worker_peer_id(&self.worker_peer_id)
            || !is_valid_claiming_worker(worker_peer_id)
        {
            return None;
        }

        let previous = self.worker_peer_id.clone();
        self.worker_peer_id = worker_peer_id.to_string();
        Some(previous)
    }
}

fn should_accept_result_for_attempt(
    attempt_watchdogs: &HashMap<String, AttemptWatchdog>,
    active_attempts: &HashMap<AttemptKey, ActiveAttempt>,
    expected_task_id: Option<&str>,
    task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    current_request_matches: bool,
) -> bool {
    if expected_task_id != Some(task_id) {
        return false;
    }

    if current_request_matches {
        return true;
    }

    if let Some(active_attempt) = active_attempts.get(&AttemptKey::new(task_id, attempt_id)) {
        return active_attempt.accepts_worker(worker_peer_id);
    }

    let Some(watchdog) = attempt_watchdogs.get(attempt_id) else {
        return false;
    };

    watchdog.task_id == task_id
        && watchdog.is_fallback_broadcast()
        && watchdog.accepts_worker(worker_peer_id)
        && watchdog.elapsed_ms() <= watchdog.late_acceptance_deadline_ms()
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

fn emit_retry_counted_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    reason: &str,
    task_retry_count: u32,
) {
    log_observability_event(
        LogLevel::Info,
        "retry_counted",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert("task_retry_count".to_string(), task_retry_count.into());
            fields
        },
    );
}

fn emit_fallback_counted_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    reason: &str,
    task_fallback_count: u32,
) {
    log_observability_event(
        LogLevel::Info,
        "fallback_counted",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert(
                "task_fallback_count".to_string(),
                task_fallback_count.into(),
            );
            fields
        },
    );
}

fn emit_fallback_attempt_created_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    topic: &str,
    candidates: &[String],
) {
    log_observability_event(
        LogLevel::Info,
        "fallback_attempt_created",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert(
                "attempt_type".to_string(),
                AttemptDispatchType::FallbackBroadcast.as_str().into(),
            );
            fields.insert("worker_peer_id".to_string(), Value::Null);
            fields.insert("claimable".to_string(), true.into());
            fields.insert("broadcast_target".to_string(), "topic_mesh".into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("candidates".to_string(), serde_json::json!(candidates));
            fields
        },
    );
}

fn emit_fallback_attempt_claimed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    previous_worker_peer_id: &str,
    claim_source: AttemptClaimSource,
) {
    log_observability_event(
        LogLevel::Info,
        "fallback_attempt_claimed",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            if is_unclaimed_worker_peer_id(previous_worker_peer_id) {
                fields.insert("previous_worker_peer_id".to_string(), Value::Null);
            } else {
                fields.insert(
                    "previous_worker_peer_id".to_string(),
                    previous_worker_peer_id.into(),
                );
            }
            fields.insert("claim_source".to_string(), claim_source.as_str().into());
            fields
        },
    );
}

fn emit_attempt_state_changed_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: Option<&str>,
    previous_state: &str,
    new_state: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_state_changed",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("previous_state".to_string(), previous_state.into());
            fields.insert("state".to_string(), new_state.into());
            if let Some(worker_peer_id) = worker_peer_id {
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            }
            fields
        },
    );
}

fn emit_attempt_timeout_extended_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: Option<&str>,
    timeout_ms: u64,
    reason: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_timeout_extended",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("timeout_ms".to_string(), timeout_ms.into());
            fields.insert("reason".to_string(), reason.into());
            if let Some(worker_peer_id) = worker_peer_id {
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            }
            fields
        },
    );
}

fn emit_attempt_stalled_event(
    trace_task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    model_id: Option<&str>,
    last_progress_stage: &str,
    elapsed_since_last_progress_ms: u64,
) {
    log_observability_event(
        LogLevel::Warn,
        "attempt_stalled",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        Some(TASK_TIMEOUT_001),
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert(
                "last_progress_stage".to_string(),
                last_progress_stage.into(),
            );
            fields.insert(
                "elapsed_since_last_progress_ms".to_string(),
                elapsed_since_last_progress_ms.into(),
            );
            fields.insert("watchdog_reason".to_string(), "no_progress".into());
            fields.insert("error_kind".to_string(), "stall".into());
            fields.insert("recoverable".to_string(), true.into());
            fields
        },
    );
}

fn emit_attempt_progress_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_progress",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            if let Some(tokens_generated_count) = tokens_generated_count {
                fields.insert(
                    "tokens_generated_count".to_string(),
                    tokens_generated_count.into(),
                );
            }
            fields
        },
    );
}

fn remote_progress_stage_event(stage: &str) -> Option<&'static str> {
    match stage {
        "model_loading" | "model_load_started" => Some("remote_model_loading_progress"),
        "inference_warmup" => Some("remote_inference_warmup_progress"),
        "still_running" => Some("remote_still_running"),
        _ => None,
    }
}

fn emit_remote_progress_published_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
    published_topics: &[&str],
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_published",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields.insert("topics".to_string(), serde_json::json!(published_topics));
            if let Some(tokens_generated_count) = tokens_generated_count {
                fields.insert(
                    "tokens_generated_count".to_string(),
                    tokens_generated_count.into(),
                );
            }
            fields
        },
    );

    if let Some(event_name) = remote_progress_stage_event(stage) {
        log_observability_event(
            LogLevel::Info,
            event_name,
            trace_task_id,
            Some(trace_task_id),
            Some(model_id),
            None,
            {
                let mut fields = Map::new();
                fields.insert("attempt_id".to_string(), attempt_id.into());
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                fields.insert("stage".to_string(), stage.into());
                fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
                if let Some(tokens_generated_count) = tokens_generated_count {
                    fields.insert(
                        "tokens_generated_count".to_string(),
                        tokens_generated_count.into(),
                    );
                }
                fields
            },
        );
    }
}

fn emit_remote_progress_publish_attempt_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    topic: &str,
    elapsed_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_publish_attempt",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields
        },
    );
}

fn emit_remote_progress_publish_success_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    topic: &str,
    elapsed_ms: u64,
    message_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_publish_success",
        trace_task_id,
        Some(trace_task_id),
        Some(model_id),
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields.insert("message_id".to_string(), message_id.into());
            fields
        },
    );
}

fn emit_remote_progress_client_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    stage: &str,
    topic: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_progress_client_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("topic".to_string(), topic.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            if let Some(tokens_generated_count) = tokens_generated_count {
                fields.insert(
                    "tokens_generated_count".to_string(),
                    tokens_generated_count.into(),
                );
            }
            fields
        },
    );
}

fn emit_remote_result_client_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    topic_or_protocol: &str,
    elapsed_ms: u64,
    success: bool,
) {
    log_observability_event(
        LogLevel::Info,
        "remote_result_client_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("transport".to_string(), topic_or_protocol.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            fields.insert("success".to_string(), success.into());
            fields
        },
    );
}

fn remote_progress_status(stage: &str, tokens_generated_count: Option<u64>) -> &'static str {
    if matches!(
        stage,
        "inference_finished" | "result_serialized" | "result_published"
    ) {
        "completed"
    } else if matches!(stage, "model_loading" | "model_load_started") {
        "preparing_model"
    } else if matches!(
        stage,
        "model_loaded" | "model_load_completed" | "inference_warmup"
    ) {
        "warming_up"
    } else if tokens_generated_count.unwrap_or_default() > 0
        || matches!(stage, "first_token_generated" | "tokens_generated_count")
    {
        "generating"
    } else {
        "thinking"
    }
}

fn format_remote_progress_line(
    worker_peer_id: &str,
    attempt_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) -> String {
    let elapsed_secs = elapsed_ms / 1_000;
    match remote_progress_status(stage, tokens_generated_count) {
        "completed" => format!(
            "[Remote] worker={} attempt={} status=completed tokens={}",
            worker_peer_id,
            attempt_id,
            tokens_generated_count.unwrap_or_default()
        ),
        "generating" => format!(
            "[Remote] worker={} attempt={} status=generating elapsed={}s tokens={}",
            worker_peer_id,
            attempt_id,
            elapsed_secs,
            tokens_generated_count.unwrap_or_default()
        ),
        "preparing_model" => format!(
            "[Remote] worker={} attempt={} status=preparing_model elapsed={}s stage={}",
            worker_peer_id, attempt_id, elapsed_secs, stage
        ),
        "warming_up" => format!(
            "[Remote] worker={} attempt={} status=warming_up elapsed={}s stage={}",
            worker_peer_id, attempt_id, elapsed_secs, stage
        ),
        _ => format!(
            "[Remote] worker={} attempt={} status=thinking elapsed={}s stage={}",
            worker_peer_id, attempt_id, elapsed_secs, stage
        ),
    }
}

fn elapsed_ms_since(started_at: tokio::time::Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
}

fn emit_attempt_progress_received_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_progress_received",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("elapsed_ms".to_string(), elapsed_ms.into());
            if let Some(tokens_generated_count) = tokens_generated_count {
                fields.insert(
                    "tokens_generated_count".to_string(),
                    tokens_generated_count.into(),
                );
            }
            fields
        },
    );
}

fn emit_attempt_progress_recovered_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "attempt_progress_recovered",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields
        },
    );
}

fn emit_watchdog_reset_on_progress_event(
    trace_task_id: &str,
    attempt_id: &str,
    model_id: Option<&str>,
    worker_peer_id: &str,
    stage: &str,
    deadline_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "watchdog_reset_on_progress",
        trace_task_id,
        Some(trace_task_id),
        model_id,
        None,
        {
            let mut fields = Map::new();
            fields.insert("attempt_id".to_string(), attempt_id.into());
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("stage".to_string(), stage.into());
            fields.insert("deadline_ms".to_string(), deadline_ms.into());
            fields
        },
    );
}

fn publish_worker_progress_message(
    swarm: &mut Swarm<IamineBehaviour>,
    task_id: &str,
    attempt_id: &str,
    request_id: &str,
    model_id: &str,
    worker_peer_id: &str,
    stage: &str,
    elapsed_ms: u64,
    tokens_generated_count: Option<u64>,
) {
    let payload = serde_json::json!({
        "type": "InferenceProgress",
        "task_id": task_id,
        "attempt_id": attempt_id,
        "request_id": request_id,
        "model_id": model_id,
        "worker_peer": worker_peer_id,
        "worker_peer_id": worker_peer_id,
        "stage": stage,
        "elapsed_ms": elapsed_ms,
        "tokens_generated_count": tokens_generated_count,
    });
    let mut published_topics = Vec::new();
    for topic in [RESULTS_TOPIC, TASK_TOPIC] {
        emit_remote_progress_publish_attempt_event(
            task_id,
            attempt_id,
            model_id,
            worker_peer_id,
            stage,
            topic,
            elapsed_ms,
        );
        match swarm.behaviour_mut().gossipsub.publish(
            gossipsub::IdentTopic::new(topic),
            serde_json::to_vec(&payload).unwrap_or_default(),
        ) {
            Ok(message_id) => {
                let message_id = message_id.to_string();
                published_topics.push(topic);
                emit_remote_progress_publish_success_event(
                    task_id,
                    attempt_id,
                    model_id,
                    worker_peer_id,
                    stage,
                    topic,
                    elapsed_ms,
                    &message_id,
                );
            }
            Err(error) => log_observability_event(
                LogLevel::Warn,
                "remote_progress_publish_failed",
                task_id,
                Some(task_id),
                Some(model_id),
                Some(TASK_DISPATCH_UNCONFIRMED_001),
                {
                    let mut fields = Map::new();
                    fields.insert("attempt_id".to_string(), attempt_id.into());
                    fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                    fields.insert("topic".to_string(), topic.into());
                    fields.insert("stage".to_string(), stage.into());
                    fields.insert("error".to_string(), error.to_string().into());
                    fields
                },
            ),
        }
    }
    emit_remote_progress_published_event(
        task_id,
        attempt_id,
        model_id,
        worker_peer_id,
        stage,
        elapsed_ms,
        tokens_generated_count,
        &published_topics,
    );
    emit_attempt_progress_event(
        task_id,
        attempt_id,
        model_id,
        worker_peer_id,
        stage,
        elapsed_ms,
        tokens_generated_count,
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

fn mock_real_inference_result(
    task_id: String,
    model_id: String,
    prompt: String,
    max_tokens: u32,
) -> RealInferenceResult {
    let started = std::time::Instant::now();
    let output = if prompt.trim().is_empty() {
        "[mock] empty prompt".to_string()
    } else {
        format!("[mock:{}] {}", model_id, prompt)
    };
    let tokens = output
        .split_whitespace()
        .count()
        .min(max_tokens as usize)
        .max(1) as u32;
    RealInferenceResult::success(
        task_id,
        model_id,
        output,
        tokens,
        false,
        started.elapsed().as_millis() as u64,
        "mock".to_string(),
    )
}

pub(crate) fn log_observability_event(
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

fn emit_health_policy_decision_event(
    trace_id: &str,
    peer_id: &str,
    model_id: Option<&str>,
    trigger: &str,
    previous_state: &str,
    health: &NodeHealth,
) {
    let state = health.policy_state();
    let event = match state {
        "blacklisted" => "node_blacklisted",
        "degraded" => "node_degraded",
        "healthy" => "node_recovered",
        _ => "node_health_policy_decision",
    };
    log_observability_event(LogLevel::Info, event, trace_id, None, model_id, None, {
        let mut fields = Map::new();
        fields.insert("peer_id".to_string(), peer_id.into());
        fields.insert("trigger".to_string(), trigger.into());
        fields.insert("previous_state".to_string(), previous_state.into());
        fields.insert("state".to_string(), state.into());
        fields.insert(
            "success_rate".to_string(),
            (health.success_rate as f64).into(),
        );
        fields.insert("timeout_count".to_string(), health.timeout_count.into());
        fields.insert("failure_count".to_string(), health.failure_count.into());
        fields.insert("total_runs".to_string(), health.total_runs.into());
        if let Some(until) = health.blacklisted_until {
            fields.insert("blacklisted_until".to_string(), until.into());
        }
        fields
    });
}

fn record_health_policy_state_transition(
    health_state_tracker: &mut HashMap<String, String>,
    trace_id: &str,
    peer_id: &str,
    model_id: Option<&str>,
    trigger: &str,
    health: &NodeHealth,
) {
    let new_state = health.policy_state().to_string();
    let previous_state = health_state_tracker
        .get(peer_id)
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    if previous_state != new_state {
        emit_health_policy_decision_event(
            trace_id,
            peer_id,
            model_id,
            trigger,
            &previous_state,
            health,
        );
    }
    health_state_tracker.insert(peer_id.to_string(), new_state);
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
            print_usage();
            std::process::exit(1);
        }
    };
    if matches!(mode, NodeMode::Help) {
        print_usage();
        return Ok(());
    }
    if debug_flags.network || debug_flags.scheduler || debug_flags.tasks {
        println!(
            "[Debug] network={} scheduler={} tasks={}",
            debug_flags.network, debug_flags.scheduler, debug_flags.tasks
        );
    }
    let runtime_version = runtime_version_metadata();
    set_global_runtime_context(mode_label(&mode), &runtime_version);
    let is_cluster_status_mode = matches!(mode, NodeMode::ClusterStatus { .. });
    let control_plane_only = is_control_plane_mode(&mode);
    let worker_port = parse_worker_port(&args);

    // v0.6.1: Registry global para smart routing (capabilities + selección de nodo)
    let registry: SharedNodeRegistry = Arc::new(RwLock::new(NodeRegistry::new()));
    let topology: SharedNetworkTopology = Arc::new(RwLock::new(NetworkTopology::new()));

    // ═══════════════════════════════════════════════
    // CLI MODELS + CAPABILITIES — salida temprana
    // ═══════════════════════════════════════════════
    if handle_pre_network_mode(&mode, &runtime_version).await? {
        return Ok(());
    }

    match &mode {
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
            display_local_inference_completion(&result);
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
                display_local_inference_completion(&result);
                return Ok(());
            } else {
                display_distributed_inference_route_notice(
                    &control_flags,
                    &selected_model,
                    &candidate_models,
                    model_id.is_some(),
                );
            }
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

        NodeMode::ClusterStatus { .. } => {
            // Lightweight network discovery mode. It must not load models or run inference.
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
        println!("║    IaMine Worker Runtime         ║");
        println!("╚══════════════════════════════════╝\n");
        println!("🏷️  Version: {}", runtime_version);
    }

    // 1️⃣ Identidad de runtime (modo infer usa identidad efimera para evitar conflicto con worker local)
    let use_ephemeral_identity = matches!(mode, NodeMode::Infer { .. });
    let node_identity = if is_cluster_status_mode {
        NodeIdentity::load_or_create_quiet()
    } else if use_ephemeral_identity {
        NodeIdentity::ephemeral("infer_mode_peer_id_isolation")
    } else {
        NodeIdentity::load_or_create()
    };
    let peer_id = node_identity.peer_id;
    set_global_node_id(&peer_id.to_string());
    let id_keys = node_identity.keypair.clone();

    if matches!(mode, NodeMode::Worker) {
        emit_worker_startup_started_event(&peer_id.to_string(), worker_port);
    }

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

    if !is_cluster_status_mode {
        display_node_startup_summary(
            &peer_id.to_string(),
            &node_identity.wallet_address,
            benchmark.as_ref(),
            worker_slots,
            (!matches!(mode, NodeMode::Topology)).then(|| format!("{:?}", mode)),
            wallet.balance,
            wallet.tasks_completed,
            wallet.reputation,
        );
    }

    // 6️⃣ MODEL INFRASTRUCTURE v0.6
    let model_runtime = if is_cluster_status_mode {
        prepare_cluster_status_model_runtime_context(&peer_id.to_string())?
    } else {
        prepare_model_runtime_context(
            matches!(mode, NodeMode::Worker),
            &peer_id.to_string(),
            benchmark.as_ref(),
            true,
        )?
    };
    let storage_config = model_runtime.storage_config;
    let model_registry = model_runtime.registry;
    let model_storage = model_runtime.storage;
    let node_caps = model_runtime.node_caps;
    let worker_startup_policy = model_runtime.worker_startup_policy;
    let inference_engine = model_runtime.inference_engine;
    let validated_advertised_models = model_runtime.validated_advertised_models;
    let worker_setup = model_runtime.worker_setup;

    if let Some(cfg) = &worker_setup {
        cfg.display();
    }

    if matches!(mode, NodeMode::Worker) {
        node_caps.display();
    }

    if matches!(mode, NodeMode::Worker) {
        display_worker_model_inventory_and_maybe_autoprovision(
            &storage_config,
            &model_registry,
            &model_storage,
            &node_caps,
            worker_setup.as_ref(),
            benchmark.as_ref(),
            auto_model,
        )
        .await?;
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

    for topic_name in BROADCAST_PUBSUB_TOPICS {
        gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(topic_name))?;
    }
    let mut pubsub_topics = PubsubTopicTracker::default();
    let local_backend = worker_startup_policy
        .as_ref()
        .map(|policy| policy.backend.as_str())
        .unwrap_or("real");
    for topic_name in BROADCAST_PUBSUB_TOPICS {
        pubsub_topics.register_local_subscription(topic_name);
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
        if matches!(mode, NodeMode::Worker) {
            emit_worker_topic_subscribed_event(topic_name, &peer_id.to_string(), local_backend);
        } else if matches!(mode, NodeMode::Broadcast { .. }) {
            emit_controller_topic_subscribed_event(topic_name, &peer_id.to_string());
        }
    }
    if matches!(mode, NodeMode::Worker) {
        emit_worker_pubsub_ready_event(
            &peer_id.to_string(),
            local_backend,
            &BROADCAST_PUBSUB_TOPICS,
        );
    } else if matches!(mode, NodeMode::Broadcast { .. }) {
        emit_broadcast_pubsub_ready_event(&peer_id.to_string(), &BROADCAST_PUBSUB_TOPICS);
    }

    let mut kad_cfg = kad::Config::default();
    kad_cfg.set_query_timeout(Duration::from_secs(30));
    let kademlia =
        kad::Behaviour::with_config(peer_id, kad::store::MemoryStore::new(peer_id), kad_cfg);

    let mut cluster_registry = ClusterRegistry::with_default_cluster_id();
    let cluster_id = cluster_registry.cluster_id().to_string();
    let cluster_status_wait_ms = cluster_status_wait_ms_from_env();
    if is_cluster_status_mode {
        emit_cluster_status_requested(&cluster_id, cluster_status_wait_ms);
    }

    let mdns_behaviour = match mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id) {
        Ok(behaviour) => behaviour,
        Err(error) if is_cluster_status_mode => {
            render_and_emit_cluster_status(
                &cluster_registry,
                &cluster_id,
                matches!(mode, NodeMode::ClusterStatus { json: true }),
                Some("mdns_unavailable"),
                Some(error.to_string()),
            )?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

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
        mdns: mdns_behaviour,
        gossipsub: gossipsub_behaviour,
    };

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(60)),
    );

    let pool = if is_cluster_status_mode {
        Arc::new(WorkerPool::with_slots_quiet(worker_slots))
    } else {
        Arc::new(WorkerPool::with_slots(worker_slots))
    };
    let queue = Arc::new(TaskQueue::new(peer_id.to_string()));
    let scheduler = Arc::new(TaskScheduler::new());
    let task_manager = Arc::new(TaskManager::new());
    let (task_response_tx, mut task_response_rx) =
        tokio::sync::mpsc::channel::<PendingTaskResponse>(64);
    let (broadcast_result_tx, mut broadcast_result_rx) =
        tokio::sync::mpsc::channel::<BroadcastResultToPublish>(64);
    let heartbeat = Arc::new(HeartbeatService::new());
    let metrics = Arc::new(RwLock::new(NodeMetrics::new()));
    let mut capabilities = if is_cluster_status_mode {
        WorkerCapabilities {
            cpu_cores: worker_slots,
            ram_gb: 0,
            gpu_available: false,
            disk_available_gb: 0,
            supported_tasks: vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string(),
            ],
            avg_latency_ms: 0.0,
        }
    } else {
        WorkerCapabilities::detect()
    };
    if let Some(policy) = worker_startup_policy.as_ref() {
        apply_worker_startup_policy_to_capabilities(&mut capabilities, policy);
    }
    let registry_models_for_display: Vec<RegistryModelDisplay> = model_registry
        .list()
        .into_iter()
        .map(RegistryModelDisplay::from)
        .collect();
    let storage_models_for_display = model_storage.list_local_models();
    let model_display_view = build_model_display_view(
        &storage_models_for_display,
        &registry_models_for_display,
        &validated_advertised_models,
        local_backend,
        worker_startup_policy
            .as_ref()
            .map(|policy| policy.real_inference_available)
            .unwrap_or(true),
        capabilities.supported_tasks.clone(),
        storage_config.max_storage_gb,
        model_storage.total_size_bytes(),
    );
    let _capability_display_snapshot = build_capability_display_snapshot(
        local_backend,
        model_display_view.real_inference_available,
        model_display_view.executable_models_count,
        model_display_view.storage_models_count,
        model_display_view.registry_models_count,
        &capabilities.supported_tasks,
    );
    let mut task_cache = TaskCache::new(1000);
    let mut peer_tracker = PeerTracker::new();
    let mut rate_limiter = RateLimiter::new(100); // ← 100 msgs/sec max
    let mut health_state_tracker: HashMap<String, String> = HashMap::new();
    let mut human_log_throttle = HumanLogThrottle::default();
    let mut worker_startup_ready_emitted = false;

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

    if !is_cluster_status_mode {
        println!("   Slots paralelos: {}", pool.max_concurrent);
        println!("   Task queue: activa (max 3 reintentos)");
        println!("   Scheduler: activo (bid window 1.5s)\n");
    }

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

    if matches!(mode, NodeMode::Worker) {
        let (timeout_blacklist_threshold, failure_blacklist_threshold) = health_policy_thresholds();
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
        log_observability_event(
            LogLevel::Info,
            "health_policy_configured",
            "startup",
            None,
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert(
                    "timeout_blacklist_threshold".to_string(),
                    timeout_blacklist_threshold.into(),
                );
                fields.insert(
                    "failure_blacklist_threshold".to_string(),
                    failure_blacklist_threshold.into(),
                );
                fields.insert(
                    "policy".to_string(),
                    "degraded_first_blacklist_after_threshold".into(),
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
    if let Err(error) = swarm.listen_on(listen_addr) {
        if is_cluster_status_mode {
            render_and_emit_cluster_status(
                &cluster_registry,
                &cluster_id,
                matches!(mode, NodeMode::ClusterStatus { json: true }),
                Some("listen_unavailable"),
                Some(error.to_string()),
            )?;
            return Ok(());
        }
        if matches!(mode, NodeMode::Worker) {
            let error_text = error.to_string();
            let hint = startup_listen_error_hint(&error_text, worker_port);
            emit_worker_startup_failed_event(&peer_id.to_string(), worker_port, &error_text, &hint);
            eprintln!(
                "❌ [Worker] No se pudo abrir puerto {}: {}",
                worker_port, error_text
            );
            eprintln!("   {}", hint);
        }
        return Err(error.into());
    }

    if matches!(mode, NodeMode::Worker) {
        start_worker_metrics_or_continue(
            Arc::clone(&metrics),
            MetricsStartupContext {
                trace_id: "startup",
                node_id: &node_identity.node_id,
                peer_id: &peer_id.to_string(),
                worker_port,
                resource_policy: &resource_policy,
                worker_slots,
                max_concurrent: pool.max_concurrent,
                available_slots: pool.available_slots(),
            },
        );
    }

    let mut heartbeat_rx = HeartbeatService::start(5);
    let mut nodes_tick = tokio::time::interval(Duration::from_secs(5)); // ← nuevo ticker dedicado
    let mut broadcast_tick = tokio::time::interval(Duration::from_millis(500));
    let cluster_status_deadline = tokio::time::sleep(Duration::from_millis(cluster_status_wait_ms));
    tokio::pin!(cluster_status_deadline);

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
    let mut broadcast_offer_state = match &mode {
        NodeMode::Broadcast { .. } => Some(BroadcastOfferState::new(uuid_simple())),
        _ => None,
    };
    let mut executed_broadcast_assignments: HashSet<String> = HashSet::new();

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
    let mut attempt_watchdogs: HashMap<String, AttemptWatchdog> = HashMap::new();
    let mut active_attempts: HashMap<AttemptKey, ActiveAttempt> = HashMap::new();
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
            _ = &mut cluster_status_deadline, if is_cluster_status_mode => {
                render_and_emit_cluster_status(
                    &cluster_registry,
                    &cluster_id,
                    matches!(mode, NodeMode::ClusterStatus { json: true }),
                    None,
                    None,
                )?;
                return Ok(());
            }

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

            _ = broadcast_tick.tick(), if matches!(mode, NodeMode::Broadcast { .. }) => {
                let NodeMode::Broadcast { task_type, data } = &mode else {
                    continue;
                };
                let Some(state) = broadcast_offer_state.as_mut() else {
                    continue;
                };

                if !state.prepared {
                    state.prepared = true;
                    emit_broadcast_task_offer_prepared_event(
                        &state.task_id,
                        task_type,
                        data,
                        &peer_id.to_string(),
                    );
                    emit_task_lifecycle_created(
                        &state.task_id,
                        task_type,
                        data,
                        &peer_id.to_string(),
                    );
                }

                if state.published && !state.assignment_published {
                    if let Some(winner) = scheduler.try_assign(&state.task_id).await {
                        let deadline_ms = 30_000u64;
                        let assign = build_broadcast_task_assign_payload(
                            &state.task_id,
                            &winner,
                            &peer_id.to_string(),
                            deadline_ms,
                            task_type,
                            data,
                        );
                        match swarm.behaviour_mut().gossipsub.publish(
                            gossipsub::IdentTopic::new(ASSIGN_TOPIC),
                            serde_json::to_vec(&assign).unwrap_or_default(),
                        ) {
                            Ok(message_id) => {
                                state.assignment_published = true;
                                state.assigned_worker = Some(winner.clone());
                                emit_broadcast_task_assign_published_event(
                                    &state.task_id,
                                    &winner,
                                    &message_id.to_string(),
                                    deadline_ms,
                                );
                                emit_task_lifecycle_assigned(
                                    &state.task_id,
                                    task_type,
                                    &winner,
                                    known_workers
                                        .iter()
                                        .map(|worker| worker.to_string())
                                        .collect(),
                                    TaskSelectionReason::CurrentBroadcastPolicy,
                                );
                                println!("🏆 Asignando worker={} task_id={}", winner, state.task_id);
                            }
                            Err(error) => {
                                log_observability_event(
                                    LogLevel::Error,
                                    "broadcast_task_assign_publish_failed",
                                    &state.task_id,
                                    Some(&state.task_id),
                                    None,
                                    Some(TASK_DISPATCH_UNCONFIRMED_001),
                                    {
                                        let mut fields = Map::new();
                                        fields.insert("assigned_worker".to_string(), winner.into());
                                        fields.insert("topic".to_string(), ASSIGN_TOPIC.into());
                                        fields.insert("error".to_string(), error.to_string().into());
                                        fields
                                    },
                                );
                            }
                        }
                    }
                    continue;
                }

                if state.published || state.failed {
                    continue;
                }

                let connected_peers = swarm.connected_peers().count();
                let newly_observed_subscriptions = sync_pubsub_tracker_from_gossipsub(
                    &mut pubsub_topics,
                    &swarm.behaviour().gossipsub,
                );
                let mesh_peer_ids =
                    gossipsub_mesh_peer_ids(&swarm.behaviour().gossipsub, TASK_TOPIC);
                let mesh_peers = mesh_peer_ids.len();
                let subscribed_peers = pubsub_topics.topic_peer_count(TASK_TOPIC).max(
                    gossipsub_topic_peer_count(&swarm.behaviour().gossipsub, TASK_TOPIC),
                );
                for (observed_peer_id, topic_name) in newly_observed_subscriptions {
                    emit_observed_peer_subscription_event(
                        "controller_observed_peer_subscription",
                        &peer_id.to_string(),
                        &observed_peer_id,
                        topic_name,
                        "broadcast",
                        "gossipsub_all_peers",
                    );
                    if topic_name == TASK_TOPIC {
                        state.mark_subscription_seen();
                        emit_broadcast_topic_subscriber_seen_event(
                            TASK_TOPIC,
                            &observed_peer_id,
                            connected_peers,
                            subscribed_peers,
                            mesh_peers,
                            "gossipsub_all_peers",
                        );
                    }
                }
                let all_peers_per_topic =
                    gossipsub_all_peers_per_topic_value(&swarm.behaviour().gossipsub);
                let elapsed_ms = state.elapsed_ms();
                let readiness_snapshot = BroadcastReadinessSnapshot::new(
                    connected_peers,
                    subscribed_peers,
                    mesh_peers,
                    elapsed_ms,
                    state.attempts,
                )
                .with_gossipsub_details(
                    all_peers_per_topic,
                    mesh_peer_ids.clone(),
                    state.last_publish_failure_reason.as_deref(),
                );
                if state.should_log_readiness_wait() {
                    emit_broadcast_readiness_state_event(&state.task_id, &readiness_snapshot);
                    if !readiness_snapshot.ready {
                        emit_broadcast_readiness_waiting_event(&state.task_id, &readiness_snapshot);
                    }
                }

                if elapsed_ms >= BROADCAST_READINESS_TIMEOUT_MS
                    || state.attempts >= BROADCAST_OFFER_MAX_ATTEMPTS
                {
                    state.failed = true;
                    let reason = if elapsed_ms >= BROADCAST_READINESS_TIMEOUT_MS {
                        "pubsub_readiness_timeout"
                    } else {
                        "publish_attempts_exhausted"
                    };
                    emit_broadcast_task_offer_publish_failed_event(
                        &state.task_id,
                        task_type,
                        reason,
                        state.attempts.saturating_add(1),
                        false,
                        &readiness_snapshot,
                    );
                    emit_broadcast_task_offer_readiness_timeout_event(
                        &state.task_id,
                        task_type,
                        &readiness_snapshot,
                        state.last_publish_failure_reason.as_deref(),
                    );
                    emit_task_lifecycle_failed(
                        &state.task_id,
                        task_type,
                        TaskLifecycleErrorCode::TaskTimeout,
                        reason,
                    );
                    eprintln!(
                        "❌ [Broadcast] PubSub no quedó listo: task_id={} peers={} subscribed_peers={} mesh_peers={} attempts={} reason={}",
                        state.task_id,
                        connected_peers,
                        subscribed_peers,
                        mesh_peers,
                        state.attempts,
                        reason
                    );
                    break;
                }

                if !broadcast_offer_ready(subscribed_peers, mesh_peers, connected_peers, elapsed_ms) {
                    continue;
                }

                if !state.can_attempt_publish() {
                    continue;
                }

                if !state.scheduler_registered {
                    scheduler
                        .register_task(state.task_id.clone(), task_type.clone())
                        .await;
                    state.scheduler_registered = true;
                }

                let offer = build_broadcast_task_offer_payload(
                    &state.task_id,
                    task_type,
                    data,
                    &peer_id.to_string(),
                );
                let payload = serde_json::to_vec(&offer)
                    .map_err(|error| format!("Broadcast TaskOffer payload error: {}", error))?;
                let attempt_number = state.attempts.saturating_add(1);
                emit_broadcast_task_offer_publish_attempt_event(
                    &state.task_id,
                    task_type,
                    data,
                    payload.len(),
                    attempt_number,
                    &readiness_snapshot,
                );

                match swarm.behaviour_mut().gossipsub.publish(
                    gossipsub::IdentTopic::new(TASK_TOPIC),
                    payload,
                ) {
                    Ok(message_id) => {
                        state.record_publish_result(true, None);
                        let published_snapshot = BroadcastReadinessSnapshot::new(
                            connected_peers,
                            subscribed_peers,
                            mesh_peers,
                            state.elapsed_ms(),
                            state.attempts,
                        )
                        .with_gossipsub_details(
                            gossipsub_all_peers_per_topic_value(&swarm.behaviour().gossipsub),
                            mesh_peer_ids.clone(),
                            state.last_publish_failure_reason.as_deref(),
                        );
                        let message_id = message_id.to_string();
                        emit_broadcast_task_offer_published_event(
                            &state.task_id,
                            task_type,
                            data,
                            &message_id,
                            attempt_number,
                            &published_snapshot,
                        );
                        waiting_for_response = true;
                        tasks_sent = true;
                        println!(
                            "📣 [Broadcast] TaskOffer publicado: task_id={} type={} data='{}'",
                            state.task_id, task_type, data
                        );
                    }
                    Err(error) => {
                        let error_text = error.to_string();
                        state.record_publish_result(false, Some(&error_text));
                        let failure_snapshot = BroadcastReadinessSnapshot::new(
                            connected_peers,
                            subscribed_peers,
                            mesh_peers,
                            state.elapsed_ms(),
                            state.attempts,
                        )
                        .with_gossipsub_details(
                            gossipsub_all_peers_per_topic_value(&swarm.behaviour().gossipsub),
                            mesh_peer_ids.clone(),
                            state.last_publish_failure_reason.as_deref(),
                        );
                        let still_recoverable = state.elapsed_ms() < BROADCAST_READINESS_TIMEOUT_MS
                            && state.attempts < BROADCAST_OFFER_MAX_ATTEMPTS;
                        emit_broadcast_task_offer_publish_failed_event(
                            &state.task_id,
                            task_type,
                            &error_text,
                            attempt_number,
                            still_recoverable,
                            &failure_snapshot,
                        );
                        if !still_recoverable {
                            state.failed = true;
                            emit_broadcast_task_offer_readiness_timeout_event(
                                &state.task_id,
                                task_type,
                                &failure_snapshot,
                                state.last_publish_failure_reason.as_deref(),
                            );
                            emit_task_lifecycle_failed(
                                &state.task_id,
                                task_type,
                                TaskLifecycleErrorCode::TaskTimeout,
                                &error_text,
                            );
                            eprintln!(
                                "❌ [Broadcast] No se pudo publicar TaskOffer tras {} intentos: {}",
                                state.attempts, error_text
                            );
                            break;
                        }
                    }
                }
            }

            Some(broadcast_result) = broadcast_result_rx.recv() => {
                let payload = build_broadcast_task_result_payload(&broadcast_result);
                let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
                emit_broadcast_result_publish_attempt_event(
                    &broadcast_result,
                    payload_bytes.len(),
                );
                match swarm.behaviour_mut().gossipsub.publish(
                    gossipsub::IdentTopic::new(RESULTS_TOPIC),
                    payload_bytes,
                ) {
                    Ok(message_id) => {
                        emit_broadcast_result_published_event(
                            &broadcast_result,
                            &message_id.to_string(),
                            payload.to_string().len(),
                        );
                        println!(
                            "📤 [Worker] TaskResult publicado: task_id={} success={}",
                            broadcast_result.task_id, broadcast_result.success
                        );
                    }
                    Err(error) => {
                        emit_broadcast_result_publish_failed_event(
                            &broadcast_result,
                            &error.to_string(),
                            true,
                        );
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
                        let hb = build_worker_heartbeat_payload(
                            &peer_id.to_string(),
                            available_slots,
                            rep.reputation_score,
                            uptime,
                            rep.avg_execution_ms,
                            &capabilities,
                        );
                        let hb_topic = gossipsub::IdentTopic::new("iamine-heartbeat");
                        let _ = swarm.behaviour_mut().gossipsub.publish(
                            hb_topic, serde_json::to_vec(&hb).unwrap(),
                        );
                    }

                    // ─── Broadcast NodeModelsBroadcast
                    if let Some(payload) = build_node_models_broadcast_payload(
                        &peer_id.to_string(),
                        &validated_advertised_models,
                    ) {
                        let _ = swarm.behaviour_mut().gossipsub.publish(
                            gossipsub::IdentTopic::new("iamine-heartbeat"),
                            serde_json::to_vec(&payload).unwrap(),
                        );
                    }

                    // Broadcast capabilities
                    let cap_hb = build_node_capability_heartbeat(
                        &peer_id.to_string(),
                        benchmark.as_ref(),
                        &node_caps,
                        &validated_advertised_models,
                        worker_slots,
                        active_tasks,
                        peer_tracker.avg_latency(),
                    );
                    let _ = swarm.behaviour_mut().gossipsub.publish(
                        gossipsub::IdentTopic::new(CAP_TOPIC),
                        serde_json::to_vec(&cap_hb).unwrap(),
                    );

                    let real_inference_available = worker_startup_policy
                        .as_ref()
                        .map(|policy| policy.real_inference_available)
                        .unwrap_or(false);
                    let execution_mode = if worker_startup_policy
                        .as_ref()
                        .map(|policy| policy.mock_backend())
                        .unwrap_or(false)
                    {
                        ClusterExecutionMode::Mock
                    } else if real_inference_available {
                        ClusterExecutionMode::Real
                    } else {
                        ClusterExecutionMode::Degraded
                    };
                    let cluster_backend = worker_startup_policy
                        .as_ref()
                        .map(|policy| {
                            ClusterBackend::from_backend_and_accelerator(
                                policy.backend.as_str(),
                                Some(&node_caps.accelerator),
                            )
                        })
                        .unwrap_or(ClusterBackend::Unknown);
                    let metrics_status = match metrics_policy::metrics_startup_decision(worker_port)
                    {
                        metrics_policy::MetricsStartupDecision::StartMetrics { .. } => {
                            ClusterMetricsStatus::Available
                        }
                        metrics_policy::MetricsStartupDecision::ContinueWithoutMetrics { .. } => {
                            ClusterMetricsStatus::Fallback
                        }
                        metrics_policy::MetricsStartupDecision::Disabled { .. } => {
                            ClusterMetricsStatus::Disabled
                        }
                    };
                    let cluster_status_message = ClusterNodeStatusMessage::worker(
                        cluster_id.clone(),
                        peer_id.to_string(),
                        local_hostname(),
                        execution_mode,
                        cluster_backend,
                        real_inference_available,
                        capabilities.supported_tasks.clone(),
                        storage_models_for_display.clone(),
                        registry_models_for_display
                            .iter()
                            .map(|model| model.id.clone())
                            .collect(),
                        validated_advertised_models.clone(),
                        metrics_status,
                        Some(peer_tracker.avg_latency().max(0.0).round() as u32),
                    );
                    let _ = swarm.behaviour_mut().gossipsub.publish(
                        gossipsub::IdentTopic::new(HEARTBEAT_TOPIC),
                        serde_json::to_vec(&cluster_status_message).unwrap(),
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
                            let duplicate_inflight = attempt_watchdogs.values().any(|watchdog| {
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
                            emit_remote_delivery_topic_ready_events(
                                &trace_task_id,
                                &rid,
                                &routed_model,
                                &pubsub_topics,
                            );

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
                                        false,
                                    );
                                    infer_state.register_attempt_record(
                                        &rid,
                                        &best_peer,
                                        &routed_model,
                                        false,
                                    );
                                    if is_retry_attempt {
                                        let _ = record_distributed_task_retry();
                                        infer_state.increment_task_retry_count();
                                        emit_retry_counted_event(
                                            &trace_task_id,
                                            &rid,
                                            Some(&routed_model),
                                            if is_model_switch_attempt {
                                                "model_switch_retry"
                                            } else {
                                                "retry_after_failure"
                                            },
                                            infer_state.task_retry_count,
                                        );
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
                                                "claim_timeout_ms".to_string(),
                                                timeout_policy.claim_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "first_progress_timeout_ms".to_string(),
                                                timeout_policy.first_progress_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "model_load_timeout_ms".to_string(),
                                                timeout_policy.model_load_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "first_token_timeout_ms".to_string(),
                                                timeout_policy.first_token_timeout_ms.into(),
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
                                                "max_execution_timeout_ms".to_string(),
                                                timeout_policy.max_execution_timeout_ms.into(),
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
                                    let previous_state = watchdog.state;
                                    let _ = watchdog.transition_state(AttemptLifecycleState::Starting);
                                    emit_attempt_state_changed_event(
                                        &trace_task_id,
                                        &rid,
                                        Some(&routed_model),
                                        Some(&best_peer),
                                        previous_state.as_str(),
                                        watchdog.state.as_str(),
                                    );
                                    attempt_watchdogs.insert(rid.clone(), watchdog);
                                    active_attempts.insert(
                                        AttemptKey::new(trace_task_id.clone(), rid.clone()),
                                        ActiveAttempt::new(
                                            routed_model.clone(),
                                            best_peer.clone(),
                                            AttemptDispatchType::Direct,
                                        ),
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
                            emit_remote_delivery_topic_ready_events(
                                &trace_task_id,
                                &rid,
                                &selected_model,
                                &pubsub_topics,
                            );

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
                                            fields.insert("worker_peer_id".to_string(), Value::Null);
                                            fields.insert(
                                                "attempt_type".to_string(),
                                                AttemptDispatchType::FallbackBroadcast.as_str().into(),
                                            );
                                            fields.insert("claimable".to_string(), true.into());
                                            fields.insert(
                                                "timeout_ms".to_string(),
                                                timeout_policy.timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "claim_timeout_ms".to_string(),
                                                timeout_policy.claim_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "first_progress_timeout_ms".to_string(),
                                                timeout_policy.first_progress_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "model_load_timeout_ms".to_string(),
                                                timeout_policy.model_load_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "first_token_timeout_ms".to_string(),
                                                timeout_policy.first_token_timeout_ms.into(),
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
                                                "max_execution_timeout_ms".to_string(),
                                                timeout_policy.max_execution_timeout_ms.into(),
                                            );
                                            fields.insert(
                                                "latency_class".to_string(),
                                                timeout_policy.latency_class.into(),
                                            );
                                            fields
                                        },
                                    );
                                    let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
                                        trace_task_id.clone(),
                                        rid.clone(),
                                        selected_model.clone(),
                                        timeout_policy,
                                    );
                                    let previous_state = watchdog.state;
                                    let _ = watchdog.transition_state(AttemptLifecycleState::Starting);
                                    emit_attempt_state_changed_event(
                                        &trace_task_id,
                                        &rid,
                                        Some(&selected_model),
                                        None,
                                        previous_state.as_str(),
                                        watchdog.state.as_str(),
                                    );
                                    attempt_watchdogs.insert(rid.clone(), watchdog);
                                    active_attempts.insert(
                                        AttemptKey::new(trace_task_id.clone(), rid.clone()),
                                        ActiveAttempt::new(
                                            selected_model.clone(),
                                            UNCLAIMED_WORKER_PEER_ID,
                                            AttemptDispatchType::FallbackBroadcast,
                                        ),
                                    );
                                    emit_fallback_attempt_created_event(
                                        &trace_task_id,
                                        &rid,
                                        &selected_model,
                                        TASK_TOPIC,
                                        &candidates,
                                    );
                                    let is_retry_attempt = infer_state.retry_state.retry_count > 0;
                                    let _ = record_task_attempt(
                                        &trace_task_id,
                                        UNCLAIMED_WORKER_PEER_ID,
                                        &selected_model,
                                        is_retry_attempt,
                                        true,
                                    );
                                    infer_state.record_attempt(
                                        UNCLAIMED_WORKER_PEER_ID.to_string(),
                                        selected_model.clone(),
                                        profile.task_type,
                                        semantic_prompt.clone(),
                                        local_cluster.clone(),
                                        candidates.clone(),
                                    );
                                    infer_state.current_peer = None;
                                    infer_state.register_attempt_record(
                                        &rid,
                                        UNCLAIMED_WORKER_PEER_ID,
                                        &selected_model,
                                        true,
                                    );
                                    if is_retry_attempt {
                                        let _ = record_distributed_task_retry();
                                        infer_state.increment_task_retry_count();
                                        emit_retry_counted_event(
                                            &trace_task_id,
                                            &rid,
                                            Some(&selected_model),
                                            "retry_after_failure",
                                            infer_state.task_retry_count,
                                        );
                                    }
                                    let _ = record_distributed_task_fallback();
                                    infer_state.increment_task_fallback_count();
                                    emit_fallback_counted_event(
                                        &trace_task_id,
                                        &rid,
                                        Some(&selected_model),
                                        "fallback_broadcast",
                                        infer_state.task_fallback_count,
                                    );
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
                        if pending_inference.contains_key(&rid) {
                            let mut timeout_context: Option<(
                                String,
                                String,
                                String,
                                String,
                                u64,
                                u64,
                                u64,
                                u64,
                            )> = None;
                            let mut should_retry = false;
                            if let Some(watchdog) = attempt_watchdogs.get_mut(&rid) {
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
                                        watchdog.no_progress_warning_emitted = true;
                                            let previous_state = watchdog.state;
                                        if watchdog.transition_state(AttemptLifecycleState::Stalled)
                                        {
                                            emit_attempt_state_changed_event(
                                                &watchdog.task_id,
                                                &watchdog.attempt_id,
                                                Some(&watchdog.model_id),
                                                Some(&watchdog.worker_peer_id),
                                                previous_state.as_str(),
                                                watchdog.state.as_str(),
                                            );
                                        }
                                        emit_attempt_stalled_event(
                                            &watchdog.task_id,
                                            &watchdog.attempt_id,
                                            &watchdog.worker_peer_id,
                                            Some(&watchdog.model_id),
                                            &watchdog.last_progress_stage,
                                            watchdog.elapsed_since_last_progress_ms(),
                                        );
                                        let before_timeout = watchdog.state;
                                        if watchdog.transition_state(AttemptLifecycleState::TimedOut)
                                        {
                                            emit_attempt_state_changed_event(
                                                &watchdog.task_id,
                                                &watchdog.attempt_id,
                                                Some(&watchdog.model_id),
                                                Some(&watchdog.worker_peer_id),
                                                before_timeout.as_str(),
                                                watchdog.state.as_str(),
                                            );
                                        }
                                        timeout_context = Some((
                                            watchdog.task_id.clone(),
                                            watchdog.attempt_id.clone(),
                                            watchdog.worker_peer_id.clone(),
                                            watchdog.model_id.clone(),
                                            watchdog.elapsed_ms(),
                                            watchdog.elapsed_since_last_progress_ms(),
                                            watchdog.policy.timeout_ms,
                                            watchdog.policy.max_wait_ms,
                                        ));
                                        should_retry = true;
                                    }
                                }
                            } else if let Some(t0) = pending_inference.get(&rid) {
                                if t0.elapsed().as_millis() as u64 >= INFER_TIMEOUT_MS {
                                    timeout_context = Some((
                                        distributed_infer_state
                                            .as_ref()
                                            .map(|state| state.trace_task_id.clone())
                                            .unwrap_or_else(|| "-".to_string()),
                                        rid.clone(),
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_peer.clone())
                                            .unwrap_or_else(|| "-".to_string()),
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.clone())
                                            .unwrap_or_else(|| "-".to_string()),
                                        INFER_TIMEOUT_MS,
                                        INFER_TIMEOUT_MS,
                                        INFER_TIMEOUT_MS,
                                        INFER_TIMEOUT_MS,
                                    ));
                                    should_retry = true;
                                }
                            }

                            if should_retry {
                                let failure_kind = FailureKind::Timeout;
                                if let Some((
                                    trace_task_id,
                                    attempt_id,
                                    worker_peer_id,
                                    model_id,
                                    elapsed_ms,
                                    elapsed_since_progress_ms,
                                    adaptive_timeout_ms,
                                    max_wait_ms,
                                )) = timeout_context
                                {
                                    eprintln!(
                                        "\n[Fault] task_id={} attempt_id={} kind={:?} timeout_ms={}",
                                        trace_task_id, attempt_id, failure_kind, adaptive_timeout_ms
                                    );
                                    if let Some(infer_state) = distributed_infer_state.as_mut() {
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
                                                record_health_policy_state_transition(
                                                    &mut health_state_tracker,
                                                    &trace_task_id,
                                                    &worker_peer_id,
                                                    infer_state.current_model.as_deref(),
                                                    "timeout",
                                                    &health,
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
                                        infer_state.update_attempt_state(
                                            &attempt_id,
                                            "timed_out",
                                            Some("timeout"),
                                            Some("watchdog_no_progress"),
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
                                            infer_request_id =
                                                Some(infer_state.current_request_id.clone());
                                            infer_broadcast_sent = false;
                                            waiting_for_response = false;
                                            continue;
                                        }

                                        finalize_and_report_distributed_task_observability(
                                            infer_state,
                                            infer_started_at,
                                            true,
                                            false,
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
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, address.clone());
                    if matches!(mode, NodeMode::Worker) && !worker_startup_ready_emitted {
                        emit_worker_listening_event(&peer_id.to_string(), worker_port, &address);
                        if let Some(policy) = worker_startup_policy.as_ref() {
                            emit_worker_startup_ready_event(
                                &peer_id.to_string(),
                                worker_port,
                                policy,
                            );
                        }
                        worker_startup_ready_emitted = true;
                    }
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
                            let inserted = cluster_registry.add_or_update_discovered_node(
                                &pid.to_string(),
                                Some(addr.to_string()),
                                unix_now_ms(),
                            );
                            emit_cluster_discovery_update(
                                inserted,
                                &cluster_id,
                                &pid.to_string(),
                                "mdns",
                                Some(addr.to_string()),
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
                    } else if message.topic == gossipsub::IdentTopic::new(BIDS_TOPIC).hash() {
                        BIDS_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(ASSIGN_TOPIC).hash() {
                        ASSIGN_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(DIRECT_INF_TOPIC).hash() {
                        DIRECT_INF_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(RESULTS_TOPIC).hash() {
                        RESULTS_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(CAP_TOPIC).hash() {
                        CAP_TOPIC
                    } else if message.topic == gossipsub::IdentTopic::new(HEARTBEAT_TOPIC).hash() {
                        HEARTBEAT_TOPIC
                    } else {
                        "unknown"
                    };

                    // 1) Procesar capabilities por TOPIC (no por msg_type)
                    if message.topic == gossipsub::IdentTopic::new(CAP_TOPIC).hash() {
                        if let Ok(hb) = serde_json::from_slice::<NodeCapabilityHeartbeat>(&message.data) {
                            let _ = registry.write().await.update_from_heartbeat(hb.clone());
                            cluster_registry.update_from_capability_heartbeat(&hb, unix_now_ms());
                            emit_cluster_capabilities_updated(
                                &cluster_registry,
                                &cluster_id,
                                &hb.peer_id,
                                CAP_TOPIC,
                                hb.models.len(),
                                Some(hb.latency_ms),
                            );
                        }
                        continue;
                    }

                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&message.data) {
                        let msg_type = msg["type"].as_str().unwrap_or("");

                        // ...existing rate limiter...

                        match msg_type {
                            CLUSTER_NODE_STATUS_TYPE => {
                                if let Ok(cluster_message) =
                                    serde_json::from_value::<ClusterNodeStatusMessage>(msg.clone())
                                {
                                    update_cluster_status_message_and_emit(
                                        &mut cluster_registry,
                                        &cluster_id,
                                        cluster_message,
                                        message_topic,
                                    );
                                }
                            }
                            "TaskOffer" if matches!(mode, NodeMode::Worker) => {
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let task_type = msg["task_type"].as_str().unwrap_or("").to_string();
                                let data = msg["data"].as_str().unwrap_or("").to_string();
                                let origin_peer = msg["origin_peer"].as_str().unwrap_or("").to_string();
                                emit_worker_task_offer_received_event(
                                    &task_id,
                                    &task_type,
                                    &data,
                                    &origin_peer,
                                    &from_peer,
                                );
                                println!(
                                    "📥 [Worker] TaskOffer recibido: task_id={} type={} data='{}'",
                                    task_id, task_type, data
                                );

                                if task_cache.is_duplicate(&task_id) {
                                    println!("⚡ [Cache] Tarea {} ya vista", &task_id[..8.min(task_id.len())]);
                                } else if !capabilities.supports(&task_type) {
                                    println!("🚫 [Worker] No soporta '{}'", task_type);
                                } else {
                                    let available = pool.available_slots();
                                    if available > 0 {
                                        println!("📋 [Worker] Bid para tarea {}", task_id);
                                        let reputation_score = queue.reputation().await.reputation_score;
                                        let worker_id = peer_id.to_string();
                                        let bid = build_task_bid_payload(
                                            &task_id,
                                            &worker_id,
                                            &origin_peer,
                                            reputation_score,
                                            available,
                                            10,
                                        );
                                        match swarm.behaviour_mut().gossipsub.publish(
                                            gossipsub::IdentTopic::new(BIDS_TOPIC),
                                            serde_json::to_vec(&bid).unwrap_or_default(),
                                        ) {
                                            Ok(message_id) => emit_worker_task_bid_published_event(
                                                &task_id,
                                                &worker_id,
                                                &message_id.to_string(),
                                                available,
                                            ),
                                            Err(error) => log_observability_event(
                                                LogLevel::Error,
                                                "task_bid_publish_failed",
                                                &task_id,
                                                Some(&task_id),
                                                None,
                                                Some(TASK_DISPATCH_UNCONFIRMED_001),
                                                {
                                                    let mut fields = Map::new();
                                                    fields.insert("worker_id".to_string(), worker_id.into());
                                                    fields.insert("topic".to_string(), BIDS_TOPIC.into());
                                                    fields.insert("error".to_string(), error.to_string().into());
                                                    fields
                                                },
                                            ),
                                        }
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

                                emit_broadcast_bid_received_event(
                                    &task_id,
                                    &worker_id,
                                    rep,
                                    slots,
                                    latency,
                                );
                                println!(
                                    "📨 [Scheduler] Bid: worker={} task_id={} rep={} slots={} latency={:.1}ms",
                                    worker_id, task_id, rep, slots, latency
                                );

                                if let Some(winner) = scheduler.receive_bid(&task_id, worker_id.clone(), rep, slots, est_ms).await {
                                    if broadcast_offer_state
                                        .as_ref()
                                        .map(|state| state.task_id == task_id && state.assignment_published)
                                        .unwrap_or(false)
                                    {
                                        continue;
                                    }
                                    println!("🏆 Asignando worker={} task_id={}", winner, task_id);

                                    let deadline_ms = 30_000u64;

                                    // ← Guardar peer_id del winner para direct result routing
                                    if let Some(winner_peer) = known_workers.iter()
                                        .find(|p| p.to_string().starts_with(&winner[..8.min(winner.len())]))
                                        .copied()
                                    {
                                        origin_peer_map.insert(task_id.clone(), winner_peer);
                                    }

                                    let (task_type_value, data_value) =
                                        if let NodeMode::Broadcast { task_type, data } = &mode {
                                            (task_type.as_str(), data.as_str())
                                        } else {
                                            ("", "")
                                        };
                                    let assign = build_broadcast_task_assign_payload(
                                        &task_id,
                                        &winner,
                                        &peer_id.to_string(),
                                        deadline_ms,
                                        task_type_value,
                                        data_value,
                                    );
                                    match swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new(ASSIGN_TOPIC),
                                        serde_json::to_vec(&assign).unwrap_or_default(),
                                    ) {
                                        Ok(message_id) => {
                                            if let Some(state) = broadcast_offer_state.as_mut() {
                                                if state.task_id == task_id {
                                                    state.mark_assigned(&winner);
                                                }
                                            }
                                            emit_broadcast_task_assign_published_event(
                                                &task_id,
                                                &winner,
                                                &message_id.to_string(),
                                                deadline_ms,
                                            );
                                            emit_task_lifecycle_assigned(
                                                &task_id,
                                                task_type_value,
                                                &winner,
                                                known_workers
                                                    .iter()
                                                    .map(|worker| worker.to_string())
                                                    .collect(),
                                                TaskSelectionReason::BroadcastBidSelected,
                                            );
                                        }
                                        Err(error) => log_observability_event(
                                            LogLevel::Error,
                                            "broadcast_task_assign_publish_failed",
                                            &task_id,
                                            Some(&task_id),
                                            None,
                                            Some(TASK_DISPATCH_UNCONFIRMED_001),
                                            {
                                                let mut fields = Map::new();
                                                fields.insert("assigned_worker".to_string(), winner.clone().into());
                                                fields.insert("topic".to_string(), ASSIGN_TOPIC.into());
                                                fields.insert("error".to_string(), error.to_string().into());
                                                fields
                                            },
                                        ),
                                    }

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
                                            emit_task_lifecycle_retrying(
                                                &rebroadcast_task_id,
                                                &task_type_clone,
                                                1,
                                                Some("broadcast_assignment_timeout".to_string()),
                                            );
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
                                            gossipsub::IdentTopic::new(TASK_TOPIC),
                                            serde_json::to_vec(&offer).unwrap(),
                                        );
                                    }
                                }
                            }

                            "TaskResult" if matches!(mode, NodeMode::Broadcast { .. }) => {
                                if message_topic != RESULTS_TOPIC {
                                    continue;
                                }
                                let result_message =
                                    BroadcastTaskResultMessage::from_pubsub_value(&msg);
                                let assigned_worker = broadcast_offer_state
                                    .as_ref()
                                    .and_then(|state| state.assigned_worker.as_deref())
                                    .map(str::to_string);
                                let acceptance = evaluate_broadcast_result_acceptance(
                                    broadcast_offer_state.as_ref(),
                                    &result_message.task_id,
                                    &result_message.worker_peer_id,
                                    result_message.success,
                                );
                                let accepted = acceptance.is_ok();
                                let rejection_reason = acceptance.err();
                                let received = BroadcastResultReceived {
                                    task_id: &result_message.task_id,
                                    task_type: &result_message.task_type,
                                    worker_peer_id: &result_message.worker_peer_id,
                                    assigned_worker_peer_id: assigned_worker.as_deref(),
                                    origin_peer: result_message.origin_peer.as_deref(),
                                    success: result_message.success,
                                    output: &result_message.output,
                                    elapsed_ms: result_message.elapsed_ms,
                                    transport: "pubsub",
                                    accepted,
                                    rejection_reason,
                                };
                                emit_broadcast_result_received_events(&received);

                                if let Some(reason) = rejection_reason {
                                    emit_broadcast_result_rejected_event(&received, reason);
                                    continue;
                                }

                                emit_task_lifecycle_result_received(
                                    &result_message.task_id,
                                    &result_message.task_type,
                                    &result_message.worker_peer_id,
                                    result_message.success,
                                    &result_message.output,
                                    result_message.elapsed_ms,
                                );
                                if let Some(state) = broadcast_offer_state.as_mut() {
                                    if state.task_id == result_message.task_id {
                                        state.mark_result_accepted();
                                    }
                                }
                                scheduler
                                    .mark_completed(
                                        &result_message.task_id,
                                        &result_message.worker_peer_id,
                                    )
                                    .await;
                                emit_broadcast_recovery_cancelled_event(
                                    &result_message.task_id,
                                    &result_message.worker_peer_id,
                                );
                                emit_broadcast_final_outcome_success_event(
                                    &result_message.task_id,
                                    &result_message.worker_peer_id,
                                    &result_message.output,
                                );
                                emit_task_lifecycle_completed(
                                    &result_message.task_id,
                                    &result_message.task_type,
                                    &result_message.worker_peer_id,
                                    &result_message.output,
                                    result_message.elapsed_ms,
                                );
                                emit_task_lifecycle_finalized(
                                    &result_message.task_id,
                                    &result_message.task_type,
                                    &result_message.worker_peer_id,
                                    "success",
                                    &result_message.output,
                                );
                                log_observability_event(
                                    LogLevel::Info,
                                    "task_completed",
                                    &result_message.task_id,
                                    Some(&result_message.task_id),
                                    None,
                                    None,
                                    {
                                        let mut fields = Map::new();
                                        fields.insert("success".to_string(), true.into());
                                        fields.insert(
                                            "worker_peer_id".to_string(),
                                            result_message.worker_peer_id.clone().into(),
                                        );
                                        fields.insert("transport".to_string(), "pubsub".into());
                                        fields.insert("source".to_string(), "broadcast_task_result".into());
                                        fields
                                    },
                                );
                                if should_print_result_output(&result_message.output) {
                                    println!("{}", result_message.output);
                                }
                                println!(
                                    "✅ [Broadcast] Resultado aceptado: task_id={} worker={} success=true",
                                    result_message.task_id, result_message.worker_peer_id
                                );
                                break;
                            }

                            "TaskAssign" if matches!(mode, NodeMode::Worker) => {
                                let assigned = msg["assigned_worker"].as_str().unwrap_or("");
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let task_type = msg["task_type"].as_str().unwrap_or("").to_string();
                                let data = msg["data"].as_str().unwrap_or("").to_string();
                                let origin_peer_str = msg["origin_peer"].as_str().unwrap_or("").to_string();
                                let deadline_ms = msg["deadline_ms"].as_u64().unwrap_or(30_000);
                                let local_peer_id = peer_id.to_string();
                                let already_executed =
                                    executed_broadcast_assignments.contains(&task_id);
                                let will_execute = should_execute_task_assignment(
                                    assigned,
                                    &local_peer_id,
                                    already_executed,
                                );
                                emit_worker_task_assign_received_event(
                                    &task_id,
                                    assigned,
                                    &local_peer_id,
                                    will_execute,
                                );

                                if will_execute {
                                    executed_broadcast_assignments.insert(task_id.clone());
                                    println!("🎯 [Worker] ¡Asignado! task_id={} (deadline: {}ms)", task_id, deadline_ms);
                                    emit_task_lifecycle_started(
                                        &task_id,
                                        &task_type,
                                        &local_peer_id,
                                    );

                                    let _origin_pid = known_workers.iter()  // ← _ prefix
                                        .find(|p| p.to_string() == origin_peer_str)
                                        .copied();

                                    let queue_ref = Arc::clone(&queue);
                                    let metrics_ref = Arc::clone(&metrics);
                                    let worker_id_str = local_peer_id.clone();
                                    let result_tx = broadcast_result_tx.clone();
                                    let task_type_for_result = task_type.clone();
                                    let origin_peer_for_result = origin_peer_str.clone();

                                    tokio::spawn(async move {
                                        let exec = async {
                                            let _ = queue_ref.push(task_id.clone(), task_type.clone(), data).await;
                                            queue_ref.outcome_rx.lock().await.recv().await
                                        };

                                        match tokio::time::timeout(Duration::from_millis(deadline_ms), exec).await {
                                            Ok(Some(outcome)) => {
                                                let success = matches!(outcome.status, task_queue::OutcomeStatus::Success);
                                                {
                                                    let mut m = metrics_ref.write().await;
                                                    if success { m.task_success(0); } else { m.task_failed(); }
                                                }
                                                log_observability_event(
                                                    LogLevel::Info,
                                                    "task_completed",
                                                    &outcome.task_id,
                                                    Some(&outcome.task_id),
                                                    None,
                                                    None,
                                                    {
                                                        let mut fields = Map::new();
                                                        fields.insert("success".to_string(), success.into());
                                                        fields.insert("worker_peer_id".to_string(), worker_id_str.clone().into());
                                                        fields.insert("attempts".to_string(), outcome.attempts.into());
                                                        fields.insert("source".to_string(), "broadcast_task_assign".into());
                                                        fields
                                                    },
                                                );
                                                println!("✅ [Worker] Tarea {} completada (success={})", outcome.task_id, success);
                                                let rep = queue_ref.reputation().await;
                                                println!("⭐ Reputación: {}/100", rep.reputation_score);

                                                let output = outcome
                                                    .result
                                                    .as_ref()
                                                    .map(|result| result.output.clone())
                                                    .unwrap_or_default();
                                                let elapsed_ms = outcome
                                                    .result
                                                    .as_ref()
                                                    .map(|result| result.execution_ms)
                                                    .unwrap_or_default();
                                                let error = if success {
                                                    None
                                                } else {
                                                    Some("broadcast task execution failed".to_string())
                                                };
                                                if success {
                                                    emit_task_lifecycle_completed(
                                                        &outcome.task_id,
                                                        &task_type_for_result,
                                                        &worker_id_str,
                                                        &output,
                                                        elapsed_ms,
                                                    );
                                                    emit_task_lifecycle_finalized(
                                                        &outcome.task_id,
                                                        &task_type_for_result,
                                                        &worker_id_str,
                                                        "success",
                                                        &output,
                                                    );
                                                } else {
                                                    emit_task_lifecycle_failed(
                                                        &outcome.task_id,
                                                        &task_type_for_result,
                                                        TaskLifecycleErrorCode::UnknownError,
                                                        "broadcast task execution failed",
                                                    );
                                                    emit_task_lifecycle_finalized(
                                                        &outcome.task_id,
                                                        &task_type_for_result,
                                                        &worker_id_str,
                                                        "failed",
                                                        &output,
                                                    );
                                                }
                                                let broadcast_result = BroadcastResultToPublish {
                                                    task_id: outcome.task_id.clone(),
                                                    task_type: task_type_for_result,
                                                    worker_peer_id: worker_id_str.clone(),
                                                    origin_peer: origin_peer_for_result.clone(),
                                                    success,
                                                    output,
                                                    elapsed_ms,
                                                    error,
                                                    attempts: outcome.attempts,
                                                    source: "broadcast_task_assign",
                                                };
                                                emit_broadcast_result_prepare_event(&broadcast_result);
                                                let _ = result_tx.send(broadcast_result).await;
                                                println!(
                                                    "📤 [Worker] Resultado listo → origin {}",
                                                    &origin_peer_for_result[..origin_peer_for_result.len().min(8)]
                                                );
                                            }
                                            Ok(None) => eprintln!("❌ [Worker] Canal cerrado"),
                                            Err(_) => {
                                                eprintln!("⏱️ [Worker] Deadline expirado {}ms", deadline_ms);
                                                metrics_ref.write().await.task_timed_out();
                                                emit_task_lifecycle_failed(
                                                    &task_id,
                                                    &task_type_for_result,
                                                    TaskLifecycleErrorCode::TaskTimeout,
                                                    "broadcast task deadline expired",
                                                );
                                            }
                                        }
                                    });
                                } else if assigned == local_peer_id {
                                    println!("⏭️  [Worker] TaskAssign duplicado ignorado: task_id={}", task_id);
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
                                cluster_registry
                                    .update_from_worker_heartbeat_value(&msg, unix_now_ms());
                                let worker_id = msg["peer_id"].as_str().unwrap_or("");
                                if !worker_id.is_empty() {
                                    emit_cluster_node_updated(&cluster_id, worker_id, "heartbeat");
                                }
                                let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
                                let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
                                let uptime = msg["uptime_secs"].as_u64().unwrap_or(0);
                                peer_tracker.update_heartbeat(worker_id, slots, rep);
                                let peer_count = peer_tracker.peer_count();
                                let heartbeat_value =
                                    format!("slots={} rep={} peers={}", slots, rep, peer_count);
                                if human_log_throttle.should_log(
                                    &format!("heartbeat:{}", worker_id),
                                    15_000,
                                    Some(&heartbeat_value),
                                ) {
                                    println!(
                                        "💓 Heartbeat {}... slots={} rep={} uptime={}s peers={}",
                                        &worker_id[..8.min(worker_id.len())],
                                        slots,
                                        rep,
                                        uptime,
                                        peer_count
                                    );
                                }
                                let mut m = metrics.write().await;
                                m.network_peers = peer_count;
                                m.mesh_peers = peer_count;
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
                                let requester_peer =
                                    msg["requester_peer"].as_str().unwrap_or("").to_string();
                                let remote_attempt_started_at = tokio::time::Instant::now();
                                let model_execution_gate = evaluate_worker_model_execution_gate(
                                    &model_id,
                                    &model_storage,
                                    &node_caps,
                                    worker_startup_policy.as_ref(),
                                );
                                let local_model_available =
                                    model_execution_gate.local_model_available;
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

                                if let Some(rejection) = model_execution_gate.rejection {
                                    println!("{}", rejection.human_warning(&model_id));
                                    continue;
                                }
                                let mock_backend_enabled = model_execution_gate.mock_backend_enabled;
                                let real_inference_available =
                                    model_execution_gate.real_inference_available;
                                for stage in [
                                    "task_received",
                                    "task_message_received",
                                    "worker_claimed",
                                    "attempt_started",
                                ] {
                                    publish_worker_progress_message(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &request_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        stage,
                                        elapsed_ms_since(remote_attempt_started_at),
                                        None,
                                    );
                                }

                                let engine_ref = inference_engine.clone();
                                let metrics_ref = Arc::clone(&metrics);
                                let registry_clone = ModelRegistry::new();
                                let peer_id_str = peer_id.to_string();
                                let request_id_clone = request_id.clone();
                                let model_id_for_inference = model_id.clone();
                                let mock_backend_for_task = mock_backend_enabled;
                                let real_inference_available_for_task = real_inference_available;

                                let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);
                                let (worker_progress_tx, mut worker_progress_rx) =
                                    tokio::sync::mpsc::channel::<&'static str>(32);

                                // Spawn inference execution
                                let inference_handle = tokio::spawn(async move {
                                    let progress_tx = worker_progress_tx;
                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id_for_inference.clone(),
                                        prompt,
                                        max_tokens,
                                        temperature,
                                    };
                                    let daemon_socket = daemon_socket_path();
                                    let result = if mock_backend_for_task {
                                        let _ = progress_tx.try_send("inference_started");
                                        mock_real_inference_result(
                                            request_id_clone.clone(),
                                            model_id_for_inference.clone(),
                                            req.prompt.clone(),
                                            req.max_tokens,
                                        )
                                    } else if !real_inference_available_for_task {
                                        return InferenceTaskResult::failure(
                                            request_id_clone.clone(),
                                            model_id_for_inference.clone(),
                                            peer_id_str.clone(),
                                            "real inference unavailable by worker startup policy".to_string(),
                                        );
                                    } else if daemon_is_available(&daemon_socket).await {
                                        let _ = progress_tx.try_send("model_loading");
                                        let _ = progress_tx.try_send("inference_warmup");
                                        let _ = progress_tx.try_send("inference_started");
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
                                        let Some(eng) = engine_ref.clone() else {
                                            return InferenceTaskResult::failure(
                                                request_id_clone.clone(),
                                                model_id_for_inference.clone(),
                                                peer_id_str.clone(),
                                                "real inference engine unavailable".to_string(),
                                            );
                                        };
                                        let hash = registry_clone.get(&model_id_for_inference)
                                            .map(|m| m.hash.clone())
                                            .unwrap_or_default();
                                        let _ = progress_tx.try_send("model_loading");
                                        if let Err(e) = eng.load_model(&model_id_for_inference, &hash) {
                                            return InferenceTaskResult::failure(
                                                request_id_clone.clone(),
                                                model_id_for_inference.clone(),
                                                peer_id_str.clone(),
                                                e,
                                            );
                                        }
                                        let _ = progress_tx.try_send("model_loaded");
                                        let _ = progress_tx.try_send("inference_warmup");
                                        let _ = progress_tx.try_send("inference_started");

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
                                let mut produced_tokens = 0u64;
                                let mut first_token_emitted = false;
                                let mut last_token_progress_published_at: Option<tokio::time::Instant> = None;
                                while !inference_handle.is_finished()
                                    || !token_rx.is_empty()
                                    || !worker_progress_rx.is_empty()
                                {
                                    tokio::select! {
                                        maybe_stage = worker_progress_rx.recv() => {
                                            if let Some(stage) = maybe_stage {
                                                publish_worker_progress_message(
                                                    &mut swarm,
                                                    &task_id,
                                                    &attempt_id,
                                                    &request_id,
                                                    &model_id,
                                                    &peer_id.to_string(),
                                                    stage,
                                                    elapsed_ms_since(remote_attempt_started_at),
                                                    None,
                                                );
                                            }
                                        }
                                        maybe_token = token_rx.recv() => {
                                            if let Some(token) = maybe_token {
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
                                                produced_tokens = produced_tokens.saturating_add(1);
                                                let should_publish_token_count = produced_tokens % 16 == 0
                                                    || last_token_progress_published_at
                                                        .map(|last| last.elapsed() >= Duration::from_secs(5))
                                                        .unwrap_or(true);
                                                if !first_token_emitted {
                                                    first_token_emitted = true;
                                                    last_token_progress_published_at =
                                                        Some(tokio::time::Instant::now());
                                                    publish_worker_progress_message(
                                                        &mut swarm,
                                                        &task_id,
                                                        &attempt_id,
                                                        &request_id,
                                                        &model_id,
                                                        &peer_id.to_string(),
                                                        "first_token_generated",
                                                        elapsed_ms_since(remote_attempt_started_at),
                                                        Some(produced_tokens),
                                                    );
                                                } else if should_publish_token_count {
                                                    last_token_progress_published_at =
                                                        Some(tokio::time::Instant::now());
                                                    publish_worker_progress_message(
                                                        &mut swarm,
                                                        &task_id,
                                                        &attempt_id,
                                                        &request_id,
                                                        &model_id,
                                                        &peer_id.to_string(),
                                                        "tokens_generated_count",
                                                        elapsed_ms_since(remote_attempt_started_at),
                                                        Some(produced_tokens),
                                                    );
                                                }
                                                token_idx += 1;
                                            }
                                        }
                                        _ = tokio::time::sleep(Duration::from_secs(5)), if !first_token_emitted => {
                                            publish_worker_progress_message(
                                            &mut swarm,
                                            &task_id,
                                            &attempt_id,
                                            &request_id,
                                            &model_id,
                                            &peer_id.to_string(),
                                            "still_running",
                                            elapsed_ms_since(remote_attempt_started_at),
                                            None,
                                            );
                                        }
                                    }
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
                                    publish_worker_progress_message(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &request_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        "inference_finished",
                                        elapsed_ms_since(remote_attempt_started_at),
                                        Some(result.tokens_generated as u64),
                                    );
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
                                        map.insert("worker_peer_id".to_string(), peer_id.to_string().into());
                                    }
                                    publish_worker_progress_message(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &request_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        "result_serialized",
                                        elapsed_ms_since(remote_attempt_started_at),
                                        Some(result.tokens_generated as u64),
                                    );
                                    let payload_size = result_payload.to_string().len();
                                    let message_id = publish_worker_result_payload(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        &result_payload,
                                    );
                                    if message_id.is_some() {
                                        publish_worker_progress_message(
                                            &mut swarm,
                                            &task_id,
                                            &attempt_id,
                                            &request_id,
                                            &model_id,
                                            &peer_id.to_string(),
                                            "result_published",
                                            elapsed_ms_since(remote_attempt_started_at),
                                            Some(result.tokens_generated as u64),
                                        );
                                        emit_worker_result_published_event(
                                            &task_id,
                                            &attempt_id,
                                            &model_id,
                                            RESULTS_TOPIC,
                                            payload_size,
                                            message_id.as_deref(),
                                            &peer_id.to_string(),
                                        )
                                    }
                                    send_worker_result_direct_response(
                                        &mut swarm,
                                        &requester_peer,
                                        &task_id,
                                        &attempt_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        &result,
                                    );
                                    println!("✅ [Worker] Inference completada: {} tokens en {}ms",
                                        result.tokens_generated, result.execution_ms);
                                }
                            }

                            "InferenceProgress" if matches!(mode, NodeMode::Infer { .. }) => {
                                if !is_remote_delivery_topic(message_topic) {
                                    continue;
                                }
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let attempt_id = msg["attempt_id"].as_str().unwrap_or("").to_string();
                                let attempt_key = AttemptKey::new(task_id.clone(), attempt_id.clone());
                                let stage = msg["stage"].as_str().unwrap_or("unknown");
                                let worker_peer = msg["worker_peer_id"]
                                    .as_str()
                                    .or_else(|| msg["worker_peer"].as_str())
                                    .unwrap_or("unknown");
                                let model_id = msg["model_id"].as_str().unwrap_or("").to_string();
                                let elapsed_ms = msg["elapsed_ms"].as_u64().unwrap_or_else(|| {
                                    attempt_watchdogs
                                        .get(&attempt_id)
                                        .map(|watchdog| watchdog.elapsed_ms())
                                        .unwrap_or_default()
                                });
                                let tokens_generated_count =
                                    msg["tokens_generated_count"].as_u64();
                                let expected_task_id = distributed_infer_state
                                    .as_ref()
                                    .map(|state| state.trace_task_id.as_str());
                                if expected_task_id != Some(task_id.as_str()) {
                                    continue;
                                }

                                let model_for_received_event = if model_id.is_empty() {
                                    active_attempts
                                        .get(&attempt_key)
                                        .map(|attempt| attempt.model_id.as_str())
                                } else {
                                    Some(model_id.as_str())
                                };
                                emit_remote_progress_client_received_event(
                                    &task_id,
                                    &attempt_id,
                                    model_for_received_event,
                                    worker_peer,
                                    stage,
                                    message_topic,
                                    elapsed_ms,
                                    tokens_generated_count,
                                );

                                if let Some(active_attempt) = active_attempts.get(&attempt_key) {
                                    if !active_attempt.accepts_worker(worker_peer) {
                                        continue;
                                    }
                                }

                                    if let Some(watchdog) = attempt_watchdogs.get_mut(&attempt_id) {
                                        if !watchdog.accepts_worker(worker_peer) {
                                            continue;
                                        }

                                        let model_for_event = if model_id.is_empty() {
                                            watchdog.model_id.clone()
                                        } else {
                                            model_id.clone()
                                        };
                                        let claim_source = claim_source_from_progress_stage(stage);
                                        if let Some(previous_worker_peer_id) =
                                            watchdog.claim_worker(worker_peer, claim_source)
                                        {
                                            if let Some(active_attempt) =
                                                active_attempts.get_mut(&attempt_key)
                                            {
                                                let _ = active_attempt.claim_worker(worker_peer);
                                            }
                                            emit_fallback_attempt_claimed_event(
                                                &task_id,
                                                &attempt_id,
                                                Some(&model_for_event),
                                                worker_peer,
                                                &previous_worker_peer_id,
                                                claim_source,
                                            );
                                            if let Some(infer_state) = distributed_infer_state.as_mut() {
                                                let _ = infer_state.claim_attempt_record(&attempt_id, worker_peer);
                                            }
                                            let _ = claim_task_attempt_peer(&task_id, worker_peer);
                                        }

                                        let previous_state = watchdog.state;
                                        let should_emit_recovered =
                                            watchdog.no_progress_warning_emitted
                                                || matches!(
                                                    previous_state,
                                                    AttemptLifecycleState::Stalled
                                                        | AttemptLifecycleState::TimedOut
                                                );
                                        let deadline_before =
                                            watchdog.deadline_at.duration_since(watchdog.started_at).as_millis() as u64;
                                        let meaningful = watchdog.record_progress(
                                            stage,
                                            tokens_generated_count,
                                        );
                                        if meaningful {
                                            watchdog.no_progress_warning_emitted = false;
                                            if let Some(infer_state) = distributed_infer_state.as_mut() {
                                                let mapped_state =
                                                    lifecycle_state_from_progress_stage(stage)
                                                        .map(|state| state.as_str())
                                                        .unwrap_or(stage);
                                                infer_state.update_attempt_state(
                                                    &attempt_id,
                                                    mapped_state,
                                                    Some("in_progress"),
                                                    None,
                                                );
                                            }
                                            let worker_peer_id = watchdog.worker_peer_id.clone();
                                            if watchdog.state != previous_state {
                                                emit_attempt_state_changed_event(
                                                    &task_id,
                                                    &attempt_id,
                                                    Some(&model_for_event),
                                                    Some(&worker_peer_id),
                                                    previous_state.as_str(),
                                                    watchdog.state.as_str(),
                                                );
                                            }
                                            let deadline_after =
                                                watchdog.deadline_at.duration_since(watchdog.started_at).as_millis() as u64;
                                            if deadline_after > deadline_before {
                                                emit_attempt_timeout_extended_event(
                                                    &task_id,
                                                    &attempt_id,
                                                    Some(&model_for_event),
                                                    Some(&worker_peer_id),
                                                    deadline_after,
                                                    "real_progress",
                                                );
                                            }
                                            emit_watchdog_reset_on_progress_event(
                                                &task_id,
                                                &attempt_id,
                                                Some(&model_for_event),
                                                &worker_peer_id,
                                                stage,
                                                deadline_after,
                                            );
                                            if should_emit_recovered {
                                                emit_attempt_progress_recovered_event(
                                                    &task_id,
                                                    &attempt_id,
                                                    Some(&model_for_event),
                                                    &worker_peer_id,
                                                );
                                            }
                                            println!(
                                                "{}",
                                                format_remote_progress_line(
                                                    &worker_peer_id,
                                                    &attempt_id,
                                                    stage,
                                                    elapsed_ms,
                                                    tokens_generated_count,
                                                )
                                            );
                                            emit_attempt_progress_received_event(
                                                &task_id,
                                                &attempt_id,
                                                Some(&model_for_event),
                                                &worker_peer_id,
                                                stage,
                                                elapsed_ms,
                                                tokens_generated_count,
                                            );
                                        }
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
                                    if !is_remote_delivery_topic(message_topic) {
                                        continue;
                                    }
                                    let rid = msg["request_id"].as_str().unwrap_or("");
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
                                    let worker = msg["worker_peer_id"]
                                        .as_str()
                                        .or_else(|| msg["worker_peer"].as_str())
                                        .unwrap_or("unknown");
                                    let accel = msg["accelerator"].as_str().unwrap_or("unknown");
                                    let full_output = msg["output"].as_str().unwrap_or("").to_string();
                                    let latency_ms = infer_started_at
                                        .as_ref()
                                        .map(|started| started.elapsed().as_millis() as u64)
                                        .unwrap_or(ms);
                                    emit_remote_result_client_received_event(
                                        &task_id,
                                        &attempt_id,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref())
                                            .or_else(|| msg["model_id"].as_str()),
                                        worker,
                                        message_topic,
                                        latency_ms,
                                        success,
                                    );
                                    let expected_task_id = distributed_infer_state
                                        .as_ref()
                                        .map(|state| state.trace_task_id.as_str());
                                    let current_request_matches = infer_request_id.as_deref() == Some(rid);
                                    if should_accept_result_for_attempt(
                                        &attempt_watchdogs,
                                        &active_attempts,
                                        expected_task_id,
                                        &task_id,
                                        &attempt_id,
                                        worker,
                                        current_request_matches,
                                    ) {
                                        pending_inference.remove(&attempt_id);
                                        let prior_state = attempt_watchdogs
                                            .get(&attempt_id)
                                            .map(|watchdog| watchdog.state.as_str().to_string())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        let accepted_after_current_moved = !current_request_matches;
                                        if let Some(watchdog) = attempt_watchdogs.get_mut(&attempt_id) {
                                            if let Some(previous_worker_peer_id) = watchdog.claim_worker(
                                                worker,
                                                AttemptClaimSource::ResultReceived,
                                            ) {
                                                if let Some(active_attempt) = active_attempts
                                                    .get_mut(&AttemptKey::new(
                                                        task_id.as_str(),
                                                        attempt_id.as_str(),
                                                    ))
                                                {
                                                    let _ = active_attempt.claim_worker(worker);
                                                }
                                                emit_fallback_attempt_claimed_event(
                                                    &task_id,
                                                    &attempt_id,
                                                    Some(&watchdog.model_id),
                                                    worker,
                                                    &previous_worker_peer_id,
                                                    AttemptClaimSource::ResultReceived,
                                                );
                                                if let Some(infer_state) = distributed_infer_state.as_mut() {
                                                    let _ = infer_state.claim_attempt_record(&attempt_id, worker);
                                                }
                                                let _ = claim_task_attempt_peer(&task_id, worker);
                                            }
                                        }
                                        if accepted_after_current_moved {
                                            emit_late_result_received_event(
                                                &task_id,
                                                &attempt_id,
                                                worker,
                                                distributed_infer_state
                                                    .as_ref()
                                                    .and_then(|state| state.current_model.as_deref()),
                                                latency_ms,
                                                true,
                                                "accepted_within_fallback_claim_window",
                                                &prior_state,
                                            );
                                        }
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
                                        if let Some(infer_state) = distributed_infer_state.as_mut() {
                                            infer_state.update_attempt_state(
                                                &attempt_id,
                                                "failed",
                                                Some("retryable_failure"),
                                                Some(&reason),
                                            );
                                        }
                                        if let Some(watchdog) = attempt_watchdogs.get_mut(&attempt_id) {
                                            let previous_state = watchdog.state;
                                            if watchdog.transition_state(AttemptLifecycleState::Failed) {
                                                emit_attempt_state_changed_event(
                                                    &task_id,
                                                    &attempt_id,
                                                    Some(&watchdog.model_id),
                                                    Some(worker),
                                                    previous_state.as_str(),
                                                    watchdog.state.as_str(),
                                                );
                                            }
                                        }
                                        if let Some(health) =
                                            registry.write().await.record_failure(worker)
                                        {
                                            let trace_id = distributed_infer_state
                                                .as_ref()
                                                .map(|state| state.trace_task_id.clone())
                                                .unwrap_or_else(|| "-".to_string());
                                            let model_id = distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.clone());
                                            log_health_update(
                                                &trace_id,
                                                worker,
                                                model_id.as_deref(),
                                                &health,
                                                Some(NODE_UNHEALTHY_002),
                                            );
                                            record_health_policy_state_transition(
                                                &mut health_state_tracker,
                                                &trace_id,
                                                worker,
                                                model_id.as_deref(),
                                                "validation_failure",
                                                &health,
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

                                            finalize_and_report_distributed_task_observability(
                                                infer_state,
                                                infer_started_at,
                                                true,
                                                false,
                                            );
                                        }

                                            break;
                                        }

                                        let accepted_retry_or_fallback = distributed_infer_state
                                            .as_ref()
                                            .map(|state| state.task_retry_count > 0)
                                            .unwrap_or(false)
                                            || attempt_watchdogs
                                                .get(&attempt_id)
                                                .map(|watchdog| watchdog.is_fallback_broadcast())
                                                .unwrap_or(false);
                                        if accepted_retry_or_fallback {
                                            emit_retry_result_accepted_event(
                                                &task_id,
                                                &attempt_id,
                                                distributed_infer_state
                                                    .as_ref()
                                                    .and_then(|state| state.current_model.as_deref()),
                                                worker,
                                                accepted_after_current_moved,
                                            );
                                        }
                                        if let Some(health) = registry.write().await.record_success(worker, ms) {
                                            let trace_id = distributed_infer_state
                                                .as_ref()
                                            .map(|state| state.trace_task_id.clone())
                                            .unwrap_or_else(|| "-".to_string());
                                        let model_id = distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.clone());
                                        log_health_update(
                                            &trace_id,
                                            worker,
                                            model_id.as_deref(),
                                            &health,
                                            None,
                                        );
                                        record_health_policy_state_transition(
                                            &mut health_state_tracker,
                                            &trace_id,
                                            worker,
                                            model_id.as_deref(),
                                            "success",
                                            &health,
                                        );
                                    }
                                    if let Some(watchdog) = attempt_watchdogs.get_mut(&attempt_id) {
                                        let previous_state = watchdog.state;
                                        if watchdog.transition_state(AttemptLifecycleState::Completed) {
                                            emit_attempt_state_changed_event(
                                                &task_id,
                                                &attempt_id,
                                                Some(&watchdog.model_id),
                                                Some(worker),
                                                previous_state.as_str(),
                                                watchdog.state.as_str(),
                                            );
                                        }
                                    }
                                    if let Some(infer_state) = distributed_infer_state.as_mut() {
                                        infer_state.update_attempt_state(
                                            &attempt_id,
                                            "completed",
                                            Some("success"),
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
                                        finalize_and_report_distributed_task_observability(
                                            infer_state,
                                            infer_started_at,
                                            false,
                                            false,
                                        );
                                    }
                                    break;
                                } else {
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
                                    let worker = msg["worker_peer_id"]
                                        .as_str()
                                        .or_else(|| msg["worker_peer"].as_str())
                                        .unwrap_or("unknown");
                                    let success = msg["success"].as_bool().unwrap_or(false);
                                    let output = msg["output"].as_str().unwrap_or("").to_string();
                                    let execution_ms = msg["execution_ms"].as_u64().unwrap_or(0);
                                    let prior_state = attempt_watchdogs
                                        .get(&attempt_id)
                                        .map(|watchdog| watchdog.state.as_str().to_string())
                                        .unwrap_or_else(|| "unknown".to_string());
                                    let elapsed_ms = attempt_watchdogs
                                        .get(&attempt_id)
                                        .map(|watchdog| watchdog.elapsed_ms())
                                        .unwrap_or(execution_ms);
                                    emit_late_result_received_event(
                                        &task_id,
                                        &attempt_id,
                                        worker,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        elapsed_ms,
                                        false,
                                        "arrived_after_timeout_policy_ignore",
                                        &prior_state,
                                    );
                                    if let Some(infer_state) = distributed_infer_state.as_mut() {
                                        infer_state.update_attempt_state(
                                            &attempt_id,
                                            "late_completed",
                                            Some("late_ignored"),
                                            Some("arrived_after_timeout_policy_ignore"),
                                        );
                                    }
                                    let _ = record_distributed_task_late_result();
                                    if let Some(watchdog) = attempt_watchdogs.get_mut(&attempt_id) {
                                        let previous_state = watchdog.state;
                                        if watchdog
                                            .transition_state(AttemptLifecycleState::LateCompleted)
                                        {
                                            emit_attempt_state_changed_event(
                                                &task_id,
                                                &attempt_id,
                                                Some(&watchdog.model_id),
                                                Some(worker),
                                                previous_state.as_str(),
                                                watchdog.state.as_str(),
                                            );
                                        }
                                    }
                                    if success && should_print_result_output(&output) {
                                        if let Some(health) =
                                            registry.write().await.record_success(worker, execution_ms)
                                        {
                                            let model_id = distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.clone());
                                            log_health_update(
                                                &task_id,
                                                worker,
                                                model_id.as_deref(),
                                                &health,
                                                None,
                                            );
                                            record_health_policy_state_transition(
                                                &mut health_state_tracker,
                                                &task_id,
                                                worker,
                                                model_id.as_deref(),
                                                "late_success",
                                                &health,
                                            );
                                        }
                                    }
                                    println!(
                                        "[LateResult] task_id={} attempt_id={} worker={} policy=ignored",
                                        task_id, attempt_id, worker
                                    );
                                }
                            }

                            "NodeCapabilities" => {
                                if let Ok(hb) = serde_json::from_value::<NodeCapabilityHeartbeat>(msg.clone()) {
                                    let _ = registry.write().await.update_from_heartbeat(hb.clone());
                                    cluster_registry.update_from_capability_heartbeat(&hb, unix_now_ms());
                                    emit_cluster_capabilities_updated(
                                        &cluster_registry,
                                        &cluster_id,
                                        &hb.peer_id,
                                        "node_capabilities",
                                        hb.models.len(),
                                        None,
                                    );
                                }
                            }

                            "DirectInferenceRequest" if matches!(mode, NodeMode::Worker) => {
                                let target = msg["target_peer"].as_str().unwrap_or("");
                                if target != peer_id.to_string() {
                                    continue;
                                }
                                let requester_peer = from_peer.clone();

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
                                let remote_attempt_started_at = tokio::time::Instant::now();
                                let model_execution_gate = evaluate_worker_model_execution_gate(
                                    &model_id,
                                    &model_storage,
                                    &node_caps,
                                    worker_startup_policy.as_ref(),
                                );
                                let local_model_available =
                                    model_execution_gate.local_model_available;
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

                                if let Some(rejection) = model_execution_gate.rejection {
                                    println!("{}", rejection.human_warning(&model_id));
                                    continue;
                                }
                                let mock_backend_enabled = model_execution_gate.mock_backend_enabled;
                                let real_inference_available =
                                    model_execution_gate.real_inference_available;
                                for stage in [
                                    "task_received",
                                    "task_message_received",
                                    "worker_claimed",
                                    "attempt_started",
                                ] {
                                    publish_worker_progress_message(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &request_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        stage,
                                        elapsed_ms_since(remote_attempt_started_at),
                                        None,
                                    );
                                }

                                let engine_ref = inference_engine.clone();
                                let metrics_ref = Arc::clone(&metrics);
                                let registry_clone = ModelRegistry::new();
                                let peer_id_str = peer_id.to_string();
                                let request_id_clone = request_id.clone();
                                let model_id_for_inference = model_id.clone();
                                let mock_backend_for_task = mock_backend_enabled;
                                let real_inference_available_for_task = real_inference_available;

                                let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);
                                let (worker_progress_tx, mut worker_progress_rx) =
                                    tokio::sync::mpsc::channel::<&'static str>(32);

                                let inference_handle = tokio::spawn(async move {
                                    let progress_tx = worker_progress_tx;
                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id_for_inference.clone(),
                                        prompt,
                                        max_tokens,
                                        temperature: 0.7,
                                    };
                                    let daemon_socket = daemon_socket_path();
                                    let result = if mock_backend_for_task {
                                        let _ = progress_tx.try_send("inference_started");
                                        mock_real_inference_result(
                                            request_id_clone.clone(),
                                            model_id_for_inference.clone(),
                                            req.prompt.clone(),
                                            req.max_tokens,
                                        )
                                    } else if !real_inference_available_for_task {
                                        return InferenceTaskResult::failure(
                                            request_id_clone.clone(),
                                            model_id_for_inference.clone(),
                                            peer_id_str.clone(),
                                            "real inference unavailable by worker startup policy".to_string(),
                                        );
                                    } else if daemon_is_available(&daemon_socket).await {
                                        let _ = progress_tx.try_send("model_loading");
                                        let _ = progress_tx.try_send("inference_warmup");
                                        let _ = progress_tx.try_send("inference_started");
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
                                        let Some(eng) = engine_ref.clone() else {
                                            return InferenceTaskResult::failure(
                                                request_id_clone.clone(),
                                                model_id_for_inference.clone(),
                                                peer_id_str.clone(),
                                                "real inference engine unavailable".to_string(),
                                            );
                                        };
                                        let hash = registry_clone.get(&model_id_for_inference)
                                            .map(|m| m.hash.clone())
                                            .unwrap_or_default();

                                        let _ = progress_tx.try_send("model_loading");
                                        if let Err(e) = eng.load_model(&model_id_for_inference, &hash) {
                                            return InferenceTaskResult::failure(
                                                request_id_clone.clone(),
                                                model_id_for_inference.clone(),
                                                peer_id_str.clone(),
                                                e,
                                            );
                                        }
                                        let _ = progress_tx.try_send("model_loaded");
                                        let _ = progress_tx.try_send("inference_warmup");
                                        let _ = progress_tx.try_send("inference_started");

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
                                let mut produced_tokens = 0u64;
                                let mut first_token_emitted = false;
                                let mut last_token_progress_published_at: Option<tokio::time::Instant> = None;
                                while !inference_handle.is_finished()
                                    || !token_rx.is_empty()
                                    || !worker_progress_rx.is_empty()
                                {
                                    tokio::select! {
                                        maybe_stage = worker_progress_rx.recv() => {
                                            if let Some(stage) = maybe_stage {
                                                publish_worker_progress_message(
                                                    &mut swarm,
                                                    &task_id,
                                                    &attempt_id,
                                                    &request_id,
                                                    &model_id,
                                                    &peer_id.to_string(),
                                                    stage,
                                                    elapsed_ms_since(remote_attempt_started_at),
                                                    None,
                                                );
                                            }
                                        }
                                        maybe_token = token_rx.recv() => {
                                            if let Some(token) = maybe_token {
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
                                                produced_tokens = produced_tokens.saturating_add(1);
                                                let should_publish_token_count = produced_tokens % 16 == 0
                                                    || last_token_progress_published_at
                                                        .map(|last| last.elapsed() >= Duration::from_secs(5))
                                                        .unwrap_or(true);
                                                if !first_token_emitted {
                                                    first_token_emitted = true;
                                                    last_token_progress_published_at =
                                                        Some(tokio::time::Instant::now());
                                                    publish_worker_progress_message(
                                                        &mut swarm,
                                                        &task_id,
                                                        &attempt_id,
                                                        &request_id,
                                                        &model_id,
                                                        &peer_id.to_string(),
                                                        "first_token_generated",
                                                        elapsed_ms_since(remote_attempt_started_at),
                                                        Some(produced_tokens),
                                                    );
                                                } else if should_publish_token_count {
                                                    last_token_progress_published_at =
                                                        Some(tokio::time::Instant::now());
                                                    publish_worker_progress_message(
                                                        &mut swarm,
                                                        &task_id,
                                                        &attempt_id,
                                                        &request_id,
                                                        &model_id,
                                                        &peer_id.to_string(),
                                                        "tokens_generated_count",
                                                        elapsed_ms_since(remote_attempt_started_at),
                                                        Some(produced_tokens),
                                                    );
                                                }
                                                token_idx += 1;
                                            }
                                        }
                                        _ = tokio::time::sleep(Duration::from_secs(5)), if !first_token_emitted => {
                                            publish_worker_progress_message(
                                            &mut swarm,
                                            &task_id,
                                            &attempt_id,
                                            &request_id,
                                            &model_id,
                                            &peer_id.to_string(),
                                            "still_running",
                                            elapsed_ms_since(remote_attempt_started_at),
                                            None,
                                            );
                                        }
                                    }
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
                                    publish_worker_progress_message(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &request_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        "inference_finished",
                                        elapsed_ms_since(remote_attempt_started_at),
                                        Some(result.tokens_generated as u64),
                                    );
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
                                        map.insert("worker_peer_id".to_string(), peer_id.to_string().into());
                                    }
                                    publish_worker_progress_message(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &request_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        "result_serialized",
                                        elapsed_ms_since(remote_attempt_started_at),
                                        Some(result.tokens_generated as u64),
                                    );
                                    let payload_size = result_payload.to_string().len();
                                    let message_id = publish_worker_result_payload(
                                        &mut swarm,
                                        &task_id,
                                        &attempt_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        &result_payload,
                                    );
                                    if message_id.is_some() {
                                        publish_worker_progress_message(
                                            &mut swarm,
                                            &task_id,
                                            &attempt_id,
                                            &request_id,
                                            &model_id,
                                            &peer_id.to_string(),
                                            "result_published",
                                            elapsed_ms_since(remote_attempt_started_at),
                                            Some(result.tokens_generated as u64),
                                        );
                                        emit_worker_result_published_event(
                                            &task_id,
                                            &attempt_id,
                                            &model_id,
                                            RESULTS_TOPIC,
                                            payload_size,
                                            message_id.as_deref(),
                                            &peer_id.to_string(),
                                        )
                                    }
                                    send_worker_result_direct_response(
                                        &mut swarm,
                                        &requester_peer,
                                        &task_id,
                                        &attempt_id,
                                        &model_id,
                                        &peer_id.to_string(),
                                        &result,
                                    );

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

                    if matches!(mode, NodeMode::Infer { .. }) {
                        let worker_peer_id = if is_valid_claiming_worker(&request.worker_id) {
                            request.worker_id.clone()
                        } else {
                            peer.to_string()
                        };
                        let expected_task_id = distributed_infer_state
                            .as_ref()
                            .map(|infer_state| infer_state.trace_task_id.as_str());
                        let current_request_matches =
                            infer_request_id.as_deref() == Some(request.attempt_id.as_str());
                        let latency_ms = infer_started_at
                            .as_ref()
                            .map(|started| started.elapsed().as_millis() as u64)
                            .unwrap_or(request.execution_ms);
                        emit_remote_result_client_received_event(
                            &request.task_id,
                            &request.attempt_id,
                            Some(&request.model_id),
                            &worker_peer_id,
                            "request_response",
                            latency_ms,
                            request.success,
                        );

                        if should_accept_result_for_attempt(
                            &attempt_watchdogs,
                            &active_attempts,
                            expected_task_id,
                            &request.task_id,
                            &request.attempt_id,
                            &worker_peer_id,
                            current_request_matches,
                        ) && request.success
                            && should_print_result_output(&request.result)
                            && distributed_infer_state
                                .as_ref()
                                .map(|infer_state| {
                                    matches!(
                                        validate_result(
                                            infer_state
                                                .current_semantic_prompt
                                                .as_deref()
                                                .unwrap_or(""),
                                            infer_state
                                                .current_task_type
                                                .unwrap_or(PromptTaskType::General),
                                            &request.result,
                                        ),
                                        ResultStatus::Valid
                                    )
                                })
                                .unwrap_or(true)
                        {
                            pending_inference.remove(&request.attempt_id);
                            let accepted_after_current_moved = !current_request_matches;
                            let prior_state = attempt_watchdogs
                                .get(&request.attempt_id)
                                .map(|watchdog| watchdog.state.as_str().to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            if let Some(watchdog) =
                                attempt_watchdogs.get_mut(&request.attempt_id)
                            {
                                if let Some(previous_worker_peer_id) = watchdog.claim_worker(
                                    &worker_peer_id,
                                    AttemptClaimSource::ResultReceived,
                                ) {
                                    if let Some(active_attempt) = active_attempts.get_mut(
                                        &AttemptKey::new(
                                            request.task_id.as_str(),
                                            request.attempt_id.as_str(),
                                        ),
                                    ) {
                                        let _ = active_attempt.claim_worker(&worker_peer_id);
                                    }
                                    emit_fallback_attempt_claimed_event(
                                        &request.task_id,
                                        &request.attempt_id,
                                        Some(&watchdog.model_id),
                                        &worker_peer_id,
                                        &previous_worker_peer_id,
                                        AttemptClaimSource::ResultReceived,
                                    );
                                    if let Some(infer_state) = distributed_infer_state.as_mut() {
                                        let _ = infer_state.claim_attempt_record(
                                            &request.attempt_id,
                                            &worker_peer_id,
                                        );
                                    }
                                    let _ = claim_task_attempt_peer(
                                        &request.task_id,
                                        &worker_peer_id,
                                    );
                                }
                                let previous_state = watchdog.state;
                                if watchdog.transition_state(AttemptLifecycleState::Completed) {
                                    emit_attempt_state_changed_event(
                                        &request.task_id,
                                        &request.attempt_id,
                                        Some(&watchdog.model_id),
                                        Some(&worker_peer_id),
                                        previous_state.as_str(),
                                        watchdog.state.as_str(),
                                    );
                                }
                            }
                            if accepted_after_current_moved {
                                emit_late_result_received_event(
                                    &request.task_id,
                                    &request.attempt_id,
                                    &worker_peer_id,
                                    Some(&request.model_id),
                                    latency_ms,
                                    true,
                                    "accepted_via_direct_result_response",
                                    &prior_state,
                                );
                            }
                            emit_result_received_event(
                                &request.task_id,
                                &request.attempt_id,
                                Some(&request.model_id),
                                &worker_peer_id,
                                latency_ms,
                            );
                            emit_retry_result_accepted_event(
                                &request.task_id,
                                &request.attempt_id,
                                Some(&request.model_id),
                                &worker_peer_id,
                                accepted_after_current_moved,
                            );
                            if rendered_output.is_empty() {
                                print!("{}", request.result);
                            } else if request.result.starts_with(&rendered_output) {
                                let suffix = &request.result[rendered_output.len()..];
                                if !suffix.is_empty() {
                                    print!("{}", suffix);
                                }
                            }
                            let _ = std::io::Write::flush(&mut std::io::stdout());

                            if let Some(health) = registry
                                .write()
                                .await
                                .record_success(&worker_peer_id, latency_ms)
                            {
                                log_health_update(
                                    &request.task_id,
                                    &worker_peer_id,
                                    Some(&request.model_id),
                                    &health,
                                    None,
                                );
                                record_health_policy_state_transition(
                                    &mut health_state_tracker,
                                    &request.task_id,
                                    &worker_peer_id,
                                    Some(&request.model_id),
                                    "success",
                                    &health,
                                );
                            }
                            if let Some(infer_state) = distributed_infer_state.as_mut() {
                                infer_state.update_attempt_state(
                                    &request.attempt_id,
                                    "completed",
                                    Some("success"),
                                    None,
                                );
                            }
                            log_observability_event(
                                LogLevel::Info,
                                "task_completed",
                                &request.task_id,
                                Some(&request.task_id),
                                Some(&request.model_id),
                                None,
                                {
                                    let mut fields = Map::new();
                                    fields.insert("attempt_id".to_string(), request.attempt_id.clone().into());
                                    fields.insert("success".to_string(), true.into());
                                    fields.insert("worker_peer_id".to_string(), worker_peer_id.clone().into());
                                    fields.insert("tokens_generated".to_string(), request.tokens_generated.into());
                                    fields.insert("transport".to_string(), "request_response".into());
                                    fields
                                },
                            );
                            finalize_and_report_direct_result_observability(
                                &request.task_id,
                                infer_started_at,
                                distributed_infer_state.as_ref(),
                            );
                            break;
                        }
                    }
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
                    cluster_registry.add_or_update_discovered_node(&pid.to_string(), None, unix_now_ms());
                    let tracked_topic_name = tracked_pubsub_topic_name(&topic);
                    let observer_mode = if matches!(mode, NodeMode::Broadcast { .. }) {
                        Some(("controller_observed_peer_subscription", "broadcast"))
                    } else if matches!(mode, NodeMode::Worker) {
                        Some(("worker_observed_peer_subscription", "worker"))
                    } else {
                        None
                    };
                    if let (Some((event_name, mode_name)), Some(topic_name)) =
                        (observer_mode, tracked_topic_name)
                    {
                        emit_observed_peer_subscription_event(
                            event_name,
                            &peer_id.to_string(),
                            &pid.to_string(),
                            topic_name,
                            mode_name,
                            "gossipsub_subscription",
                        );
                    }
                    if matches!(mode, NodeMode::Broadcast { .. })
                        && tracked_topic_name == Some(TASK_TOPIC)
                    {
                        if let Some(state) = broadcast_offer_state.as_mut() {
                            state.mark_subscription_seen();
                        }
                        let connected_peers = swarm.connected_peers().count();
                        let subscribed_peers = pubsub_topics.topic_peer_count(TASK_TOPIC).max(
                            gossipsub_topic_peer_count(&swarm.behaviour().gossipsub, TASK_TOPIC),
                        );
                        let mesh_peers =
                            gossipsub_mesh_peer_ids(&swarm.behaviour().gossipsub, TASK_TOPIC)
                                .len();
                        emit_broadcast_topic_subscriber_seen_event(
                            TASK_TOPIC,
                            &pid.to_string(),
                            connected_peers,
                            subscribed_peers,
                            mesh_peers,
                            "gossipsub_subscription",
                        );
                    }
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
                    if let Some(state) = broadcast_offer_state.as_mut() {
                        state.mark_peer_connected();
                    }
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&pid);
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
                    cluster_registry.add_or_update_discovered_node(
                        &pid.to_string(),
                        Some(endpoint.get_remote_address().to_string()),
                        unix_now_ms(),
                    );
                    emit_cluster_discovery_update(
                        false,
                        &cluster_id,
                        &pid.to_string(),
                        "peer_connected",
                        Some(endpoint.get_remote_address().to_string()),
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
                    if matches!(mode, NodeMode::Infer { .. }) {
                        let disconnected_peer = pid.to_string();
                        let active_peer = distributed_infer_state
                            .as_ref()
                            .and_then(|state| state.current_peer.as_deref())
                            == Some(disconnected_peer.as_str());
                        let inflight = infer_request_id
                            .as_ref()
                            .map(|attempt_id| pending_inference.contains_key(attempt_id))
                            .unwrap_or(false);
                        if active_peer && inflight {
                            log_observability_event(
                                LogLevel::Warn,
                                "node_disconnect_during_active_task",
                                distributed_infer_state
                                    .as_ref()
                                    .map(|state| state.trace_task_id.as_str())
                                    .unwrap_or("network"),
                                distributed_infer_state
                                    .as_ref()
                                    .map(|state| state.trace_task_id.as_str()),
                                distributed_infer_state
                                    .as_ref()
                                    .and_then(|state| state.current_model.as_deref()),
                                Some(NET_PEER_DISCONNECTED_002),
                                {
                                    let mut fields = Map::new();
                                    fields.insert("peer_id".to_string(), disconnected_peer.into());
                                    fields.insert(
                                        "policy".to_string(),
                                        "degraded_first_blacklist_after_threshold".into(),
                                    );
                                    fields.insert(
                                        "action".to_string(),
                                        "wait_for_watchdog_timeout_before_retry".into(),
                                    );
                                    fields
                                },
                            );
                        }
                    }
                    known_workers.remove(&pid);
                    pubsub_topics.unregister_peer(&pid);
                    if is_client
                        && !waiting_for_response
                        && !matches!(mode, NodeMode::Broadcast { .. } | NodeMode::ClusterStatus { .. })
                    {
                        println!("\n📊 Completado: {}/{} tareas", completed, total_tasks);
                        break;
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
                    if let Ok(rtt) = result {
                        let rtt_ms = rtt.as_micros() as f64 / 1000.0;
                        let peer_id = peer.to_string();
                        let latency_bucket = format!("{:.0}", (rtt_ms / 5.0).round());
                        if human_log_throttle.should_log(
                            &format!("latency:{}", peer_id),
                            20_000,
                            Some(&latency_bucket),
                        ) {
                            println!(
                                "[Latency] RTT to peer {} = {:.1} ms",
                                &peer_id[..12.min(peer_id.len())],
                                rtt_ms
                            );
                        }
                        peer_tracker.update_latency(&peer.to_string(), rtt_ms);
                        topology.write().await.update_latency(&peer.to_string(), rtt_ms);
                        cluster_registry.update_latency(
                            &peer.to_string(),
                            rtt_ms.max(0.0).round() as u32,
                            unix_now_ms(),
                        );
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
                    let peer_id = peer.to_string();
                    cluster_registry.add_or_update_discovered_node(&peer_id, None, unix_now_ms());
                    emit_cluster_node_updated(&cluster_id, &peer_id, "kademlia_routing_updated");
                    if human_log_throttle.should_log(
                        &format!("kademlia:{}", peer_id),
                        30_000,
                        Some("routing_updated"),
                    ) {
                        println!("📡 Kademlia: nodo añadido {}", peer);
                    }
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
                                let mock_backend_for_task = worker_startup_policy
                                    .as_ref()
                                    .map(|policy| policy.mock_backend())
                                    .unwrap_or(false);
                                let real_inference_available_for_task = worker_startup_policy
                                    .as_ref()
                                    .map(|policy| policy.real_inference_available)
                                    .unwrap_or(true);
                                let engine_ref = inference_engine.clone();

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

                                    let result = if mock_backend_for_task {
                                        Ok(mock_real_inference_result(
                                            task_id.clone(),
                                            model.clone(),
                                            semantic_prompt.clone(),
                                            output_policy.max_tokens as u32,
                                        ))
                                    } else if !real_inference_available_for_task {
                                        Err("real inference unavailable by worker startup policy".to_string())
                                    } else if let Some(runtime) = choose_inference_runtime().await {
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
                                        let Some(engine) = engine_ref.clone() else {
                                            let error = "real inference engine unavailable".to_string();
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
                                        };
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
                                    let current_request_matches = infer_request_id.as_deref()
                                        == Some(distributed_result.attempt_id.as_str());
                                    let peer_id = peer.to_string();
                                    emit_remote_result_client_received_event(
                                        &distributed_result.task_id,
                                        &distributed_result.attempt_id,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        &peer_id,
                                        "task_request_response",
                                        infer_started_at
                                            .as_ref()
                                            .map(|started| started.elapsed().as_millis() as u64)
                                            .unwrap_or_default(),
                                        distributed_result.success,
                                    );
                                    if !should_accept_result_for_attempt(
                                        &attempt_watchdogs,
                                        &active_attempts,
                                        expected_task_id,
                                        &distributed_result.task_id,
                                        &distributed_result.attempt_id,
                                        &peer_id,
                                        current_request_matches,
                                    ) {
                                        let attempt_id = distributed_result.attempt_id.clone();
                                        let task_id = distributed_result.task_id.clone();
                                        let prior_state = attempt_watchdogs
                                            .get(&attempt_id)
                                            .map(|watchdog| watchdog.state.as_str().to_string())
                                        .unwrap_or_else(|| "unknown".to_string());
                                    let elapsed_ms = attempt_watchdogs
                                        .get(&attempt_id)
                                        .map(|watchdog| watchdog.elapsed_ms())
                                        .unwrap_or_default();
                                    emit_late_result_received_event(
                                        &task_id,
                                        &attempt_id,
                                        &peer_id,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        elapsed_ms,
                                        false,
                                        "arrived_after_timeout_policy_ignore",
                                        &prior_state,
                                    );
                                    if let Some(infer_state) = distributed_infer_state.as_mut() {
                                        infer_state.update_attempt_state(
                                            &attempt_id,
                                            "late_completed",
                                            Some("late_ignored"),
                                            Some("arrived_after_timeout_policy_ignore"),
                                        );
                                    }
                                    let _ = record_distributed_task_late_result();
                                    if distributed_result.success
                                        && should_print_result_output(&distributed_result.output)
                                    {
                                        if let Some(health) = registry
                                            .write()
                                            .await
                                            .record_success(&peer_id, elapsed_ms)
                                        {
                                            let model_id = distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.clone());
                                            log_health_update(
                                                &task_id,
                                                &peer_id,
                                                model_id.as_deref(),
                                                &health,
                                                None,
                                            );
                                            record_health_policy_state_transition(
                                                &mut health_state_tracker,
                                                &task_id,
                                                &peer_id,
                                                model_id.as_deref(),
                                                "late_success",
                                                &health,
                                            );
                                        }
                                    }
                                    if let Some(watchdog) = attempt_watchdogs.get_mut(&attempt_id) {
                                        let previous_state = watchdog.state;
                                        if watchdog
                                            .transition_state(AttemptLifecycleState::LateCompleted)
                                        {
                                            emit_attempt_state_changed_event(
                                                &task_id,
                                                &attempt_id,
                                                Some(&watchdog.model_id),
                                                Some(&peer_id),
                                                previous_state.as_str(),
                                                watchdog.state.as_str(),
                                            );
                                        }
                                    }
                                    println!(
                                        "[Task] Ignoring stale response task_id={} attempt_id={} from peer_id={}",
                                        distributed_result.task_id, distributed_result.attempt_id, peer
                                    );
                                    continue;
                                    }

                                    pending_inference.remove(&distributed_result.attempt_id);
                                    let latency_ms = infer_started_at
                                        .as_ref()
                                        .map(|started| started.elapsed().as_millis() as u64)
                                        .unwrap_or_default();
                                    let prior_state = attempt_watchdogs
                                        .get(&distributed_result.attempt_id)
                                        .map(|watchdog| watchdog.state.as_str().to_string())
                                        .unwrap_or_else(|| "unknown".to_string());
                                    if let Some(watchdog) =
                                        attempt_watchdogs.get_mut(&distributed_result.attempt_id)
                                    {
                                        if let Some(previous_worker_peer_id) = watchdog.claim_worker(
                                            &peer_id,
                                            AttemptClaimSource::ResultReceived,
                                        ) {
                                            if let Some(active_attempt) = active_attempts.get_mut(
                                                &AttemptKey::new(
                                                    distributed_result.task_id.as_str(),
                                                    distributed_result.attempt_id.as_str(),
                                                ),
                                            ) {
                                                let _ = active_attempt.claim_worker(&peer_id);
                                            }
                                            emit_fallback_attempt_claimed_event(
                                                &distributed_result.task_id,
                                                &distributed_result.attempt_id,
                                                Some(&watchdog.model_id),
                                                &peer_id,
                                                &previous_worker_peer_id,
                                                AttemptClaimSource::ResultReceived,
                                            );
                                            if let Some(infer_state) = distributed_infer_state.as_mut() {
                                                let _ = infer_state.claim_attempt_record(
                                                    &distributed_result.attempt_id,
                                                    &peer_id,
                                                );
                                            }
                                            let _ = claim_task_attempt_peer(
                                                &distributed_result.task_id,
                                                &peer_id,
                                            );
                                        }
                                    }
                                    let accepted_after_current_moved = !current_request_matches;
                                    if accepted_after_current_moved {
                                        emit_late_result_received_event(
                                            &distributed_result.task_id,
                                            &distributed_result.attempt_id,
                                            &peer_id,
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref()),
                                            latency_ms,
                                            true,
                                            "accepted_within_fallback_claim_window",
                                            &prior_state,
                                        );
                                    }
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
                                    if let Some(infer_state) = distributed_infer_state.as_mut() {
                                        infer_state.update_attempt_state(
                                            &distributed_result.attempt_id,
                                            "failed",
                                            Some("retryable_failure"),
                                            Some(&reason),
                                        );
                                    }
                                    if let Some(watchdog) =
                                        attempt_watchdogs.get_mut(&distributed_result.attempt_id)
                                    {
                                        let previous_state = watchdog.state;
                                        if watchdog.transition_state(AttemptLifecycleState::Failed) {
                                            emit_attempt_state_changed_event(
                                                &distributed_result.task_id,
                                                &distributed_result.attempt_id,
                                                Some(&watchdog.model_id),
                                                Some(&peer.to_string()),
                                                previous_state.as_str(),
                                                watchdog.state.as_str(),
                                            );
                                        }
                                    }
                                    if let Some(health) = registry.write().await.record_failure(&peer_id) {
                                        let model_id = distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.clone());
                                        log_health_update(
                                            &distributed_result.task_id,
                                            &peer_id,
                                            model_id.as_deref(),
                                            &health,
                                            Some(NODE_UNHEALTHY_002),
                                        );
                                        record_health_policy_state_transition(
                                            &mut health_state_tracker,
                                            &distributed_result.task_id,
                                            &peer_id,
                                            model_id.as_deref(),
                                            "task_failure",
                                            &health,
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

                                        finalize_and_report_distributed_task_observability(
                                            infer_state,
                                            infer_started_at,
                                            true,
                                            false,
                                        );
                                    }

                                        break;
                                    }

                                    let accepted_retry_or_fallback = distributed_infer_state
                                        .as_ref()
                                        .map(|state| state.task_retry_count > 0)
                                        .unwrap_or(false)
                                        || attempt_watchdogs
                                            .get(&distributed_result.attempt_id)
                                            .map(|watchdog| watchdog.is_fallback_broadcast())
                                            .unwrap_or(false);
                                    if accepted_retry_or_fallback {
                                        emit_retry_result_accepted_event(
                                            &distributed_result.task_id,
                                            &distributed_result.attempt_id,
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref()),
                                            &peer_id,
                                            accepted_after_current_moved,
                                        );
                                    }
                                    if let Some(health) =
                                        registry.write().await.record_success(&peer_id, latency_ms)
                                    {
                                    let model_id = distributed_infer_state
                                        .as_ref()
                                        .and_then(|state| state.current_model.clone());
                                    log_health_update(
                                        &distributed_result.task_id,
                                        &peer_id,
                                        model_id.as_deref(),
                                        &health,
                                        None,
                                    );
                                    record_health_policy_state_transition(
                                        &mut health_state_tracker,
                                        &distributed_result.task_id,
                                        &peer_id,
                                        model_id.as_deref(),
                                        "success",
                                        &health,
                                    );
                                }
                                if let Some(watchdog) =
                                    attempt_watchdogs.get_mut(&distributed_result.attempt_id)
                                {
                                    let previous_state = watchdog.state;
                                    if watchdog.transition_state(AttemptLifecycleState::Completed) {
                                        emit_attempt_state_changed_event(
                                            &distributed_result.task_id,
                                            &distributed_result.attempt_id,
                                            Some(&watchdog.model_id),
                                            Some(&peer_id),
                                            previous_state.as_str(),
                                            watchdog.state.as_str(),
                                        );
                                    }
                                }
                                if let Some(infer_state) = distributed_infer_state.as_mut() {
                                    infer_state.update_attempt_state(
                                        &distributed_result.attempt_id,
                                        "completed",
                                        Some("success"),
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
                                    finalize_and_report_distributed_task_observability(
                                        infer_state,
                                        infer_started_at,
                                        false,
                                        false,
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

    fn test_attempt_timeout_policy() -> AttemptTimeoutPolicy {
        AttemptTimeoutPolicy {
            timeout_ms: 20,
            claim_timeout_ms: 10,
            first_progress_timeout_ms: 20,
            model_load_timeout_ms: 200,
            first_token_timeout_ms: 220,
            stall_timeout_ms: 25,
            extension_step_ms: 25,
            max_wait_ms: 120,
            max_execution_timeout_ms: 250,
            latency_class: "test",
        }
    }

    #[test]
    fn ram_mb_display_is_not_labeled_as_gb() {
        assert_eq!(
            resource_policy::format_ram_limit_for_display(4096),
            "4096 MB (4 GB)"
        );
        assert_eq!(
            resource_policy::format_ram_limit_for_display(16_384),
            "16384 MB (16 GB)"
        );
        assert_eq!(resource_policy::format_ram_limit_for_display(4), "4 GB");
    }

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
    fn test_adaptive_timeout_for_mistral_cpu_is_higher_than_fixed_timeout() {
        let node_capability = NodeCapability {
            peer_id: "peer-slow-cpu".to_string(),
            cpu_score: 38_000,
            ram_gb: 16,
            gpu_available: false,
            storage_available_gb: 100,
            accelerator: "cpu".to_string(),
            models: vec!["mistral-7b".to_string()],
            worker_slots: 2,
            active_tasks: 1,
            latency_ms: 120,
            last_seen: std::time::Instant::now(),
            cluster_id: None,
            health: NodeHealth::default(),
        };

        let policy =
            AttemptTimeoutPolicy::from_model_and_node("mistral-7b", Some(&node_capability));
        assert!(policy.timeout_ms > INFER_TIMEOUT_MS);
        assert!(policy.timeout_ms >= 12_000);
        assert!(policy.model_load_timeout_ms >= 120_000);
        assert!(policy.first_token_timeout_ms >= policy.model_load_timeout_ms);
    }

    #[test]
    fn test_watchdog_extends_timeout_only_with_meaningful_progress() {
        let mut watchdog = AttemptWatchdog::new(
            "task-watchdog".to_string(),
            "attempt-watchdog".to_string(),
            "peer-1".to_string(),
            "mistral-7b".to_string(),
            AttemptTimeoutPolicy {
                timeout_ms: 20,
                claim_timeout_ms: 10,
                first_progress_timeout_ms: 20,
                model_load_timeout_ms: 200,
                first_token_timeout_ms: 220,
                stall_timeout_ms: 50,
                extension_step_ms: 25,
                max_wait_ms: 120,
                max_execution_timeout_ms: 250,
                latency_class: "high",
            },
        );
        let base_deadline = watchdog
            .deadline_at
            .duration_since(watchdog.started_at)
            .as_millis() as u64;
        assert!(watchdog.record_progress("inference_started", Some(0)));
        let extended_deadline = watchdog
            .deadline_at
            .duration_since(watchdog.started_at)
            .as_millis() as u64;
        assert!(extended_deadline >= base_deadline);

        assert!(!watchdog.record_progress("inference_started", Some(0)));
        let repeated_deadline = watchdog
            .deadline_at
            .duration_since(watchdog.started_at)
            .as_millis() as u64;
        assert_eq!(extended_deadline, repeated_deadline);

        assert!(watchdog.record_progress("tokens_generated_count", Some(8)));
    }

    #[test]
    fn test_no_progress_attempt_becomes_stalled() {
        let mut watchdog = AttemptWatchdog::new(
            "task-stall".to_string(),
            "attempt-stall".to_string(),
            "peer-2".to_string(),
            "llama3-3b".to_string(),
            AttemptTimeoutPolicy {
                timeout_ms: 15,
                claim_timeout_ms: 10,
                first_progress_timeout_ms: 15,
                model_load_timeout_ms: 160,
                first_token_timeout_ms: 190,
                stall_timeout_ms: 25,
                extension_step_ms: 5,
                max_wait_ms: 120,
                max_execution_timeout_ms: 250,
                latency_class: "medium",
            },
        );
        std::thread::sleep(Duration::from_millis(35));
        assert_eq!(watchdog.check(), WatchdogCheck::Stalled);
    }

    #[test]
    fn test_broadcast_fallback_attempt_starts_unclaimed() {
        let watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-fallback".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );

        assert_eq!(
            watchdog.attempt_type,
            AttemptDispatchType::FallbackBroadcast
        );
        assert_eq!(watchdog.worker_peer_id, UNCLAIMED_WORKER_PEER_ID);
        assert!(watchdog.claimable);
        assert!(watchdog.is_unclaimed());
        assert!(watchdog.accepts_worker("TS140"));
    }

    #[test]
    fn test_task_message_received_claims_fallback_attempt() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-claim".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );

        let previous = watchdog.claim_worker("TS140", AttemptClaimSource::TaskMessageReceived);

        assert_eq!(previous.as_deref(), Some(UNCLAIMED_WORKER_PEER_ID));
        assert_eq!(watchdog.worker_peer_id, "TS140");
        assert!(!watchdog.claimable);
        assert!(watchdog.accepts_worker("TS140"));
        assert!(!watchdog.accepts_worker("Mac"));
    }

    #[test]
    fn test_attempt_progress_from_unclaimed_worker_claims_attempt() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-progress-claim".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );

        assert!(watchdog
            .claim_worker("TS140", AttemptClaimSource::AttemptProgress)
            .is_some());
        assert!(watchdog.record_progress("first_token_generated", Some(1)));

        assert_eq!(watchdog.worker_peer_id, "TS140");
        assert_eq!(watchdog.state, AttemptLifecycleState::ProducingTokens);
        assert_eq!(watchdog.last_tokens_generated, 1);
    }

    #[test]
    fn test_active_attempts_map_accepts_fallback_progress_by_task_and_attempt() {
        let key = AttemptKey::new("task-active", "attempt-2");
        let mut active_attempts = HashMap::new();
        active_attempts.insert(
            key.clone(),
            ActiveAttempt::new(
                "mistral-7b",
                UNCLAIMED_WORKER_PEER_ID,
                AttemptDispatchType::FallbackBroadcast,
            ),
        );

        let active_attempt = active_attempts.get_mut(&key).unwrap();
        assert!(active_attempt.accepts_worker("TS140"));
        assert_eq!(
            active_attempt.claim_worker("TS140").as_deref(),
            Some(UNCLAIMED_WORKER_PEER_ID)
        );
        assert_eq!(active_attempt.worker_peer_id, "TS140");
        assert!(active_attempt.accepts_worker("TS140"));
        assert!(!active_attempt.accepts_worker("Mac"));
    }

    #[test]
    fn test_progress_resets_no_progress_watchdog_warning() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-progress-reset".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);
        watchdog.no_progress_warning_emitted = true;
        let _ = watchdog.transition_state(AttemptLifecycleState::Stalled);

        let meaningful = watchdog.record_progress("tokens_generated_count", Some(8));
        if meaningful {
            watchdog.no_progress_warning_emitted = false;
        }

        assert!(meaningful);
        assert!(!watchdog.no_progress_warning_emitted);
        assert_eq!(watchdog.state, AttemptLifecycleState::ProducingTokens);
        assert_eq!(watchdog.last_tokens_generated, 8);
    }

    #[test]
    fn test_watchdog_no_progress_is_not_returned_while_tokens_increase() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-token-progress".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);

        for tokens in [1, 2, 3] {
            assert!(watchdog.record_progress("tokens_generated_count", Some(tokens)));
            std::thread::sleep(Duration::from_millis(5));
            assert_ne!(watchdog.check(), WatchdogCheck::Stalled);
            assert_ne!(watchdog.check(), WatchdogCheck::TimedOut);
        }
    }

    #[test]
    fn test_worker_claimed_progress_prevents_unclaimed_peer() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-worker-claimed".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );

        let claim_source = claim_source_from_progress_stage("worker_claimed");
        let previous = watchdog.claim_worker("TS140", claim_source);
        assert_eq!(previous.as_deref(), Some(UNCLAIMED_WORKER_PEER_ID));
        assert!(watchdog.record_progress("worker_claimed", None));

        assert_eq!(watchdog.worker_peer_id, "TS140");
        assert!(!watchdog.is_unclaimed());
        assert_eq!(watchdog.state, AttemptLifecycleState::Starting);
    }

    #[test]
    fn test_model_loading_progress_resets_claim_watchdog() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-model-loading".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);
        watchdog.no_progress_warning_emitted = true;
        let _ = watchdog.transition_state(AttemptLifecycleState::Stalled);

        assert!(watchdog.record_progress("model_loading", None));
        watchdog.no_progress_warning_emitted = false;

        assert_eq!(watchdog.state, AttemptLifecycleState::LoadingModel);
        assert!(!watchdog.no_progress_warning_emitted);
        assert_ne!(watchdog.check(), WatchdogCheck::Stalled);
    }

    #[test]
    fn test_still_running_does_not_extend_beyond_max_execution_timeout() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-max-cap".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);
        assert!(watchdog.record_progress("model_loading", None));
        watchdog.max_deadline_at = tokio::time::Instant::now() - Duration::from_millis(1);

        assert!(watchdog.record_progress("still_running", None));
        assert_eq!(watchdog.check(), WatchdogCheck::TimedOut);
    }

    #[test]
    fn test_first_token_timeout_differs_from_stall_timeout() {
        let policy = AttemptTimeoutPolicy::from_model_and_node("mistral-7b", None);

        assert!(policy.model_load_timeout_ms > policy.stall_timeout_ms);
        assert!(policy.first_token_timeout_ms > policy.stall_timeout_ms);
        assert_ne!(policy.first_token_timeout_ms, policy.stall_timeout_ms);
    }

    #[test]
    fn test_heavy_cpu_mistral_timeout_policy_allows_over_180s() {
        let node_capability = NodeCapability {
            peer_id: "TS140".to_string(),
            cpu_score: 38_000,
            ram_gb: 16,
            gpu_available: false,
            storage_available_gb: 100,
            accelerator: "cpu".to_string(),
            models: vec!["mistral-7b".to_string()],
            worker_slots: 2,
            active_tasks: 0,
            latency_ms: 120,
            last_seen: std::time::Instant::now(),
            cluster_id: None,
            health: NodeHealth::default(),
        };
        let policy =
            AttemptTimeoutPolicy::from_model_and_node("mistral-7b", Some(&node_capability));

        assert!(policy.claim_timeout_ms >= 120_000);
        assert!(policy.first_progress_timeout_ms >= 120_000);
        assert!(policy.max_execution_timeout_ms >= 180_000);
    }

    #[test]
    fn test_heavy_startup_delay_does_not_fail_before_model_load_timeout() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-heavy-startup".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);
        assert!(watchdog.record_progress("model_loading", None));
        watchdog.started_at = tokio::time::Instant::now() - Duration::from_millis(150);
        watchdog.last_progress_at = tokio::time::Instant::now();

        assert_eq!(watchdog.check(), WatchdogCheck::Healthy);

        watchdog.started_at = tokio::time::Instant::now() - Duration::from_millis(210);
        watchdog.last_progress_at = tokio::time::Instant::now();
        assert_eq!(watchdog.check(), WatchdogCheck::Stalled);
    }

    #[test]
    fn test_result_from_claimed_fallback_worker_is_accepted_after_current_request_moves() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-result-claim".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);
        let mut watchdogs = HashMap::new();
        watchdogs.insert("attempt-2".to_string(), watchdog);
        let mut active_attempts = HashMap::new();
        active_attempts.insert(
            AttemptKey::new("task-result-claim", "attempt-2"),
            ActiveAttempt::new(
                "mistral-7b",
                "TS140",
                AttemptDispatchType::FallbackBroadcast,
            ),
        );

        assert!(should_accept_result_for_attempt(
            &watchdogs,
            &active_attempts,
            Some("task-result-claim"),
            "task-result-claim",
            "attempt-2",
            "TS140",
            false,
        ));
        assert!(!should_accept_result_for_attempt(
            &watchdogs,
            &active_attempts,
            Some("task-result-claim"),
            "task-result-claim",
            "attempt-2",
            "Mac",
            false,
        ));
    }

    #[test]
    fn test_result_for_older_fallback_attempt_is_accepted_from_active_attempts_map() {
        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            "task-old-fallback".to_string(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        let _ = watchdog.claim_worker("TS140", AttemptClaimSource::AttemptProgress);
        let mut watchdogs = HashMap::new();
        watchdogs.insert("attempt-2".to_string(), watchdog);
        let mut active_attempts = HashMap::new();
        active_attempts.insert(
            AttemptKey::new("task-old-fallback", "attempt-2"),
            ActiveAttempt::new(
                "mistral-7b",
                "TS140",
                AttemptDispatchType::FallbackBroadcast,
            ),
        );

        assert!(should_accept_result_for_attempt(
            &watchdogs,
            &active_attempts,
            Some("task-old-fallback"),
            "task-old-fallback",
            "attempt-2",
            "TS140",
            false,
        ));
    }

    #[test]
    fn test_human_remote_progress_output_formatting() {
        assert_eq!(
            format_remote_progress_line("TS140", "attempt-2", "inference_started", 12_300, None),
            "[Remote] worker=TS140 attempt=attempt-2 status=thinking elapsed=12s stage=inference_started"
        );
        assert_eq!(
            format_remote_progress_line("TS140", "attempt-2", "model_loading", 15_000, None),
            "[Remote] worker=TS140 attempt=attempt-2 status=preparing_model elapsed=15s stage=model_loading"
        );
        assert_eq!(
            format_remote_progress_line("TS140", "attempt-2", "inference_warmup", 21_000, None),
            "[Remote] worker=TS140 attempt=attempt-2 status=warming_up elapsed=21s stage=inference_warmup"
        );
        assert_eq!(
            format_remote_progress_line(
                "TS140",
                "attempt-2",
                "tokens_generated_count",
                65_000,
                Some(32),
            ),
            "[Remote] worker=TS140 attempt=attempt-2 status=generating elapsed=65s tokens=32"
        );
        assert_eq!(
            format_remote_progress_line("TS140", "attempt-2", "result_published", 70_000, Some(48)),
            "[Remote] worker=TS140 attempt=attempt-2 status=completed tokens=48"
        );
    }

    #[test]
    fn test_late_result_received_event_emitted() {
        let trace_id = format!("late-result-{}", uuid_simple());
        emit_late_result_received_event(
            &trace_id,
            "attempt-late-1",
            "peer-late-1",
            Some("mistral-7b"),
            16_500,
            false,
            "arrived_after_timeout_policy_ignore",
            "timed_out",
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "late_result_received")
            .expect("late_result_received entry not found");

        assert_eq!(
            entry
                .fields
                .get("accepted")
                .and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            entry.fields.get("reason").and_then(|value| value.as_str()),
            Some("arrived_after_timeout_policy_ignore")
        );
    }

    #[test]
    fn test_progress_signals_are_emitted_for_long_running_attempts() {
        let trace_id = format!("progress-signals-{}", uuid_simple());
        emit_attempt_progress_event(
            &trace_id,
            "attempt-progress-1",
            "mistral-7b",
            "peer-progress-1",
            "first_token_generated",
            1_250,
            Some(1),
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "attempt_progress")
            .expect("attempt_progress entry not found");

        assert_eq!(
            entry.fields.get("stage").and_then(|value| value.as_str()),
            Some("first_token_generated")
        );
        assert_eq!(
            entry
                .fields
                .get("elapsed_ms")
                .and_then(|value| value.as_u64()),
            Some(1_250)
        );
    }

    #[test]
    fn test_remote_startup_progress_observability_events_are_emitted() {
        let trace_id = format!("remote-startup-progress-{}", uuid_simple());
        emit_remote_progress_published_event(
            &trace_id,
            "attempt-2",
            "mistral-7b",
            "TS140",
            "model_loading",
            5_000,
            None,
            &[RESULTS_TOPIC, TASK_TOPIC],
        );
        emit_remote_progress_published_event(
            &trace_id,
            "attempt-2",
            "mistral-7b",
            "TS140",
            "inference_warmup",
            15_000,
            None,
            &[RESULTS_TOPIC, TASK_TOPIC],
        );
        emit_remote_progress_published_event(
            &trace_id,
            "attempt-2",
            "mistral-7b",
            "TS140",
            "still_running",
            20_000,
            None,
            &[RESULTS_TOPIC, TASK_TOPIC],
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();

        assert!(entries
            .iter()
            .any(|entry| entry.trace_id == trace_id && entry.event == "remote_progress_published"));
        assert!(entries.iter().any(|entry| {
            entry.trace_id == trace_id && entry.event == "remote_model_loading_progress"
        }));
        assert!(entries.iter().any(|entry| {
            entry.trace_id == trace_id && entry.event == "remote_inference_warmup_progress"
        }));
        assert!(entries
            .iter()
            .any(|entry| entry.trace_id == trace_id && entry.event == "remote_still_running"));
    }

    #[test]
    fn test_remote_progress_delivery_observability_events_are_emitted() {
        let trace_id = format!("remote-progress-delivery-{}", uuid_simple());
        emit_remote_progress_publish_attempt_event(
            &trace_id,
            "attempt-2",
            "mistral-7b",
            "TS140",
            "tokens_generated_count",
            RESULTS_TOPIC,
            108_000,
        );
        emit_remote_progress_publish_success_event(
            &trace_id,
            "attempt-2",
            "mistral-7b",
            "TS140",
            "tokens_generated_count",
            RESULTS_TOPIC,
            108_000,
            "msg-1",
        );
        emit_remote_progress_client_received_event(
            &trace_id,
            "attempt-2",
            Some("mistral-7b"),
            "TS140",
            "tokens_generated_count",
            RESULTS_TOPIC,
            108_000,
            Some(340),
        );
        emit_remote_result_client_received_event(
            &trace_id,
            "attempt-2",
            Some("mistral-7b"),
            "TS140",
            RESULTS_TOPIC,
            109_000,
            true,
        );

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();

        for event in [
            "remote_progress_publish_attempt",
            "remote_progress_publish_success",
            "remote_progress_client_received",
            "remote_result_client_received",
        ] {
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.trace_id == trace_id && entry.event == event),
                "{event} entry missing"
            );
        }
    }

    #[test]
    fn test_fallback_claim_observability_events_are_emitted() {
        let trace_id = format!("fallback-claim-{}", uuid_simple());
        let candidates = vec!["mistral-7b".to_string()];
        emit_fallback_attempt_created_event(
            &trace_id,
            "attempt-2",
            "mistral-7b",
            TASK_TOPIC,
            &candidates,
        );
        emit_fallback_attempt_claimed_event(
            &trace_id,
            "attempt-2",
            Some("mistral-7b"),
            "TS140",
            UNCLAIMED_WORKER_PEER_ID,
            AttemptClaimSource::AttemptProgress,
        );
        emit_watchdog_reset_on_progress_event(
            &trace_id,
            "attempt-2",
            Some("mistral-7b"),
            "TS140",
            "tokens_generated_count",
            45_000,
        );
        emit_attempt_progress_recovered_event(&trace_id, "attempt-2", Some("mistral-7b"), "TS140");
        emit_retry_result_accepted_event(&trace_id, "attempt-2", Some("mistral-7b"), "TS140", true);

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();

        let created = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "fallback_attempt_created")
            .expect("fallback_attempt_created entry not found");
        assert_eq!(
            created
                .fields
                .get("attempt_type")
                .and_then(|value| value.as_str()),
            Some("fallback_broadcast")
        );
        assert_eq!(
            created
                .fields
                .get("claimable")
                .and_then(|value| value.as_bool()),
            Some(true)
        );

        let claimed = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == trace_id && entry.event == "fallback_attempt_claimed")
            .expect("fallback_attempt_claimed entry not found");
        assert_eq!(
            claimed
                .fields
                .get("worker_peer_id")
                .and_then(|value| value.as_str()),
            Some("TS140")
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.trace_id == trace_id
                    && entry.event == "watchdog_reset_on_progress")
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.trace_id == trace_id
                    && entry.event == "attempt_progress_recovered")
        );
        assert!(entries
            .iter()
            .any(|entry| entry.trace_id == trace_id && entry.event == "retry_result_accepted"));
    }

    #[test]
    fn test_inflight_guard_distinguishes_active_vs_timed_out_attempts() {
        assert!(is_meaningfully_in_flight(AttemptLifecycleState::Running));
        assert!(is_meaningfully_in_flight(
            AttemptLifecycleState::InferenceWarmup
        ));
        assert!(is_meaningfully_in_flight(
            AttemptLifecycleState::ProducingTokens
        ));
        assert!(!is_meaningfully_in_flight(AttemptLifecycleState::TimedOut));
        assert!(!is_meaningfully_in_flight(
            AttemptLifecycleState::LateCompleted
        ));
    }

    #[test]
    fn test_per_task_retry_and_fallback_counters_increment() {
        let mut state = DistributedInferState::new("2+2".to_string(), None, None);
        state.increment_task_retry_count();
        state.increment_task_retry_count();
        state.increment_task_fallback_count();

        assert_eq!(state.task_retry_count, 2);
        assert_eq!(state.task_fallback_count, 1);
    }

    #[test]
    fn test_human_final_trace_includes_multiple_attempts() {
        let mut state = DistributedInferState::new("Explain gravity".to_string(), None, None);
        state.register_attempt_record("attempt-1", "Mac", "mistral-7b", false);
        state.update_attempt_state(
            "attempt-1",
            "stalled",
            Some("timeout"),
            Some("watchdog_no_progress"),
        );
        state.register_attempt_record("attempt-2", "TS140", "mistral-7b", false);
        state.update_attempt_state("attempt-2", "completed", Some("success"), None);

        let lines = state.attempt_summary_lines();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("peer=Mac"));
        assert!(lines[0].contains("state=stalled"));
        assert!(lines[1].contains("peer=TS140"));
        assert!(lines[1].contains("outcome=success"));
    }

    #[test]
    fn test_final_trace_success_shows_claimed_retry_worker() {
        let mut state = DistributedInferState::new("Explain gravity".to_string(), None, None);
        let trace_id = state.trace_task_id.clone();
        state.register_attempt_record("attempt-1", "Mac", "mistral-7b", false);
        state.update_attempt_state(
            "attempt-1",
            "timed_out",
            Some("timeout"),
            Some("watchdog_no_progress"),
        );
        state.register_attempt_record("attempt-2", UNCLAIMED_WORKER_PEER_ID, "mistral-7b", true);
        assert!(state.claim_attempt_record("attempt-2", "TS140"));
        state.update_attempt_state("attempt-2", "completed", Some("success"), None);
        state.increment_task_retry_count();
        state.increment_task_fallback_count();
        let mut metrics = DistributedTaskMetrics::default();
        metrics.total_tasks = 1;
        metrics.retries_count = 1;
        metrics.fallback_count = 1;
        metrics.avg_latency_ms = 250.0;

        emit_final_trace_summary_event(&state, &metrics, false);

        iamine_network::flush_structured_logs().unwrap();
        let path = iamine_network::default_node_log_path();
        let entries = iamine_network::read_log_entries(&path).unwrap();
        let summary = entries
            .iter()
            .rev()
            .find(|entry| {
                entry.trace_id == trace_id && entry.event == "final_trace_summary_constructed"
            })
            .expect("final_trace_summary_constructed entry not found");
        assert_eq!(
            summary
                .fields
                .get("failed")
                .and_then(|value| value.as_bool()),
            Some(false)
        );
        let attempts = summary
            .fields
            .get("attempts")
            .and_then(|value| value.as_array())
            .expect("attempt summary lines missing");
        assert!(attempts
            .iter()
            .any(|line| line.as_str().unwrap_or("").contains("peer=TS140")
                && line.as_str().unwrap_or("").contains("outcome=success")));
        assert!(entries
            .iter()
            .any(|entry| entry.trace_id == trace_id && entry.event == "final_outcome_success"));
    }

    #[test]
    fn test_field_like_disconnect_fallback_progress_result_sequence_succeeds() {
        let mut state = DistributedInferState::new("Explain gravity".to_string(), None, None);
        state.register_attempt_record("attempt-1", "Mac", "mistral-7b", false);
        state.update_attempt_state(
            "attempt-1",
            "timed_out",
            Some("timeout"),
            Some("worker_disconnected"),
        );
        state.register_attempt_record("attempt-2", UNCLAIMED_WORKER_PEER_ID, "mistral-7b", true);
        state.increment_task_retry_count();
        state.increment_task_fallback_count();

        let mut watchdog = AttemptWatchdog::new_fallback_broadcast(
            state.trace_task_id.clone(),
            "attempt-2".to_string(),
            "mistral-7b".to_string(),
            test_attempt_timeout_policy(),
        );
        assert_eq!(watchdog.worker_peer_id, UNCLAIMED_WORKER_PEER_ID);

        let previous = watchdog.claim_worker("TS140", AttemptClaimSource::TaskMessageReceived);
        assert_eq!(previous.as_deref(), Some(UNCLAIMED_WORKER_PEER_ID));
        assert!(state.claim_attempt_record("attempt-2", "TS140"));
        assert!(watchdog.record_progress("task_received", None));
        assert!(watchdog.record_progress("worker_claimed", None));
        assert!(watchdog.record_progress("model_loading", None));
        assert!(watchdog.record_progress("still_running", None));
        assert!(watchdog.record_progress("model_loaded", None));
        assert!(watchdog.record_progress("inference_warmup", None));
        assert!(watchdog.record_progress("inference_started", None));
        assert!(watchdog.record_progress("first_token_generated", Some(1)));
        assert!(watchdog.record_progress("tokens_generated_count", Some(32)));
        assert_ne!(watchdog.check(), WatchdogCheck::Stalled);
        assert_ne!(watchdog.check(), WatchdogCheck::TimedOut);

        let mut watchdogs = HashMap::new();
        watchdogs.insert("attempt-2".to_string(), watchdog.clone());
        let mut active_attempts = HashMap::new();
        active_attempts.insert(
            AttemptKey::new(state.trace_task_id.as_str(), "attempt-2"),
            ActiveAttempt::new(
                "mistral-7b",
                "TS140",
                AttemptDispatchType::FallbackBroadcast,
            ),
        );
        assert!(should_accept_result_for_attempt(
            &watchdogs,
            &active_attempts,
            Some(&state.trace_task_id),
            &state.trace_task_id,
            "attempt-2",
            "TS140",
            false,
        ));

        assert!(watchdog.transition_state(AttemptLifecycleState::Completed));
        state.update_attempt_state("attempt-2", "completed", Some("success"), None);
        let lines = state.attempt_summary_lines();

        assert!(lines[0].contains("peer=Mac"));
        assert!(lines[0].contains("outcome=timeout"));
        assert!(lines[1].contains("peer=TS140"));
        assert!(!lines[1].contains("peer=-"));
        assert!(lines[1].contains("outcome=success"));
    }

    #[test]
    fn test_runtime_version_metadata_is_current_release_line() {
        let version = runtime_version_metadata();
        assert!(version.starts_with("v0.6.35"));
        assert!(!version.contains("v0.6.24"));
    }

    #[test]
    fn test_human_log_throttle_deduplicates_repetitive_values() {
        let mut throttle = HumanLogThrottle::default();
        assert!(throttle.should_log("heartbeat:peer-a", 60_000, Some("slots=4 rep=100 peers=2")));
        assert!(!throttle.should_log("heartbeat:peer-a", 60_000, Some("slots=4 rep=100 peers=2")));
        assert!(throttle.should_log("heartbeat:peer-a", 60_000, Some("slots=3 rep=100 peers=2")));
    }

    #[test]
    fn test_health_policy_is_degraded_before_blacklist() {
        let mut health = NodeHealth::default();
        health.record_timeout();
        assert_eq!(health.policy_state(), "degraded");
        health.record_timeout();
        assert_eq!(health.policy_state(), "blacklisted");
    }
}
