mod protocol;
mod network;
mod executor;
mod task_protocol;
mod task_codec;
mod worker_pool;
mod task_queue;
mod task_scheduler;
mod heartbeat;
mod metrics;
mod worker_capabilities;
mod task_cache;
mod peer_tracker;
mod result_protocol;
mod rate_limiter;
mod node_identity;
mod wallet;
mod benchmark;
mod resource_policy;
mod setup_wizard;
mod model_selector_cli;
mod daemon_runtime;

use iamine_models::{
    ModelRegistry, ModelStorage,
    StorageConfig,
    ModelInstaller, InstallResult,
    RealInferenceEngine, RealInferenceRequest,
    RealInferenceResult,
    HardwareAcceleration,
    InferenceTask, InferenceTaskResult, StreamedToken,
    ModelNodeCapabilities, ModelRequirements, can_node_run_model,
    DirectInferenceRequest,
    AutoProvisionProfile, ModelAutoProvision,
    normalize_output,
};

use iamine_network::{
    analyze_prompt_semantics,
    Complexity as PromptComplexityLevel,
    detect_exact_subtype,
    describe_output_policy,
    evaluate_default_dataset,
    ExactSubtype as PromptExactSubtype,
    Language as PromptLanguage,
    ModelPolicyEngine,
    NodeCapabilityHeartbeat,
    NodeRegistry,
    NetworkTopology,
    normalize_expression,
    OutputPolicyDecision,
    PromptProfile,
    SemanticRoutingDecision,
    SharedNetworkTopology,
    SharedNodeRegistry,
    TaskType as PromptTaskType,
};

#[cfg(test)]
use iamine_network::analyze_prompt;

use daemon_runtime::{daemon_is_available, daemon_socket_path, infer_via_daemon, run_daemon};
use model_selector_cli::ModelSelectorCLI;
use worker_pool::WorkerPool;
use task_queue::TaskQueue;
use task_scheduler::TaskScheduler;
use heartbeat::HeartbeatService;
use metrics::{NodeMetrics, start_metrics_server};
use worker_capabilities::WorkerCapabilities;
use task_cache::TaskCache;
use peer_tracker::PeerTracker;
use result_protocol::{TaskResultRequest, TaskResultResponse};
use rate_limiter::RateLimiter;
use node_identity::NodeIdentity;  // ← actualizado
use wallet::Wallet;
use benchmark::NodeBenchmark;
use resource_policy::ResourcePolicy;
use setup_wizard::{DetectedHardware, NodeSetupConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::{HashMap, HashSet};

use libp2p::{
    gossipsub, identify, kad, mdns, noise, ping,  // ← quitado identity de aquí
    request_response::{self, cbor, Event as RREvent, Message, ProtocolSupport},
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux,
    Multiaddr, PeerId, StreamProtocol, Transport,
};
use futures::StreamExt;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use task_protocol::{TaskRequest, TaskResponse};
use executor::TaskExecutor;

const TASK_TOPIC: &str = "iamine-tasks";
const CAP_TOPIC: &str = "iamine-capabilities";
const DIRECT_INF_TOPIC: &str = "iamine-direct-inference";
const INFER_TIMEOUT_MS: u64 = 30_000;
const INFER_FALLBACK_AFTER_MS: u64 = 8_000;

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
    fn from(e: ping::Event) -> Self { IaMineEvent::Ping(e) }
}
impl From<identify::Event> for IaMineEvent {
    fn from(e: identify::Event) -> Self { IaMineEvent::Identify(e) }
}
impl From<RREvent<TaskRequest, TaskResponse>> for IaMineEvent {
    fn from(e: RREvent<TaskRequest, TaskResponse>) -> Self { IaMineEvent::RequestResponse(e) }
}
impl From<RREvent<TaskResultRequest, TaskResultResponse>> for IaMineEvent {
    fn from(e: RREvent<TaskResultRequest, TaskResultResponse>) -> Self { IaMineEvent::ResultResponse(e) }
}
impl From<kad::Event> for IaMineEvent {
    fn from(e: kad::Event) -> Self { IaMineEvent::Kademlia(e) }
}
impl From<mdns::Event> for IaMineEvent {
    fn from(e: mdns::Event) -> Self { IaMineEvent::Mdns(e) }
}
impl From<gossipsub::Event> for IaMineEvent {
    fn from(e: gossipsub::Event) -> Self { IaMineEvent::Gossipsub(e) }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum NodeMode {
    Daemon,
    Worker,
    Relay,
    Client { peer: Option<Multiaddr>, task_type: String, data: String },
    Stress { peer: Option<Multiaddr>, count: usize },
    Broadcast { task_type: String, data: String },
    SimulateWorkers { count: usize },
    ModelsList,
    ModelsDownload { model_id: String },
    ModelsRemove { model_id: String },
    ModelsMenu,                          // ← nuevo
    ModelsSearch { query: String },      // ← nuevo
    TestInference { prompt: String },
    Infer { prompt: String, model_id: Option<String>, max_tokens_override: Option<u32> },
    SemanticEval,
    Capabilities,
    Nodes,
    Topology,  // ← NEW
    ModelsRecommend,
}

fn parse_args() -> Result<NodeMode, String> {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("--daemon") => Ok(NodeMode::Daemon),
        Some("--worker") | None => Ok(NodeMode::Worker),
        Some("--relay") => Ok(NodeMode::Relay),
        Some("models") => {
            match args.get(2).map(|s| s.as_str()) {
                Some("list") => Ok(NodeMode::ModelsList),
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
                _ => Err("Uso: iamine models [list|recommend|menu|search <q>|download <id>|remove <id>]".to_string()),
            }
        }
        Some("--client") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?), 3)
            } else { (None, 2) };
            let task_type = args.get(offset).ok_or("Falta <task_type>")?.clone();
            let data = args.get(offset + 1).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Client { peer, task_type, data })
        }

        Some("--stress") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?), 3)
            } else { (None, 2) };
            let count = args.get(offset).unwrap_or(&"10".to_string()).parse::<usize>().unwrap_or(10);
            Ok(NodeMode::Stress { peer, count })
        }

        Some("--broadcast") => {
            let task_type = args.get(2).ok_or("Falta <task_type>")?.clone();
            let data = args.get(3).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Broadcast { task_type, data })
        }

        Some("--simulate-workers") => {
            let count = args.get(2).unwrap_or(&"10".to_string())
                .parse::<usize>().unwrap_or(10);
            Ok(NodeMode::SimulateWorkers { count })
        }

        Some("test-inference") => {
            let prompt = args.get(2)
                .cloned()
                .unwrap_or_else(|| "What is 2+2?".to_string());
            Ok(NodeMode::TestInference { prompt })
        }

        Some("capabilities") => Ok(NodeMode::Capabilities),

        Some("infer") => {
            let prompt = args.get(2).ok_or("Falta <prompt>")?.clone();
            let model_id = args.iter()
                .position(|a| a == "--model")
                .and_then(|i| args.get(i + 1).cloned());
            let max_tokens_override = parse_optional_u32_flag(&args, "--max-tokens")?;
            Ok(NodeMode::Infer { prompt, model_id, max_tokens_override })
        }

        Some("semantic-eval") => Ok(NodeMode::SemanticEval),

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
        PromptTaskType::StructuredList => "StructuredList",
        PromptTaskType::Deterministic => "Deterministic",
        PromptTaskType::Code => "Code",
        PromptTaskType::Conceptual => "Conceptual",
        PromptTaskType::Reasoning => "Reasoning",
        PromptTaskType::Summarization => "Summarization",
        PromptTaskType::General => "General",
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
    println!("[Semantic] Task: {}", prompt_task_label(decision.profile.task_type));
    println!("[Semantic] Confidence: {:.2}", decision.profile.confidence);
    println!("[Semantic] Fallback: {}", decision.fallback_applied);
    if decision.fallback_applied {
        println!(
            "[Semantic] Original task: {}",
            prompt_task_label(decision.original_task_type)
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
            format!(
                "{}\n\n{}",
                retry_guard, prompt
            )
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
            format!(
                "{}\n\n{}",
                retry_guard, prompt
            )
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
    let mut last_result: Option<RealInferenceResult> = None;

    for attempt in 0..=1 {
        let guarded_prompt = with_task_guard(&prompt, task_type, attempt == 1);
        let attempt_temperature = if task_requires_validation(task_type) {
            temperature.min(0.2)
        } else {
            temperature
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
        if attempt == 0 {
            print!("📤 Respuesta: ");
        } else {
            print!("\n[Validator] Retrying once with stronger formatting guard...\n📤 Retry: ");
        }

        let req = RealInferenceRequest {
            task_id: task_id.clone(),
            model_id: model_id.clone(),
            prompt: guarded_prompt,
            max_tokens,
            temperature: attempt_temperature,
        };

        let result = match &runtime {
            InferenceRuntime::Engine(engine) => {
                let engine_clone = Arc::clone(engine);
                let inference_handle = tokio::spawn(async move {
                    engine_clone.run_inference(req, Some(tx)).await
                });

                while let Some(token) = rx.recv().await {
                    print!("{}", token);
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                }

                inference_handle
                    .await
                    .map_err(|e| format!("Inference task join error: {}", e))?
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
                daemon_response.result
            }
        };

        let mut result = result;
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

        let should_retry = task_requires_validation(task_type) && !valid && attempt == 0;
        last_result = Some(result);
        if !should_retry {
            break;
        }
    }

    last_result.ok_or_else(|| "Inference returned no result".to_string())
}

async fn choose_inference_runtime() -> Option<InferenceRuntime> {
    let socket = daemon_socket_path();
    if daemon_is_available(&socket).await {
        println!("[Daemon] Connected to persistent runtime: {}", socket.display());
        Some(InferenceRuntime::Daemon(socket))
    } else {
        None
    }
}

fn resolve_policy_for_prompt(
    prompt: &str,
    model_override: Option<&str>,
    available_models: &[String],
) -> (PromptProfile, Vec<String>, String, String) {
    let semantic_prompt = normalize_expression(prompt).unwrap_or_else(|| prompt.to_string());
    if semantic_prompt != prompt {
        println!("[Parser] Normalized expression: {} → {}", prompt, semantic_prompt);
    }

    let semantic_decision = analyze_prompt_semantics(&semantic_prompt);
    let profile = semantic_decision.profile.clone();
    println!(
        "[Analyzer] Language: {}, Complexity: {}, Task: {}",
        prompt_language_label(profile.language),
        prompt_complexity_label(profile.complexity),
        prompt_task_label(profile.task_type),
    );
    log_semantic_decision(&semantic_decision);

    if let Some(model_id) = model_override {
        println!("[Policy] Manual override: {}", model_id);
        return (
            profile,
            vec![model_id.to_string()],
            model_id.to_string(),
            semantic_prompt,
        );
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
        decision.model,
        decision.reason
    );
    (profile, candidates, decision.model, semantic_prompt)
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let auto_model = args.iter().any(|a| a == "--auto-model");
    let mode = match parse_args() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("❌ {}", e);
            eprintln!("Uso:");
            eprintln!("  iamine-node --worker [--port=N] [--cpu=N] [--ram=N] [--gpu]");
            eprintln!("  iamine-node --relay");
            eprintln!("  iamine-node --broadcast <type> <data>");
            eprintln!("  iamine-node models list");
            eprintln!("  iamine-node models download <model_id>");
            eprintln!("  iamine-node models remove <model_id>");
            eprintln!("  iamine-node semantic-eval");
            eprintln!("  iamine-node --daemon");
            // eprintln!("  iamine-node nodes");
            std::process::exit(1);
        }
    };

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
            for m in &models { m.display(); }
            let used = ModelStorage::new().total_size_bytes();
            let cfg = StorageConfig::load();
            println!("\n💾 Storage: {:.1}/{} GB usados",
                used as f64 / 1_073_741_824.0, cfg.max_storage_gb);
            return Ok(());
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
                InstallResult::Installed(id) =>
                    println!("✅ Modelo {} instalado en ~/.iamine/models/", id),
                InstallResult::AlreadyExists(id) =>
                    println!("ℹ️  Modelo {} ya está instalado", id),
                InstallResult::InsufficientStorage { needed_gb, available_gb } =>
                    println!("❌ Espacio insuficiente: necesita {:.1} GB, disponible {:.1} GB", needed_gb, available_gb),
                InstallResult::DownloadFailed(e) =>
                    println!("❌ Descarga fallida: {}", e),
                InstallResult::ValidationFailed(e) =>
                    println!("❌ Validación fallida: {}", e),
            }
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
                        "- prompt='{}' expected={} predicted={} normalize(expected={}, predicted={}) confidence={:.2} fallback={}",
                        error.prompt,
                        prompt_task_label(error.expected_task),
                        prompt_task_label(error.predicted_task),
                        error.expected_normalize,
                        error.predicted_normalize,
                        error.confidence,
                        error.fallback_applied
                    );
                }
            }
            return Ok(());
        }

        NodeMode::Daemon => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Inference Daemon      ║");
            println!("╚══════════════════════════════════╝\n");
            let socket = daemon_socket_path();
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

            let semantic_prompt = normalize_expression(prompt).unwrap_or_else(|| prompt.clone());
            if semantic_prompt != *prompt {
                println!("[Parser] Normalized expression: {} → {}", prompt, semantic_prompt);
            }
            let semantic_decision = analyze_prompt_semantics(&semantic_prompt);
            let prompt_profile = semantic_decision.profile.clone();
            println!(
                "[Analyzer] Language: {}, Complexity: {}, Task: {}",
                prompt_language_label(prompt_profile.language),
                prompt_complexity_label(prompt_profile.complexity),
                prompt_task_label(prompt_profile.task_type),
            );
            log_semantic_decision(&semantic_decision);
            let output_policy = resolve_output_policy(&prompt_profile, &semantic_prompt, None);
            println!(
                "[OutputPolicy] max_tokens: {} (reason: {})",
                output_policy.max_tokens,
                output_policy.reason
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
            ).await?;
            println!("\n\n✅ Inference completada");
            println!("[Inference] tokens_generated: {}", result.tokens_generated);
            println!("[Inference] truncated: {}", result.truncated);
            println!("[Inference] continuation_steps: {}", result.continuation_steps);
            if result.truncated {
                println!("[Warning] Output truncated at token budget");
            }
            return Ok(());
        }

        NodeMode::Infer { prompt, model_id, max_tokens_override } => {
            println!("╔══════════════════════════════════╗");
            println!("║      IaMine — Inference          ║");
            println!("╚══════════════════════════════════╝\n");

            let registry = ModelRegistry::new();
            let storage = ModelStorage::new();
            let local_models = storage.list_local_models();
            let (profile, candidate_models, selected_model, semantic_prompt) = resolve_policy_for_prompt(
                prompt,
                model_id.as_deref(),
                &local_models,
            );
            let output_policy = resolve_output_policy(&profile, &semantic_prompt, *max_tokens_override);
            println!(
                "[OutputPolicy] max_tokens: {} (reason: {})",
                output_policy.max_tokens,
                output_policy.reason
            );

            if storage.has_model(&selected_model) {
                println!("🤖 Modelo: {}", selected_model);
                println!("💬 Prompt: {}\n", prompt);

                let model_desc = registry.get(&selected_model)
                    .ok_or_else(|| format!("Modelo {} no encontrado en registry", selected_model))?;
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
                ).await?;
                println!("\n\n✅ Inference completada");
                println!("[Inference] tokens_generated: {}", result.tokens_generated);
                println!("[Inference] truncated: {}", result.truncated);
                println!("[Inference] continuation_steps: {}", result.continuation_steps);
                if result.truncated {
                    println!("[Warning] Output truncated at token budget");
                }
                return Ok(());
            } else if model_id.is_some() {
                println!("⚠️  Override {} no esta instalado localmente, intentando ruta distribuida.", selected_model);
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
                println!("   {} {} (min {}GB RAM, {}GB storage{})",
                    icon, mid, req.min_ram_gb, req.min_storage_gb,
                    if req.requires_gpu { ", GPU requerida" } else { "" });
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

    // ════════════════════════════════════════
    // WORKER STARTUP FLOW
    // ════════════════════════════════════════
    if matches!(mode, NodeMode::Worker) {
        println!("╔══════════════════════════════════╗");
        println!("║       IaMine Worker v0.6         ║");
        println!("╚══════════════════════════════════╝\n");
    }

    // 1️⃣ Identidad persistente
    let node_identity = NodeIdentity::load_or_create();
    let peer_id = node_identity.peer_id;
    let id_keys = node_identity.keypair.clone();

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
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2)
    };

    println!("\n═══════════════════════════════════");
    println!("  Peer ID:      {}", peer_id);
    println!("  Wallet:       {}", node_identity.wallet_address);
    if let Some(ref b) = benchmark {
        println!("  CPU Score:    {:.0}", b.cpu_score);
        println!("  RAM:          {} GB", b.ram_available_gb);
        println!("  GPU:          {}", if b.gpu_available { "✅" } else { "❌" });
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

    let worker_setup = if matches!(mode, NodeMode::Worker) {
        let detected = DetectedHardware {
            cpu_cores: std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(1),
            ram_gb: benchmark.as_ref().map(|b| b.ram_available_gb as u32).unwrap_or(node_caps.ram_gb),
            gpu_available: benchmark.as_ref().map(|b| b.gpu_available).unwrap_or(node_caps.gpu_type.is_some()),
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
                    println!("{} ({:.0}MB)", model.id, model.size_bytes as f64 / 1_048_576.0);
                }

                let auto_download_enabled = auto_model
                    || worker_setup
                        .as_ref()
                        .map(|cfg| cfg.auto_download_enabled())
                        .unwrap_or(false);

                if auto_download_enabled {
                    if let Some(model_id) = provision.auto_download_recommended(&profile, None, false).await? {
                        println!("\n⬇ Auto-downloaded: {}", model_id);
                    }
                } else {
                    print!("\nDownload now? [Y/n] ");
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                    let mut input = String::new();
                    let _ = std::io::stdin().read_line(&mut input);

                    if input.trim().is_empty() || matches!(input.trim(), "y" | "Y" | "yes" | "YES") {
                        if let Some(model_id) = provision.auto_download_recommended(&profile, None, false).await? {
                            println!("✅ Modelo descargado: {}", model_id);
                        }
                    }
                }
            }
        } else {
            for m in &local {
                // Verificar espacio antes de mostrar
                let used = model_storage.total_size_bytes();
                println!("   ✅ {} (storage: {:.1}/{} GB)",
                    m,
                    used as f64 / 1_073_741_824.0,
                    storage_config.max_storage_gb);
            }
        }
        println!("📋 Modelos en registry:");
        for m in model_registry.list() {
            let available = if model_storage.has_model(&m.id) { "✅" } else { "⬜" };
            let fits = storage_config.has_space_for(m.size_bytes, model_storage.total_size_bytes());
            let fits_str = if fits { "" } else { " ⚠️ sin espacio" };
            println!("   {} {} v{} ({}GB RAM){}", available, m.id, m.version, m.required_ram_gb, fits_str);
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
    ).map_err(|e| format!("Gossipsub error: {}", e))?;

    let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC);
    gossipsub_behaviour.subscribe(&task_topic)?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-bids"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-assign"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-heartbeat"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-results"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(CAP_TOPIC))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(DIRECT_INF_TOPIC))?;

    let mut kad_cfg = kad::Config::default();
    kad_cfg.set_query_timeout(Duration::from_secs(30));
    let kademlia = kad::Behaviour::with_config(
        peer_id,
        kad::store::MemoryStore::new(peer_id),
        kad_cfg,
    );

    let behaviour = IamineBehaviour {
        ping: ping::Behaviour::default(),
        identify: identify::Behaviour::new(identify::Config::new(
            "/iamine/1.0".to_string(),
            id_keys.public(),
        )),
        request_response: cbor::Behaviour::<TaskRequest, TaskResponse>::new(
            [(StreamProtocol::new("/iamine/task/1.0"), ProtocolSupport::Full)],
            request_response::Config::default(),
        ),
        // ← Protocolo directo para resultados
        result_response: cbor::Behaviour::<TaskResultRequest, TaskResultResponse>::new(
            [(StreamProtocol::new("/iamine/result/1.0"), ProtocolSupport::Full)],
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
        } else { 9000 }
    };

    let listen_addr: Multiaddr = if matches!(mode, NodeMode::Worker) {
        format!("/ip4/0.0.0.0/tcp/{}", worker_port).parse()?
    } else if matches!(mode, NodeMode::Relay) {
        "/ip4/0.0.0.0/tcp/9999".parse()?
    } else {
        "/ip4/0.0.0.0/tcp/0".parse()?
    };
    swarm.listen_on(listen_addr)?;

    let metrics_port = 9090 + (worker_port - 9000);
    if matches!(mode, NodeMode::Worker) {
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
    let mut origin_peer_map: std::collections::HashMap<String, PeerId> = std::collections::HashMap::new();
    let mut tasks_sent = false;

    match &mode {
        NodeMode::Client { task_type, data, .. } => {
            pending_tasks.push(TaskRequest {
                task_id: uuid_simple(),
                task_type: task_type.clone(),
                data: data.clone(),
            });
            total_tasks = 1;
        }
        NodeMode::Stress { count, .. } => {
            total_tasks = *count;
            for i in 0..*count {
                pending_tasks.push(TaskRequest {
                    task_id: format!("stress_{:03}", i + 1),
                    task_type: "reverse_string".to_string(),
                    data: format!("iamine_{}", i + 1),
                });
            }
            println!("🔥 Stress test: {} tareas preparadas\n", count);
        }
        _ => {}
    }

    // ← v0.6: Si es modo Infer, preparar solicitud de inferencia
    let mut infer_request_id: Option<String> = None;
    let mut infer_broadcast_sent = false;
    let mut pending_inference: std::collections::HashMap<String, tokio::time::Instant> = std::collections::HashMap::new();
    let mut infer_started_at: Option<tokio::time::Instant> = None;

    // v0.6.2: estado de streaming ordenado
    let mut token_buffer: HashMap<u32, String> = HashMap::new();
    let mut next_token_idx: u32 = 0;
    let mut rendered_output = String::new();

    if let NodeMode::Infer { prompt, model_id, max_tokens_override } = &mode {
        let rid = uuid_simple();
        let local_available = model_storage.list_local_models();
        let (profile, candidates, selected, semantic_prompt) = resolve_policy_for_prompt(
            prompt,
            model_id.as_deref(),
            &local_available,
        );
        let output_policy = resolve_output_policy(&profile, &semantic_prompt, *max_tokens_override);
        println!("🧠 Distributing inference: model={} prompt='{}'", selected, semantic_prompt);
        println!("   Candidates: {}", candidates.join(", "));
        println!(
            "   [OutputPolicy] max_tokens: {} (reason: {})",
            output_policy.max_tokens,
            output_policy.reason
        );
        println!("   Request ID: {}", rid);
        infer_request_id = Some(rid);
        infer_started_at = Some(tokio::time::Instant::now());
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

            // Heartbeat tick
            Some(_) = heartbeat_rx.recv() => {
                if matches!(mode, NodeMode::Worker) {
                    let rep = queue.reputation().await;
                    let uptime = heartbeat.uptime_secs();
                    let slots = pool.available_slots();
                    {
                        let mut m = metrics.write().await;
                        m.uptime_secs = uptime;
                        m.reputation_score = rep.reputation_score;
                        m.active_workers = pool.max_concurrent - slots;
                        m.network_peers = peer_tracker.peer_count();
                        m.direct_peers = known_workers.len();
                        m.avg_latency_ms = peer_tracker.avg_latency();
                    }

                    if rate_limiter.allow("Heartbeat") {
                        let hb = serde_json::json!({
                            "type": "Heartbeat",
                            "peer_id": peer_id.to_string(),
                            "available_slots": slots,
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
                    let nm = installer.build_node_models(&peer_id.to_string());
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
                        models: model_storage.list_local_models(),
                        worker_slots: worker_slots as u32,
                        active_tasks: (pool.max_concurrent - pool.available_slots()) as u32,
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
                                reg.set_cluster(node_id, &cluster.id);
                            }
                        }
                    }
                }

                // Smart routing: intento envío directo cuando ya hay registry
                if matches!(mode, NodeMode::Infer { .. }) && !infer_broadcast_sent {
                    if let NodeMode::Infer { prompt, model_id, max_tokens_override } = &mode {
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

                        let (profile, mut candidates, selected_model, semantic_prompt) = resolve_policy_for_prompt(
                            prompt,
                            model_id.as_deref(),
                            &available_models,
                        );
                        let output_policy = resolve_output_policy(&profile, &semantic_prompt, *max_tokens_override);
                        if !candidates.contains(&selected_model) {
                            candidates.insert(0, selected_model.clone());
                        }

                        let selected = reg.select_best_node_for_models_with_cluster(
                            &candidates,
                            local_cluster.as_deref(),
                        );
                        drop(reg);

                        if let Some((best_peer, routed_model)) = selected {
                            let rid = infer_request_id.clone().unwrap_or_else(uuid_simple);
                            let direct = DirectInferenceRequest {
                                request_id: rid.clone(),
                                target_peer: best_peer.to_string(),
                                model: routed_model.clone(),
                                prompt: semantic_prompt.clone(),
                                max_tokens: output_policy.max_tokens as u32,
                            };

                            let _ = swarm.behaviour_mut().gossipsub.publish(
                                gossipsub::IdentTopic::new(DIRECT_INF_TOPIC),
                                serde_json::to_vec(&direct.to_gossip_json()).unwrap(),
                            );

                            metrics.write().await.routing_decision(start.elapsed().as_millis() as u64);
                            infer_broadcast_sent = true;
                            pending_inference.insert(rid.clone(), tokio::time::Instant::now());
                            println!("[Routing] Sending to node {} with model {}", best_peer, routed_model);
                            println!("🧠 DirectInferenceRequest enviado [{}] → {}", &rid[..8.min(rid.len())], best_peer);
                            print!("📤 Respuesta: ");
                            let _ = std::io::Write::flush(&mut std::io::stdout());
                        } else if total_nodes > 0 && candidates.iter().all(|m| !available_models.contains(m)) {
                            eprintln!("❌ Ningún nodo en la red tiene un modelo compatible instalado.");
                            eprintln!("   Nodos conocidos: {}", total_nodes);
                            eprintln!("   Candidates: {}", candidates.join(", "));
                            break;
                        } else if infer_started_at.map(|t| t.elapsed().as_millis() as u64).unwrap_or(0) >= INFER_FALLBACK_AFTER_MS {
                            // Fallback automático a broadcast legacy
                            let rid = infer_request_id.clone().unwrap_or_else(uuid_simple);
                            let mid = selected_model.clone();
                            let task = InferenceTask::new(
                                rid.clone(),
                                mid,
                                semantic_prompt.clone(),
                                output_policy.max_tokens as u32,
                                peer_id.to_string(),
                            );

                            let _ = swarm.behaviour_mut().gossipsub.publish(
                                gossipsub::IdentTopic::new(TASK_TOPIC),
                                serde_json::to_vec(&task.to_gossip_json()).unwrap(),
                            );

                            infer_broadcast_sent = true;
                            pending_inference.insert(rid.clone(), tokio::time::Instant::now());
                            println!("↪️  Fallback: InferenceRequest broadcast enviado [{}]", &rid[..8.min(rid.len())]);
                            print!("📤 Respuesta: ");
                            let _ = std::io::Write::flush(&mut std::io::stdout());
                        }
                    }
                }

                // Timeout total de inferencia distribuida
                if matches!(mode, NodeMode::Infer { .. }) {
                    if let Some(rid) = infer_request_id.clone() {
                        if let Some(t0) = pending_inference.get(&rid) {
                            if t0.elapsed().as_millis() as u64 >= INFER_TIMEOUT_MS {
                                eprintln!("\n❌ Timeout de inferencia distribuida ({} ms)", INFER_TIMEOUT_MS);
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
                        known_workers.remove(&pid);
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&pid);
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: _,
                    message,
                    ..
                })) => {
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
                                let model_id = msg["model_id"].as_str().unwrap_or("").to_string();
                                let prompt = msg["prompt"].as_str().unwrap_or("").to_string();
                                let max_tokens = msg["max_tokens"].as_u64().unwrap_or(200) as u32;
                                let temperature = msg["temperature"].as_f64().unwrap_or(0.7) as f32;
                                let _requester = msg["requester_peer"].as_str().unwrap_or("").to_string();

                                println!("🧠 [Worker] InferenceRequest: model={} prompt='{}'",
                                    model_id, &prompt[..prompt.len().min(40)]);

                                // Check model installed
                                if !model_storage.has_model(&model_id) {
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

                                // Crear canal para tokens streaming
                                let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);

                                // Spawn inference execution
                                let inference_handle = tokio::spawn(async move {
                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id.clone(),
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
                                                    request_id_clone, model_id, peer_id_str, e,
                                                );
                                            }
                                        }
                                    } else {
                                        let eng = Arc::clone(&engine_ref);
                                        let hash = registry_clone.get(&model_id)
                                            .map(|m| m.hash.clone())
                                            .unwrap_or_default();
                                        if let Err(e) = eng.load_model(&model_id, &hash) {
                                            return InferenceTaskResult::failure(
                                                request_id_clone, model_id, peer_id_str, e,
                                            );
                                        }

                                        eng.run_inference(req, Some(token_tx)).await
                                    };

                                    InferenceTaskResult::success(
                                        request_id_clone,
                                        model_id,
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
                                        gossipsub::IdentTopic::new("iamine-results"),
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

                                    let _ = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new("iamine-results"),
                                        serde_json::to_vec(&result.to_gossip_json()).unwrap(),
                                    );
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
                                    let success = msg["success"].as_bool().unwrap_or(false);
                                    let tokens = msg["tokens_generated"].as_u64().unwrap_or(0);
                                    let truncated = msg["truncated"].as_bool().unwrap_or(false);
                                    let continuation_steps = msg["continuation_steps"].as_u64().unwrap_or(0);
                                    let ms = msg["execution_ms"].as_u64().unwrap_or(0);
                                    let worker = msg["worker_peer"].as_str().unwrap_or("unknown");
                                    let accel = msg["accelerator"].as_str().unwrap_or("unknown");

                                    // v0.6.2: solo imprimir parte faltante, sin duplicar
                                    if success {
                                        if let Some(full_output) = msg["output"].as_str() {
                                            if rendered_output.is_empty() {
                                                print!("{}", full_output);
                                            } else if full_output.starts_with(&rendered_output) {
                                                let suffix = &full_output[rendered_output.len()..];
                                                if !suffix.is_empty() {
                                                    print!("{}", suffix);
                                                }
                                            }
                                        }
                                        let _ = std::io::Write::flush(&mut std::io::stdout());
                                    }

                                    println!("\n\n═══════════════════════════════════");
                                    if success {
                                        println!("✅ Distributed inference completada");
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
                                        println!("❌ Inference falló: {}", err);
                                    }
                                    println!("═══════════════════════════════════");
                                    token_buffer.clear();
                                    rendered_output.clear(); // <- nuevo
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
                                let model_id = msg["model"].as_str().unwrap_or("tinyllama-1b").to_string();
                                let prompt = msg["prompt"].as_str().unwrap_or("").to_string();
                                let max_tokens = msg["max_tokens"].as_u64().unwrap_or(200) as u32;

                                println!("🧠 [Worker] DirectInferenceRequest: model={} prompt='{}'",
                                    model_id, &prompt[..prompt.len().min(40)]);

                                if !model_storage.has_model(&model_id) {
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

                                let (token_tx, mut token_rx) = tokio::sync::mpsc::channel::<String>(100);

                                let inference_handle = tokio::spawn(async move {
                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id.clone(),
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
                                                    request_id_clone, model_id, peer_id_str, e,
                                                );
                                            }
                                        }
                                    } else {
                                        let eng = Arc::clone(&engine_ref);
                                        let hash = registry_clone.get(&model_id)
                                            .map(|m| m.hash.clone())
                                            .unwrap_or_default();

                                        if let Err(e) = eng.load_model(&model_id, &hash) {
                                            return InferenceTaskResult::failure(
                                                request_id_clone, model_id, peer_id_str, e,
                                            );
                                        }

                                        eng.run_inference(req, Some(token_tx)).await
                                    };

                                    InferenceTaskResult::success(
                                        request_id_clone,
                                        model_id,
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
                                        gossipsub::IdentTopic::new("iamine-results"),
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

                                    let _ = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new("iamine-results"),
                                        serde_json::to_vec(&result.to_gossip_json()).unwrap(),
                                    );

                                    println!("✅ [Worker] Direct inference completada: {} tokens en {}ms",
                                        result.tokens_generated, result.execution_ms);
                                }
                            }

                            _ => {}
                        }
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

                    // Desactivar ruta vieja: no enviar InferenceRequest broadcast aquí.
                    // (Se envía por smart routing en heartbeat tick cuando registry tenga candidatos)

                    // ...existing code...
                }

                SwarmEvent::ConnectionEstablished { peer_id: pid, endpoint, .. } => {
                    println!("✅ Conectado a: {} ({})", pid, endpoint.get_remote_address());
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
                    known_workers.remove(&pid);
                    if is_client && !waiting_for_response && !matches!(mode, NodeMode::Broadcast { .. }) {
                        println!("\n📊 Completado: {}/{} tareas", completed, total_tasks);
                        break;
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
                    if let Ok(rtt) = result {
                        let rtt_ms = rtt.as_micros() as f64 / 1000.0;
                        peer_tracker.update_latency(&peer.to_string(), rtt_ms);
                        topology.write().await.update_latency(&peer.to_string(), rtt_ms);
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Kademlia(kad::Event::RoutingUpdated { peer, .. })) => {
                    println!("📡 Kademlia: nodo añadido {}", peer);
                }

                SwarmEvent::Behaviour(IaMineEvent::RequestResponse(event)) => {
                    match event {
                        RREvent::Message { peer, message: Message::Request { request, channel, .. } } => {
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
                        RREvent::Message { peer, message: Message::Response { response, .. } } => {
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
    println!("✅ {} workers simulados activos. Ctrl+C para detener.", count);
    tokio::time::sleep(Duration::from_secs(60)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_tokens_override() {
        let profile = analyze_prompt("explica la teoria de la relatividad");
        let decision = resolve_output_policy(&profile, "explica la teoria de la relatividad", Some(100));

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

        assert_eq!(parse_optional_u32_flag(&args, "--max-tokens").unwrap(), Some(1024));
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
}
