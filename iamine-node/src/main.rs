mod backend_config;
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
#[cfg(test)]
mod config_test_utils;
mod controller_broadcast_runtime;
mod controller_cluster_runtime;
mod controller_result_runtime;
mod cpu_feature_guard;
mod daemon_runtime;
mod discovery_runtime;
mod env_config;
mod executor;
mod final_outcome;
mod gossipsub_message_runtime;
mod heartbeat;
mod infer_observability;
mod infer_retry;
mod infer_runtime;
mod infer_watchdog;
mod metrics;
mod metrics_policy;
mod metrics_server;
mod mode_dispatch;
mod model_capability_consistency;
mod model_display_policy;
mod model_executability;
mod model_selector_cli;
mod network;
mod network_config;
mod network_event_observability;
mod node_identity;
mod node_modes;
mod path_config;
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
mod router_scheduler;
mod runtime_config;
mod runtime_observability;
mod scheduler_capability_matching;
mod scheduler_events;
mod scheduler_policy;
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
mod worker_assignment_runtime;
mod worker_capabilities;
mod worker_capability_advertisement;
mod worker_pool;
mod worker_pubsub_runtime;
mod worker_result_runtime;
mod worker_runtime;
mod worker_startup_policy;

use iamine_models::{
    DirectInferenceRequest, HardwareAcceleration, InferenceTask, ModelRegistry, ModelStorage,
    RealInferenceEngine, RealInferenceRequest,
};

use iamine_network::{
    claim_task_attempt_peer, health_policy_thresholds, log_structured, prompt_log_entry,
    record_distributed_task_fallback, record_distributed_task_late_result,
    record_distributed_task_retry, record_distributed_task_started, record_task_attempt,
    select_retry_target, set_global_node_id, set_global_runtime_context, validate_result,
    DistributedTaskResult, FailureKind, IntelligentScheduler, NetworkTopology, NodeRegistry,
    ResultStatus, SharedNetworkTopology, SharedNodeRegistry, TaskClaim, TaskManager,
    TaskType as PromptTaskType, NET_PEER_DISCONNECTED_002, NODE_UNHEALTHY_002, SCH_NO_NODE_001,
    TASK_DISPATCH_UNCONFIRMED_001, TASK_EMPTY_RESULT_003, TASK_FAILED_002, TASK_TIMEOUT_001,
    WORKER_STARTUP_OVERFLOW_001,
};

#[cfg(test)]
use iamine_network::analyze_prompt;
#[cfg(test)]
use iamine_network::DistributedTaskMetrics;
#[cfg(test)]
use iamine_network::NodeCapability;
#[cfg(test)]
use iamine_network::NodeHealth;

use daemon_runtime::{daemon_socket_path, run_daemon};
use heartbeat::HeartbeatService;
pub(crate) use infer_observability::*;
pub(crate) use infer_retry::DistributedInferState;
pub(crate) use infer_runtime::*;
pub(crate) use infer_watchdog::*;
use metrics::NodeMetrics;
use metrics_server::*;
use network_event_observability::HumanLogThrottle;
use node_identity::NodeIdentity; // ← actualizado
use peer_tracker::PeerTracker;
use quality_gate::runtime_version_metadata;
use rate_limiter::RateLimiter;
use result_observability::*;
use result_protocol::TaskResultResponse;
use runtime_config::{
    load_runtime_args, maybe_print_debug_flags, prepare_runtime_startup_config, simulate_workers,
    RuntimeModeConfig, INFER_FALLBACK_AFTER_MS, INFER_TIMEOUT_MS, UNCLAIMED_WORKER_PEER_ID,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use task_cache::TaskCache;
use task_queue::TaskQueue;
use task_scheduler::TaskScheduler;
use tokio::sync::RwLock;
use worker_capabilities::WorkerCapabilities;
use worker_pool::WorkerPool;

use futures::StreamExt;
use libp2p::{
    gossipsub,
    kad,
    mdns,
    noise,
    ping, // ← quitado identity de aquí
    request_response::{self, Event as RREvent, Message},
    swarm::{Swarm, SwarmEvent},
    tcp,
    yamux,
    PeerId,
    Transport,
};
use std::error::Error;
use std::time::Duration;

use broadcast_protocol::*;
use broadcast_runtime::*;
use broadcast_worker::*;
use capability_display::*;
use cluster_registry::*;
use controller_broadcast_runtime::{
    emit_controller_pubsub_ready, handle_controller_broadcast_tick, handle_controller_task_bid,
    ControllerBroadcastTickContext, ControllerTaskBidContext,
};
use controller_cluster_runtime::{
    emit_controller_cluster_status_requested, handle_controller_cluster_status_message,
    render_controller_cluster_status,
};
use controller_result_runtime::handle_controller_task_result;
use discovery_runtime::{
    handle_connection_closed, handle_connection_established, handle_gossipsub_subscribed,
    handle_gossipsub_unsubscribed, handle_kademlia_routing_updated, handle_mdns_discovered,
    handle_mdns_expired, handle_new_listen_addr, handle_ping_event,
};
use executor::TaskExecutor;
use final_outcome::*;
use gossipsub_message_runtime::{
    broadcast_mode_task_payload, direct_inference_request_for_local_worker,
    handle_capability_topic_message, handle_heartbeat_message, handle_inference_progress_message,
    handle_inference_token_message, handle_node_capabilities_message, handle_task_cancel_message,
    log_invalid_worker_task_message, message_topic_name,
};
use mode_dispatch::handle_pre_network_mode;
use model_display_policy::*;
use network_config::{
    bootnode_addresses_from_args, build_gossipsub_behaviour, build_iamine_behaviour,
    build_kademlia, cluster_status_wait_ms_from_env, listen_addr_for_mode,
    register_local_broadcast_pubsub_topics, swarm_idle_connection_timeout, IaMineEvent,
    RuntimeNetworkIntervals,
};
use node_modes::{mode_label, InferenceControlFlags, NodeMode};
use task_protocol::{TaskRequest, TaskResponse};
use usage::print_usage;
use worker_assignment_runtime::{handle_worker_task_assignment, WorkerTaskAssignment};
use worker_capability_advertisement::*;
use worker_pubsub_runtime::{
    handle_worker_task_offer, WorkerTaskOffer, WorkerTaskOfferRuntimeContext,
};
use worker_result_runtime::publish_worker_broadcast_task_result;
use worker_runtime::{
    handle_worker_inference_request, WorkerInferenceRuntimeContext, WorkerInferenceRuntimeRequest,
};
use worker_startup_policy::*;

pub(crate) use network_config::IamineBehaviour;
pub(crate) use pubsub_observability::*;
pub(crate) use pubsub_readiness::*;
pub(crate) use pubsub_topic_tracker::*;
pub(crate) use pubsub_topics::*;
pub(crate) use runtime_config::uuid_simple;
pub(crate) use runtime_observability::*;

struct PendingTaskResponse {
    channel: request_response::ResponseChannel<TaskResponse>,
    response: TaskResponse,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime_args = match load_runtime_args() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("❌ {}", e);
            print_usage();
            std::process::exit(1);
        }
    };
    let args = runtime_args.args;
    let auto_model = runtime_args.auto_model;
    let debug_flags = runtime_args.debug_flags;
    let mode = runtime_args.mode;
    if matches!(mode, NodeMode::Help) {
        print_usage();
        return Ok(());
    }
    maybe_print_debug_flags(debug_flags);
    let runtime_version = runtime_version_metadata();
    set_global_runtime_context(mode_label(&mode), &runtime_version);
    let mode_config = RuntimeModeConfig::from_mode(&mode);
    let is_cluster_status_mode = mode_config.is_cluster_status_mode;
    let control_plane_only = mode_config.control_plane_only;
    let worker_port = runtime_args.worker_port;

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
            emit_daemon_started_event(
                &node_identity.peer_id.to_string(),
                &socket.display().to_string(),
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

    let runtime_startup = prepare_runtime_startup_config(
        &mode,
        &args,
        is_cluster_status_mode,
        &runtime_version,
        worker_port,
    );
    let node_identity = runtime_startup.node_identity;
    let peer_id = runtime_startup.peer_id;
    let id_keys = runtime_startup.id_keys;
    let wallet = runtime_startup.wallet;
    let benchmark = runtime_startup.benchmark;
    let resource_policy = runtime_startup.resource_policy;
    let worker_slots = runtime_startup.worker_slots;

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

    let mut gossipsub_behaviour = build_gossipsub_behaviour(&id_keys)?;
    let local_backend = worker_startup_policy
        .as_ref()
        .map(|policy| policy.backend.as_str())
        .unwrap_or("real");
    let mut pubsub_topics = register_local_broadcast_pubsub_topics(
        &mut gossipsub_behaviour,
        &mode,
        &peer_id,
        local_backend,
    )?;

    let kademlia = build_kademlia(peer_id);

    let mut cluster_registry = ClusterRegistry::with_default_cluster_id();
    let cluster_id = cluster_registry.cluster_id().to_string();
    let cluster_status_wait_ms = cluster_status_wait_ms_from_env();
    if is_cluster_status_mode {
        emit_controller_cluster_status_requested(&cluster_id, cluster_status_wait_ms);
    }

    let mdns_behaviour = match mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id) {
        Ok(behaviour) => behaviour,
        Err(error) if is_cluster_status_mode => {
            render_controller_cluster_status(
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

    let behaviour = build_iamine_behaviour(&id_keys, kademlia, mdns_behaviour, gossipsub_behaviour);

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(swarm_idle_connection_timeout()),
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

    for addr in bootnode_addresses_from_args(&args) {
        println!("🔗 Conectando a bootnode: {}", addr);
        let _ = swarm.dial(addr);
    }

    if matches!(mode, NodeMode::Worker) {
        let (timeout_blacklist_threshold, failure_blacklist_threshold) = health_policy_thresholds();
        emit_worker_started_event(
            &peer_id.to_string(),
            worker_port,
            worker_slots,
            resource_policy_value(&resource_policy),
        );
        emit_health_policy_configured_event(
            timeout_blacklist_threshold,
            failure_blacklist_threshold,
        );
    }

    let listen_addr = listen_addr_for_mode(&mode, worker_port)?;
    if let Err(error) = swarm.listen_on(listen_addr) {
        if is_cluster_status_mode {
            render_controller_cluster_status(
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

    let network_intervals = RuntimeNetworkIntervals::default();
    let mut heartbeat_rx = HeartbeatService::start(network_intervals.heartbeat_secs);
    let mut nodes_tick = tokio::time::interval(network_intervals.nodes_tick_interval());
    let mut broadcast_tick = tokio::time::interval(network_intervals.broadcast_tick_interval());
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
                render_controller_cluster_status(
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

                let outcome = handle_controller_broadcast_tick(
                    &mut swarm,
                    state,
                    ControllerBroadcastTickContext {
                        task_type,
                        data,
                        controller_peer_id: &peer_id,
                        scheduler: &scheduler,
                        known_workers: &known_workers,
                        pubsub_topics: &mut pubsub_topics,
                        waiting_for_response: &mut waiting_for_response,
                        tasks_sent: &mut tasks_sent,
                    },
                )
                .await?;
                if outcome.should_break() {
                    break;
                }
            }

            Some(broadcast_result) = broadcast_result_rx.recv() => {
                publish_worker_broadcast_task_result(&mut swarm, broadcast_result);
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
                                emit_dispatch_deduplicated_inflight_event(
                                    &trace_task_id,
                                    &rid,
                                    &best_peer,
                                    &routed_model,
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
                            emit_scheduler_node_selected_event(
                                &trace_task_id,
                                &routed_model,
                                &best_peer,
                                selected_cluster_id.as_deref(),
                                node_score,
                                &candidates,
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
                                    emit_attempt_timeout_policy_event(
                                        &trace_task_id,
                                        &rid,
                                        &routed_model,
                                        Some(&best_peer),
                                        None,
                                        None,
                                        &timeout_policy,
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
                            emit_node_rejected_no_compatible_model_event(
                                &infer_state.trace_task_id,
                                total_nodes,
                                &candidates,
                                SCH_NO_NODE_001,
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
                                    emit_attempt_timeout_policy_event(
                                        &trace_task_id,
                                        &rid,
                                        &selected_model,
                                        None,
                                        Some(AttemptDispatchType::FallbackBroadcast),
                                        Some(true),
                                        &timeout_policy,
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
                                        emit_task_timeout_event(
                                            &TaskTimeoutEvent {
                                                trace_task_id: &trace_task_id,
                                                attempt_id: &attempt_id,
                                                model_id: &model_id,
                                                worker_peer_id: (worker_peer_id != "-")
                                                    .then_some(worker_peer_id.as_str()),
                                                latency_ms: elapsed_ms,
                                                elapsed_since_last_progress_ms:
                                                    elapsed_since_progress_ms,
                                                adaptive_timeout_ms,
                                                max_wait_ms,
                                            },
                                            TASK_TIMEOUT_001,
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
                    handle_new_listen_addr(
                        &mut swarm,
                        peer_id,
                        &address,
                        &mode,
                        &mut worker_startup_ready_emitted,
                        worker_port,
                        worker_startup_policy.as_ref(),
                    );
                }

                SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Discovered(peers))) => {
                    handle_mdns_discovered(
                        &mut swarm,
                        peers,
                        peer_id,
                        debug_flags,
                        &mut cluster_registry,
                        &cluster_id,
                        &mut known_workers,
                        is_client,
                        connected_peer,
                        tasks_sent,
                    );
                }

                SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Expired(peers))) => {
                    handle_mdns_expired(
                        &mut swarm,
                        peers,
                        &mut known_workers,
                        &mut pubsub_topics,
                    );
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source,
                    message,
                    ..
                })) => {
                    let from_peer = propagation_source.to_string();
                    let message_topic = message_topic_name(&message.topic);

                    if message.topic == gossipsub::IdentTopic::new(CAP_TOPIC).hash() {
                        handle_capability_topic_message(
                            &message.data,
                            &registry,
                            &mut cluster_registry,
                            &cluster_id,
                        )
                        .await;
                        continue;
                    }

                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&message.data) {
                        let msg_type = msg["type"].as_str().unwrap_or("");

                        // ...existing rate limiter...

                        match msg_type {
                            CLUSTER_NODE_STATUS_TYPE => {
                                handle_controller_cluster_status_message(
                                    &mut cluster_registry,
                                    &cluster_id,
                                    &msg,
                                    message_topic,
                                );
                            }
                            "TaskOffer" if matches!(mode, NodeMode::Worker) => {
                                let offer = WorkerTaskOffer::from_value(&msg);
                                handle_worker_task_offer(
                                    &mut swarm,
                                    offer,
                                    WorkerTaskOfferRuntimeContext {
                                        from_peer: &from_peer,
                                        task_cache: &mut task_cache,
                                        capabilities: &capabilities,
                                        pool: &pool,
                                        queue: &queue,
                                        peer_id: &peer_id,
                                    },
                                )
                                .await;
                            }

                            "TaskBid" if matches!(mode, NodeMode::Broadcast { .. }) => {
                                let (task_type_value, data_value) =
                                    broadcast_mode_task_payload(&mode);
                                handle_controller_task_bid(
                                    &mut swarm,
                                    &msg,
                                    ControllerTaskBidContext {
                                        task_type: task_type_value,
                                        data: data_value,
                                        controller_peer_id: &peer_id,
                                        scheduler: &scheduler,
                                        broadcast_offer_state: &mut broadcast_offer_state,
                                        known_workers: &known_workers,
                                        origin_peer_map: &mut origin_peer_map,
                                        peer_tracker: &peer_tracker,
                                        cluster_registry: &cluster_registry,
                                    },
                                )
                                .await;
                            }

                            "TaskResult" if matches!(mode, NodeMode::Broadcast { .. }) => {
                                if handle_controller_task_result(
                                    &msg,
                                    message_topic,
                                    &scheduler,
                                    &mut broadcast_offer_state,
                                )
                                .await
                                {
                                    break;
                                }
                            }

                            "TaskAssign" if matches!(mode, NodeMode::Worker) => {
                                handle_worker_task_assignment(
                                    WorkerTaskAssignment::from_value(&msg),
                                    peer_id.to_string(),
                                    &mut executed_broadcast_assignments,
                                    Arc::clone(&queue),
                                    Arc::clone(&metrics),
                                    broadcast_result_tx.clone(),
                                );
                            }

                            "TaskCancel" if matches!(mode, NodeMode::Worker) => {
                                handle_task_cancel_message(&msg);
                            }

                            "Heartbeat" => {
                                handle_heartbeat_message(
                                    &msg,
                                    &mut peer_tracker,
                                    Arc::clone(&metrics),
                                    &mut cluster_registry,
                                    &cluster_id,
                                    &mut human_log_throttle,
                                )
                                .await;
                            }

                            // ═══════════════════════════════════════════
                            // v0.6: DISTRIBUTED INFERENCE
                            // ═══════════════════════════════════════════

                            "InferenceRequest" if matches!(mode, NodeMode::Worker) => {
                                let request = WorkerInferenceRuntimeRequest::from_inference_request_value(&msg);
                                handle_worker_inference_request(
                                    &mut swarm,
                                    request,
                                    WorkerInferenceRuntimeContext {
                                        peer_id: &peer_id,
                                        message_topic,
                                        from_peer: &from_peer,
                                        model_storage: &model_storage,
                                        node_caps: &node_caps,
                                        worker_startup_policy: worker_startup_policy.as_ref(),
                                        inference_engine: inference_engine.clone(),
                                        metrics: Arc::clone(&metrics),
                                    },
                                )
                                .await;
                            }

                            "InferenceProgress" if matches!(mode, NodeMode::Infer { .. }) => {
                                handle_inference_progress_message(
                                    &msg,
                                    message_topic,
                                    &mut distributed_infer_state,
                                    &mut attempt_watchdogs,
                                    &mut active_attempts,
                                );
                            }

                            "InferenceToken" if matches!(mode, NodeMode::Infer { .. }) => {
                                handle_inference_token_message(
                                    &msg,
                                    infer_request_id.as_ref(),
                                    &mut token_buffer,
                                    &mut next_token_idx,
                                    &mut rendered_output,
                                );
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
                                        emit_distributed_task_failed_event(
                                            distributed_infer_state
                                                .as_ref()
                                                .map(|state| state.trace_task_id.as_str())
                                                .unwrap_or("-"),
                                            &attempt_id,
                                            distributed_infer_state
                                                .as_ref()
                                                .and_then(|state| state.current_model.as_deref()),
                                            worker,
                                            &reason,
                                            "validation_failure",
                                            error_code,
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
                                    emit_distributed_task_completed_event(
                                        &task_id,
                                        &attempt_id,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        worker,
                                        latency_ms,
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
                                handle_node_capabilities_message(
                                    &msg,
                                    &registry,
                                    &mut cluster_registry,
                                    &cluster_id,
                                )
                                .await;
                            }

                            "DirectInferenceRequest" if matches!(mode, NodeMode::Worker) => {
                                let local_peer_id = peer_id.to_string();
                                if let Some(request) = direct_inference_request_for_local_worker(
                                    &msg,
                                    &local_peer_id,
                                    &from_peer,
                                ) {
                                    handle_worker_inference_request(
                                        &mut swarm,
                                        request,
                                        WorkerInferenceRuntimeContext {
                                            peer_id: &peer_id,
                                            message_topic,
                                            from_peer: &from_peer,
                                            model_storage: &model_storage,
                                            node_caps: &node_caps,
                                            worker_startup_policy: worker_startup_policy.as_ref(),
                                            inference_engine: inference_engine.clone(),
                                            metrics: Arc::clone(&metrics),
                                        },
                                    )
                                    .await;
                                }
                            }

                            _ => {}
                        }
                    } else if matches!(mode, NodeMode::Worker)
                        && (message_topic == TASK_TOPIC || message_topic == DIRECT_INF_TOPIC)
                    {
                        log_invalid_worker_task_message(
                            message_topic,
                            &from_peer,
                            message.data.len(),
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
                            emit_direct_task_completed_event(
                                &request.task_id,
                                &request.attempt_id,
                                &request.model_id,
                                &worker_peer_id,
                                request.tokens_generated,
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
                    handle_gossipsub_subscribed(
                        &swarm,
                        pid,
                        topic,
                        peer_id,
                        &mode,
                        &mut pubsub_topics,
                        &mut cluster_registry,
                        &mut broadcast_offer_state,
                    );
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id: pid, topic })) => {
                    handle_gossipsub_unsubscribed(pid, topic, &mut pubsub_topics);
                }

                SwarmEvent::ConnectionEstablished { peer_id: pid, endpoint, .. } => {
                    handle_connection_established(
                        &mut swarm,
                        pid,
                        &endpoint,
                        &mode,
                        &mut cluster_registry,
                        &cluster_id,
                        &mut broadcast_offer_state,
                        &mut connected_peer,
                        &mut known_workers,
                        is_client,
                        &mut tasks_sent,
                        &pending_tasks,
                        total_tasks,
                        &mut waiting_for_response,
                    );
                }

                SwarmEvent::ConnectionClosed { peer_id: pid, .. } => {
                    if handle_connection_closed(
                        pid,
                        &mode,
                        distributed_infer_state.as_ref(),
                        infer_request_id.as_ref(),
                        &pending_inference,
                        &mut known_workers,
                        &mut pubsub_topics,
                        is_client,
                        waiting_for_response,
                        completed,
                        total_tasks,
                    ) {
                        break;
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
                    handle_ping_event(
                        peer,
                        result,
                        &mut peer_tracker,
                        &topology,
                        &mut cluster_registry,
                        &mut human_log_throttle,
                    )
                    .await;
                }

                SwarmEvent::Behaviour(IaMineEvent::Kademlia(kad::Event::RoutingUpdated { peer, .. })) => {
                    handle_kademlia_routing_updated(
                        peer,
                        &mut cluster_registry,
                        &cluster_id,
                        &mut human_log_throttle,
                        debug_flags,
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
                                emit_worker_task_received_event(
                                    &task.id,
                                    &task.attempt_id,
                                    &task.model,
                                    &peer.to_string(),
                                    local_cluster_id.as_deref(),
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
                                    emit_distributed_task_failed_event(
                                        &distributed_result.task_id,
                                        &distributed_result.attempt_id,
                                        distributed_infer_state
                                            .as_ref()
                                            .and_then(|state| state.current_model.as_deref()),
                                        &peer_id,
                                        &reason,
                                        "task_failure",
                                        error_code,
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
                                emit_distributed_task_completed_event(
                                    &distributed_result.task_id,
                                    &distributed_result.attempt_id,
                                    distributed_infer_state
                                        .as_ref()
                                        .and_then(|state| state.current_model.as_deref()),
                                    &peer_id,
                                    latency_ms,
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
    fn test_health_policy_is_degraded_before_blacklist() {
        let mut health = NodeHealth::default();
        health.record_timeout();
        assert_eq!(health.policy_state(), "degraded");
        health.record_timeout();
        assert_eq!(health.policy_state(), "blacklisted");
    }
}
