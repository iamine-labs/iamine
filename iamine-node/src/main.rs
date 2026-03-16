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

use iamine_models::{
    ModelRegistry, ModelStorage,
    StorageConfig,
    ModelInstaller, InstallResult,
    RealInferenceEngine, RealInferenceRequest,
    HardwareAcceleration,
    InferenceTask, InferenceTaskResult, StreamedToken,
    // v0.5.4
    ModelNodeCapabilities, ModelRequirements, can_node_run_model,
    select_best_model, CapabilitiesUpdatedEvent,
    ModelInstalledEvent,
    DirectInferenceRequest,
};
use iamine_network::{NodeRegistry, SharedNodeRegistry, NodeCapabilityHeartbeat};

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
use std::sync::Arc;
use tokio::sync::RwLock;

use libp2p::{
    gossipsub, identify, kad, mdns, noise, ping,  // ← quitado identity de aquí
    request_response::{self, cbor, Event as RREvent, Message, ProtocolSupport},
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux,
    Multiaddr, PeerId, StreamProtocol, Transport,
};
use futures::StreamExt;
use std::error::Error;
use std::str::FromStr;
use std::collections::HashSet;
use std::time::Duration;

use task_protocol::{TaskRequest, TaskResponse};
use executor::TaskExecutor;

const TASK_TOPIC: &str = "iamine-tasks";
const CAP_TOPIC: &str = "iamine-capabilities";
const DIRECT_INF_TOPIC: &str = "iamine-direct-inference";

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

#[derive(Debug, Clone)]
enum NodeMode {
    Worker,
    Relay,
    Client { peer: Option<Multiaddr>, task_type: String, data: String },
    Stress { peer: Option<Multiaddr>, count: usize },
    Broadcast { task_type: String, data: String },
    SimulateWorkers { count: usize },
    ModelsList,
    ModelsDownload { model_id: String },
    ModelsRemove { model_id: String },
    TestInference { prompt: String },
    Infer { prompt: String, model_id: Option<String> },
    Capabilities, // ← v0.5.4
    Nodes, // ← nuevo
}

fn parse_args() -> Result<NodeMode, String> {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("--worker") | None => Ok(NodeMode::Worker),
        Some("--relay") => Ok(NodeMode::Relay),
        Some("models") => {
            match args.get(2).map(|s| s.as_str()) {
                Some("list") => Ok(NodeMode::ModelsList),
                Some("download") => {
                    let id = args.get(3).ok_or("Falta <model_id>")?.clone();
                    Ok(NodeMode::ModelsDownload { model_id: id })
                }
                Some("remove") => {
                    let id = args.get(3).ok_or("Falta <model_id>")?.clone();
                    Ok(NodeMode::ModelsRemove { model_id: id })
                }
                _ => Err("Uso: iamine models [list|download <id>|remove <id>]".to_string()),
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
            Ok(NodeMode::Infer { prompt, model_id })
        }

        Some("nodes") => Ok(NodeMode::Nodes),

        Some(unknown) => Err(format!("Modo desconocido: {}", unknown)),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
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
            // eprintln!("  iamine-node nodes");
            std::process::exit(1);
        }
    };

    // v0.6.1: Registry global para smart routing (capabilities + selección de nodo)
    let registry: SharedNodeRegistry = Arc::new(RwLock::new(NodeRegistry::new()));

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

        NodeMode::ModelsRemove { model_id } => {
            let installer = ModelInstaller::new();
            match installer.remove(model_id) {
                Ok(_) => println!("✅ Modelo {} eliminado", model_id),
                Err(e) => println!("❌ {}", e),
            }
            return Ok(());
        }

        NodeMode::TestInference { prompt } => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Test Inference       ║");
            println!("╚══════════════════════════════════╝\n");

            let hw = HardwareAcceleration::detect();
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
            let mut engine = RealInferenceEngine::new(ModelStorage::new());

            // Cargar modelo
            engine.load_model(model_id, &model_desc.hash)?;

            // Streaming tokens
            let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);

            print!("📤 Respuesta: ");
            let req = RealInferenceRequest {
                task_id: "test-001".to_string(),
                model_id: model_id.to_string(),
                prompt: prompt.clone(),
                max_tokens: 200,
                temperature: 0.7,
            };

            // Spawn inference + print tokens en tiempo real
            let engine_ref = std::sync::Arc::new(tokio::sync::Mutex::new(engine));
            let engine_clone = std::sync::Arc::clone(&engine_ref);

            tokio::spawn(async move {
                let eng = engine_clone.lock().await;
                eng.run_inference(req, Some(tx)).await
            });

            // Imprimir tokens según llegan
            while let Some(token) = rx.recv().await {
                print!("{}", token);
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }

            println!("\n\n✅ Inference completada");
            return Ok(());
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

        _ => {} // continuar con startup normal
    }

    // ════════════════════════════════════════
    // WORKER STARTUP FLOW
    // ════════════════════════════════════════
    println!("╔══════════════════════════════════╗");
    println!("║       IaMine Worker v0.6         ║");
    println!("╚══════════════════════════════════╝\n");

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
    println!("  Modo:         {:?}", mode);
    println!("  Balance:      {} $MIND", wallet.balance);
    println!("  Tareas:       {}", wallet.tasks_completed);
    println!("  Reputación:   {:.1}/100", wallet.reputation);
    println!("═══════════════════════════════════\n");

    // 6️⃣ MODEL INFRASTRUCTURE v0.6
    let storage_config = StorageConfig::load();
    let model_registry = ModelRegistry::new();
    let model_storage = ModelStorage::new();
    let inference_engine = Arc::new(tokio::sync::Mutex::new(
        RealInferenceEngine::new(ModelStorage::new())
    ));

    // v0.5.4: Detectar capabilities del nodo
    let node_caps = ModelNodeCapabilities::detect(&peer_id.to_string());
    if matches!(mode, NodeMode::Worker) {
        node_caps.display();
    }

    if matches!(mode, NodeMode::Worker) {
        println!("💾 Storage limit: {} GB", storage_config.max_storage_gb);
        println!("🤖 Modelos disponibles localmente:");
        let local = model_storage.list_local_models();
        if local.is_empty() {
            println!("   (ninguno — usa --download-model <id>)");
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
    let mut broadcast_sent = false;
    let mut broadcast_attempts = 0u32;
    let mut subscribed_peers = 0usize;

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
    let mut last_nodes_print = std::time::Instant::now(); // ← faltaba esta línea

    if let NodeMode::Infer { prompt, model_id } = &mode {
        let rid = uuid_simple();
        let mid = model_id.clone().unwrap_or_else(|| "tinyllama-1b".to_string());
        println!("🧠 Distributing inference: model={} prompt='{}'", mid, prompt);
        println!("   Request ID: {}", rid);
        infer_request_id = Some(rid);
    }

    let is_client = !matches!(mode, NodeMode::Worker | NodeMode::Relay);

    loop {
        tokio::select! {
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
                }

                // Modo nodes: imprimir snapshot cada 5s
                if matches!(mode, NodeMode::Nodes) && last_nodes_print.elapsed() >= Duration::from_secs(5) {
                    let snapshot = registry.read().await.all_nodes();
                    println!("\nPeer ID        Models                CPU Score    Load");
                    println!("-------------------------------------------------------");
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
                    last_nodes_print = std::time::Instant::now();
                }

                // Smart routing: intentar envío directo cuando ya hay registry
                if matches!(mode, NodeMode::Infer { .. }) && !infer_broadcast_sent {
                    if let NodeMode::Infer { prompt, model_id } = &mode {
                        let model = model_id.clone().unwrap_or_else(|| "tinyllama-1b".to_string());
                        let start = std::time::Instant::now();
                        let selected = registry.read().await.select_best_node(&model);

                        if let Some(best_peer) = selected {
                            let rid = infer_request_id.clone().unwrap_or_else(uuid_simple);
                            let direct = DirectInferenceRequest {
                                request_id: rid.clone(),
                                target_peer: best_peer.to_string(),
                                model,
                                prompt: prompt.clone(),
                                max_tokens: 200,
                            };

                            let _ = swarm.behaviour_mut().gossipsub.publish(
                                gossipsub::IdentTopic::new(DIRECT_INF_TOPIC),
                                serde_json::to_vec(&direct.to_gossip_json()).unwrap(),
                            );

                            metrics.write().await.routing_decision(start.elapsed().as_millis() as u64);
                            infer_broadcast_sent = true;
                            pending_inference.insert(rid.clone(), tokio::time::Instant::now());
                            println!("🧠 DirectInferenceRequest enviado [{}] → {}", &rid[..8.min(rid.len())], best_peer);
                            print!("📤 Respuesta: ");
                            let _ = std::io::Write::flush(&mut std::io::stdout());
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
                                let requester = msg["requester_peer"].as_str().unwrap_or("").to_string();

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
                                    let mut eng = engine_ref.lock().await;

                                    // Load model if not cached
                                    let hash = registry_clone.get(&model_id)
                                        .map(|m| m.hash.clone())
                                        .unwrap_or_default();
                                    if let Err(e) = eng.load_model(&model_id, &hash) {
                                        return InferenceTaskResult::failure(
                                            request_id_clone, model_id, peer_id_str, e,
                                        );
                                    }

                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id.clone(),
                                        prompt,
                                        max_tokens,
                                        temperature,
                                    };

                                    let result = eng.run_inference(req, Some(token_tx)).await;

                                    InferenceTaskResult::success(
                                        request_id_clone,
                                        model_id,
                                        result.output,
                                        result.tokens_generated,
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

                                // Send final result
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
                                    let token = msg["token"].as_str().unwrap_or("");
                                    print!("{}", token);
                                    let _ = std::io::Write::flush(&mut std::io::stdout());
                                }
                            }

                            "InferenceResult" if matches!(mode, NodeMode::Infer { .. }) => {
                                let rid = msg["request_id"].as_str().unwrap_or("");
                                if infer_request_id.as_deref() == Some(rid) {
                                    let success = msg["success"].as_bool().unwrap_or(false);
                                    let tokens = msg["tokens_generated"].as_u64().unwrap_or(0);
                                    let ms = msg["execution_ms"].as_u64().unwrap_or(0);
                                    let worker = msg["worker_peer"].as_str().unwrap_or("unknown");
                                    let accel = msg["accelerator"].as_str().unwrap_or("unknown");

                                    println!("\n\n═══════════════════════════════════");
                                    if success {
                                        println!("✅ Distributed inference completada");
                                        println!("   Worker:  {}...", &worker[..12.min(worker.len())]);
                                        println!("   Tokens:  {}", tokens);
                                        println!("   Tiempo:  {}ms", ms);
                                        println!("   Accel:   {}", accel);
                                    } else {
                                        let err = msg["error"].as_str().unwrap_or("unknown");
                                        println!("❌ Inference falló: {}", err);
                                    }
                                    println!("═══════════════════════════════════");
                                    break;
                                }
                            }

                            "NodeCapabilities" | "CapabilitiesUpdated" => {
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
                                    let mut eng = engine_ref.lock().await;

                                    let hash = registry_clone.get(&model_id)
                                        .map(|m| m.hash.clone())
                                        .unwrap_or_default();

                                    if let Err(e) = eng.load_model(&model_id, &hash) {
                                        return InferenceTaskResult::failure(
                                            request_id_clone, model_id, peer_id_str, e,
                                        );
                                    }

                                    let req = RealInferenceRequest {
                                        task_id: request_id_clone.clone(),
                                        model_id: model_id.clone(),
                                        prompt,
                                        max_tokens,
                                        temperature: 0.7,
                                    };

                                    let result = eng.run_inference(req, Some(token_tx)).await;

                                    InferenceTaskResult::success(
                                        request_id_clone,
                                        model_id,
                                        result.output,
                                        result.tokens_generated,
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
