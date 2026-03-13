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

use worker_pool::WorkerPool;
use task_queue::TaskQueue;
use task_scheduler::TaskScheduler;
use heartbeat::HeartbeatService;
use metrics::{NodeMetrics, start_metrics_server};
use std::sync::Arc;
use tokio::sync::RwLock;

use libp2p::{
    gossipsub, identify, identity, kad, mdns, noise, ping,
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

// Topic de gossipsub para broadcast de tareas
const TASK_TOPIC: &str = "iamine-tasks";

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "IaMineEvent")]
struct IamineBehaviour {
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    request_response: cbor::Behaviour<TaskRequest, TaskResponse>,
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
    Client { peer: Option<Multiaddr>, task_type: String, data: String },
    Stress { peer: Option<Multiaddr>, count: usize },
    /// --broadcast <task_type> <data>: anuncia tarea a toda la red
    Broadcast { task_type: String, data: String },
}

fn parse_args() -> Result<NodeMode, String> {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("--worker") | None => Ok(NodeMode::Worker),

        Some("--client") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?), 3)
            } else {
                (None, 2)
            };
            let task_type = args.get(offset).ok_or("Falta <task_type>")?.clone();
            let data = args.get(offset + 1).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Client { peer, task_type, data })
        }

        Some("--stress") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?), 3)
            } else {
                (None, 2)
            };
            let count = args.get(offset).unwrap_or(&"10".to_string()).parse::<usize>().unwrap_or(10);
            Ok(NodeMode::Stress { peer, count })
        }

        Some("--broadcast") => {
            let task_type = args.get(2).ok_or("Falta <task_type>")?.clone();
            let data = args.get(3).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Broadcast { task_type, data })
        }

        Some(unknown) => Err(format!("Modo desconocido: {}", unknown)),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mode = match parse_args() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("❌ {}", e);
            eprintln!("Uso:");
            eprintln!("  iamine-node --worker");
            eprintln!("  iamine-node --client [/ip4/...] reverse_string iamine");
            eprintln!("  iamine-node --stress [/ip4/...] 10");
            eprintln!("  iamine-node --broadcast reverse_string iamine");
            std::process::exit(1);
        }
    };

    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());

    println!("🔥 IaMine Node iniciado");
    println!("   Peer ID: {}", peer_id);
    println!("   Modo: {:?}\n", mode);

    let transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys)?)
        .multiplex(yamux::Config::default())
        .boxed();

    // Gossipsub
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(1))
        .validation_mode(gossipsub::ValidationMode::Permissive)
        .build()
        .map_err(|e| format!("Gossipsub config error: {}", e))?;

    let mut gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(id_keys.clone()),
        gossipsub_config,
    ).map_err(|e| format!("Gossipsub error: {}", e))?;

    // Suscribirse al topic de tareas Y bids Y assign
    let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC);
    let bid_topic = gossipsub::IdentTopic::new("iamine-bids");
    let assign_topic = gossipsub::IdentTopic::new("iamine-assign");
    gossipsub.subscribe(&task_topic)?;
    gossipsub.subscribe(&bid_topic)?;
    gossipsub.subscribe(&assign_topic)?;
    gossipsub.subscribe(&gossipsub::IdentTopic::new("iamine-heartbeat"))?; // ← nuevo
    gossipsub.subscribe(&gossipsub::IdentTopic::new("iamine-results"))?;   // ← nuevo

    // Kademlia
    let mut kad_cfg = kad::Config::default();
    kad_cfg.set_query_timeout(Duration::from_secs(30));
    let kademlia = kad::Behaviour::with_config(
        peer_id,
        kad::store::MemoryStore::new(peer_id),
        kad_cfg,
    );

    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;

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
        kademlia,
        mdns,
        gossipsub,
    };

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(60)),
    );

    // ✅ Crear worker pool ANTES del loop
    let pool = Arc::new(WorkerPool::auto());
    // ✅ Task queue con retries + timeouts
    let queue = Arc::new(TaskQueue::new(peer_id.to_string()));
    let scheduler = Arc::new(TaskScheduler::new());
    let heartbeat = Arc::new(HeartbeatService::new());
    let metrics = Arc::new(RwLock::new(NodeMetrics::new()));

    println!("   Slots paralelos: {}", pool.max_concurrent);
    println!("   Task queue: activa (max 3 reintentos)");
    println!("   Scheduler: activo (bid window 1.5s)\n");

    // Bootnodes desde CLI --bootnode=/ip4/.../tcp/.../p2p/...
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

    // Puerto del worker (default 9000)
    let worker_port: u16 = {
        let args: Vec<String> = std::env::args().collect();
        if let Some(port_arg) = args.iter().find(|a| a.starts_with("--port=")) {
            port_arg.replace("--port=", "").parse().unwrap_or(9000)
        } else {
            9000
        }
    };

    let metrics_port = 9090 + (worker_port - 9000);

    // Iniciar metrics server — puerto único por worker
    if matches!(mode, NodeMode::Worker) {
        let m = Arc::clone(&metrics);
        let port = metrics_port;
        tokio::spawn(async move {
            start_metrics_server(m, port).await;
        });
        // ← un solo println!, fuera del spawn
        println!("📊 Metrics en http://localhost:{}/metrics", metrics_port);
    }

    // Heartbeat cada 5s
    let mut heartbeat_rx = HeartbeatService::start(5);

    // Estado
    let mut pending_tasks: Vec<TaskRequest> = Vec::new();
    let mut waiting_for_response = false;
    let mut completed = 0usize;
    let mut total_tasks = 0usize;
    let mut connected_peer: Option<PeerId> = None;
    let mut known_workers: HashSet<PeerId> = HashSet::new();
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

    let is_client = !matches!(mode, NodeMode::Worker);

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
                    }
                    let hb = serde_json::json!({
                        "type": "Heartbeat",
                        "peer_id": peer_id.to_string(),
                        "available_slots": slots,
                        "reputation_score": rep.reputation_score,
                        "uptime_secs": uptime,
                        "avg_latency_ms": rep.avg_execution_ms,
                    });
                    let hb_topic = gossipsub::IdentTopic::new("iamine-heartbeat");
                    let _ = swarm.behaviour_mut().gossipsub.publish(
                        hb_topic, serde_json::to_vec(&hb).unwrap(),
                    );
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
                                println!("📡 Conectando via mDNS a: {}", addr);
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
                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&message.data) {
                        let msg_type = msg["type"].as_str().unwrap_or("");
                        match msg_type {
                            "TaskOffer" if matches!(mode, NodeMode::Worker) => {
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let origin_peer = msg["origin_peer"].as_str().unwrap_or("").to_string();
                                let available = pool.available_slots();
                                if available > 0 {
                                    println!("📋 [Worker] Bid para tarea {} ({} slots libres)", task_id, available);
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
                                } else {
                                    println!("⏳ [Worker] Pool lleno, ignorando tarea {}", task_id);
                                }
                            }

                            "TaskBid" if matches!(mode, NodeMode::Broadcast { .. }) => {
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let worker_id = msg["worker_id"].as_str().unwrap_or("").to_string();
                                let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
                                let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
                                let est_ms = msg["estimated_ms"].as_u64().unwrap_or(1000);
                                println!("📨 [Scheduler] Bid: worker={}... rep={} slots={}",
                                    &worker_id[..8.min(worker_id.len())], rep, slots);
                                if let Some(winner) = scheduler.receive_bid(&task_id, worker_id, rep, slots, est_ms).await {
                                    println!("🏆 Asignando {} a {}...", task_id, &winner[..8.min(winner.len())]);
                                    let assign = serde_json::json!({
                                        "type": "TaskAssign",
                                        "task_id": task_id,
                                        "assigned_worker": winner,
                                        "origin_peer": peer_id.to_string(),
                                        "task_type": if let NodeMode::Broadcast { task_type, .. } = &mode { task_type } else { "" },
                                        "data": if let NodeMode::Broadcast { data, .. } = &mode { data } else { "" },
                                    });
                                    let _ = swarm.behaviour_mut().gossipsub.publish(
                                        gossipsub::IdentTopic::new("iamine-assign"),
                                        serde_json::to_vec(&assign).unwrap(),
                                    );
                                }
                            }

                            "TaskAssign" if matches!(mode, NodeMode::Worker) => {
                                let assigned = msg["assigned_worker"].as_str().unwrap_or("");
                                let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
                                let task_type = msg["task_type"].as_str().unwrap_or("").to_string();
                                let data = msg["data"].as_str().unwrap_or("").to_string();
                                let origin_peer = msg["origin_peer"].as_str().unwrap_or("").to_string();

                                if assigned == peer_id.to_string() {
                                    println!("🎯 [Worker] ¡Asignado! Ejecutando tarea {}...", task_id);
                                    let queue_ref = Arc::clone(&queue);
                                    let metrics_ref = Arc::clone(&metrics);

                                    tokio::spawn(async move {
                                        let _ = queue_ref.push(task_id.clone(), task_type, data).await;
                                        if let Some(outcome) = queue_ref.outcome_rx.lock().await.recv().await {
                                            {
                                                let mut m = metrics_ref.write().await;
                                                match outcome.status {
                                                    task_queue::OutcomeStatus::Success => m.task_success(0),
                                                    task_queue::OutcomeStatus::TimedOut => m.task_timed_out(),
                                                    _ => m.task_failed(),
                                                }
                                            }
                                            println!("📤 [Worker] Resultado listo para origin {}...",
                                                &origin_peer[..8.min(origin_peer.len())]);
                                            let rep = queue_ref.reputation().await;
                                            println!("⭐ Reputación: {}/100 | Éxito: {:.1}%",
                                                rep.reputation_score, rep.success_rate() * 100.0);
                                        }
                                    });
                                } else {
                                    println!("⏭️  [Worker] Tarea {} → otro worker ganó", task_id);
                                }
                            }

                            "TaskResult" if matches!(mode, NodeMode::Broadcast { .. }) => {
                                let task_id = msg["task_id"].as_str().unwrap_or("");
                                let success = msg["success"].as_bool().unwrap_or(false);
                                let origin = msg["origin_peer"].as_str().unwrap_or("");
                                if origin == peer_id.to_string() {
                                    if success {
                                        println!("🎉 [Broadcaster] Tarea {} completada!", task_id);
                                    } else {
                                        println!("❌ [Broadcaster] Tarea {} falló", task_id);
                                    }
                                }
                            }

                            "Heartbeat" => {
                                let worker_id = msg["peer_id"].as_str().unwrap_or("");
                                let slots = msg["available_slots"].as_u64().unwrap_or(0);
                                let rep = msg["reputation_score"].as_u64().unwrap_or(0);
                                let uptime = msg["uptime_secs"].as_u64().unwrap_or(0);
                                println!("💓 Heartbeat {}... slots={} rep={} uptime={}s",
                                    &worker_id[..8.min(worker_id.len())], slots, rep, uptime);
                                metrics.write().await.network_peers = known_workers.len();
                            }

                            _ => {}
                        }
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id: pid, topic })) => {
                    println!("📢 Peer {} suscrito a topic: {}", pid, topic);
                    if topic.as_str() == TASK_TOPIC && matches!(mode, NodeMode::Broadcast { .. }) {
                        subscribed_peers += 1;
                        println!("👥 Workers suscritos: {}", subscribed_peers);
                    }
                    if matches!(mode, NodeMode::Broadcast { .. }) && !broadcast_sent && subscribed_peers >= 1 {
                        if let NodeMode::Broadcast { task_type, data } = &mode {
                            let task_id = uuid_simple();
                            scheduler.register_task(task_id.clone(), task_type.clone()).await;
                            let offer = serde_json::json!({
                                "type": "TaskOffer",
                                "task_id": task_id,
                                "task_type": task_type,
                                "data": data,
                                "requester_id": peer_id.to_string(),
                                "origin_peer": peer_id.to_string(),
                            });
                            match swarm.behaviour_mut().gossipsub.publish(
                                task_topic.clone(),
                                serde_json::to_vec(&offer).unwrap(),
                            ) {
                                Ok(_) => {
                                    println!("📢 ✅ TaskOffer enviado [id: {}]", task_id);
                                    broadcast_sent = true;
                                }
                                Err(e) => {
                                    broadcast_attempts += 1;
                                    eprintln!("❌ Error broadcast (intento {}): {:?}", broadcast_attempts, e);
                                }
                            }
                        }
                    }
                }

                SwarmEvent::ConnectionEstablished { peer_id: pid, endpoint, .. } => {
                    println!("✅ Conectado a: {} ({})", pid, endpoint.get_remote_address());
                    connected_peer = Some(pid);
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

                SwarmEvent::ConnectionClosed { peer_id: _, .. } => {
                    if is_client && !waiting_for_response && !matches!(mode, NodeMode::Broadcast { .. }) {
                        println!("\n📊 Completado: {}/{} tareas", completed, total_tasks);
                        break;
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
                    if let Ok(d) = result {
                        println!("🏓 Ping {}: {:?}", peer, d);
                    }
                }

                SwarmEvent::Behaviour(IaMineEvent::Kademlia(kad::Event::RoutingUpdated { peer, .. })) => {
                    println!("📡 Kademlia: nodo añadido {}", peer);
                }

                SwarmEvent::Behaviour(IaMineEvent::RequestResponse(event)) => {
                    match event {
                        RREvent::Message { peer, message: Message::Request { request, channel, .. } } => {
                            println!("📨 Tarea recibida de {}: [{}] {} → '{}'",
                                peer, request.task_id, request.task_type, request.data);
                            let queue_ref = Arc::clone(&queue);
                            let t_id = request.task_id.clone();
                            let t_type = request.task_type.clone();
                            let t_data = request.data.clone();
                            tokio::spawn(async move {
                                if let Err(e) = queue_ref.push(t_id, t_type, t_data).await {
                                    eprintln!("❌ Error encolando: {}", e);
                                }
                                if let Some(outcome) = queue_ref.outcome_rx.lock().await.recv().await {
                                    println!("🏁 [Queue] {} → {:?} en {} intentos",
                                        outcome.task_id, outcome.status, outcome.attempts);
                                    let rep = queue_ref.reputation().await;
                                    println!("⭐ Reputación: {}/100", rep.reputation_score);
                                }
                            });
                            let response = TaskExecutor::execute_task(
                                request.task_id, request.task_type, request.data,
                            );
                            match swarm.behaviour_mut().request_response.send_response(channel, response) {
                                Ok(_) => println!("📤 Respuesta enviada"),
                                Err(e) => eprintln!("❌ Error: {:?}", e),
                            }
                        }
                        RREvent::Message { peer, message: Message::Response { response, .. } } => {
                            waiting_for_response = false;
                            completed += 1;
                            if !pending_tasks.is_empty() { pending_tasks.remove(0); }
                            if response.success {
                                println!("📩 ✅ [{}/{}] Resultado de {}: '{}'",
                                    completed, total_tasks, peer, response.result);
                            } else {
                                println!("📩 ❌ [{}/{}] Error: '{}'", completed, total_tasks, response.result);
                            }
                            if let Some(next_task) = pending_tasks.first().cloned() {
                                if let Some(pid) = connected_peer {
                                    println!("📤 [{}/{}] Enviando: {} → '{}'",
                                        completed + 1, total_tasks, next_task.task_type, next_task.data);
                                    swarm.behaviour_mut().request_response.send_request(&pid, next_task);
                                    waiting_for_response = true;
                                }
                            } else {
                                println!("\n🎉 Todas las tareas completadas: {}/{}", completed, total_tasks);
                                break;
                            }
                        }
                        RREvent::OutboundFailure { peer, error, .. } => {
                            eprintln!("❌ Error outbound con {}: {:?}", peer, error);
                            if is_client && !waiting_for_response { break; }
                        }
                        RREvent::InboundFailure { peer, error, .. } => {
                            eprintln!("❌ Error inbound con {}: {:?}", peer, error);
                        }
                        RREvent::ResponseSent { peer, .. } => {
                            println!("✅ Respuesta entregada a {}", peer);
                        }
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
