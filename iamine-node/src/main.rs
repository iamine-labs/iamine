mod protocol;
mod network;
mod executor;
mod task_protocol;
mod task_codec;
mod worker_pool;
mod task_queue;  // ← nuevo

use worker_pool::WorkerPool;
use task_queue::TaskQueue;
use std::sync::Arc;

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

    // Suscribirse al topic de tareas
    let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC);
    gossipsub.subscribe(&task_topic)?;

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

    println!("   Slots paralelos: {}", pool.max_concurrent);
    println!("   Task queue: activa (max 3 reintentos, timeout por tipo)\n");

    let listen_addr: Multiaddr = match &mode {
        NodeMode::Worker => "/ip4/0.0.0.0/tcp/9000".parse()?,
        _ => "/ip4/0.0.0.0/tcp/0".parse()?,
    };
    swarm.listen_on(listen_addr)?;

    // Conectar a peer explícito si se indicó
    match &mode {
        NodeMode::Client { peer: Some(p), .. } | NodeMode::Stress { peer: Some(p), .. } => {
            println!("📡 Conectando a: {}", p);
            swarm.dial(p.clone())?;
        }
        _ => {}
    };

    match &mode {
        NodeMode::Worker => println!("✅ Worker listo en puerto 9000. Escuchando gossipsub...\n"),
        NodeMode::Broadcast { .. } => println!("📢 Modo broadcast — esperando peers via mDNS...\n"),
        _ => {}
    }

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
        match swarm.select_next_some().await {
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
                        // ⚠️ NO publicar aquí — gossipsub aún no está listo
                    }
                }
            }

            SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Expired(peers))) => {
                for (pid, _) in peers {
                    known_workers.remove(&pid);
                    swarm.behaviour_mut().gossipsub.remove_explicit_peer(&pid);
                }
            }

            // 📢 WORKER: recibe tarea via gossipsub y la ejecuta
            SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source,
                message,
                ..
            })) => {
                if let Ok(task) = serde_json::from_slice::<TaskRequest>(&message.data) {
                    println!("📢 Tarea broadcast recibida de {}: [{}] {} → '{}'",
                        propagation_source, task.task_id, task.task_type, task.data);

                    if matches!(mode, NodeMode::Worker) {
                        let response = TaskExecutor::execute_task(
                            task.task_id.clone(), task.task_type, task.data,
                        );
                        println!("✅ [Broadcast] Tarea {} completada: {}", task.task_id, response.result);

                        if let Ok(result_msg) = serde_json::to_vec(&response) {
                            let result_topic = gossipsub::IdentTopic::new("iamine-results");
                            let _ = swarm.behaviour_mut().gossipsub.publish(result_topic, result_msg);
                        }
                    }
                }
            }

            // ✅ Gossipsub listo cuando un peer se suscribe al mismo topic
            SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id: pid, topic })) => {
                println!("📢 Peer {} suscrito a topic: {}", pid, topic);

                // Ahora sí publicar el broadcast
                if matches!(mode, NodeMode::Broadcast { .. }) && !broadcast_sent {
                    if let NodeMode::Broadcast { task_type, data } = &mode {
                        let task = TaskRequest {
                            task_id: uuid_simple(),
                            task_type: task_type.clone(),
                            data: data.clone(),
                        };
                        let msg = serde_json::to_vec(&task)?;
                        match swarm.behaviour_mut().gossipsub.publish(task_topic.clone(), msg) {
                            Ok(_) => {
                                println!("📢 ✅ Tarea broadcast enviada: {} → '{}'", task_type, data);
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
                if matches!(mode, NodeMode::Broadcast { .. }) && broadcast_sent {
                    // Broadcast ya enviado, podemos salir
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

                        // ✅ Encolar con retries y timeout
                        let queue_ref = Arc::clone(&queue);
                        let t_id = request.task_id.clone();
                        let t_type = request.task_type.clone();
                        let t_data = request.data.clone();

                        tokio::spawn(async move {
                            if let Err(e) = queue_ref.push(t_id, t_type, t_data).await {
                                eprintln!("❌ Error encolando tarea: {}", e);
                            }
                            // Leer outcome
                            if let Some(outcome) = queue_ref.outcome_rx.lock().await.recv().await {
                                match outcome.status {
                                    task_queue::OutcomeStatus::Success =>
                                        println!("🏁 [Queue] {} completada en {} intentos",
                                            outcome.task_id, outcome.attempts),
                                    task_queue::OutcomeStatus::MaxRetriesExceeded =>
                                        println!("💀 [Queue] {} falló tras {} intentos",
                                            outcome.task_id, outcome.attempts),
                                    task_queue::OutcomeStatus::TimedOut =>
                                        println!("⏱️ [Queue] {} timeout tras {} intentos",
                                            outcome.task_id, outcome.attempts),
                                    task_queue::OutcomeStatus::Failed =>
                                        println!("❌ [Queue] {} error crítico",
                                            outcome.task_id),
                                }
                                // Mostrar reputación actualizada
                                let rep = queue_ref.reputation().await;
                                println!("⭐ Reputación: {}/100 | Éxito: {:.1}%",
                                    rep.reputation_score,
                                    rep.success_rate() * 100.0);
                            }
                        });

                        // Respuesta inmediata P2P (no bloqueante)
                        let response = TaskExecutor::execute_task(
                            request.task_id, request.task_type, request.data,
                        );
                        match swarm.behaviour_mut().request_response.send_response(channel, response) {
                            Ok(_) => println!("📤 Respuesta enviada"),
                            Err(e) => eprintln!("❌ Error enviando respuesta: {:?}", e),
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
                        if waiting_for_response {
                            println!("⚠️ Substream cerrado, esperando respuesta de {}...", peer);
                        } else {
                            eprintln!("❌ Error outbound con {}: {:?}", peer, error);
                            if is_client { break; }
                        }
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

    Ok(())
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{}{}", t.as_secs(), t.subsec_nanos())
}
