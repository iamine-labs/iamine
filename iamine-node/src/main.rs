mod protocol;
mod network;
mod executor;
mod task_protocol;
mod task_codec;

use libp2p::{
    identify, identity, noise, ping,
    request_response::{self, cbor, Event as RREvent, Message, ProtocolSupport},
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux,
    Multiaddr, PeerId, StreamProtocol, Transport,
};
use futures::StreamExt;
use std::error::Error;
use std::str::FromStr;

use task_protocol::{TaskRequest, TaskResponse};
use executor::TaskExecutor;

// ✅ cbor::Behaviour<Req, Resp> ES el behaviour completo, no necesita wrapper
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "IaMineEvent")]
struct IamineBehaviour {
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    request_response: cbor::Behaviour<TaskRequest, TaskResponse>,
}

#[derive(Debug)]
enum IaMineEvent {
    Ping(ping::Event),
    #[allow(dead_code)]
    Identify(identify::Event),
    RequestResponse(RREvent<TaskRequest, TaskResponse>),
}

impl From<ping::Event> for IaMineEvent {
    fn from(e: ping::Event) -> Self { IaMineEvent::Ping(e) }
}

impl From<identify::Event> for IaMineEvent {
    fn from(e: identify::Event) -> Self { IaMineEvent::Identify(e) }
}

impl From<RREvent<TaskRequest, TaskResponse>> for IaMineEvent {
    fn from(e: RREvent<TaskRequest, TaskResponse>) -> Self {
        IaMineEvent::RequestResponse(e)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());

    println!("🔥 IaMine Node iniciado");
    println!("   Peer ID: {}", peer_id);

    let transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys)?)
        .multiplex(yamux::Config::default())
        .boxed();

    let ping_behaviour = ping::Behaviour::default();
    let identify_behaviour = identify::Behaviour::new(identify::Config::new(
        "/iamine/1.0".to_string(),
        id_keys.public(),
    ));

    // ✅ cbor::Behaviour<Req, Resp>::new es el constructor correcto
    let request_response = cbor::Behaviour::<TaskRequest, TaskResponse>::new(
        [(StreamProtocol::new("/iamine/task/1.0"), ProtocolSupport::Full)],
        request_response::Config::default(),
    );

    let behaviour = IamineBehaviour {
        ping: ping_behaviour,
        identify: identify_behaviour,
        request_response,
    };

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(std::time::Duration::from_secs(30)), // ← mantener conexión abierta
    );

    let args: Vec<String> = std::env::args().collect();
    let listen_addr: Multiaddr = if args.len() > 1 {
        "/ip4/0.0.0.0/tcp/0".parse()?
    } else {
        "/ip4/0.0.0.0/tcp/9000".parse()?
    };
    swarm.listen_on(listen_addr)?;

    let mut pending_task: Option<(String, String)> = None;

    if args.len() > 1 {
        if let Ok(multiaddr) = Multiaddr::from_str(&args[1]) {
            println!("📡 Conectando a: {}", multiaddr);
            swarm.dial(multiaddr)?;
        }
    }
    if args.len() > 3 {
        pending_task = Some((args[2].clone(), args[3].clone()));
        println!("📋 Tarea pendiente: {} con datos '{}'", args[2], args[3]);
    }

    println!("✅ Nodo listo. Esperando conexiones...\n");

    let is_client = args.len() > 1;
    let mut waiting_for_response = false;

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("🌐 Escuchando en: {}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id: connected_peer, endpoint, .. } => {
                println!("✅ Conectado a: {} ({})", connected_peer, endpoint.get_remote_address());
                if let Some((task_type, data)) = pending_task.take() {
                    let request = TaskRequest { task_id: uuid_simple(), task_type, data };
                    println!("📤 Enviando tarea: {:?}", request);
                    swarm.behaviour_mut().request_response.send_request(&connected_peer, request);
                    waiting_for_response = true;
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                println!("❌ Desconectado de: {}", peer_id);
                // Solo salir si NO estamos esperando respuesta
                if is_client && !waiting_for_response {
                    break;
                }
            }
            SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
                if let Ok(d) = result {
                    println!("🏓 Ping de {}: {:?}", peer, d);
                }
            }
            SwarmEvent::Behaviour(IaMineEvent::RequestResponse(event)) => {
                match event {
                    RREvent::Message { peer, message: Message::Request { request, channel, .. } } => {
                        println!("📨 Tarea recibida de {}: {} ({})", peer, request.task_id, request.task_type);
                        let response = TaskExecutor::execute_task(request.task_id, request.task_type, request.data);
                        match swarm.behaviour_mut().request_response.send_response(channel, response) {
                            Ok(_) => println!("📤 Respuesta enviada al cliente"),
                            Err(e) => eprintln!("❌ Error enviando respuesta: {:?}", e),
                        }
                    }
                    RREvent::Message { peer, message: Message::Response { response, .. } } => {
                        #[allow(unused_assignments)]
                        { waiting_for_response = false; }
                        if response.success {
                            println!("📩 ✅ Resultado de {}: '{}'", peer, response.result);
                        } else {
                            println!("📩 ❌ Error de {}: '{}'", peer, response.result);
                        }
                        println!("✅ Tarea completada. Saliendo...");
                        break;
                    }
                    RREvent::OutboundFailure { peer, error, .. } => {
                        if waiting_for_response {
                            // Ignorar — la respuesta puede llegar aún en otro evento
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
