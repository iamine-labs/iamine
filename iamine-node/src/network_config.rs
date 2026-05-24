use crate::env_config::{env_u64_clamped, IAMINE_CLUSTER_STATUS_WAIT_MS};
use crate::node_modes::NodeMode;
use crate::pubsub_topic_tracker::PubsubTopicTracker;
use crate::pubsub_topics::BROADCAST_PUBSUB_TOPICS;
use crate::result_protocol::{TaskResultRequest, TaskResultResponse};
use crate::task_protocol::{TaskRequest, TaskResponse};
use crate::{
    emit_controller_pubsub_ready, emit_controller_topic_subscribed_event,
    emit_worker_pubsub_ready_event, emit_worker_topic_subscribed_event, log_observability_event,
};
use iamine_network::LogLevel;
use libp2p::{
    gossipsub, identify, kad, mdns, ping,
    request_response::{self, cbor, Event as RREvent, ProtocolSupport},
    swarm::NetworkBehaviour,
    Multiaddr, PeerId, StreamProtocol,
};
use serde_json::Map;
use std::time::Duration;

pub(crate) const CLUSTER_STATUS_WAIT_DEFAULT_MS: u64 = 6_500;
pub(crate) const CLUSTER_STATUS_WAIT_MIN_MS: u64 = 250;
pub(crate) const CLUSTER_STATUS_WAIT_MAX_MS: u64 = 30_000;
pub(crate) const GOSSIPSUB_HEARTBEAT_SECS: u64 = 1;
pub(crate) const KADEMLIA_QUERY_TIMEOUT_SECS: u64 = 30;
pub(crate) const SWARM_IDLE_CONNECTION_TIMEOUT_SECS: u64 = 60;
pub(crate) const RUNTIME_HEARTBEAT_SECS: u64 = 5;
pub(crate) const NODES_TICK_SECS: u64 = 5;
pub(crate) const BROADCAST_TICK_MS: u64 = 500;
pub(crate) const SIMULATED_WORKER_TICK_MS: u64 = 500;
pub(crate) const SIMULATED_WORKER_RUN_SECS: u64 = 60;
pub(crate) const RELAY_LISTEN_ADDR: &str = "/ip4/0.0.0.0/tcp/9999";
pub(crate) const EPHEMERAL_LISTEN_ADDR: &str = "/ip4/0.0.0.0/tcp/0";

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "IaMineEvent")]
pub(crate) struct IamineBehaviour {
    pub(crate) ping: ping::Behaviour,
    pub(crate) identify: identify::Behaviour,
    pub(crate) request_response: cbor::Behaviour<TaskRequest, TaskResponse>,
    pub(crate) result_response: cbor::Behaviour<TaskResultRequest, TaskResultResponse>,
    pub(crate) kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub(crate) mdns: mdns::tokio::Behaviour,
    pub(crate) gossipsub: gossipsub::Behaviour,
}

#[derive(Debug)]
pub(crate) enum IaMineEvent {
    Ping(ping::Event),
    #[allow(dead_code)]
    Identify(identify::Event),
    RequestResponse(RREvent<TaskRequest, TaskResponse>),
    ResultResponse(RREvent<TaskResultRequest, TaskResultResponse>),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeNetworkIntervals {
    pub(crate) heartbeat_secs: u64,
    pub(crate) nodes_tick_secs: u64,
    pub(crate) broadcast_tick_ms: u64,
}

impl Default for RuntimeNetworkIntervals {
    fn default() -> Self {
        Self {
            heartbeat_secs: RUNTIME_HEARTBEAT_SECS,
            nodes_tick_secs: NODES_TICK_SECS,
            broadcast_tick_ms: BROADCAST_TICK_MS,
        }
    }
}

impl RuntimeNetworkIntervals {
    pub(crate) fn nodes_tick_interval(&self) -> Duration {
        Duration::from_secs(self.nodes_tick_secs)
    }

    pub(crate) fn broadcast_tick_interval(&self) -> Duration {
        Duration::from_millis(self.broadcast_tick_ms)
    }
}

pub(crate) fn cluster_status_wait_ms_from_env() -> u64 {
    env_u64_clamped(
        IAMINE_CLUSTER_STATUS_WAIT_MS,
        CLUSTER_STATUS_WAIT_DEFAULT_MS,
        CLUSTER_STATUS_WAIT_MIN_MS,
        CLUSTER_STATUS_WAIT_MAX_MS,
    )
}

pub(crate) fn gossipsub_heartbeat_interval() -> Duration {
    Duration::from_secs(GOSSIPSUB_HEARTBEAT_SECS)
}

pub(crate) fn kademlia_query_timeout() -> Duration {
    Duration::from_secs(KADEMLIA_QUERY_TIMEOUT_SECS)
}

pub(crate) fn swarm_idle_connection_timeout() -> Duration {
    Duration::from_secs(SWARM_IDLE_CONNECTION_TIMEOUT_SECS)
}

pub(crate) fn simulated_worker_tick_interval() -> Duration {
    Duration::from_millis(SIMULATED_WORKER_TICK_MS)
}

pub(crate) fn simulated_worker_run_duration() -> Duration {
    Duration::from_secs(SIMULATED_WORKER_RUN_SECS)
}

pub(crate) fn listen_addr_for_mode(mode: &NodeMode, worker_port: u16) -> Result<Multiaddr, String> {
    let addr = if matches!(mode, NodeMode::Worker) {
        format!("/ip4/0.0.0.0/tcp/{}", worker_port)
    } else if matches!(mode, NodeMode::Relay) {
        RELAY_LISTEN_ADDR.to_string()
    } else {
        EPHEMERAL_LISTEN_ADDR.to_string()
    };
    addr.parse::<Multiaddr>().map_err(|error| error.to_string())
}

pub(crate) fn bootnode_addresses_from_args(args: &[String]) -> Vec<Multiaddr> {
    args.iter()
        .filter_map(|arg| arg.strip_prefix("--bootnode="))
        .filter_map(|addr| addr.parse::<Multiaddr>().ok())
        .collect()
}

pub(crate) fn build_gossipsub_behaviour(
    id_keys: &libp2p::identity::Keypair,
) -> Result<gossipsub::Behaviour, String> {
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(gossipsub_heartbeat_interval())
        .validation_mode(gossipsub::ValidationMode::Permissive)
        .build()
        .map_err(|e| format!("Gossipsub config error: {}", e))?;

    gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(id_keys.clone()),
        gossipsub_config,
    )
    .map_err(|e| format!("Gossipsub error: {}", e))
}

pub(crate) fn build_kademlia(peer_id: PeerId) -> kad::Behaviour<kad::store::MemoryStore> {
    let mut kad_cfg = kad::Config::default();
    kad_cfg.set_query_timeout(kademlia_query_timeout());
    kad::Behaviour::with_config(peer_id, kad::store::MemoryStore::new(peer_id), kad_cfg)
}

pub(crate) fn build_iamine_behaviour(
    id_keys: &libp2p::identity::Keypair,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    mdns_behaviour: mdns::tokio::Behaviour,
    gossipsub_behaviour: gossipsub::Behaviour,
) -> IamineBehaviour {
    IamineBehaviour {
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
    }
}

pub(crate) fn register_local_broadcast_pubsub_topics(
    gossipsub_behaviour: &mut gossipsub::Behaviour,
    mode: &NodeMode,
    peer_id: &PeerId,
    local_backend: &str,
) -> Result<PubsubTopicTracker, gossipsub::SubscriptionError> {
    for topic_name in BROADCAST_PUBSUB_TOPICS {
        gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(topic_name))?;
    }
    let mut pubsub_topics = PubsubTopicTracker::default();
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
        emit_controller_pubsub_ready(peer_id, &BROADCAST_PUBSUB_TOPICS);
    }
    Ok(pubsub_topics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_test_utils::with_env_var;
    use crate::env_config::IAMINE_CLUSTER_STATUS_WAIT_MS;

    #[test]
    fn cluster_status_wait_ms_default_preserved() {
        with_env_var(IAMINE_CLUSTER_STATUS_WAIT_MS, None, || {
            assert_eq!(
                cluster_status_wait_ms_from_env(),
                CLUSTER_STATUS_WAIT_DEFAULT_MS
            );
        });
    }

    #[test]
    fn cluster_status_wait_ms_env_clamped_preserved() {
        with_env_var(IAMINE_CLUSTER_STATUS_WAIT_MS, Some("10"), || {
            assert_eq!(
                cluster_status_wait_ms_from_env(),
                CLUSTER_STATUS_WAIT_MIN_MS
            );
        });
        with_env_var(IAMINE_CLUSTER_STATUS_WAIT_MS, Some("999999"), || {
            assert_eq!(
                cluster_status_wait_ms_from_env(),
                CLUSTER_STATUS_WAIT_MAX_MS
            );
        });
        with_env_var(IAMINE_CLUSTER_STATUS_WAIT_MS, Some("bad"), || {
            assert_eq!(
                cluster_status_wait_ms_from_env(),
                CLUSTER_STATUS_WAIT_DEFAULT_MS
            );
        });
    }

    #[test]
    fn listen_addr_defaults_preserved() {
        assert_eq!(
            listen_addr_for_mode(&NodeMode::Worker, 4101)
                .expect("worker addr")
                .to_string(),
            "/ip4/0.0.0.0/tcp/4101"
        );
        assert_eq!(
            listen_addr_for_mode(&NodeMode::Relay, 4101)
                .expect("relay addr")
                .to_string(),
            RELAY_LISTEN_ADDR
        );
        assert_eq!(
            listen_addr_for_mode(&NodeMode::Help, 4101)
                .expect("default addr")
                .to_string(),
            EPHEMERAL_LISTEN_ADDR
        );
    }

    #[test]
    fn bootnode_args_parse_valid_and_ignore_invalid() {
        let args = vec![
            "iamine-node".to_string(),
            "--bootnode=/ip4/127.0.0.1/tcp/9999".to_string(),
            "--bootnode=bad".to_string(),
        ];
        let addresses = bootnode_addresses_from_args(&args);
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].to_string(), "/ip4/127.0.0.1/tcp/9999");
    }

    #[test]
    fn broadcast_config_defaults_preserved() {
        let intervals = RuntimeNetworkIntervals::default();
        assert_eq!(intervals.heartbeat_secs, 5);
        assert_eq!(intervals.nodes_tick_interval(), Duration::from_secs(5));
        assert_eq!(
            intervals.broadcast_tick_interval(),
            Duration::from_millis(500)
        );
        assert_eq!(gossipsub_heartbeat_interval(), Duration::from_secs(1));
        assert_eq!(kademlia_query_timeout(), Duration::from_secs(30));
        assert_eq!(swarm_idle_connection_timeout(), Duration::from_secs(60));
    }
}
