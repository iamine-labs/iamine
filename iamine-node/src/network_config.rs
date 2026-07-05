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
use iamine_network::{
    bootnodes_from_args, nat_relay_policy_from_args, testnet_admission_policy_from_args,
    wan_peer_seeds_from_args, Bootnode, BootnodeArgError, LogLevel, NatRelayArgError,
    NatRelayPolicy, RelayPeerSeed, TestnetAdmissionArgError, TestnetAdmissionPolicy,
    WanPeerArgError, WanPeerSeed, IAMINE_IDENTIFY_PROTOCOL, IAMINE_RESULT_PROTOCOL,
    IAMINE_TASK_PROTOCOL,
};
use libp2p::{
    gossipsub, identify, kad, mdns, ping,
    request_response::{self, cbor, Event as RREvent, ProtocolSupport},
    swarm::{NetworkBehaviour, Swarm},
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

pub(crate) fn bootnodes_from_runtime_args(
    args: &[String],
) -> Result<Vec<Bootnode>, BootnodeArgError> {
    bootnodes_from_args(args)
}

pub(crate) fn wan_peers_from_runtime_args(
    args: &[String],
) -> Result<Vec<WanPeerSeed>, WanPeerArgError> {
    wan_peer_seeds_from_args(args)
}

pub(crate) fn nat_relay_policy_from_runtime_args(
    args: &[String],
) -> Result<NatRelayPolicy, NatRelayArgError> {
    nat_relay_policy_from_args(args)
}

pub(crate) fn testnet_admission_policy_from_runtime_args(
    args: &[String],
) -> Result<TestnetAdmissionPolicy, TestnetAdmissionArgError> {
    testnet_admission_policy_from_args(args)
}

pub(crate) fn admitted_bootnodes_for_testnet_policy(
    bootnodes: &[Bootnode],
    policy: &TestnetAdmissionPolicy,
) -> Result<Vec<Bootnode>, String> {
    if !policy.is_restricted() {
        return Ok(bootnodes.to_vec());
    }

    let mut admitted = Vec::with_capacity(bootnodes.len());
    for bootnode in bootnodes {
        ensure_optional_peer_admitted(
            bootnode.peer_id(),
            policy,
            "bootnode",
            "bootnode must end with /p2p/<peer_id> when testnet admission allowlist is enabled",
        )?;
        admitted.push(bootnode.clone());
    }

    Ok(admitted)
}

pub(crate) fn admitted_wan_peers_for_testnet_policy(
    peers: &[WanPeerSeed],
    policy: &TestnetAdmissionPolicy,
) -> Result<Vec<WanPeerSeed>, String> {
    if !policy.is_restricted() {
        return Ok(peers.to_vec());
    }

    let mut admitted = Vec::with_capacity(peers.len());
    for peer in peers {
        ensure_peer_admitted(peer.peer_id(), policy, "WAN peer")?;
        admitted.push(peer.clone());
    }

    Ok(admitted)
}

pub(crate) fn admitted_nat_relay_policy_for_testnet_policy(
    relay_policy: &NatRelayPolicy,
    admission_policy: &TestnetAdmissionPolicy,
) -> Result<NatRelayPolicy, String> {
    if !admission_policy.is_restricted() || !relay_policy.is_enabled() {
        return Ok(relay_policy.clone());
    }

    let mut admitted_peers = Vec::with_capacity(relay_policy.relay_peers().len());
    for peer in relay_policy.relay_peers() {
        ensure_peer_admitted(peer.peer_id(), admission_policy, "relay peer")?;
        admitted_peers.push(peer.clone());
    }

    Ok(NatRelayPolicy::operator_configured(admitted_peers))
}

fn ensure_optional_peer_admitted(
    peer_id: Option<PeerId>,
    policy: &TestnetAdmissionPolicy,
    role: &str,
    missing_identity_message: &str,
) -> Result<(), String> {
    let peer_id = peer_id.ok_or_else(|| missing_identity_message.to_string())?;
    ensure_peer_admitted(peer_id, policy, role)
}

fn ensure_peer_admitted(
    peer_id: PeerId,
    policy: &TestnetAdmissionPolicy,
    role: &str,
) -> Result<(), String> {
    if policy.allows_peer(&peer_id) {
        Ok(())
    } else {
        Err(format!(
            "{} is not authorized by the testnet admission allowlist",
            role
        ))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BootnodeDialSummary {
    pub(crate) dial_attempts: usize,
    pub(crate) routed_peers: usize,
}

pub(crate) fn dial_configured_bootnodes(
    swarm: &mut Swarm<IamineBehaviour>,
    bootnodes: &[Bootnode],
) -> Result<BootnodeDialSummary, String> {
    let mut summary = BootnodeDialSummary::default();

    for bootnode in bootnodes {
        if let Some(peer_id) = bootnode.peer_id() {
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(&peer_id, bootnode.routing_addr().clone());
            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            summary.routed_peers += 1;
        }

        swarm
            .dial(bootnode.dial_addr().clone())
            .map_err(|_| "bootnode dial setup failed".to_string())?;
        summary.dial_attempts += 1;
    }

    Ok(summary)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WanDiscoverySummary {
    pub(crate) dial_attempts: usize,
    pub(crate) routed_peers: usize,
    pub(crate) bootstrap_queries: usize,
}

pub(crate) fn start_wan_peer_discovery(
    swarm: &mut Swarm<IamineBehaviour>,
    peers: &[WanPeerSeed],
) -> Result<WanDiscoverySummary, String> {
    let mut summary = WanDiscoverySummary::default();

    for peer in peers {
        let peer_id = peer.peer_id();
        swarm
            .behaviour_mut()
            .kademlia
            .add_address(&peer_id, peer.routing_addr().clone());
        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
        summary.routed_peers += 1;

        swarm
            .dial(peer.dial_addr().clone())
            .map_err(|_| "WAN peer discovery dial setup failed".to_string())?;
        summary.dial_attempts += 1;
    }

    if !peers.is_empty() {
        swarm
            .behaviour_mut()
            .kademlia
            .bootstrap()
            .map_err(|_| "WAN peer discovery bootstrap setup failed".to_string())?;
        summary.bootstrap_queries = 1;
    }

    Ok(summary)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct NatRelayStartupSummary {
    pub(crate) policy_enabled: bool,
    pub(crate) dial_attempts: usize,
    pub(crate) routed_peers: usize,
}

pub(crate) fn start_nat_relay_policy(
    swarm: &mut Swarm<IamineBehaviour>,
    policy: &NatRelayPolicy,
) -> Result<NatRelayStartupSummary, String> {
    let mut summary = NatRelayStartupSummary {
        policy_enabled: policy.is_enabled(),
        ..NatRelayStartupSummary::default()
    };

    if !policy.is_enabled() {
        return Ok(summary);
    }

    register_relay_peers(swarm, policy.relay_peers(), &mut summary)?;
    Ok(summary)
}

fn register_relay_peers(
    swarm: &mut Swarm<IamineBehaviour>,
    peers: &[RelayPeerSeed],
    summary: &mut NatRelayStartupSummary,
) -> Result<(), String> {
    for peer in peers {
        let peer_id = peer.peer_id();
        swarm
            .behaviour_mut()
            .kademlia
            .add_address(&peer_id, peer.routing_addr().clone());
        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
        summary.routed_peers += 1;

        swarm
            .dial(peer.dial_addr().clone())
            .map_err(|_| "NAT relay peer dial setup failed".to_string())?;
        summary.dial_attempts += 1;
    }

    Ok(())
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
            IAMINE_IDENTIFY_PROTOCOL.to_string(),
            id_keys.public(),
        )),
        request_response: cbor::Behaviour::<TaskRequest, TaskResponse>::new(
            [(
                StreamProtocol::new(IAMINE_TASK_PROTOCOL),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        ),
        result_response: cbor::Behaviour::<TaskResultRequest, TaskResultResponse>::new(
            [(
                StreamProtocol::new(IAMINE_RESULT_PROTOCOL),
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
    fn bootnode_args_parse_repeated_forms() {
        let args = vec![
            "iamine-node".to_string(),
            "--bootnode=/ip4/127.0.0.1/tcp/9999".to_string(),
            "--bootnode".to_string(),
            "/ip4/127.0.0.1/tcp/9001".to_string(),
        ];
        let result = bootnodes_from_runtime_args(&args);
        assert!(result.is_ok(), "bootnodes should parse: {result:?}");
        let bootnodes: Vec<Bootnode> = result.ok().into_iter().flatten().collect();
        assert_eq!(bootnodes.len(), 2);
        assert_eq!(
            bootnodes[0].dial_addr().to_string(),
            "/ip4/127.0.0.1/tcp/9999"
        );
        assert_eq!(
            bootnodes[1].dial_addr().to_string(),
            "/ip4/127.0.0.1/tcp/9001"
        );
    }

    #[test]
    fn bootnode_args_reject_invalid_values() {
        let args = vec!["iamine-node".to_string(), "--bootnode=bad".to_string()];

        assert!(bootnodes_from_runtime_args(&args).is_err());
    }

    #[test]
    fn wan_peer_args_require_peer_qualified_addresses() {
        let args = vec![
            "iamine-node".to_string(),
            "--wan-peer=/ip4/127.0.0.1/tcp/9001".to_string(),
        ];

        assert!(wan_peers_from_runtime_args(&args).is_err());
    }

    #[test]
    fn relay_policy_args_require_explicit_operator_mode() {
        let relay = PeerId::random();
        let args = vec![
            "iamine-node".to_string(),
            format!("--relay-peer=/ip4/127.0.0.1/tcp/9101/p2p/{relay}"),
        ];

        assert!(nat_relay_policy_from_runtime_args(&args).is_err());
    }

    #[test]
    fn relay_policy_args_parse_operator_mode() {
        let relay = PeerId::random();
        let args = vec![
            "iamine-node".to_string(),
            "--relay-policy=operator-configured".to_string(),
            format!("--relay-peer=/ip4/127.0.0.1/tcp/9101/p2p/{relay}"),
        ];

        let result = nat_relay_policy_from_runtime_args(&args);
        let Ok(policy) = result else {
            assert!(result.is_ok(), "relay policy should parse");
            return;
        };

        assert!(policy.is_enabled());
        assert_eq!(policy.relay_peers().len(), 1);
    }

    #[test]
    fn testnet_admission_args_default_to_open_policy() {
        let result = testnet_admission_policy_from_runtime_args(&["iamine-node".to_string()]);
        let Ok(policy) = result else {
            assert!(result.is_ok(), "default admission policy should parse");
            return;
        };

        assert!(!policy.is_restricted());
        assert!(policy.allows_peer(&PeerId::random()));
    }

    #[test]
    fn testnet_admission_filter_accepts_allowed_bootnode() {
        let peer = PeerId::random();
        let parsed = Bootnode::parse(&format!("/ip4/127.0.0.1/tcp/9001/p2p/{peer}"));
        let Ok(bootnode) = parsed else {
            assert!(parsed.is_ok(), "bootnode should parse");
            return;
        };
        let policy = TestnetAdmissionPolicy::allowlist(vec![peer]);

        let result = admitted_bootnodes_for_testnet_policy(&[bootnode], &policy);
        let Ok(admitted) = result else {
            assert!(result.is_ok(), "bootnode should be admitted");
            return;
        };

        assert_eq!(admitted.len(), 1);
    }

    #[test]
    fn testnet_admission_filter_rejects_unqualified_bootnode() {
        let parsed = Bootnode::parse("/ip4/127.0.0.1/tcp/9001");
        let Ok(bootnode) = parsed else {
            assert!(parsed.is_ok(), "bootnode should parse");
            return;
        };
        let policy = TestnetAdmissionPolicy::allowlist(vec![PeerId::random()]);

        let result = admitted_bootnodes_for_testnet_policy(&[bootnode], &policy);

        assert!(result.is_err());
    }

    #[test]
    fn testnet_admission_filter_rejects_unauthorized_wan_peer() {
        let allowed = PeerId::random();
        let denied = PeerId::random();
        let parsed = WanPeerSeed::parse(&format!("/ip4/127.0.0.1/tcp/9001/p2p/{denied}"));
        let Ok(seed) = parsed else {
            assert!(parsed.is_ok(), "WAN peer should parse");
            return;
        };
        let policy = TestnetAdmissionPolicy::allowlist(vec![allowed]);

        let result = admitted_wan_peers_for_testnet_policy(&[seed], &policy);

        assert!(result.is_err());
    }

    #[test]
    fn testnet_admission_filter_preserves_allowed_relay_policy() {
        let relay = PeerId::random();
        let parsed = RelayPeerSeed::parse(&format!("/ip4/127.0.0.1/tcp/9101/p2p/{relay}"));
        let Ok(peer) = parsed else {
            assert!(parsed.is_ok(), "relay peer should parse");
            return;
        };
        let relay_policy = NatRelayPolicy::operator_configured(vec![peer]);
        let admission_policy = TestnetAdmissionPolicy::allowlist(vec![relay]);

        let result = admitted_nat_relay_policy_for_testnet_policy(&relay_policy, &admission_policy);
        let Ok(admitted) = result else {
            assert!(result.is_ok(), "relay policy should be admitted");
            return;
        };

        assert!(admitted.is_enabled());
        assert_eq!(admitted.relay_peers().len(), 1);
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
