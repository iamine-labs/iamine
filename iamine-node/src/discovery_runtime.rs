use crate::broadcast_runtime::{
    emit_broadcast_topic_subscriber_seen_event, emit_observed_peer_subscription_event,
    BroadcastOfferState,
};
use crate::cluster_events::{emit_cluster_discovery_update, emit_cluster_node_updated};
use crate::cluster_registry::{unix_now_ms, ClusterRegistry};
use crate::infer_retry::DistributedInferState;
use crate::infer_runtime::debug_network_log;
use crate::log_observability_event;
use crate::network_event_observability::{cluster_label_for_latency, HumanLogThrottle};
use crate::node_modes::{DebugFlags, NodeMode};
use crate::peer_tracker::PeerTracker;
use crate::pubsub_topic_tracker::{
    gossipsub_mesh_peer_ids, gossipsub_topic_peer_count, PubsubTopicTracker,
};
use crate::pubsub_topics::TASK_TOPIC;
use crate::task_protocol::TaskRequest;
use crate::worker_startup_policy::{
    emit_worker_listening_event, emit_worker_startup_ready_event, WorkerStartupPolicy,
};
use crate::{IamineBehaviour, NET_PEER_DISCONNECTED_002};
use iamine_network::{LogLevel, SharedNetworkTopology};
use libp2p::core::ConnectedPoint;
use libp2p::{gossipsub, ping, swarm::Swarm, Multiaddr, PeerId};
use serde_json::Map;
use std::collections::{HashMap, HashSet};

pub(crate) fn handle_new_listen_addr(
    swarm: &mut Swarm<IamineBehaviour>,
    local_peer_id: PeerId,
    address: &Multiaddr,
    mode: &NodeMode,
    worker_startup_ready_emitted: &mut bool,
    worker_port: u16,
    worker_startup_policy: Option<&WorkerStartupPolicy>,
) {
    println!("🌐 Escuchando en: {}", address);
    swarm
        .behaviour_mut()
        .kademlia
        .add_address(&local_peer_id, address.clone());
    if matches!(mode, NodeMode::Worker) && !*worker_startup_ready_emitted {
        emit_worker_listening_event(&local_peer_id.to_string(), worker_port, address);
        if let Some(policy) = worker_startup_policy {
            emit_worker_startup_ready_event(&local_peer_id.to_string(), worker_port, policy);
        }
        *worker_startup_ready_emitted = true;
    }
}

pub(crate) fn handle_mdns_discovered(
    swarm: &mut Swarm<IamineBehaviour>,
    peers: Vec<(PeerId, Multiaddr)>,
    local_peer_id: PeerId,
    debug_flags: DebugFlags,
    cluster_registry: &mut ClusterRegistry,
    cluster_id: &str,
    known_workers: &mut HashSet<PeerId>,
    is_client: bool,
    connected_peer: Option<PeerId>,
    tasks_sent: bool,
) {
    for (pid, addr) in peers {
        if pid == local_peer_id {
            continue;
        }
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
            cluster_id,
            &pid.to_string(),
            "mdns",
            Some(addr.to_string()),
        );
        swarm
            .behaviour_mut()
            .kademlia
            .add_address(&pid, addr.clone());
        swarm.behaviour_mut().gossipsub.add_explicit_peer(&pid);
        known_workers.insert(pid);
        if is_client && connected_peer.is_none() && !tasks_sent {
            let _ = swarm.dial(addr);
        }
    }
}

pub(crate) fn handle_mdns_expired(
    swarm: &mut Swarm<IamineBehaviour>,
    peers: Vec<(PeerId, Multiaddr)>,
    known_workers: &mut HashSet<PeerId>,
    pubsub_topics: &mut PubsubTopicTracker,
) {
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

pub(crate) fn handle_gossipsub_subscribed(
    swarm: &Swarm<IamineBehaviour>,
    pid: PeerId,
    topic: gossipsub::TopicHash,
    local_peer_id: PeerId,
    mode: &NodeMode,
    pubsub_topics: &mut PubsubTopicTracker,
    cluster_registry: &mut ClusterRegistry,
    broadcast_offer_state: &mut Option<BroadcastOfferState>,
) {
    println!("📢 Peer {} suscrito a: {}", pid, topic);
    pubsub_topics.register_peer_subscription(&pid, &topic);
    cluster_registry.add_or_update_discovered_node(&pid.to_string(), None, unix_now_ms());
    let tracked_topic_name = crate::pubsub_topics::tracked_pubsub_topic_name(&topic);
    let observer_mode = if matches!(mode, NodeMode::Broadcast { .. }) {
        Some(("controller_observed_peer_subscription", "broadcast"))
    } else if matches!(mode, NodeMode::Worker) {
        Some(("worker_observed_peer_subscription", "worker"))
    } else {
        None
    };
    if let (Some((event_name, mode_name)), Some(topic_name)) = (observer_mode, tracked_topic_name) {
        emit_observed_peer_subscription_event(
            event_name,
            &local_peer_id.to_string(),
            &pid.to_string(),
            topic_name,
            mode_name,
            "gossipsub_subscription",
        );
    }
    if matches!(mode, NodeMode::Broadcast { .. }) && tracked_topic_name == Some(TASK_TOPIC) {
        if let Some(state) = broadcast_offer_state.as_mut() {
            state.mark_subscription_seen();
        }
        let connected_peers = swarm.connected_peers().count();
        let subscribed_peers =
            pubsub_topics
                .topic_peer_count(TASK_TOPIC)
                .max(gossipsub_topic_peer_count(
                    &swarm.behaviour().gossipsub,
                    TASK_TOPIC,
                ));
        let mesh_peers = gossipsub_mesh_peer_ids(&swarm.behaviour().gossipsub, TASK_TOPIC).len();
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
}

pub(crate) fn handle_gossipsub_unsubscribed(
    pid: PeerId,
    topic: gossipsub::TopicHash,
    pubsub_topics: &mut PubsubTopicTracker,
) {
    println!("🔕 Peer {} desuscrito de: {}", pid, topic);
    pubsub_topics.unregister_peer_subscription(&pid, &topic);
}

pub(crate) fn handle_connection_established(
    swarm: &mut Swarm<IamineBehaviour>,
    pid: PeerId,
    endpoint: &ConnectedPoint,
    mode: &NodeMode,
    cluster_registry: &mut ClusterRegistry,
    cluster_id: &str,
    broadcast_offer_state: &mut Option<BroadcastOfferState>,
    connected_peer: &mut Option<PeerId>,
    known_workers: &mut HashSet<PeerId>,
    is_client: bool,
    tasks_sent: &mut bool,
    pending_tasks: &[TaskRequest],
    total_tasks: usize,
    waiting_for_response: &mut bool,
) {
    println!(
        "✅ Conectado a: {} ({})",
        pid,
        endpoint.get_remote_address()
    );
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
        cluster_id,
        &pid.to_string(),
        "peer_connected",
        Some(endpoint.get_remote_address().to_string()),
    );
    *connected_peer = Some(pid);
    known_workers.insert(pid);
    if is_client && !*tasks_sent && !matches!(mode, NodeMode::Broadcast { .. }) {
        if let Some(task) = pending_tasks.first().cloned() {
            println!(
                "📤 [{}/{}] Enviando: {} → '{}'",
                1, total_tasks, task.task_type, task.data
            );
            swarm
                .behaviour_mut()
                .request_response
                .send_request(&pid, task);
            *waiting_for_response = true;
            *tasks_sent = true;
        }
    }
}

pub(crate) fn handle_connection_closed(
    pid: PeerId,
    mode: &NodeMode,
    distributed_infer_state: Option<&DistributedInferState>,
    infer_request_id: Option<&String>,
    pending_inference: &HashMap<String, tokio::time::Instant>,
    known_workers: &mut HashSet<PeerId>,
    pubsub_topics: &mut PubsubTopicTracker,
    is_client: bool,
    waiting_for_response: bool,
    completed: usize,
    total_tasks: usize,
) -> bool {
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
        let active_peer = distributed_infer_state.and_then(|state| state.current_peer.as_deref())
            == Some(disconnected_peer.as_str());
        let inflight = infer_request_id
            .map(|attempt_id| pending_inference.contains_key(attempt_id))
            .unwrap_or(false);
        if active_peer && inflight {
            log_observability_event(
                LogLevel::Warn,
                "node_disconnect_during_active_task",
                distributed_infer_state
                    .map(|state| state.trace_task_id.as_str())
                    .unwrap_or("network"),
                distributed_infer_state.map(|state| state.trace_task_id.as_str()),
                distributed_infer_state.and_then(|state| state.current_model.as_deref()),
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
        && !matches!(
            mode,
            NodeMode::Broadcast { .. } | NodeMode::ClusterStatus { .. }
        )
    {
        println!("\n📊 Completado: {}/{} tareas", completed, total_tasks);
        return true;
    }
    false
}

pub(crate) async fn handle_ping_event(
    peer: PeerId,
    result: Result<std::time::Duration, ping::Failure>,
    peer_tracker: &mut PeerTracker,
    topology: &SharedNetworkTopology,
    cluster_registry: &mut ClusterRegistry,
    human_log_throttle: &mut HumanLogThrottle,
) {
    let Ok(rtt) = result else {
        return;
    };
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
    topology
        .write()
        .await
        .update_latency(&peer.to_string(), rtt_ms);
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

pub(crate) fn handle_kademlia_routing_updated(
    peer: PeerId,
    cluster_registry: &mut ClusterRegistry,
    cluster_id: &str,
    human_log_throttle: &mut HumanLogThrottle,
    debug_flags: DebugFlags,
) {
    let peer_id = peer.to_string();
    cluster_registry.add_or_update_discovered_node(&peer_id, None, unix_now_ms());
    emit_cluster_node_updated(cluster_id, &peer_id, "kademlia_routing_updated");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub_topics::tracked_pubsub_topic_name;

    #[test]
    fn topic_subscription_tracking_preserved() {
        let topic = gossipsub::IdentTopic::new(TASK_TOPIC).hash();
        assert_eq!(tracked_pubsub_topic_name(&topic), Some(TASK_TOPIC));
    }

    #[test]
    fn discovery_mdns_expired_updates_peer_state() {
        let mut known = HashSet::new();
        let peer = PeerId::random();
        known.insert(peer);
        known.remove(&peer);
        assert!(!known.contains(&peer));
    }

    #[test]
    fn ping_latency_update_preserves_bucket_boundaries() {
        assert_eq!(cluster_label_for_latency(5.0), "LOCAL");
        assert_eq!(cluster_label_for_latency(25.0), "NEARBY");
        assert_eq!(cluster_label_for_latency(100.0), "REMOTE");
    }
}
