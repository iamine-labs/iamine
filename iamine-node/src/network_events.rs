use super::*;

pub(super) fn handle_result_response_request(
    swarm: &mut Swarm<IamineBehaviour>,
    peer: &PeerId,
    request: TaskResultRequest,
    channel: request_response::ResponseChannel<TaskResultResponse>,
) {
    println!(
        "🎉 [Origin] Resultado directo de {}:",
        &peer.to_string()[..8]
    );
    println!(
        "   task_id={} success={} ms={}",
        request.task_id, request.success, request.execution_ms
    );

    let _ = swarm
        .behaviour_mut()
        .result_response
        .send_response(channel, TaskResultResponse { acknowledged: true });
}

pub(super) fn handle_mdns_discovered(
    swarm: &mut Swarm<IamineBehaviour>,
    client_state: &mut ClientRuntimeState,
    is_client: bool,
    debug_flags: DebugFlags,
    local_peer_id: PeerId,
    peers: Vec<(PeerId, Multiaddr)>,
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

        swarm
            .behaviour_mut()
            .kademlia
            .add_address(&pid, addr.clone());
        swarm.behaviour_mut().gossipsub.add_explicit_peer(&pid);
        client_state.known_workers.insert(pid);

        if is_client && client_state.connected_peer.is_none() && !client_state.tasks_sent {
            let _ = swarm.dial(addr);
        }
    }
}

pub(super) fn handle_mdns_expired(
    swarm: &mut Swarm<IamineBehaviour>,
    client_state: &mut ClientRuntimeState,
    pubsub_topics: &mut PubsubTopicTracker,
    peers: Vec<(PeerId, Multiaddr)>,
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
        client_state.known_workers.remove(&pid);
        pubsub_topics.unregister_peer(&pid);
        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&pid);
    }
}

pub(super) fn handle_result_response_ack(peer: &PeerId, response: TaskResultResponse) {
    if response.acknowledged {
        println!(
            "✅ [Worker] Origin {} confirmó resultado",
            &peer.to_string()[..8]
        );
    }
}

pub(super) fn handle_pubsub_subscribed(
    pubsub_topics: &mut PubsubTopicTracker,
    pid: PeerId,
    topic: gossipsub::TopicHash,
) {
    println!("📢 Peer {} suscrito a: {}", pid, topic);
    pubsub_topics.register_peer_subscription(&pid, &topic);
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

pub(super) fn handle_pubsub_unsubscribed(
    pubsub_topics: &mut PubsubTopicTracker,
    pid: PeerId,
    topic: gossipsub::TopicHash,
) {
    println!("🔕 Peer {} desuscrito de: {}", pid, topic);
    pubsub_topics.unregister_peer_subscription(&pid, &topic);
}

pub(super) fn handle_connection_established(
    swarm: &mut Swarm<IamineBehaviour>,
    client_state: &mut ClientRuntimeState,
    is_client: bool,
    mode: &NodeMode,
    pid: PeerId,
    remote_address: Multiaddr,
) {
    println!("✅ Conectado a: {} ({})", pid, remote_address);
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
            fields.insert("address".to_string(), remote_address.to_string().into());
            fields
        },
    );

    client_state.connected_peer = Some(pid);
    client_state.known_workers.insert(pid);

    if is_client && !client_state.tasks_sent && !matches!(mode, NodeMode::Broadcast { .. }) {
        if let Some(task) = client_state.pending_tasks.first().cloned() {
            println!(
                "📤 [{}/{}] Enviando: {} → '{}'",
                1, client_state.total_tasks, task.task_type, task.data
            );
            swarm
                .behaviour_mut()
                .request_response
                .send_request(&pid, task);
            client_state.waiting_for_response = true;
            client_state.tasks_sent = true;
        }
    }
}

pub(super) fn handle_connection_closed(
    client_state: &mut ClientRuntimeState,
    pubsub_topics: &mut PubsubTopicTracker,
    is_client: bool,
    mode: &NodeMode,
    pid: PeerId,
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

    client_state.known_workers.remove(&pid);
    pubsub_topics.unregister_peer(&pid);

    if is_client
        && !client_state.waiting_for_response
        && !matches!(mode, NodeMode::Broadcast { .. })
    {
        println!(
            "\n📊 Completado: {}/{} tareas",
            client_state.completed, client_state.total_tasks
        );
        return true;
    }

    false
}

pub(super) async fn handle_ping_event(
    peer_tracker: &mut PeerTracker,
    topology: &SharedNetworkTopology,
    peer: PeerId,
    result: Result<Duration, ping::Failure>,
) {
    if let Ok(rtt) = result {
        let rtt_ms = rtt.as_micros() as f64 / 1000.0;
        println!(
            "[Latency] RTT to peer {} = {:.1} ms",
            &peer.to_string()[..12.min(peer.to_string().len())],
            rtt_ms
        );
        peer_tracker.update_latency(&peer.to_string(), rtt_ms);
        topology
            .write()
            .await
            .update_latency(&peer.to_string(), rtt_ms);
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

pub(super) fn handle_kademlia_routing_updated(debug_flags: DebugFlags, peer: PeerId) {
    println!("📡 Kademlia: nodo añadido {}", peer);
    debug_network_log(
        debug_flags,
        format!("kademlia routing updated peer_id={}", peer),
    );
}
