use super::*;
use event_loop_control::EventLoopDirective;

pub(super) struct SwarmEventRuntimeContext<'a> {
    pub(super) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(super) event: SwarmEvent<IaMineEvent>,
    pub(super) mode: &'a NodeMode,
    pub(super) peer_id: PeerId,
    pub(super) debug_flags: DebugFlags,
    pub(super) is_client: bool,
    pub(super) client_state: &'a mut ClientRuntimeState,
    pub(super) pubsub_topics: &'a mut PubsubTopicTracker,
    pub(super) registry: &'a SharedNodeRegistry,
    pub(super) task_cache: &'a mut TaskCache,
    pub(super) capabilities: &'a WorkerCapabilities,
    pub(super) pool: &'a Arc<WorkerPool>,
    pub(super) queue: &'a Arc<TaskQueue>,
    pub(super) scheduler: &'a Arc<TaskScheduler>,
    pub(super) peer_tracker: &'a mut PeerTracker,
    pub(super) metrics: &'a Arc<RwLock<NodeMetrics>>,
    pub(super) infer_runtime: &'a mut InferRuntimeState,
    pub(super) model_storage: &'a ModelStorage,
    pub(super) node_caps: &'a ModelNodeCapabilities,
    pub(super) inference_engine: &'a Arc<RealInferenceEngine>,
    pub(super) task_manager: &'a Arc<TaskManager>,
    pub(super) topology: &'a SharedNetworkTopology,
    pub(super) task_response_tx: &'a tokio::sync::mpsc::Sender<PendingTaskResponse>,
    pub(super) task_topic: &'a gossipsub::IdentTopic,
}

pub(super) async fn handle_swarm_event(ctx: SwarmEventRuntimeContext<'_>) -> EventLoopDirective {
    let SwarmEventRuntimeContext {
        swarm,
        event,
        mode,
        peer_id,
        debug_flags,
        is_client,
        client_state,
        pubsub_topics,
        registry,
        task_cache,
        capabilities,
        pool,
        queue,
        scheduler,
        peer_tracker,
        metrics,
        infer_runtime,
        model_storage,
        node_caps,
        inference_engine,
        task_manager,
        topology,
        task_response_tx,
        task_topic,
    } = ctx;

    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            println!("🌐 Escuchando en: {}", address);
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(&peer_id, address);
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Discovered(peers))) => {
            handle_mdns_discovered(swarm, client_state, is_client, debug_flags, peer_id, peers);
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::Mdns(mdns::Event::Expired(peers))) => {
            handle_mdns_expired(swarm, client_state, pubsub_topics, peers);
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Message {
            propagation_source,
            message,
            ..
        })) => {
            handle_gossipsub_message(GossipsubHandlerContext {
                swarm,
                mode,
                propagation_source,
                message,
                registry,
                task_cache,
                capabilities,
                pool,
                queue,
                peer_id,
                scheduler,
                client_state,
                task_topic,
                peer_tracker,
                metrics,
                infer_runtime,
                model_storage,
                node_caps,
                inference_engine,
                task_manager,
            })
            .await
        }

        SwarmEvent::Behaviour(IaMineEvent::ResultResponse(RREvent::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
        })) => {
            handle_result_response_request(swarm, &peer, request, channel);
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::ResultResponse(RREvent::Message {
            peer,
            message: Message::Response { response, .. },
        })) => {
            handle_result_response_ack(&peer, response);
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Subscribed {
            peer_id: pid,
            topic,
        })) => {
            handle_pubsub_subscribed(pubsub_topics, pid, topic);
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::Gossipsub(gossipsub::Event::Unsubscribed {
            peer_id: pid,
            topic,
        })) => {
            handle_pubsub_unsubscribed(pubsub_topics, pid, topic);
            EventLoopDirective::None
        }

        SwarmEvent::ConnectionEstablished {
            peer_id: pid,
            endpoint,
            ..
        } => {
            handle_connection_established(
                swarm,
                client_state,
                is_client,
                mode,
                pid,
                endpoint.get_remote_address().clone(),
            );
            EventLoopDirective::None
        }

        SwarmEvent::ConnectionClosed { peer_id: pid, .. } => {
            if handle_connection_closed(client_state, pubsub_topics, is_client, mode, pid) {
                EventLoopDirective::Break
            } else {
                EventLoopDirective::None
            }
        }

        SwarmEvent::Behaviour(IaMineEvent::Ping(ping::Event { peer, result, .. })) => {
            handle_ping_event(peer_tracker, topology, peer, result).await;
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::Kademlia(kad::Event::RoutingUpdated {
            peer, ..
        })) => {
            handle_kademlia_routing_updated(debug_flags, peer);
            EventLoopDirective::None
        }

        SwarmEvent::Behaviour(IaMineEvent::RequestResponse(event)) => {
            handle_request_response_event(RequestResponseHandlerContext {
                swarm,
                event,
                peer_id,
                debug_flags,
                is_client,
                topology,
                queue,
                task_manager,
                task_response_tx,
                registry,
                model_storage,
                infer_runtime,
                client_state,
            })
            .await
        }

        _ => EventLoopDirective::None,
    }
}
