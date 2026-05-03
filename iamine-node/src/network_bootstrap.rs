use super::*;

pub(super) fn build_swarm_and_topics(
    peer_id: PeerId,
    id_keys: &libp2p::identity::Keypair,
) -> Result<(Swarm<IamineBehaviour>, PubsubTopicTracker), Box<dyn Error>> {
    let transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::Config::new(id_keys)?)
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
    )
    .map_err(|e| format!("Gossipsub error: {}", e))?;

    let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC);
    gossipsub_behaviour.subscribe(&task_topic)?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-bids"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-assign"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new("iamine-heartbeat"))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(RESULTS_TOPIC))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(CAP_TOPIC))?;
    gossipsub_behaviour.subscribe(&gossipsub::IdentTopic::new(DIRECT_INF_TOPIC))?;

    let mut pubsub_topics = PubsubTopicTracker::default();
    pubsub_topics.register_local_subscription(TASK_TOPIC);
    pubsub_topics.register_local_subscription(DIRECT_INF_TOPIC);
    pubsub_topics.register_local_subscription(RESULTS_TOPIC);
    for topic_name in [TASK_TOPIC, DIRECT_INF_TOPIC, RESULTS_TOPIC] {
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
    }

    let mut kad_cfg = kad::Config::default();
    kad_cfg.set_query_timeout(Duration::from_secs(30));
    let kademlia =
        kad::Behaviour::with_config(peer_id, kad::store::MemoryStore::new(peer_id), kad_cfg);

    let behaviour = IamineBehaviour {
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
        mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?,
        gossipsub: gossipsub_behaviour,
    };

    let swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(60)),
    );

    Ok((swarm, pubsub_topics))
}

pub(super) fn dial_bootnodes_from_args(swarm: &mut Swarm<IamineBehaviour>, args: &[String]) {
    for arg in args {
        if arg.starts_with("--bootnode=") {
            let addr_str = arg.replace("--bootnode=", "");
            if let Ok(addr) = addr_str.parse::<Multiaddr>() {
                println!("🔗 Conectando a bootnode: {}", addr);
                let _ = swarm.dial(addr);
            }
        }
    }
}
