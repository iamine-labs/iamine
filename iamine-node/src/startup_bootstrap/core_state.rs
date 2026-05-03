use super::*;
use crate::runtime_config::should_use_ephemeral_identity;

pub(crate) struct CoreStartupState {
    pub(crate) node_identity: NodeIdentity,
    pub(crate) peer_id: PeerId,
    pub(crate) wallet: Wallet,
    pub(crate) benchmark: Option<NodeBenchmark>,
    pub(crate) resource_policy: ResourcePolicy,
    pub(crate) worker_slots: usize,
}

pub(crate) fn bootstrap_core_state(args: &[String], mode: &NodeMode) -> CoreStartupState {
    if matches!(mode, NodeMode::Worker) {
        println!("╔══════════════════════════════════╗");
        println!("║   IaMine Worker {:<14}║", current_release_version());
        println!("╚══════════════════════════════════╝\n");
    }

    let use_ephemeral_identity = should_use_ephemeral_identity(mode);
    let node_identity = if use_ephemeral_identity {
        NodeIdentity::ephemeral("infer_mode_peer_id_isolation")
    } else {
        NodeIdentity::load_or_create()
    };
    let peer_id = node_identity.peer_id;
    set_global_node_id(&peer_id.to_string());

    if use_ephemeral_identity {
        log_observability_event(
            LogLevel::Info,
            "identity_mode_selected",
            "startup",
            None,
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("mode".to_string(), "infer".into());
                fields.insert("identity_kind".to_string(), "ephemeral".into());
                fields.insert(
                    "reason".to_string(),
                    "avoid_peer_id_conflict_with_local_worker".into(),
                );
                fields.insert("peer_id".to_string(), peer_id.to_string().into());
                fields
            },
        );
    }

    let wallet = Wallet::load_or_create(&node_identity.wallet_address);

    let benchmark = if matches!(mode, NodeMode::Worker) {
        Some(NodeBenchmark::run())
    } else {
        None
    };

    let resource_policy = ResourcePolicy::from_args(args);
    if matches!(mode, NodeMode::Worker) {
        resource_policy.display();
    }

    let worker_slots = if let Some(ref b) = benchmark {
        b.calculate_slots(&resource_policy)
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
    };

    println!("\n═══════════════════════════════════");
    println!("  Peer ID:      {}", peer_id);
    println!("  Wallet:       {}", node_identity.wallet_address);
    if let Some(ref b) = benchmark {
        println!("  CPU Score:    {:.0}", b.cpu_score);
        println!("  RAM:          {} GB", b.ram_available_gb);
        println!(
            "  GPU:          {}",
            if b.gpu_available { "✅" } else { "❌" }
        );
    }
    println!("  Worker Slots: {}", worker_slots);
    if !matches!(mode, NodeMode::Topology) {
        println!("  Modo:         {:?}", mode);
    }
    println!("  Balance:      {} $MIND", wallet.balance);
    println!("  Tareas:       {}", wallet.tasks_completed);
    println!("  Reputación:   {:.1}/100", wallet.reputation);
    println!("═══════════════════════════════════\n");

    CoreStartupState {
        node_identity,
        peer_id,
        wallet,
        benchmark,
        resource_policy,
        worker_slots,
    }
}
