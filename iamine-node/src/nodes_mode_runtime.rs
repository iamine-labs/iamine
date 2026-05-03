use super::*;

pub(super) async fn handle_nodes_or_topology_tick(
    mode: &NodeMode,
    registry: &SharedNodeRegistry,
    topology: &SharedNetworkTopology,
    peer_id: PeerId,
) {
    if matches!(mode, NodeMode::Nodes) {
        let snapshot = registry.read().await.all_nodes();
        println!("\nPeer ID        Models                CPU Score    Load");
        println!("-------------------------------------------------------");
        if snapshot.is_empty() {
            println!("(esperando capabilities de peers...)");
        } else {
            for (pid, capability) in snapshot {
                let models = if capability.models.is_empty() {
                    "-".to_string()
                } else {
                    capability.models.join(",")
                };
                println!(
                    "{}  {:20} {:10} {}/{}",
                    &pid.to_string()[..10.min(pid.to_string().len())],
                    models,
                    capability.cpu_score,
                    capability.active_tasks,
                    capability.worker_slots
                );
            }
        }
    }

    if matches!(mode, NodeMode::Topology) {
        {
            let mut topo = topology.write().await;
            topo.assign_clusters(&peer_id.to_string());
        }
        let topo = topology.read().await;
        topo.display();
        if topo.peer_count() > 0 {
            println!("   ℹ️  Latencias: medidas reales (libp2p ping RTT)");
            println!("\n(Ctrl+C para salir)");
        } else {
            println!("(esperando peers...)");
        }
    }
}
