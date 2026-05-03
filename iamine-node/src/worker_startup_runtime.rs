use super::*;

pub(super) fn print_runtime_mode_banner(mode: &NodeMode, peer_id: PeerId, pool: &Arc<WorkerPool>) {
    if matches!(mode, NodeMode::Relay) {
        println!("🔀 Modo RELAY activo — ayudando a peers detrás de NAT");
        println!("   Peer ID (compartir con otros nodos): {}", peer_id);
    }

    println!("   Slots paralelos: {}", pool.max_concurrent);
    println!("   Task queue: activa (max 3 reintentos)");
    println!("   Scheduler: activo (bid window 1.5s)\n");
}

pub(super) fn emit_worker_started_event(
    mode: &NodeMode,
    peer_id: PeerId,
    worker_port: u16,
    worker_slots: usize,
    resource_policy: &ResourcePolicy,
) {
    if !matches!(mode, NodeMode::Worker) {
        return;
    }

    log_observability_event(
        LogLevel::Info,
        "worker_started",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.to_string().into());
            fields.insert("port".to_string(), (worker_port as u64).into());
            fields.insert("worker_slots".to_string(), (worker_slots as u64).into());
            fields.insert(
                "resource_policy".to_string(),
                resource_policy_value(resource_policy),
            );
            fields
        },
    );
}

pub(super) fn compute_worker_metrics_port(
    mode: &NodeMode,
    worker_port: u16,
    node_identity: &NodeIdentity,
    peer_id: PeerId,
    resource_policy: &ResourcePolicy,
    worker_slots: usize,
    pool: &Arc<WorkerPool>,
) -> Option<u16> {
    if !matches!(mode, NodeMode::Worker) {
        return None;
    }

    match compute_metrics_port(worker_port) {
        Ok(port) => Some(port),
        Err(error) => {
            emit_worker_startup_overflow_event(WorkerStartupOverflowContext {
                trace_id: "startup",
                node_id: &node_identity.node_id,
                peer_id: &peer_id.to_string(),
                port: worker_port,
                resource_policy,
                worker_slots,
                max_concurrent: pool.max_concurrent,
                available_slots: pool.available_slots(),
                error: &error,
                fallback_behavior: "continue_without_metrics_server",
            });
            println!(
                "⚠️  [Startup] Cálculo de metrics port inválido: {}. Continuando en modo degradado (metrics deshabilitado).",
                error.describe()
            );
            None
        }
    }
}
