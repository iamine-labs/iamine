use super::*;
use crate::startup_math::compute_active_tasks;
use crate::startup_model_validation::refresh_models_for_advertising;

pub(super) struct WorkerHeartbeatContext<'a> {
    pub(super) mode: &'a NodeMode,
    pub(super) queue: &'a Arc<TaskQueue>,
    pub(super) heartbeat: &'a Arc<HeartbeatService>,
    pub(super) pool: &'a Arc<WorkerPool>,
    pub(super) metrics: &'a Arc<RwLock<NodeMetrics>>,
    pub(super) peer_tracker: &'a mut PeerTracker,
    pub(super) direct_peers_count: usize,
    pub(super) rate_limiter: &'a mut RateLimiter,
    pub(super) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(super) peer_id: PeerId,
    pub(super) capabilities: &'a WorkerCapabilities,
    pub(super) validated_advertised_models: &'a mut Vec<String>,
    pub(super) benchmark: Option<&'a NodeBenchmark>,
    pub(super) node_caps: &'a ModelNodeCapabilities,
    pub(super) worker_slots: usize,
    pub(super) topology: &'a SharedNetworkTopology,
    pub(super) registry: &'a SharedNodeRegistry,
    pub(super) resource_policy: &'a ResourcePolicy,
    pub(super) node_identity: &'a NodeIdentity,
    pub(super) worker_port: u16,
    pub(super) model_storage: &'a ModelStorage,
    pub(super) inference_backend_state: &'a InferenceBackendState,
}

pub(super) async fn handle_worker_heartbeat_tick(
    ctx: WorkerHeartbeatContext<'_>,
) -> Result<(), Box<dyn Error>> {
    if !matches!(ctx.mode, NodeMode::Worker) {
        return Ok(());
    }

    let rep = ctx.queue.reputation().await;
    let uptime = ctx.heartbeat.uptime_secs();
    let available_slots = ctx.pool.available_slots();
    let active_tasks = match compute_active_tasks(ctx.pool.max_concurrent, available_slots) {
        Ok(value) => value,
        Err(error) => {
            emit_worker_startup_overflow_event(WorkerStartupOverflowContext {
                trace_id: "startup",
                node_id: &ctx.node_identity.node_id,
                peer_id: &ctx.peer_id.to_string(),
                port: ctx.worker_port,
                resource_policy: ctx.resource_policy,
                worker_slots: ctx.worker_slots,
                max_concurrent: ctx.pool.max_concurrent,
                available_slots,
                error: &error,
                fallback_behavior: "exit_cleanly_invalid_startup_state",
            });
            return Err(format!(
                "Worker startup math invalid: {} [{}]",
                error.describe(),
                WORKER_STARTUP_OVERFLOW_001
            )
            .into());
        }
    };

    let registry_models = ModelRegistry::new();
    let refreshed_models = refresh_models_for_advertising(
        &registry_models,
        ctx.model_storage,
        ctx.node_caps,
        ctx.inference_backend_state,
    );
    if refreshed_models != *ctx.validated_advertised_models {
        let previous_models = ctx.validated_advertised_models.clone();
        *ctx.validated_advertised_models = refreshed_models.clone();
        log_observability_event(
            LogLevel::Info,
            "capabilities_republished",
            "heartbeat",
            None,
            None,
            Some(CAPABILITIES_STALE_REFRESH_REQUIRED_001),
            {
                let mut fields = Map::new();
                fields.insert(
                    "previous_models".to_string(),
                    serde_json::json!(previous_models),
                );
                fields.insert("models".to_string(), serde_json::json!(refreshed_models));
                fields.insert("reason".to_string(), "detected_models_change".into());
                fields
            },
        );
    }

    {
        let mut m = ctx.metrics.write().await;
        m.uptime_secs = uptime;
        m.reputation_score = rep.reputation_score;
        m.active_workers = active_tasks;
        m.network_peers = ctx.peer_tracker.peer_count();
        m.direct_peers = ctx.direct_peers_count;
        m.avg_latency_ms = ctx.peer_tracker.avg_latency();
    }

    if ctx.rate_limiter.allow("Heartbeat") {
        let hb = serde_json::json!({
            "type": "Heartbeat",
            "peer_id": ctx.peer_id.to_string(),
            "available_slots": available_slots,
            "reputation_score": rep.reputation_score,
            "uptime_secs": uptime,
            "avg_latency_ms": rep.avg_execution_ms,
            "capabilities": {
                "cpu_cores": ctx.capabilities.cpu_cores,
                "ram_gb": ctx.capabilities.ram_gb,
                "gpu_available": ctx.capabilities.gpu_available,
                "supported_tasks": ctx.capabilities.supported_tasks,
                "inference_backend": ctx.capabilities.inference_backend,
                "real_inference_available": ctx.capabilities.real_inference_available,
                "mock_inference_enabled": ctx.capabilities.mock_inference_enabled,
            }
        });
        let hb_topic = gossipsub::IdentTopic::new("iamine-heartbeat");
        let _ = ctx
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(hb_topic, serde_json::to_vec(&hb).unwrap());
    }

    let installer = ModelInstaller::new();
    let allowed_model_ids: HashSet<_> = ctx.validated_advertised_models.iter().cloned().collect();
    let mut nm = installer.build_node_models(&ctx.peer_id.to_string());
    nm.models
        .retain(|model| allowed_model_ids.contains(&model.id));
    if !nm.models.is_empty() {
        let payload = serde_json::json!({
            "type": "NodeModelsBroadcast",
            "node_id": ctx.peer_id.to_string(),
            "models": nm.models,
        });
        let _ = ctx.swarm.behaviour_mut().gossipsub.publish(
            gossipsub::IdentTopic::new("iamine-heartbeat"),
            serde_json::to_vec(&payload).unwrap(),
        );
    }

    let cap_hb = NodeCapabilityHeartbeat {
        peer_id: ctx.peer_id.to_string(),
        cpu_score: ctx.benchmark.map(|b| b.cpu_score as u64).unwrap_or(0),
        ram_gb: ctx
            .benchmark
            .map(|b| b.ram_available_gb as u32)
            .unwrap_or(8),
        gpu_available: ctx.benchmark.map(|b| b.gpu_available).unwrap_or(false),
        storage_available_gb: ctx.node_caps.storage_available_gb,
        accelerator: ctx.node_caps.accelerator.clone(),
        models: ctx.validated_advertised_models.to_vec(),
        worker_slots: ctx.worker_slots as u32,
        active_tasks: active_tasks as u32,
        latency_ms: ctx.peer_tracker.avg_latency().max(1.0) as u32,
        inference_backend: ctx.capabilities.inference_backend.clone(),
        real_inference_available: ctx.capabilities.real_inference_available,
        mock_inference_enabled: ctx.capabilities.mock_inference_enabled,
    };
    let _ = ctx.swarm.behaviour_mut().gossipsub.publish(
        gossipsub::IdentTopic::new(CAP_TOPIC),
        serde_json::to_vec(&cap_hb).unwrap(),
    );

    {
        let mut topo = ctx.topology.write().await;
        topo.assign_clusters(&ctx.peer_id.to_string());
        let mut reg = ctx.registry.write().await;
        for cluster in topo.all_clusters() {
            for node_id in &cluster.nodes {
                reg.set_cluster(&node_id.to_string(), &cluster.id);
            }
        }
    }

    Ok(())
}
