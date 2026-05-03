use super::*;
use crate::runtime_config::should_use_ephemeral_identity;
use crate::startup_model_validation::validate_models_for_advertising;

pub(super) struct CoreStartupState {
    pub(super) node_identity: NodeIdentity,
    pub(super) peer_id: PeerId,
    pub(super) wallet: Wallet,
    pub(super) benchmark: Option<NodeBenchmark>,
    pub(super) resource_policy: ResourcePolicy,
    pub(super) worker_slots: usize,
}

pub(super) struct ModelStartupState {
    pub(super) model_storage: ModelStorage,
    pub(super) inference_engine: Arc<RealInferenceEngine>,
    pub(super) node_caps: ModelNodeCapabilities,
    pub(super) validated_advertised_models: Vec<String>,
}

pub(super) struct RuntimeServicesState {
    pub(super) pool: Arc<WorkerPool>,
    pub(super) queue: Arc<TaskQueue>,
    pub(super) scheduler: Arc<TaskScheduler>,
    pub(super) task_manager: Arc<TaskManager>,
    pub(super) task_response_tx: tokio::sync::mpsc::Sender<PendingTaskResponse>,
    pub(super) task_response_rx: tokio::sync::mpsc::Receiver<PendingTaskResponse>,
    pub(super) heartbeat: Arc<HeartbeatService>,
    pub(super) metrics: Arc<RwLock<NodeMetrics>>,
    pub(super) capabilities: WorkerCapabilities,
    pub(super) task_cache: TaskCache,
    pub(super) peer_tracker: PeerTracker,
    pub(super) rate_limiter: RateLimiter,
}

pub(super) fn bootstrap_core_state(args: &[String], mode: &NodeMode) -> CoreStartupState {
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

pub(super) async fn bootstrap_model_state(
    mode: &NodeMode,
    auto_model: bool,
    peer_id: &PeerId,
    benchmark: Option<&NodeBenchmark>,
) -> Result<ModelStartupState, Box<dyn Error>> {
    let storage_config = StorageConfig::load();
    let model_registry = ModelRegistry::new();
    let model_storage = ModelStorage::new();
    let inference_engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));

    let node_caps = ModelNodeCapabilities::detect(&peer_id.to_string());
    let validated_advertised_models = if matches!(mode, NodeMode::Worker) {
        validate_models_for_advertising(&model_registry, &model_storage, &node_caps)
    } else {
        model_storage.list_local_models()
    };

    let worker_setup = if matches!(mode, NodeMode::Worker) {
        let detected = DetectedHardware {
            cpu_cores: std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(1),
            ram_gb: benchmark
                .map(|b| b.ram_available_gb as u32)
                .unwrap_or(node_caps.ram_gb),
            gpu_available: benchmark
                .map(|b| b.gpu_available)
                .unwrap_or(node_caps.gpu_type.is_some()),
            disk_available_gb: node_caps.storage_available_gb,
        };
        Some(NodeSetupConfig::load_or_run(&detected)?)
    } else {
        None
    };

    if let Some(cfg) = &worker_setup {
        cfg.display();
    }

    if matches!(mode, NodeMode::Worker) {
        node_caps.display();
    }

    if matches!(mode, NodeMode::Worker) {
        println!("💾 Storage limit: {} GB", storage_config.max_storage_gb);
        println!("🤖 Modelos disponibles localmente:");
        let local = model_storage.list_local_models();
        if local.is_empty() {
            println!("   (ninguno — usa --download-model <id>)");

            let provision = ModelAutoProvision::new(ModelRegistry::new(), ModelStorage::new());
            let profile = AutoProvisionProfile {
                cpu_score: benchmark.map(|b| b.cpu_score as u64).unwrap_or(0),
                ram_gb: node_caps.ram_gb,
                gpu_available: benchmark.map(|b| b.gpu_available).unwrap_or(false),
                storage_available_gb: worker_setup
                    .as_ref()
                    .map(|cfg| cfg.storage_limit_gb)
                    .unwrap_or(node_caps.storage_available_gb),
            };
            let recommended = provision.startup_recommendations(&profile);

            if !recommended.is_empty() {
                println!("\n⚠ No models installed\n");
                println!("Recommended:");
                for model in &recommended {
                    println!(
                        "{} ({:.0}MB)",
                        model.id,
                        model.size_bytes as f64 / 1_048_576.0
                    );
                }

                let auto_download_enabled = auto_model
                    || worker_setup
                        .as_ref()
                        .map(|cfg| cfg.auto_download_enabled())
                        .unwrap_or(false);

                if auto_download_enabled {
                    if let Some(model_id) = provision
                        .auto_download_recommended(&profile, None, false)
                        .await?
                    {
                        println!("\n⬇ Auto-downloaded: {}", model_id);
                    }
                } else {
                    print!("\nDownload now? [Y/n] ");
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                    let mut input = String::new();
                    let _ = std::io::stdin().read_line(&mut input);

                    if input.trim().is_empty() || matches!(input.trim(), "y" | "Y" | "yes" | "YES")
                    {
                        if let Some(model_id) = provision
                            .auto_download_recommended(&profile, None, false)
                            .await?
                        {
                            println!("✅ Modelo descargado: {}", model_id);
                        }
                    }
                }
            }
        } else {
            for m in &local {
                let used = model_storage.total_size_bytes();
                println!(
                    "   ✅ {} (storage: {:.1}/{} GB)",
                    m,
                    used as f64 / 1_073_741_824.0,
                    storage_config.max_storage_gb
                );
            }
        }
        println!("📋 Modelos en registry:");
        for m in model_registry.list() {
            let available = if model_storage.has_model(&m.id) {
                "✅"
            } else {
                "⬜"
            };
            let fits = storage_config.has_space_for(m.size_bytes, model_storage.total_size_bytes());
            let fits_str = if fits { "" } else { " ⚠️ sin espacio" };
            println!(
                "   {} {} v{} ({}GB RAM){}",
                available, m.id, m.version, m.required_ram_gb, fits_str
            );
        }
    }

    Ok(ModelStartupState {
        model_storage,
        inference_engine,
        node_caps,
        validated_advertised_models,
    })
}

pub(super) async fn bootstrap_runtime_services(
    peer_id: PeerId,
    worker_slots: usize,
    wallet_reputation: f32,
) -> RuntimeServicesState {
    let pool = Arc::new(WorkerPool::with_slots(worker_slots));
    let queue = Arc::new(TaskQueue::new(peer_id.to_string()));
    let scheduler = Arc::new(TaskScheduler::new());
    let task_manager = Arc::new(TaskManager::new());
    let (task_response_tx, task_response_rx) =
        tokio::sync::mpsc::channel::<PendingTaskResponse>(64);
    let heartbeat = Arc::new(HeartbeatService::new());
    let metrics = Arc::new(RwLock::new(NodeMetrics::new()));
    let capabilities = WorkerCapabilities::detect();
    let task_cache = TaskCache::new(1000);
    let peer_tracker = PeerTracker::new();
    let rate_limiter = RateLimiter::new(100);

    {
        let mut runtime_metrics = metrics.write().await;
        runtime_metrics.reputation_score = wallet_reputation as u32;
    }

    RuntimeServicesState {
        pool,
        queue,
        scheduler,
        task_manager,
        task_response_tx,
        task_response_rx,
        heartbeat,
        metrics,
        capabilities,
        task_cache,
        peer_tracker,
        rate_limiter,
    }
}
