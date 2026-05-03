use super::*;
use crate::startup_model_validation::validate_models_for_advertising;

pub(crate) struct ModelStartupState {
    pub(crate) model_storage: ModelStorage,
    pub(crate) inference_engine: Arc<RealInferenceEngine>,
    pub(crate) node_caps: ModelNodeCapabilities,
    pub(crate) validated_advertised_models: Vec<String>,
}

pub(crate) async fn bootstrap_model_state(
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
