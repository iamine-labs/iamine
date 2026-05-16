use crate::benchmark::NodeBenchmark;
use crate::model_executability::{
    classify_model_executability, ModelExecutability, ModelExecutabilityInput,
};
use crate::node_modes::InferenceControlFlags;
use crate::setup_wizard::{DetectedHardware, NodeSetupConfig};
use crate::worker_capability_advertisement::validate_models_for_advertising;
use crate::worker_startup_policy::{
    emit_backend_cpu_feature_incompatible_event, emit_inference_backend_selected_event,
    emit_worker_model_load_skipped_event, WorkerStartupPolicy,
};
use iamine_models::{
    AutoProvisionProfile, ModelAutoProvision, ModelDescriptor, ModelNodeCapabilities,
    ModelRegistry, ModelStorage, RealInferenceEngine, RealInferenceResult, StorageConfig,
};
use iamine_network::{
    Complexity as PromptComplexityLevel, DeterministicLevel as PromptDeterministicLevel,
    Domain as PromptDomain, ExactSubtype as PromptExactSubtype, Language as PromptLanguage,
    OutputStyle as PromptOutputStyle, SemanticRoutingDecision, TaskType as PromptTaskType,
    ValidationResult as SemanticValidationResult,
};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegistryModelDisplay {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) required_ram_gb: u32,
    pub(crate) size_bytes: u64,
}

impl From<&ModelDescriptor> for RegistryModelDisplay {
    fn from(model: &ModelDescriptor) -> Self {
        Self {
            id: model.id.clone(),
            version: model.version.clone(),
            required_ram_gb: model.required_ram_gb,
            size_bytes: model.size_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelDisplayRow {
    pub(crate) id: String,
    pub(crate) in_storage: bool,
    pub(crate) in_registry: bool,
    pub(crate) executable: bool,
    pub(crate) fits_storage: bool,
    pub(crate) classification: ModelExecutability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelDisplayView {
    pub(crate) backend: String,
    pub(crate) real_inference_available: bool,
    pub(crate) storage_models_count: usize,
    pub(crate) registry_models_count: usize,
    pub(crate) executable_models_count: usize,
    pub(crate) simple_tasks: Vec<String>,
    pub(crate) rows: Vec<ModelDisplayRow>,
}

pub(crate) struct ModelRuntimeContext {
    pub(crate) storage_config: StorageConfig,
    pub(crate) registry: ModelRegistry,
    pub(crate) storage: ModelStorage,
    pub(crate) node_caps: ModelNodeCapabilities,
    pub(crate) worker_startup_policy: Option<WorkerStartupPolicy>,
    pub(crate) inference_engine: Option<Arc<RealInferenceEngine>>,
    pub(crate) validated_advertised_models: Vec<String>,
    pub(crate) worker_setup: Option<NodeSetupConfig>,
}

pub(crate) fn prepare_model_runtime_context(
    is_worker: bool,
    peer_id: &str,
    benchmark: Option<&NodeBenchmark>,
    enable_non_worker_inference_engine: bool,
) -> Result<ModelRuntimeContext, String> {
    let storage_config = StorageConfig::load();
    let registry = ModelRegistry::new();
    let storage = ModelStorage::new();
    let mut node_caps = ModelNodeCapabilities::detect(peer_id);

    let worker_startup_policy = if is_worker {
        let policy = WorkerStartupPolicy::from_env(&node_caps);
        emit_inference_backend_selected_event(&policy);
        if !policy.cpu_feature_compatible {
            emit_backend_cpu_feature_incompatible_event(
                &node_caps.cpu_features,
                &node_caps.accelerator,
                "continue_degraded_without_real_inference",
            );
        }
        if let Some(reason) = policy.model_load_skip_reason {
            emit_worker_model_load_skipped_event(reason);
        }
        Some(policy)
    } else {
        None
    };

    let inference_engine = if is_worker {
        worker_startup_policy
            .as_ref()
            .filter(|policy| policy.real_inference_available)
            .map(|_| Arc::new(RealInferenceEngine::new(ModelStorage::new())))
    } else if enable_non_worker_inference_engine {
        Some(Arc::new(RealInferenceEngine::new(ModelStorage::new())))
    } else {
        None
    };

    let validated_advertised_models = if is_worker {
        let policy = worker_startup_policy
            .as_ref()
            .expect("worker startup policy must be present in worker mode");
        validate_models_for_advertising(&registry, &storage, &node_caps, policy)
    } else {
        storage.list_local_models()
    };

    if is_worker {
        node_caps.supported_models = validated_advertised_models.clone();
    }

    let worker_setup = if is_worker {
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

    Ok(ModelRuntimeContext {
        storage_config,
        registry,
        storage,
        node_caps,
        worker_startup_policy,
        inference_engine,
        validated_advertised_models,
        worker_setup,
    })
}

pub(crate) fn prepare_cluster_status_model_runtime_context(
    peer_id: &str,
) -> Result<ModelRuntimeContext, String> {
    let storage_config = StorageConfig::load();
    let registry = ModelRegistry::new();
    let storage = ModelStorage::new();
    let local_models = storage.list_local_models();
    let used_gb = (storage.total_size_bytes() / 1_073_741_824) as u32;
    let max_gb = storage_config.max_storage_gb as u32;

    Ok(ModelRuntimeContext {
        storage_config,
        registry,
        storage,
        node_caps: ModelNodeCapabilities {
            node_id: peer_id.to_string(),
            cpu_cores: std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(1),
            ram_gb: 0,
            gpu_type: None,
            npu_type: None,
            storage_available_gb: max_gb.saturating_sub(used_gb),
            worker_slots: 0,
            supported_models: local_models.clone(),
            cpu_features: Vec::new(),
            accelerator: "unknown".to_string(),
        },
        worker_startup_policy: None,
        inference_engine: None,
        validated_advertised_models: local_models,
        worker_setup: None,
    })
}

pub(crate) fn build_model_display_view(
    storage_models: &[String],
    registry_models: &[RegistryModelDisplay],
    executable_models: &[String],
    backend: impl Into<String>,
    real_inference_available: bool,
    simple_tasks: Vec<String>,
    max_storage_gb: u64,
    current_usage_bytes: u64,
) -> ModelDisplayView {
    let storage_set: HashSet<&str> = storage_models.iter().map(String::as_str).collect();
    let executable_set: HashSet<&str> = executable_models.iter().map(String::as_str).collect();
    let mut seen = HashSet::new();
    let backend = backend.into();
    let backend_is_mock = backend.eq_ignore_ascii_case("mock");
    let mut rows = Vec::new();

    for model in registry_models {
        seen.insert(model.id.clone());
        let in_storage = storage_set.contains(model.id.as_str());
        let executable = executable_set.contains(model.id.as_str());
        rows.push(ModelDisplayRow {
            id: model.id.clone(),
            in_storage,
            in_registry: true,
            executable,
            fits_storage: current_usage_bytes + model.size_bytes <= max_storage_gb * 1_073_741_824,
            classification: classify_model_executability(&ModelExecutabilityInput {
                in_storage,
                in_registry: true,
                backend_is_mock,
                real_inference_available,
                hardware_supported: executable,
            }),
        });
    }

    for model_id in storage_models {
        if seen.contains(model_id) {
            continue;
        }
        rows.push(ModelDisplayRow {
            id: model_id.clone(),
            in_storage: true,
            in_registry: false,
            executable: executable_set.contains(model_id.as_str()),
            fits_storage: true,
            classification: ModelExecutability::StorageOnly,
        });
    }

    ModelDisplayView {
        backend,
        real_inference_available,
        storage_models_count: storage_models.len(),
        registry_models_count: registry_models.len(),
        executable_models_count: rows
            .iter()
            .filter(|row| row.classification.is_executable())
            .count(),
        simple_tasks,
        rows,
    }
}

pub(crate) fn prompt_language_label(language: PromptLanguage) -> &'static str {
    match language {
        PromptLanguage::English => "English",
        PromptLanguage::Spanish => "Spanish",
        PromptLanguage::Unknown => "Unknown",
    }
}

pub(crate) fn prompt_complexity_label(complexity: PromptComplexityLevel) -> &'static str {
    match complexity {
        PromptComplexityLevel::Low => "Low",
        PromptComplexityLevel::Medium => "Medium",
        PromptComplexityLevel::High => "High",
    }
}

pub(crate) fn prompt_task_label(task_type: PromptTaskType) -> &'static str {
    match task_type {
        PromptTaskType::Math => "Math",
        PromptTaskType::ExactMath => "ExactMath",
        PromptTaskType::SymbolicMath => "SymbolicMath",
        PromptTaskType::Generative => "Generative",
        PromptTaskType::StructuredList => "StructuredList",
        PromptTaskType::Deterministic => "Deterministic",
        PromptTaskType::Code => "Code",
        PromptTaskType::Conceptual => "Conceptual",
        PromptTaskType::Reasoning => "Reasoning",
        PromptTaskType::Summarization => "Summarization",
        PromptTaskType::General => "General",
    }
}

pub(crate) fn prompt_output_style_label(output_style: PromptOutputStyle) -> &'static str {
    match output_style {
        PromptOutputStyle::Exact => "Exact",
        PromptOutputStyle::Explanatory => "Explanatory",
        PromptOutputStyle::Structured => "Structured",
        PromptOutputStyle::Generative => "Generative",
        PromptOutputStyle::Hybrid => "Hybrid",
    }
}

pub(crate) fn prompt_deterministic_level_label(level: PromptDeterministicLevel) -> &'static str {
    match level {
        PromptDeterministicLevel::High => "High",
        PromptDeterministicLevel::Medium => "Medium",
        PromptDeterministicLevel::Low => "Low",
    }
}

pub(crate) fn prompt_domain_label(domain: Option<PromptDomain>) -> &'static str {
    match domain {
        Some(PromptDomain::Math) => "Math",
        Some(PromptDomain::Physics) => "Physics",
        Some(PromptDomain::Business) => "Business",
        Some(PromptDomain::Philosophy) => "Philosophy",
        Some(PromptDomain::Code) => "Code",
        Some(PromptDomain::General) | None => "General",
    }
}

pub(crate) fn exact_subtype_label(exact_subtype: PromptExactSubtype) -> &'static str {
    match exact_subtype {
        PromptExactSubtype::Integer => "Integer",
        PromptExactSubtype::DecimalSequence => "DecimalSequence",
        PromptExactSubtype::Sequence => "Sequence",
    }
}

pub(crate) fn log_semantic_decision(decision: &SemanticRoutingDecision) {
    let secondary = if decision.profile.semantic.secondary_tasks.is_empty() {
        "[]".to_string()
    } else {
        format!(
            "[{}]",
            decision
                .profile
                .semantic
                .secondary_tasks
                .iter()
                .map(|task| prompt_task_label(*task))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    println!(
        "[Semantic] Primary: {}",
        prompt_task_label(decision.profile.semantic.primary_task)
    );
    println!("[Semantic] Secondary: {}", secondary);
    println!(
        "[Semantic] Style: {}",
        prompt_output_style_label(decision.profile.semantic.output_style)
    );
    println!(
        "[Semantic] Context: {}",
        decision.profile.semantic.requires_context
    );
    println!(
        "[Semantic] Domain: {}",
        prompt_domain_label(decision.profile.semantic.domain)
    );
    println!(
        "[Semantic] Deterministic: {}",
        prompt_deterministic_level_label(decision.profile.semantic.deterministic_level)
    );
    println!("[Semantic] Confidence: {:.2}", decision.profile.confidence);
    println!("[Semantic] Fallback: {}", decision.fallback_applied);
    if decision.fallback_applied {
        println!(
            "[Semantic] Original task: {}",
            prompt_task_label(decision.original_task_type)
        );
    }
}

pub(crate) fn log_semantic_validation(validation: &SemanticValidationResult) {
    println!(
        "[SemanticValidator] Confidence: {:.2} -> {:.2}",
        validation.confidence_before, validation.confidence_after
    );
    println!(
        "[SemanticValidator] Model validation: {}",
        validation.model_validation_used
    );
    println!(
        "[SemanticValidator] Correction applied: {}",
        validation.correction_applied
    );
    if !validation.conflicts.is_empty() {
        println!(
            "[SemanticValidator] Conflicts: {}",
            validation.conflicts.join(", ")
        );
    }
}

pub(crate) fn display_local_inference_completion(result: &RealInferenceResult) {
    println!("\n\n✅ Inference completada");
    println!("[Inference] tokens_generated: {}", result.tokens_generated);
    println!("[Inference] truncated: {}", result.truncated);
    println!(
        "[Inference] continuation_steps: {}",
        result.continuation_steps
    );
    if result.truncated {
        println!("[Warning] Output truncated at token budget");
    }
}

pub(crate) fn display_distributed_inference_route_notice(
    control_flags: &InferenceControlFlags,
    selected_model: &str,
    candidate_models: &[String],
    model_override_present: bool,
) {
    if control_flags.force_network {
        println!(
            "🌐 Flag --force-network activo; omitiendo inferencia local para {}.",
            selected_model
        );
        println!("   Candidates: {}", candidate_models.join(", "));
        println!("   Intentando ruta distribuida...\n");
    } else if control_flags.no_local {
        println!(
            "🚫 Flag --no-local activo; omitiendo inferencia local para {}.",
            selected_model
        );
        println!("   Candidates: {}", candidate_models.join(", "));
        println!("   Intentando ruta distribuida...\n");
    } else if model_override_present {
        println!(
            "⚠️  Override {} no esta instalado localmente, intentando ruta distribuida.",
            selected_model
        );
    } else {
        println!("🤖 Modelo local preferido no disponible.");
        println!("   Candidates: {}", candidate_models.join(", "));
        println!("   Intentando ruta distribuida...\n");
    }
}

pub(crate) async fn display_worker_model_inventory_and_maybe_autoprovision(
    storage_config: &StorageConfig,
    model_registry: &ModelRegistry,
    model_storage: &ModelStorage,
    node_caps: &ModelNodeCapabilities,
    worker_setup: Option<&NodeSetupConfig>,
    benchmark: Option<&NodeBenchmark>,
    auto_model: bool,
) -> Result<(), String> {
    println!("💾 Storage limit: {} GB", storage_config.max_storage_gb);
    println!("🤖 Modelos disponibles localmente:");
    let local = model_storage.list_local_models();
    if local.is_empty() {
        println!("   (ninguno — usa --download-model <id>)");
        maybe_offer_startup_model_download(node_caps, worker_setup, benchmark, auto_model).await?;
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

    Ok(())
}

async fn maybe_offer_startup_model_download(
    node_caps: &ModelNodeCapabilities,
    worker_setup: Option<&NodeSetupConfig>,
    benchmark: Option<&NodeBenchmark>,
    auto_model: bool,
) -> Result<(), String> {
    let provision = ModelAutoProvision::new(ModelRegistry::new(), ModelStorage::new());
    let profile = AutoProvisionProfile {
        cpu_score: benchmark.map(|b| b.cpu_score as u64).unwrap_or(0),
        ram_gb: node_caps.ram_gb,
        gpu_available: benchmark.map(|b| b.gpu_available).unwrap_or(false),
        storage_available_gb: worker_setup
            .map(|cfg| cfg.storage_limit_gb)
            .unwrap_or(node_caps.storage_available_gb),
    };
    let recommended = provision.startup_recommendations(&profile);

    if recommended.is_empty() {
        return Ok(());
    }

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

        if input.trim().is_empty() || matches!(input.trim(), "y" | "Y" | "yes" | "YES") {
            if let Some(model_id) = provision
                .auto_download_recommended(&profile, None, false)
                .await?
            {
                println!("✅ Modelo descargado: {}", model_id);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry_models() -> Vec<RegistryModelDisplay> {
        vec![
            RegistryModelDisplay {
                id: "tinyllama-1b".to_string(),
                version: "1.1".to_string(),
                required_ram_gb: 2,
                size_bytes: 669_000_000,
            },
            RegistryModelDisplay {
                id: "mistral-7b".to_string(),
                version: "0.3".to_string(),
                required_ram_gb: 8,
                size_bytes: 4_370_000_000,
            },
        ]
    }

    #[test]
    fn model_display_separates_storage_registry_executable() {
        let view = build_model_display_view(
            &["tinyllama-1b".to_string(), "storage-only".to_string()],
            &registry_models(),
            &["tinyllama-1b".to_string()],
            "cpu",
            true,
            vec!["reverse_string".to_string()],
            50,
            0,
        );

        assert_eq!(view.storage_models_count, 2);
        assert_eq!(view.registry_models_count, 2);
        assert_eq!(view.executable_models_count, 1);
        assert!(view
            .rows
            .iter()
            .any(|row| row.id == "storage-only"
                && row.classification == ModelExecutability::StorageOnly));
        assert!(view
            .rows
            .iter()
            .any(|row| row.id == "mistral-7b"
                && row.classification == ModelExecutability::RegistryOnly));
    }

    #[test]
    fn model_display_mock_worker_does_not_claim_llm_executable() {
        let view = build_model_display_view(
            &["tinyllama-1b".to_string()],
            &registry_models(),
            &[],
            "mock",
            false,
            vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string(),
            ],
            50,
            0,
        );

        assert_eq!(view.backend, "mock");
        assert!(!view.real_inference_available);
        assert_eq!(view.executable_models_count, 0);
        assert!(view
            .rows
            .iter()
            .all(|row| !row.executable && !row.classification.is_executable()));
    }

    #[test]
    fn model_display_preserves_simple_tasks_under_mock() {
        let view = build_model_display_view(
            &[],
            &registry_models(),
            &[],
            "mock",
            false,
            vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string(),
            ],
            50,
            0,
        );

        assert!(view.simple_tasks.contains(&"reverse_string".to_string()));
        assert!(view.simple_tasks.contains(&"test".to_string()));
        assert!(view.simple_tasks.contains(&"echo".to_string()));
    }

    #[test]
    fn capability_display_handles_empty_registry() {
        let view = build_model_display_view(&[], &[], &[], "mock", false, vec![], 50, 0);

        assert_eq!(view.storage_models_count, 0);
        assert_eq!(view.registry_models_count, 0);
        assert_eq!(view.executable_models_count, 0);
        assert!(view.rows.is_empty());
    }

    #[test]
    fn capability_display_handles_storage_only_models() {
        let view = build_model_display_view(
            &["storage-only".to_string()],
            &[],
            &[],
            "cpu",
            true,
            vec![],
            50,
            0,
        );

        let row = view
            .rows
            .iter()
            .find(|row| row.id == "storage-only")
            .unwrap();
        assert!(row.in_storage);
        assert!(!row.in_registry);
        assert_eq!(row.classification, ModelExecutability::StorageOnly);
    }

    #[test]
    fn capability_display_handles_registry_only_models() {
        let view =
            build_model_display_view(&[], &registry_models(), &[], "cpu", true, vec![], 50, 0);

        let row = view
            .rows
            .iter()
            .find(|row| row.id == "tinyllama-1b")
            .unwrap();
        assert!(!row.in_storage);
        assert!(row.in_registry);
        assert_eq!(row.classification, ModelExecutability::RegistryOnly);
    }
}
