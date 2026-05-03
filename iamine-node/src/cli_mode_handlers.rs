use super::*;
use crate::code_quality::run_code_quality_checks;
use crate::quality_gate::run_release_validation;
use crate::regression_runner::run_default_regression_suite;
use crate::security_checks::run_security_checks;

fn print_banner(title: &str) {
    println!("╔══════════════════════════════════╗");
    println!("║{:^34}║", title);
    println!("╚══════════════════════════════════╝\n");
}

pub(super) fn handle_models_list() {
    print_banner("IaMine — Modelos");
    let installer = ModelInstaller::new();
    let models = installer.list_models();
    println!("📋 Modelos disponibles:\n");
    for m in &models {
        m.display();
    }
    let used = ModelStorage::new().total_size_bytes();
    let cfg = StorageConfig::load();
    println!(
        "\n💾 Storage: {:.1}/{} GB usados",
        used as f64 / 1_073_741_824.0,
        cfg.max_storage_gb
    );
}

pub(super) fn handle_models_stats() {
    print_banner("IaMine — Model Karma");
    print_model_karma_stats();
}

pub(super) fn handle_tasks_stats() {
    print_banner("IaMine — Task Observability");
    let metrics = distributed_task_metrics();
    print_distributed_task_stats(&metrics);
}

pub(super) fn handle_tasks_trace(task_id: &str) -> Result<(), Box<dyn Error>> {
    print_banner("IaMine — Task Trace");
    if let Some(trace) = task_trace(task_id) {
        print_task_trace_entry(&trace);
        return Ok(());
    }

    Err(format!("No existe trace para task_id={}", task_id).into())
}

pub(super) fn handle_models_menu() -> Result<(), Box<dyn Error>> {
    let node_identity = NodeIdentity::load_or_create();
    ModelSelectorCLI::show_model_menu(&node_identity.peer_id.to_string())?;
    Ok(())
}

pub(super) async fn handle_models_search(query: &str) {
    print_banner("IaMine — Buscar en HF");
    println!("🔍 Buscando '{}' en HuggingFace...", query);

    match iamine_models::HuggingFaceSearch::search_gguf_models(query, 10).await {
        Ok(models) => {
            let filtered = iamine_models::HuggingFaceSearch::filter_suitable(models);

            if filtered.is_empty() {
                println!("❌ Sin modelos encontrados para '{}'", query);
            } else {
                println!("\n📦 {} modelos encontrados:\n", filtered.len());
                println!("{:<50} {:>12} {:>8}", "Modelo", "Downloads", "Likes");
                println!("{}", "─".repeat(72));

                for m in &filtered {
                    let name = if m.id.len() > 48 {
                        format!("{}…", &m.id[..47])
                    } else {
                        m.id.clone()
                    };
                    println!("{:<50} {:>12} {:>8}", name, m.downloads, m.likes);
                }

                println!("\n💡 Descarga:");
                println!("   iamine-node models download <model_id>");
            }
        }
        Err(e) => println!("❌ Error: {}", e),
    }
}

pub(super) async fn handle_models_download(model_id: &str) {
    print_banner("IaMine — Descargar Modelo");
    let installer = ModelInstaller::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    match installer.install(model_id, "local", Some(tx)).await {
        InstallResult::Installed(id) => {
            println!("✅ Modelo {} instalado en ~/.iamine/models/", id)
        }
        InstallResult::AlreadyExists(id) => println!("ℹ️  Modelo {} ya está instalado", id),
        InstallResult::InsufficientStorage {
            needed_gb,
            available_gb,
        } => println!(
            "❌ Espacio insuficiente: necesita {:.1} GB, disponible {:.1} GB",
            needed_gb, available_gb
        ),
        InstallResult::DownloadFailed(e) => println!("❌ Descarga fallida: {}", e),
        InstallResult::ValidationFailed(e) => println!("❌ Validación fallida: {}", e),
    }
}

pub(super) fn handle_models_remove(model_id: &str) -> Result<(), Box<dyn Error>> {
    print_banner("IaMine — Remove Model");
    let installer = ModelInstaller::new();
    installer.remove(model_id)?;
    let remaining = ModelStorage::new().list_local_models();
    println!("📦 Modelos locales restantes: {}", remaining.join(", "));
    Ok(())
}

pub(super) fn handle_semantic_eval() -> Result<(), Box<dyn Error>> {
    print_banner("IaMine — Semantic Eval");

    let report = evaluate_default_dataset()
        .map_err(|e| format!("No se pudo cargar semantic_dataset.json: {}", e))?;

    println!("📊 Accuracy: {:.1}%", report.accuracy * 100.0);
    println!("📉 Fallback rate: {:.1}%", report.fallback_rate * 100.0);
    println!(
        "⚠️  Low-confidence rate: {:.1}%",
        report.low_confidence_rate * 100.0
    );
    println!("🧪 Total prompts: {}", report.total);
    println!("❌ Error cases: {}", report.error_cases.len());

    if !report.error_cases.is_empty() {
        println!("\nError breakdown:");
        for error in &report.error_cases {
            println!(
                "- prompt='{}' expected={} predicted={} secondary(expected={:?}, predicted={:?}) style(expected={:?}, predicted={:?}) context(expected={:?}, predicted={}) normalize(expected={}, predicted={}) confidence={:.2} fallback={}",
                error.prompt,
                prompt_task_label(error.expected_task),
                prompt_task_label(error.predicted_task),
                error.expected_secondary,
                error.predicted_secondary,
                error.expected_output_style,
                error.predicted_output_style,
                error.expected_requires_context,
                error.predicted_requires_context,
                error.expected_normalize,
                error.predicted_normalize,
                error.confidence,
                error.fallback_applied
            );
        }
    }

    Ok(())
}

pub(super) fn handle_regression_run() -> Result<(), Box<dyn Error>> {
    print_banner("IaMine — Regression Runner");

    let report = run_default_regression_suite()?;
    println!("🧪 Versions covered: {}", report.total_versions);
    println!("🧪 Total prompts: {}", report.total_prompts);
    println!("❌ Total failures: {}", report.total_failures);

    for version in &report.version_results {
        println!(
            "- {}: {}/{} passed",
            version.version, version.passed, version.total
        );
        for failure in &version.failures {
            println!(
                "  prompt='{}' expected={} predicted={} normalize(expected={}, predicted={})",
                failure.prompt,
                prompt_task_label(failure.expected_task),
                prompt_task_label(failure.predicted_task),
                failure.expected_normalize,
                failure.predicted_normalize
            );
        }
    }

    if !report.passed() {
        return Err("Regression suite detecto fallos".into());
    }

    Ok(())
}

pub(super) fn handle_check_code() -> Result<(), Box<dyn Error>> {
    print_banner("IaMine — Code Quality");

    let report = run_code_quality_checks()?;
    println!("✅ fmt passed: {}", report.format_passed);
    println!("✅ clippy passed: {}", report.clippy_passed);
    println!("⚠️  warnings: {}", report.warning_count);
    println!("🚦 minor gate pass: {}", report.passed_for_minor());

    for command in &report.commands {
        println!(
            "- {} => success={} warnings={}",
            command.name, command.success, command.warning_count
        );
        if !command.output_excerpt.is_empty() {
            println!("{}", command.output_excerpt);
        }
    }

    if !report.passed_for_minor() {
        return Err("Code quality checks fallaron".into());
    }

    Ok(())
}

pub(super) fn handle_check_security() -> Result<(), Box<dyn Error>> {
    print_banner("IaMine — Security Check");

    let report = run_security_checks()?;
    println!("✅ security passed: {}", report.passed);
    println!("⚠️  warnings: {}", report.warning_count);
    println!("❌ errors: {}", report.error_count);

    for finding in &report.findings {
        println!(
            "- {:?} {}:{} {}",
            finding.severity, finding.path, finding.line, finding.message
        );
    }

    if !report.passed {
        return Err("Security checks detectaron hallazgos bloqueantes".into());
    }

    Ok(())
}

pub(super) fn handle_validate_release() -> Result<(), Box<dyn Error>> {
    print_banner("IaMine — Release Validation");

    let report = run_release_validation()?;
    println!("🏷️  Version: {}", current_release_version());
    println!("✅ tests_passed: {}", report.quality.tests_passed);
    println!("✅ regression_passed: {}", report.quality.regression_passed);
    println!("✅ security_passed: {}", report.quality.security_passed);
    println!("✅ code_clean_passed: {}", report.quality.code_clean_passed);
    println!("🚦 release_blocked: {}", report.quality.blocks_release());
    println!("⚠️  code warnings: {}", report.code_quality.warning_count);
    println!("⚠️  security warnings: {}", report.security.warning_count);

    if let Some(semantic_eval) = &report.semantic_eval {
        println!(
            "🧠 semantic accuracy: {:.1}% (errors={})",
            semantic_eval.accuracy * 100.0,
            semantic_eval.error_cases.len()
        );
    }

    if report.quality.blocks_release() {
        return Err("Release bloqueada por quality gates".into());
    }

    Ok(())
}

pub(super) fn handle_capabilities() {
    print_banner("IaMine — Node Capabilities");

    let node_identity = NodeIdentity::load_or_create();
    let caps = ModelNodeCapabilities::detect(&node_identity.peer_id.to_string());
    caps.display();

    let runnable = iamine_models::runnable_models(&caps);
    println!("\n📋 Modelos ejecutables según hardware:");
    for mid in &runnable {
        let installed = caps.has_model(mid);
        let icon = if installed { "✅" } else { "⬜" };
        let req = ModelRequirements::for_model(mid).unwrap();
        println!(
            "   {} {} (min {}GB RAM, {}GB storage{})",
            icon,
            mid,
            req.min_ram_gb,
            req.min_storage_gb,
            if req.requires_gpu {
                ", GPU requerida"
            } else {
                ""
            }
        );
    }
}

pub(super) fn handle_models_recommend() {
    let node_identity = NodeIdentity::load_or_create();
    let caps = ModelNodeCapabilities::detect(&node_identity.peer_id.to_string());
    let benchmark = NodeBenchmark::run();

    let profile = AutoProvisionProfile {
        cpu_score: benchmark.cpu_score as u64,
        ram_gb: caps.ram_gb,
        gpu_available: benchmark.gpu_available,
        storage_available_gb: caps.storage_available_gb,
    };

    let provision = ModelAutoProvision::new(ModelRegistry::new(), ModelStorage::new());
    let recommended = provision.recommend_for_empty_node(&profile);

    println!("Compatible models for this node:");
    if recommended.is_empty() {
        println!("(none)");
    } else {
        for model in recommended {
            println!("{}", model.id);
        }
    }
}
