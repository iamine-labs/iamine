use crate::node_modes::NodeMode;
use crate::{
    benchmark::NodeBenchmark, code_quality::run_code_quality_checks,
    model_selector_cli::ModelSelectorCLI, node_identity::NodeIdentity, prompt_task_label,
    quality_gate::run_release_validation, regression_runner::run_default_regression_suite,
    security_checks::run_security_checks,
};
use iamine_models::{
    AutoProvisionProfile, InstallResult, ModelAutoProvision, ModelInstaller, ModelNodeCapabilities,
    ModelRegistry, ModelRequirements, ModelStorage, StorageConfig,
};
use iamine_network::{
    distributed_task_metrics, evaluate_default_dataset, ranked_models, task_trace,
    DistributedTaskMetrics, ModelKarma, TaskTrace,
};
use std::error::Error;

pub(crate) fn is_control_plane_mode(mode: &NodeMode) -> bool {
    matches!(
        mode,
        NodeMode::ModelsList
            | NodeMode::ModelsStats
            | NodeMode::ModelsDownload { .. }
            | NodeMode::ModelsRemove { .. }
            | NodeMode::TasksStats
            | NodeMode::TasksTrace { .. }
    )
}

pub(crate) async fn handle_pre_network_mode(
    mode: &NodeMode,
    runtime_version: &str,
) -> Result<bool, Box<dyn Error>> {
    match mode {
        NodeMode::ModelsList => {
            println!("╔══════════════════════════════════╗");
            println!("║       IaMine — Modelos           ║");
            println!("╚══════════════════════════════════╝\n");
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
            Ok(true)
        }

        NodeMode::ModelsStats => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Model Karma          ║");
            println!("╚══════════════════════════════════╝\n");
            print_model_karma_stats();
            Ok(true)
        }

        NodeMode::TasksStats => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Task Observability    ║");
            println!("╚══════════════════════════════════╝\n");
            let metrics = distributed_task_metrics();
            print_distributed_task_stats(&metrics);
            Ok(true)
        }

        NodeMode::TasksTrace { task_id } => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Task Trace           ║");
            println!("╚══════════════════════════════════╝\n");
            if let Some(trace) = task_trace(task_id) {
                print_task_trace_entry(&trace);
                return Ok(true);
            }
            Err(format!("No existe trace para task_id={}", task_id).into())
        }

        NodeMode::ModelsMenu => {
            let node_identity = NodeIdentity::load_or_create();
            ModelSelectorCLI::show_model_menu(&node_identity.peer_id.to_string())?;
            Ok(true)
        }

        NodeMode::ModelsSearch { query } => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Buscar en HF          ║");
            println!("╚══════════════════════════════════╝\n");

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
            Ok(true)
        }

        NodeMode::ModelsDownload { model_id } => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Descargar Modelo     ║");
            println!("╚══════════════════════════════════╝\n");
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
            Ok(true)
        }

        NodeMode::ModelsRemove { model_id } => {
            println!("╔══════════════════════════════════╗");
            println!("║     IaMine — Remove Model        ║");
            println!("╚══════════════════════════════════╝\n");
            let installer = ModelInstaller::new();
            installer.remove(model_id)?;
            let remaining = ModelStorage::new().list_local_models();
            println!("📦 Modelos locales restantes: {}", remaining.join(", "));
            Ok(true)
        }

        NodeMode::SemanticEval => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Semantic Eval         ║");
            println!("╚══════════════════════════════════╝\n");

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
            Ok(true)
        }

        NodeMode::RegressionRun => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Regression Runner     ║");
            println!("╚══════════════════════════════════╝\n");

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

            Ok(true)
        }

        NodeMode::CheckCode => {
            println!("╔══════════════════════════════════╗");
            println!("║     IaMine — Code Quality        ║");
            println!("╚══════════════════════════════════╝\n");

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

            Ok(true)
        }

        NodeMode::CheckSecurity => {
            println!("╔══════════════════════════════════╗");
            println!("║     IaMine — Security Check      ║");
            println!("╚══════════════════════════════════╝\n");

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

            Ok(true)
        }

        NodeMode::ValidateRelease => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Release Validation    ║");
            println!("╚══════════════════════════════════╝\n");

            let report = run_release_validation()?;
            println!("🏷️  Version: {}", runtime_version);
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

            Ok(true)
        }

        NodeMode::Capabilities => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Node Capabilities     ║");
            println!("╚══════════════════════════════════╝\n");

            let node_identity = NodeIdentity::load_or_create();
            let caps = ModelNodeCapabilities::detect(&node_identity.peer_id.to_string());
            caps.display();

            // Mostrar qué modelos puede ejecutar
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
            Ok(true)
        }

        NodeMode::ModelsRecommend => {
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
            Ok(true)
        }

        _ => Ok(false),
    }
}

fn print_model_karma_stats() {
    let registry = ModelRegistry::new();
    let mut stats = ranked_models();

    for descriptor in registry.list() {
        if !stats.iter().any(|entry| entry.model_id == descriptor.id) {
            stats.push(ModelKarma::new(descriptor.id.clone()));
        }
    }

    stats.sort_by(|left, right| {
        right
            .karma_score()
            .partial_cmp(&left.karma_score())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.model_id.cmp(&right.model_id))
    });

    println!("model | score | latency | success rate | semantic | runs");
    for entry in stats {
        println!(
            "{} | {:.3} | {:.3} | {:.1}% | {:.1}% | {}",
            entry.model_id,
            entry.karma_score(),
            entry.latency_score,
            entry.accuracy_score * 100.0,
            entry.semantic_success_rate * 100.0,
            entry.total_runs
        );
    }
}

fn print_distributed_task_stats(metrics: &DistributedTaskMetrics) {
    println!("metric | value");
    println!("total_tasks | {}", metrics.total_tasks);
    println!("failed_tasks | {}", metrics.failed_tasks);
    println!("retries_count | {}", metrics.retries_count);
    println!("fallback_count | {}", metrics.fallback_count);
    println!("late_results_count | {}", metrics.late_results_count);
    println!("avg_latency_ms | {:.1}", metrics.avg_latency_ms);
}

fn print_task_trace_entry(trace: &TaskTrace) {
    println!("task_id: {}", trace.task_id);
    println!(
        "node_history: {}",
        if trace.node_history.is_empty() {
            "-".to_string()
        } else {
            trace.node_history.join(" -> ")
        }
    );
    println!(
        "model_history: {}",
        if trace.model_history.is_empty() {
            "-".to_string()
        } else {
            trace.model_history.join(" -> ")
        }
    );
    println!("retries: {}", trace.retries);
    println!("fallbacks: {}", trace.fallbacks);
    println!("total_latency_ms: {}", trace.total_latency_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_no_runtime_start() {
        assert!(is_control_plane_mode(&NodeMode::ModelsList));
        assert!(is_control_plane_mode(&NodeMode::ModelsStats));
        assert!(is_control_plane_mode(&NodeMode::ModelsDownload {
            model_id: "llama3-3b".to_string()
        }));
        assert!(is_control_plane_mode(&NodeMode::ModelsRemove {
            model_id: "llama3-3b".to_string()
        }));
        assert!(is_control_plane_mode(&NodeMode::TasksStats));
        assert!(is_control_plane_mode(&NodeMode::TasksTrace {
            task_id: "task-1".to_string()
        }));
        assert!(!is_control_plane_mode(&NodeMode::Worker));
    }

    #[test]
    fn mode_dispatch_is_parse_only_for_help() {
        assert!(!is_control_plane_mode(&NodeMode::Help));
    }

    #[test]
    fn broadcast_regression_after_cli_dispatch_refactor() {
        assert!(!is_control_plane_mode(&NodeMode::Broadcast {
            task_type: "reverse_string".to_string(),
            data: "abc".to_string(),
        }));
    }
}
