use super::*;
use crate::backend_runtime::{choose_inference_runtime, InferenceBackendState};
use crate::local_inference_runtime::{run_local_inference_with_validation, InferenceRuntime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InferModeOutcome {
    CompletedLocally,
    ContinueDistributed,
}

pub(super) async fn handle_infer_mode(
    prompt: &str,
    model_id: Option<&str>,
    max_tokens_override: Option<u32>,
    control_flags: InferenceControlFlags,
) -> Result<InferModeOutcome, Box<dyn Error>> {
    println!("╔══════════════════════════════════╗");
    println!("║      IaMine — Inference          ║");
    println!("╚══════════════════════════════════╝\n");

    let registry = ModelRegistry::new();
    let storage = ModelStorage::new();
    let local_models = storage.list_local_models();
    let resolution = resolve_policy_for_prompt(prompt, model_id, &local_models);
    let profile = resolution.profile.clone();
    let candidate_models = resolution.candidate_models.clone();
    let selected_model = resolution.selected_model.clone();
    let semantic_prompt = resolution.semantic_prompt.clone();
    let output_policy = resolve_output_policy(&profile, &semantic_prompt, max_tokens_override);

    println!(
        "[OutputPolicy] max_tokens: {} (reason: {})",
        output_policy.max_tokens, output_policy.reason
    );
    if control_flags.force_network || control_flags.no_local || control_flags.prefer_local {
        println!(
            "[InferenceControl] force_network={} no_local={} prefer_local={}",
            control_flags.force_network, control_flags.no_local, control_flags.prefer_local
        );
    }

    if control_flags.should_use_local(storage.has_model(&selected_model)) {
        println!("🤖 Modelo: {}", selected_model);
        println!("💬 Prompt: {}\n", prompt);

        let model_desc = registry
            .get(&selected_model)
            .ok_or_else(|| format!("Modelo {} no encontrado en registry", selected_model))?;

        let backend_state = InferenceBackendState::from_args(&std::env::args().collect::<Vec<_>>());
        let base_engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));
        let runtime = choose_inference_runtime(&backend_state, Arc::clone(&base_engine)).await?;
        let runtime = if let InferenceRuntime::Engine(engine) = runtime {
            engine.load_model(&selected_model, &model_desc.hash)?;
            InferenceRuntime::Engine(engine)
        } else {
            runtime
        };
        let result = run_local_inference_with_validation(
            runtime,
            "infer-local-001".to_string(),
            selected_model,
            semantic_prompt,
            profile.task_type,
            output_policy.max_tokens as u32,
            0.7,
        )
        .await?;
        record_semantic_feedback(prompt, &resolution.validation);
        print_inference_summary(&result);
        return Ok(InferModeOutcome::CompletedLocally);
    }

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
    } else if model_id.is_some() {
        println!(
            "⚠️  Override {} no esta instalado localmente, intentando ruta distribuida.",
            selected_model
        );
    } else {
        println!("🤖 Modelo local preferido no disponible.");
        println!("   Candidates: {}", candidate_models.join(", "));
        println!("   Intentando ruta distribuida...\n");
    }

    Ok(InferModeOutcome::ContinueDistributed)
}

fn print_inference_summary(result: &RealInferenceResult) {
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
