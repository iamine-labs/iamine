use super::*;
use crate::backend_runtime::{choose_inference_runtime, InferenceBackendState};
use crate::local_inference_runtime::{run_local_inference_with_validation, InferenceRuntime};

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

pub(super) async fn handle_test_inference(prompt: &str) -> Result<(), Box<dyn Error>> {
    println!("╔══════════════════════════════════╗");
    println!("║    IaMine — Test Inference       ║");
    println!("╚══════════════════════════════════╝\n");

    let _hw = HardwareAcceleration::detect();
    let registry = ModelRegistry::new();
    let storage = ModelStorage::new();

    let model_id = ["tinyllama-1b", "llama3-3b", "mistral-7b"]
        .iter()
        .find(|&&id| storage.has_model(id))
        .copied()
        .unwrap_or("tinyllama-1b");

    println!("🤖 Modelo: {}", model_id);
    println!("💬 Prompt: {}\n", prompt);

    if !storage.has_model(model_id) {
        println!("❌ Modelo no instalado. Ejecuta primero:");
        println!("   iamine-node models download {}", model_id);
        return Ok(());
    }

    let model_desc = registry.get(model_id).unwrap();

    let resolution = resolve_policy_for_prompt(prompt, Some(model_id), &[]);
    let prompt_profile = resolution.profile.clone();
    let semantic_prompt = resolution.semantic_prompt.clone();
    let output_policy = resolve_output_policy(&prompt_profile, &semantic_prompt, None);
    println!(
        "[OutputPolicy] max_tokens: {} (reason: {})",
        output_policy.max_tokens, output_policy.reason
    );

    let req = RealInferenceRequest {
        task_id: "test-001".to_string(),
        model_id: model_id.to_string(),
        prompt: with_task_guard(prompt, prompt_profile.task_type, false),
        max_tokens: output_policy.max_tokens as u32,
        temperature: 0.7,
    };

    let backend_state = InferenceBackendState::from_args(&std::env::args().collect::<Vec<_>>());
    let base_engine = Arc::new(RealInferenceEngine::new(ModelStorage::new()));
    let runtime = choose_inference_runtime(&backend_state, Arc::clone(&base_engine)).await?;
    let runtime = if let InferenceRuntime::Engine(engine) = runtime {
        engine.load_model(model_id, &model_desc.hash)?;
        InferenceRuntime::Engine(engine)
    } else {
        runtime
    };
    let result = run_local_inference_with_validation(
        runtime,
        req.task_id,
        req.model_id,
        semantic_prompt,
        prompt_profile.task_type,
        req.max_tokens,
        req.temperature,
    )
    .await?;
    record_semantic_feedback(prompt, &resolution.validation);
    print_inference_summary(&result);
    Ok(())
}
