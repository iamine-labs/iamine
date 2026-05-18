use crate::daemon_runtime::{daemon_is_available, daemon_socket_path, infer_via_daemon};
use crate::infer_watchdog::AttemptTimeoutPolicy;
use crate::model_display_policy::{
    exact_subtype_label, log_semantic_decision, log_semantic_validation, prompt_complexity_label,
    prompt_language_label, prompt_task_label,
};
use crate::node_modes::DebugFlags;
use iamine_models::{
    normalize_output, RealInferenceEngine, RealInferenceRequest, RealInferenceResult,
};
use iamine_network::{
    analyze_prompt_semantics, describe_output_policy, detect_exact_subtype, normalize_expression,
    record_model_metrics, validate_semantic_decision, ModelMetrics, ModelPolicyEngine,
    OutputPolicyDecision, PromptProfile, TaskType as PromptTaskType,
    ValidationResult as SemanticValidationResult,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) enum InferenceRuntime {
    Engine(Arc<RealInferenceEngine>),
    Daemon(PathBuf),
}

pub(crate) fn task_requires_validation(task_type: PromptTaskType) -> bool {
    matches!(
        task_type,
        PromptTaskType::ExactMath | PromptTaskType::StructuredList | PromptTaskType::Deterministic
    )
}

fn prompt_requests_decimal_sequence(prompt: &str) -> bool {
    let lower = prompt.to_lowercase();
    ["pi", "π", "digit", "digits", "digito", "digitos", "dígitos"]
        .iter()
        .any(|needle| lower.contains(needle))
}

pub(crate) fn with_task_guard(prompt: &str, task_type: PromptTaskType, retry: bool) -> String {
    match task_type {
        PromptTaskType::ExactMath => {
            if !retry {
                return prompt.to_string();
            }

            if prompt_requests_decimal_sequence(prompt) {
                format!(
                    "Return only the requested numeric sequence in plain text. Do not add words. Keep the decimal point attached to the digits.\n\n{}",
                    prompt
                )
            } else {
                format!(
                    "Return only the final numeric answer in plain text. Do not add words or extra symbols.\n\n{}",
                    prompt
                )
            }
        }
        PromptTaskType::StructuredList => {
            if !retry {
                return prompt.to_string();
            }

            let retry_guard = if retry {
                "Be stricter: if the task is the alphabet, return exactly: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z"
            } else {
                "Return only the full ordered list in plain text, with no missing items and no duplicates."
            };
            format!("{}\n\n{}", retry_guard, prompt)
        }
        PromptTaskType::Deterministic => {
            if !retry {
                return prompt.to_string();
            }

            let retry_guard = if retry {
                "Be stricter: return only the requested data, exactly and in order, in plain text."
            } else {
                "Return only the requested data, exactly and in order, in plain text."
            };
            format!("{}\n\n{}", retry_guard, prompt)
        }
        _ => prompt.to_string(),
    }
}

pub(crate) async fn run_local_inference_with_validation(
    runtime: InferenceRuntime,
    task_id: String,
    model_id: String,
    prompt: String,
    task_type: PromptTaskType,
    max_tokens: u32,
    temperature: f32,
) -> Result<RealInferenceResult, String> {
    let started_at = std::time::Instant::now();
    let mut last_result: Option<RealInferenceResult> = None;
    let mut final_success = false;
    let mut final_semantic_success = false;
    let mut retry_count = 0u32;

    for attempt in 0..=1 {
        retry_count = attempt as u32;
        let guarded_prompt = with_task_guard(&prompt, task_type, attempt == 1);
        let attempt_temperature = if task_requires_validation(task_type) {
            temperature.min(0.2)
        } else {
            temperature
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
        if attempt > 0 {
            print!("\n[Validator] Retrying once with stronger formatting guard...\n");
        }

        let req = RealInferenceRequest {
            task_id: task_id.clone(),
            model_id: model_id.clone(),
            prompt: guarded_prompt,
            max_tokens,
            temperature: attempt_temperature,
        };

        let result: Result<RealInferenceResult, String> = match &runtime {
            InferenceRuntime::Engine(engine) => {
                let engine_clone = Arc::clone(engine);
                let inference_handle =
                    tokio::spawn(async move { engine_clone.run_inference(req, Some(tx)).await });

                while let Some(token) = rx.recv().await {
                    print!("{}", token);
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                }

                Ok(inference_handle
                    .await
                    .map_err(|e| format!("Inference task join error: {}", e))?)
            }
            InferenceRuntime::Daemon(socket_path) => {
                drop(rx);
                let daemon_response = infer_via_daemon(socket_path, req, |token| {
                    print!("{}", token);
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                })
                .await?;
                println!(
                    "\n[Daemon] requests_handled={} model_load_ms={} actual_loads={} reuse_hits={}",
                    daemon_response.requests_handled,
                    daemon_response.model_load_ms,
                    daemon_response.actual_model_loads,
                    daemon_response.reuse_hits
                );
                Ok(daemon_response.result)
            }
        };

        let mut result = match result {
            Ok(result) => result,
            Err(error) => {
                record_model_metrics(
                    &model_id,
                    ModelMetrics::new(
                        false,
                        started_at.elapsed().as_millis() as u64,
                        false,
                        retry_count,
                    ),
                );
                return Err(error);
            }
        };
        let task_label = prompt_task_label(task_type);
        let exact_subtype = if matches!(task_type, PromptTaskType::ExactMath) {
            Some(detect_exact_subtype(&prompt, &result.output))
        } else {
            None
        };
        let exact_subtype_label = exact_subtype.map(exact_subtype_label);
        if matches!(task_type, PromptTaskType::ExactMath) {
            let (normalized_output, normalization_reason) =
                normalize_output(task_label, exact_subtype_label, &result.output);
            if let Some(reason) = normalization_reason {
                println!("\n[Normalizer] Applied: {}", reason);
                println!("[Normalizer] Output corrected");
                result.output = normalized_output;
            }
        }

        let valid = if task_requires_validation(task_type) {
            iamine_models::output_validator::validate_structure(task_label, &result.output)
                && iamine_models::output_validator::validate_exactness(
                    task_label,
                    exact_subtype_label,
                    &result.output,
                )
        } else {
            true
        };

        if task_requires_validation(task_type) {
            println!("\n[Validator] Output valid: {}", valid);
        }
        if task_requires_validation(task_type) && !result.output.trim().is_empty() {
            println!("[Validator] Final output:\n{}", result.output);
        }

        final_semantic_success = if task_requires_validation(task_type) {
            valid
        } else {
            true
        };
        final_success = result.success
            && final_semantic_success
            && result.error.is_none()
            && !result.output.trim().is_empty();
        let should_retry = task_requires_validation(task_type) && !valid && attempt == 0;
        last_result = Some(result);
        if !should_retry {
            break;
        }
    }

    if let Some(result) = last_result {
        record_model_metrics(
            &model_id,
            ModelMetrics::new(
                final_success,
                started_at.elapsed().as_millis() as u64,
                final_semantic_success,
                retry_count,
            ),
        );
        Ok(result)
    } else {
        record_model_metrics(
            &model_id,
            ModelMetrics::new(
                false,
                started_at.elapsed().as_millis() as u64,
                false,
                retry_count,
            ),
        );
        Err("Inference returned no result".to_string())
    }
}

pub(crate) async fn choose_inference_runtime() -> Option<InferenceRuntime> {
    let socket = daemon_socket_path();
    if daemon_is_available(&socket).await {
        println!(
            "[Daemon] Connected to persistent runtime: {}",
            socket.display()
        );
        Some(InferenceRuntime::Daemon(socket))
    } else {
        None
    }
}

#[derive(Clone)]
pub(crate) struct PromptResolution {
    pub(crate) profile: PromptProfile,
    pub(crate) candidate_models: Vec<String>,
    pub(crate) selected_model: String,
    pub(crate) semantic_prompt: String,
    pub(crate) validation: SemanticValidationResult,
}

pub(crate) fn clear_stream_state(
    token_buffer: &mut HashMap<u32, String>,
    next_token_idx: &mut u32,
    rendered_output: &mut String,
) {
    token_buffer.clear();
    *next_token_idx = 0;
    rendered_output.clear();
}

pub(crate) fn local_inference_timeout_ms(model_id: &str) -> u64 {
    AttemptTimeoutPolicy::from_model_and_node(model_id, None).timeout_ms
}

pub(crate) async fn run_local_inference_with_timeout(
    runtime: InferenceRuntime,
    task_id: String,
    model_id: String,
    prompt: String,
    task_type: PromptTaskType,
    max_tokens: u32,
    temperature: f32,
) -> Result<RealInferenceResult, String> {
    let timeout_ms = local_inference_timeout_ms(&model_id);
    match tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        run_local_inference_with_validation(
            runtime,
            task_id,
            model_id,
            prompt,
            task_type,
            max_tokens,
            temperature,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(format!("Inference exceeded timeout of {} ms", timeout_ms)),
    }
}

pub(crate) fn resolve_policy_for_prompt(
    prompt: &str,
    model_override: Option<&str>,
    available_models: &[String],
) -> PromptResolution {
    let semantic_prompt = normalize_expression(prompt).unwrap_or_else(|| prompt.to_string());
    if semantic_prompt != prompt {
        println!(
            "[Parser] Normalized expression: {} → {}",
            prompt, semantic_prompt
        );
    }

    let semantic_decision = analyze_prompt_semantics(&semantic_prompt);
    let validated = validate_semantic_decision(&semantic_prompt, semantic_decision);
    let profile = validated.decision.profile.clone();
    println!(
        "[Analyzer] Language: {}, Complexity: {}, Task: {}",
        prompt_language_label(profile.language),
        prompt_complexity_label(profile.complexity),
        prompt_task_label(profile.task_type),
    );
    log_semantic_decision(&validated.decision);
    log_semantic_validation(&validated.validation);

    if let Some(model_id) = model_override {
        println!("[Policy] Manual override: {}", model_id);
        return PromptResolution {
            profile,
            candidate_models: vec![model_id.to_string()],
            selected_model: model_id.to_string(),
            semantic_prompt,
            validation: validated.validation,
        };
    }

    let policy = ModelPolicyEngine::default();
    let candidates = policy.candidate_models(&profile);
    let decision = if available_models.is_empty() {
        policy.select_model_decision(&profile)
    } else {
        policy.select_model_decision_from_available(&profile, available_models)
    };
    println!(
        "[Policy] Selected model: {} (reason: {})",
        decision.model, decision.reason
    );
    PromptResolution {
        profile,
        candidate_models: candidates,
        selected_model: decision.model,
        semantic_prompt,
        validation: validated.validation,
    }
}

pub(crate) fn resolve_output_policy(
    profile: &PromptProfile,
    prompt: &str,
    max_tokens_override: Option<u32>,
) -> OutputPolicyDecision {
    if let Some(override_value) = max_tokens_override {
        let clamped = override_value.clamp(64, 2048) as usize;
        return OutputPolicyDecision {
            max_tokens: clamped,
            reason: "cli override".to_string(),
        };
    }

    describe_output_policy(profile, prompt)
}

pub(crate) fn debug_task_log(
    flags: DebugFlags,
    peer_id: &str,
    cluster_id: Option<&str>,
    model_id: &str,
    task_id: &str,
    attempt_id: &str,
    message: &str,
) {
    if flags.tasks {
        println!(
            "[DebugTasks] peer_id={} cluster_id={} model_id={} task_id={} attempt_id={} {}",
            peer_id,
            cluster_id.unwrap_or("-"),
            model_id,
            task_id,
            attempt_id,
            message
        );
    }
}

pub(crate) fn debug_scheduler_log(flags: DebugFlags, message: impl AsRef<str>) {
    if flags.scheduler {
        println!("[DebugScheduler] {}", message.as_ref());
    }
}

pub(crate) fn debug_network_log(flags: DebugFlags, message: impl AsRef<str>) {
    if flags.network {
        println!("[DebugNetwork] {}", message.as_ref());
    }
}

pub(crate) fn mock_real_inference_result(
    task_id: String,
    model_id: String,
    prompt: String,
    max_tokens: u32,
) -> RealInferenceResult {
    let started = std::time::Instant::now();
    let output = if prompt.trim().is_empty() {
        "[mock] empty prompt".to_string()
    } else {
        format!("[mock:{}] {}", model_id, prompt)
    };
    let tokens = output
        .split_whitespace()
        .count()
        .min(max_tokens as usize)
        .max(1) as u32;
    RealInferenceResult::success(
        task_id,
        model_id,
        output,
        tokens,
        false,
        started.elapsed().as_millis() as u64,
        "mock".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use iamine_network::analyze_prompt;

    #[test]
    fn infer_runtime_preserves_default_infer_guard() {
        assert_eq!(
            with_task_guard("What is 2+2?", PromptTaskType::ExactMath, false),
            "What is 2+2?"
        );
    }

    #[test]
    fn infer_runtime_preserves_retry_guard_for_exact_math() {
        let guarded = with_task_guard("6x9", PromptTaskType::ExactMath, true);

        assert!(guarded.contains("Return only the final numeric answer"));
        assert!(guarded.ends_with("\n\n6x9"));
    }

    #[test]
    fn infer_runtime_preserves_output_policy_override_clamp() {
        let profile = analyze_prompt("What is 2+2?");
        let decision = resolve_output_policy(&profile, "What is 2+2?", Some(10));

        assert_eq!(decision.max_tokens, 64);
        assert_eq!(decision.reason, "cli override");
    }

    #[test]
    fn infer_runtime_mock_result_is_controlled_success() {
        let result = mock_real_inference_result(
            "task-1".to_string(),
            "tinyllama-1b".to_string(),
            "hello".to_string(),
            8,
        );

        assert!(result.success);
        assert_eq!(result.task_id, "task-1");
        assert_eq!(result.model_id, "tinyllama-1b");
        assert!(result.output.contains("[mock:tinyllama-1b] hello"));
    }
}
