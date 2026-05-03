use super::*;

#[derive(Clone)]
pub(super) enum InferenceRuntime {
    Engine(Arc<RealInferenceEngine>),
    Daemon(PathBuf),
}

fn local_inference_timeout_ms(model_id: &str) -> u64 {
    AttemptTimeoutPolicy::from_model_and_node(model_id, None).timeout_ms
}

pub(super) async fn run_local_inference_with_validation(
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

pub(super) async fn run_local_inference_with_timeout(
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
