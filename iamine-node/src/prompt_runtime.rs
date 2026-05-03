use super::*;

pub(super) fn prompt_task_label(task_type: PromptTaskType) -> &'static str {
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

pub(super) fn exact_subtype_label(exact_subtype: PromptExactSubtype) -> &'static str {
    match exact_subtype {
        PromptExactSubtype::Integer => "Integer",
        PromptExactSubtype::DecimalSequence => "DecimalSequence",
        PromptExactSubtype::Sequence => "Sequence",
    }
}

pub(super) fn task_requires_validation(task_type: PromptTaskType) -> bool {
    matches!(
        task_type,
        PromptTaskType::ExactMath | PromptTaskType::StructuredList | PromptTaskType::Deterministic
    )
}

pub(super) fn with_task_guard(prompt: &str, task_type: PromptTaskType, retry: bool) -> String {
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

#[derive(Clone)]
pub(super) struct PromptResolution {
    pub(super) profile: PromptProfile,
    pub(super) candidate_models: Vec<String>,
    pub(super) selected_model: String,
    pub(super) semantic_prompt: String,
    pub(super) validation: SemanticValidationResult,
}

pub(super) fn resolve_policy_for_prompt(
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

pub(super) fn resolve_output_policy(
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

pub(super) fn record_semantic_feedback(prompt: &str, validation: &SemanticValidationResult) {
    let engine = SemanticFeedbackEngine::default();
    if let Err(error) = engine.append_from_validation(prompt, validation) {
        eprintln!("[Feedback] Logging failed: {}", error);
    } else {
        println!(
            "[Feedback] Logged: {}",
            default_semantic_log_path().display()
        );
    }
}

fn prompt_language_label(language: PromptLanguage) -> &'static str {
    match language {
        PromptLanguage::English => "English",
        PromptLanguage::Spanish => "Spanish",
        PromptLanguage::Unknown => "Unknown",
    }
}

fn prompt_complexity_label(complexity: PromptComplexityLevel) -> &'static str {
    match complexity {
        PromptComplexityLevel::Low => "Low",
        PromptComplexityLevel::Medium => "Medium",
        PromptComplexityLevel::High => "High",
    }
}

fn prompt_output_style_label(output_style: PromptOutputStyle) -> &'static str {
    match output_style {
        PromptOutputStyle::Exact => "Exact",
        PromptOutputStyle::Explanatory => "Explanatory",
        PromptOutputStyle::Structured => "Structured",
        PromptOutputStyle::Generative => "Generative",
        PromptOutputStyle::Hybrid => "Hybrid",
    }
}

fn prompt_deterministic_level_label(level: PromptDeterministicLevel) -> &'static str {
    match level {
        PromptDeterministicLevel::High => "High",
        PromptDeterministicLevel::Medium => "Medium",
        PromptDeterministicLevel::Low => "Low",
    }
}

fn prompt_domain_label(domain: Option<PromptDomain>) -> &'static str {
    match domain {
        Some(PromptDomain::Math) => "Math",
        Some(PromptDomain::Physics) => "Physics",
        Some(PromptDomain::Business) => "Business",
        Some(PromptDomain::Philosophy) => "Philosophy",
        Some(PromptDomain::Code) => "Code",
        Some(PromptDomain::General) | None => "General",
    }
}

fn log_semantic_decision(decision: &SemanticRoutingDecision) {
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

fn log_semantic_validation(validation: &SemanticValidationResult) {
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

fn prompt_requests_decimal_sequence(prompt: &str) -> bool {
    let lower = prompt.to_lowercase();
    ["pi", "π", "digit", "digits", "digito", "digitos", "dígitos"]
        .iter()
        .any(|needle| lower.contains(needle))
}
