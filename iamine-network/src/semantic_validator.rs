use crate::prompt_analyzer::{
    DeterministicLevel, Domain, OutputStyle, PromptProfile, SemanticProfile, SemanticRoutingDecision,
    Signal, SignalKind, CONFIDENCE_THRESHOLD,
};
use crate::task_analyzer::TaskType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationResult {
    pub predicted_profile: SemanticProfile,
    pub validated_profile: SemanticProfile,
    pub confidence_before: f32,
    pub confidence_after: f32,
    pub correction_applied: bool,
    pub model_validation_used: bool,
    pub conflicts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidatedSemanticDecision {
    pub decision: SemanticRoutingDecision,
    pub validation: ValidationResult,
}

pub fn validate_semantic_decision(
    prompt: &str,
    decision: SemanticRoutingDecision,
) -> ValidatedSemanticDecision {
    validate_semantic_decision_with_context(prompt, decision, false)
}

pub fn validate_semantic_decision_with_context(
    prompt: &str,
    decision: SemanticRoutingDecision,
    has_context: bool,
) -> ValidatedSemanticDecision {
    let predicted_profile = decision.profile.semantic.clone();
    let confidence_before = predicted_profile.confidence;
    let conflicts = detect_conflicts(prompt, &predicted_profile, &decision.signals, has_context);
    let mut adjusted_profile = predicted_profile.clone();
    adjusted_profile.confidence = adjust_confidence(
        confidence_before,
        &conflicts,
        &decision.signals,
        predicted_profile.requires_context,
        has_context,
    );

    let mut validation = ValidationResult {
        predicted_profile: predicted_profile.clone(),
        validated_profile: adjusted_profile.clone(),
        confidence_before,
        confidence_after: adjusted_profile.confidence,
        correction_applied: adjusted_profile != predicted_profile,
        model_validation_used: false,
        conflicts: conflicts.clone(),
    };

    let has_signal_conflict = decision
        .signals
        .iter()
        .any(|signal| matches!(signal.kind, SignalKind::Ambiguity | SignalKind::Conflict));
    let should_model_validate =
        adjusted_profile.confidence < CONFIDENCE_THRESHOLD || !conflicts.is_empty() || has_signal_conflict;

    if should_model_validate {
        let model_validation = model_validate_with_context(prompt, &adjusted_profile, has_context);
        let mut merged_conflicts = conflicts;
        for conflict in model_validation.conflicts.iter() {
            if !merged_conflicts.contains(conflict) {
                merged_conflicts.push(conflict.clone());
            }
        }
        let mut validated_profile = model_validation.validated_profile.clone();
        if model_validation.confidence_after < validation.confidence_after
            && validated_profile == validation.validated_profile
        {
            validated_profile.confidence = validation.confidence_after;
        }

        validation = ValidationResult {
            predicted_profile,
            confidence_before,
            confidence_after: validated_profile.confidence,
            correction_applied: validated_profile != validation.predicted_profile,
            model_validation_used: true,
            validated_profile,
            conflicts: merged_conflicts,
        };
    }

    let mut updated_decision = decision;
    apply_validation(&mut updated_decision.profile, &validation.validated_profile);

    ValidatedSemanticDecision {
        decision: updated_decision,
        validation,
    }
}

pub fn model_validate(prompt: &str, profile: &SemanticProfile) -> ValidationResult {
    model_validate_with_context(prompt, profile, false)
}

fn model_validate_with_context(
    prompt: &str,
    profile: &SemanticProfile,
    has_context: bool,
) -> ValidationResult {
    let lower = format!(" {} ", prompt.trim().to_lowercase());
    let mut validated = profile.clone();
    let mut conflicts = detect_conflicts(prompt, profile, &[], has_context);

    if is_context_dependent_prompt(&lower) {
        validated.requires_context = true;
        validated.output_style = OutputStyle::Explanatory;
        validated.deterministic_level = DeterministicLevel::Low;
        if !has_context {
            validated.primary_task = TaskType::General;
            validated.secondary_tasks.clear();
        }
        validated.domain = Some(Domain::General);
        validated.confidence = profile.confidence.max(0.66);
    } else if is_hybrid_example_prompt(&lower) {
        validated.primary_task = TaskType::Conceptual;
        ensure_secondary(&mut validated.secondary_tasks, TaskType::Generative);
        ensure_secondary(&mut validated.secondary_tasks, TaskType::Math);
        validated.secondary_tasks.retain(|task| *task != TaskType::ExactMath);
        validated.domain = Some(Domain::Physics);
        validated.output_style = OutputStyle::Hybrid;
        validated.deterministic_level = DeterministicLevel::Low;
        validated.confidence = profile.confidence.max(0.74);
    } else if is_scientific_reasoning_prompt(&lower) {
        validated.primary_task = TaskType::Reasoning;
        ensure_secondary(&mut validated.secondary_tasks, TaskType::Math);
        validated.secondary_tasks.retain(|task| *task != TaskType::ExactMath);
        validated.domain = Some(Domain::Physics);
        validated.output_style = OutputStyle::Hybrid;
        validated.deterministic_level = DeterministicLevel::Medium;
        validated.confidence = profile.confidence.max(0.72);
    } else if is_ideation_prompt(&lower) {
        validated.primary_task = TaskType::Generative;
        if requests_structured_output(&lower) {
            ensure_secondary(&mut validated.secondary_tasks, TaskType::StructuredList);
            validated.output_style = OutputStyle::Hybrid;
        } else {
            validated.output_style = OutputStyle::Generative;
        }
        validated.secondary_tasks.retain(|task| *task != TaskType::Deterministic);
        validated.domain = Some(Domain::Business);
        validated.deterministic_level = DeterministicLevel::Low;
        validated.confidence = profile.confidence.max(0.78);
    } else if is_philosophical_prompt(&lower) {
        validated.primary_task = TaskType::Conceptual;
        validated.domain = Some(Domain::Philosophy);
        validated.output_style = OutputStyle::Explanatory;
        validated.deterministic_level = DeterministicLevel::Low;
        validated.secondary_tasks.retain(|task| *task != TaskType::ExactMath);
        validated.confidence = profile.confidence.max(0.76);
    } else if is_conceptual_math_prompt(prompt, &lower) {
        validated.primary_task = TaskType::Conceptual;
        ensure_secondary(&mut validated.secondary_tasks, TaskType::SymbolicMath);
        validated.secondary_tasks.retain(|task| *task != TaskType::ExactMath);
        validated.domain = Some(Domain::Math);
        validated.output_style = OutputStyle::Hybrid;
        validated.deterministic_level = DeterministicLevel::Low;
        validated.confidence = profile.confidence.max(0.68);
    } else if has_symbolic_math_markers(prompt, &lower) && matches!(profile.primary_task, TaskType::General | TaskType::Math) {
        validated.primary_task = TaskType::SymbolicMath;
        validated.secondary_tasks.retain(|task| *task != TaskType::ExactMath);
        validated.domain = Some(Domain::Math);
        validated.output_style = OutputStyle::Explanatory;
        validated.deterministic_level = DeterministicLevel::Medium;
        validated.confidence = profile.confidence.max(0.71);
    } else {
        validated.confidence = profile.confidence.max(0.61);
    }

    validated.secondary_tasks.sort_by_key(task_order_key);
    conflicts = detect_conflicts(prompt, &validated, &[], has_context)
        .into_iter()
        .fold(conflicts, |mut acc, item| {
            if !acc.contains(&item) {
                acc.push(item);
            }
            acc
        });
    validated.confidence = adjust_confidence(
        validated.confidence,
        &conflicts,
        &[],
        validated.requires_context,
        has_context,
    )
    .max(profile.confidence.min(0.95));

    ValidationResult {
        predicted_profile: profile.clone(),
        validated_profile: validated.clone(),
        confidence_before: profile.confidence,
        confidence_after: validated.confidence,
        correction_applied: &validated != profile,
        model_validation_used: true,
        conflicts,
    }
}

fn apply_validation(profile: &mut PromptProfile, validated: &SemanticProfile) {
    profile.task_type = validated.primary_task;
    profile.confidence = validated.confidence;
    profile.semantic = validated.clone();
}

fn adjust_confidence(
    base_confidence: f32,
    conflicts: &[String],
    signals: &[Signal],
    requires_context: bool,
    has_context: bool,
) -> f32 {
    let mut adjusted = base_confidence;

    for signal in signals {
        match signal.kind {
            SignalKind::Ambiguity => adjusted -= 0.04 * signal.strength.clamp(0.0, 1.0),
            SignalKind::Conflict => adjusted -= 0.06 * signal.strength.clamp(0.0, 1.0),
            SignalKind::KeywordMatch | SignalKind::PatternMatch => {}
        }
    }

    adjusted -= 0.05 * conflicts.len() as f32;

    if requires_context && !has_context {
        adjusted -= 0.08;
    }

    adjusted.clamp(0.0, 1.0)
}

fn detect_conflicts(
    prompt: &str,
    profile: &SemanticProfile,
    signals: &[Signal],
    has_context: bool,
) -> Vec<String> {
    let lower = format!(" {} ", prompt.trim().to_lowercase());
    let mut conflicts = Vec::new();

    if signals
        .iter()
        .any(|signal| matches!(signal.kind, SignalKind::Conflict))
    {
        conflicts.push("signal-conflict".to_string());
    }

    if profile.requires_context && !has_context {
        conflicts.push("missing-context".to_string());
    }

    if profile.secondary_tasks.contains(&TaskType::ExactMath) && has_symbolic_math_markers(prompt, &lower) {
        conflicts.push("symbolic-exact-overlap".to_string());
    }

    if matches!(profile.primary_task, TaskType::Deterministic | TaskType::ExactMath) && is_ideation_prompt(&lower) {
        conflicts.push("ideation-vs-deterministic".to_string());
    }

    if matches!(profile.primary_task, TaskType::General) && is_philosophical_prompt(&lower) {
        conflicts.push("philosophy-vs-general".to_string());
    }

    if matches!(profile.primary_task, TaskType::SymbolicMath) && is_conceptual_math_prompt(prompt, &lower) {
        conflicts.push("conceptual-math-vs-symbolic".to_string());
    }

    if matches!(profile.primary_task, TaskType::SymbolicMath | TaskType::ExactMath) && is_hybrid_example_prompt(&lower) {
        conflicts.push("hybrid-example-vs-math-only".to_string());
    }

    if matches!(profile.primary_task, TaskType::General) && has_symbolic_math_markers(prompt, &lower) {
        conflicts.push("math-vs-general".to_string());
    }

    conflicts.sort();
    conflicts.dedup();
    conflicts
}

fn is_context_dependent_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "continua",
            "continúa",
            "complementa",
            "expande",
            "hazlo mas simple",
            "hazlo más simple",
            "hazlo simple",
            "la respuesta anterior",
            "respuesta anterior",
            "previous answer",
            "continue",
            "expand",
            "make it simpler",
            "make it shorter",
        ],
    )
}

fn is_ideation_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "idea",
            "ideas",
            "brainstorm",
            "propon",
            "propón",
            "sugiere",
            "suggest",
            "negocio",
            "negocios",
            "startup",
        ],
    )
}

fn requests_structured_output(lower: &str) -> bool {
    contains_any(
        lower,
        &["dame 3", "dame tres", "3 ideas", "tres ideas", "lista", "list"],
    )
}

fn is_hybrid_example_prompt(lower: &str) -> bool {
    contains_any(lower, &["ejemplo", "example"])
        && contains_any(lower, &["ejercicio", "exercise"])
        && contains_any(lower, &["ecuacion", "ecuación", "emc2", "e=mc2", "math", "matem"])
}

fn is_scientific_reasoning_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "que pasaria",
            "qué pasaría",
            "what would happen",
            "agujero negro",
            "black hole",
            "fuerza gravitacional",
            "gravity",
            "gravedad",
        ],
    ) && contains_any(
        lower,
        &["calcula", "calculate", "ecuaciones", "equations", "explica", "explain"],
    )
}

fn is_conceptual_math_prompt(prompt: &str, lower: &str) -> bool {
    let explain_cues = contains_any(
        lower,
        &[
            "que es",
            "qué es",
            "explica",
            "explain",
            "como aplicarlos",
            "cómo aplicarlos",
            "how to apply",
            "vida real",
            "real life",
        ],
    );
    let math_cues = contains_any(
        lower,
        &[
            "derivada",
            "derivative",
            "limite",
            "límite",
            "limites",
            "límites",
            "integral",
            "calculo",
            "cálculo",
            "matematic",
            "mathemat",
        ],
    );

    let strong_solving_markers = prompt.contains("f(x)")
        || prompt.contains("dx")
        || prompt.contains("dy/dx")
        || prompt.contains("[[")
        || prompt.contains("]]")
        || (prompt.chars().any(|c| c.is_ascii_digit())
            && prompt
                .chars()
                .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | '^')));

    explain_cues && math_cues && !strong_solving_markers
}

fn is_philosophical_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "filosof",
            "dios",
            "justice",
            "justicia",
            "consciousness",
            "conciencia",
            "ética",
            "etica",
        ],
    )
}

fn has_symbolic_math_markers(prompt: &str, lower: &str) -> bool {
    prompt.contains("f(x)")
        || prompt.contains("dx")
        || prompt.contains("dy/dx")
        || prompt.contains("[[")
        || prompt.contains("]]")
        || contains_any(
            lower,
            &[
                "derivada",
                "integral",
                "matriz",
                "matrices",
                "sin(",
                "cos(",
                "tan(",
                "derivative",
                "matrix",
                "limit",
                "limite",
                "límite",
            ],
        )
}

fn ensure_secondary(tasks: &mut Vec<TaskType>, task: TaskType) {
    if !tasks.contains(&task) {
        tasks.push(task);
    }
}

fn task_order_key(task: &TaskType) -> usize {
    match task {
        TaskType::Conceptual => 0,
        TaskType::Reasoning => 1,
        TaskType::Math => 2,
        TaskType::ExactMath => 3,
        TaskType::SymbolicMath => 4,
        TaskType::StructuredList => 5,
        TaskType::Generative => 6,
        TaskType::Summarization => 7,
        TaskType::Deterministic => 8,
        TaskType::Code => 9,
        TaskType::General => 10,
    }
}

fn contains_any(prompt: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| prompt.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_analyzer::{Complexity, Language};

    fn semantic_profile(primary_task: TaskType) -> SemanticProfile {
        SemanticProfile {
            primary_task,
            secondary_tasks: Vec::new(),
            domain: Some(Domain::General),
            output_style: OutputStyle::Explanatory,
            requires_context: false,
            deterministic_level: DeterministicLevel::Low,
            confidence: 0.52,
        }
    }

    fn prompt_profile(primary_task: TaskType) -> PromptProfile {
        let semantic = semantic_profile(primary_task);
        PromptProfile {
            language: Language::Spanish,
            complexity: Complexity::Medium,
            length: 32,
            task_type: semantic.primary_task,
            confidence: semantic.confidence,
            semantic,
        }
    }

    #[test]
    fn test_validation_conflict() {
        let mut decision = SemanticRoutingDecision {
            profile: prompt_profile(TaskType::Deterministic),
            original_task_type: TaskType::Deterministic,
            fallback_applied: false,
            signals: vec![Signal {
                kind: SignalKind::Conflict,
                strength: 0.8,
                note: "forced conflict".to_string(),
            }],
        };
        decision.profile.semantic.secondary_tasks.push(TaskType::ExactMath);
        let validated =
            validate_semantic_decision("dame 3 ideas de negocio de IA", decision).validation;
        assert!(validated.conflicts.iter().any(|conflict| conflict == "ideation-vs-deterministic"));
    }

    #[test]
    fn test_model_validation_trigger() {
        let decision = SemanticRoutingDecision {
            profile: prompt_profile(TaskType::General),
            original_task_type: TaskType::General,
            fallback_applied: false,
            signals: vec![Signal {
                kind: SignalKind::Ambiguity,
                strength: 0.8,
                note: "ambiguous".to_string(),
            }],
        };
        let validated =
            validate_semantic_decision("Filosoficamente hablando que es dios?", decision).validation;
        assert!(validated.model_validation_used);
        assert_eq!(validated.validated_profile.primary_task, TaskType::Conceptual);
        assert_eq!(validated.validated_profile.domain, Some(Domain::Philosophy));
    }

    #[test]
    fn test_correction_tracking() {
        let decision = SemanticRoutingDecision {
            profile: prompt_profile(TaskType::General),
            original_task_type: TaskType::General,
            fallback_applied: false,
            signals: Vec::new(),
        };
        let validated = validate_semantic_decision("Que es una derivada?", decision).validation;
        assert!(validated.correction_applied);
        assert_eq!(validated.validated_profile.primary_task, TaskType::Conceptual);
        assert!(validated
            .validated_profile
            .secondary_tasks
            .contains(&TaskType::SymbolicMath));
    }
}
