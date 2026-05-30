use crate::prompt_routing_hints::{
    detect_deterministic_level, detect_domain, detect_output_style, fallback_task_type,
};
use crate::prompt_semantic_signals::collect_signals;
use crate::prompt_task_detection::{
    detect_secondary_tasks, determine_primary_task, is_context_dependent_prompt,
    is_multi_intent_prompt,
};
use crate::task_analyzer::{detect_task_type, TaskType};
use serde::{Deserialize, Serialize};

pub const CONFIDENCE_THRESHOLD: f32 = 0.6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Language {
    English,
    Spanish,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Complexity {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputStyle {
    Exact,
    Explanatory,
    Structured,
    Generative,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeterministicLevel {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Domain {
    Math,
    Physics,
    Business,
    Philosophy,
    Code,
    General,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticProfile {
    pub primary_task: TaskType,
    pub secondary_tasks: Vec<TaskType>,
    pub domain: Option<Domain>,
    pub output_style: OutputStyle,
    pub requires_context: bool,
    pub deterministic_level: DeterministicLevel,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptProfile {
    pub language: Language,
    pub complexity: Complexity,
    pub length: usize,
    pub task_type: TaskType,
    pub confidence: f32,
    pub semantic: SemanticProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalKind {
    KeywordMatch,
    PatternMatch,
    Ambiguity,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signal {
    pub kind: SignalKind,
    pub strength: f32,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticRoutingDecision {
    pub profile: PromptProfile,
    pub original_task_type: TaskType,
    pub fallback_applied: bool,
    pub signals: Vec<Signal>,
}

pub fn analyze_prompt(prompt: &str) -> PromptProfile {
    analyze_prompt_semantics(prompt).profile
}

pub fn analyze_prompt_semantics(prompt: &str) -> SemanticRoutingDecision {
    analyze_prompt_semantics_with_context(prompt, false)
}

pub fn analyze_prompt_semantics_with_context(
    prompt: &str,
    has_context: bool,
) -> SemanticRoutingDecision {
    let trimmed = prompt.trim();
    let lower = format!(" {} ", trimmed.to_lowercase());
    let words = trimmed.split_whitespace().count();
    let chars = trimmed.chars().count();

    let language = detect_language(&lower);
    let complexity = detect_complexity(trimmed, &lower, words, chars);
    let detected_task_type = detect_task_type(trimmed);
    let requires_context = is_context_dependent_prompt(&lower);
    let original_task_type =
        determine_primary_task(trimmed, &lower, detected_task_type, requires_context);
    let secondary_tasks =
        detect_secondary_tasks(trimmed, &lower, original_task_type, requires_context);
    let domain = detect_domain(trimmed, &lower, original_task_type, &secondary_tasks);
    let signals = collect_signals(
        trimmed,
        &lower,
        original_task_type,
        &secondary_tasks,
        requires_context,
        has_context,
        words,
        chars,
    );
    let confidence = estimate_confidence(trimmed, &signals);

    let fallback_applied = (requires_context && !has_context)
        || (confidence < CONFIDENCE_THRESHOLD && original_task_type != TaskType::General);
    let final_task_type = if requires_context && !has_context {
        TaskType::General
    } else if fallback_applied {
        fallback_task_type(
            trimmed,
            &lower,
            original_task_type,
            &secondary_tasks,
            complexity,
        )
    } else {
        original_task_type
    };
    let final_secondary_tasks = if fallback_applied {
        Vec::new()
    } else {
        secondary_tasks
    };
    let final_output_style = detect_output_style(final_task_type, &final_secondary_tasks);
    let final_deterministic_level =
        detect_deterministic_level(final_task_type, &final_secondary_tasks, final_output_style);

    let semantic = SemanticProfile {
        primary_task: final_task_type,
        secondary_tasks: final_secondary_tasks,
        domain,
        output_style: final_output_style,
        requires_context,
        deterministic_level: final_deterministic_level,
        confidence,
    };

    let profile = PromptProfile {
        language,
        complexity,
        length: chars,
        task_type: semantic.primary_task,
        confidence,
        semantic,
    };

    SemanticRoutingDecision {
        profile,
        original_task_type,
        fallback_applied,
        signals,
    }
}

pub fn estimate_confidence(prompt: &str, signals: &[Signal]) -> f32 {
    let words = prompt.split_whitespace().count();
    let mut score = if words >= 4 { 0.52 } else { 0.44 };

    if prompt.chars().any(|c| c.is_ascii_digit()) && prompt.chars().any(|c| c.is_alphabetic()) {
        score += 0.04;
    }

    for signal in signals {
        let strength = signal.strength.clamp(0.0, 1.0);
        match signal.kind {
            SignalKind::KeywordMatch => score += 0.18 * strength,
            SignalKind::PatternMatch => score += 0.24 * strength,
            SignalKind::Ambiguity => score -= 0.20 * strength,
            SignalKind::Conflict => score -= 0.24 * strength,
        }
    }

    if words <= 1 {
        score -= 0.06;
    }

    score.clamp(0.0, 1.0)
}

fn detect_language(lower: &str) -> Language {
    let spanish_hits = [
        " que ",
        " explica",
        " explicame",
        " dame",
        " lista",
        " listalos",
        " enumera",
        " escribe",
        " primeros",
        " digitos",
        " dígitos",
        " por que",
        " como ",
        " agujero",
        " negro",
        " relatividad",
        " teoria",
        " resumen",
        " derivada",
        " integral",
        " ecuación",
        " ecuacion",
        " matriz",
        " matrices",
        " calcula",
        " evalúa",
        " evalua",
        " resuelve",
        " filosofía",
        " filosoficamente",
        " filosofía",
        " español",
        " espanol",
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    let english_hits = [
        " what ",
        " explain",
        " list",
        " write",
        " first ",
        " digits",
        " why ",
        " how ",
        " gravity",
        " theory",
        " black hole",
        " summarize",
        " summary",
        " derivative",
        " integral",
        " matrix",
        " function",
        " philosophy",
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    if spanish_hits > english_hits && spanish_hits > 0 {
        Language::Spanish
    } else if english_hits > spanish_hits && english_hits > 0 {
        Language::English
    } else {
        Language::Unknown
    }
}

fn detect_complexity(_trimmed: &str, lower: &str, words: usize, chars: usize) -> Complexity {
    let conceptual_hits = [
        " explain",
        " theory",
        " why ",
        " how ",
        " relatividad",
        " agujero negro",
        " black hole",
        " compare",
        " difference",
        " concept",
        " porque",
        " por que",
        " como funciona",
        " resumen",
        " summarize",
        " derivada",
        " integral",
        " ecuación",
        " ecuacion",
        " matriz",
        " matrices",
        " derivative",
        " matrix",
        " ejemplo",
        " example",
        " ejercicio",
        " exercise",
        " negocio",
        " ideas",
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    if chars >= 80 || words >= 14 || conceptual_hits >= 2 || is_multi_intent_prompt(lower) {
        Complexity::High
    } else if chars >= 25 || words >= 5 || conceptual_hits >= 1 {
        Complexity::Medium
    } else {
        Complexity::Low
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_detection_spanish() {
        let profile = analyze_prompt("explica la teoria de la relatividad");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.semantic.primary_task, TaskType::Conceptual);
        assert!(profile.confidence >= 0.6);
    }

    #[test]
    fn test_summarization_detection() {
        let profile = analyze_prompt("genera un resumen de la relatividad");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.semantic.primary_task, TaskType::Summarization);
        assert!(profile.confidence >= 0.6);
    }

    #[test]
    fn test_pi_digits_classification() {
        let profile = analyze_prompt("dame los primeros 100 digitos de pi");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.semantic.primary_task, TaskType::ExactMath);
    }

    #[test]
    fn test_complexity_detection() {
        let low = analyze_prompt("What is 2+2?");
        let high = analyze_prompt(
            "Explain the theory of relativity and why space-time bends around massive objects.",
        );

        assert_eq!(low.complexity, Complexity::Low);
        assert_eq!(high.complexity, Complexity::High);
    }

    #[test]
    fn test_confidence_high() {
        let decision = analyze_prompt_semantics("6*9");
        assert_eq!(decision.profile.semantic.primary_task, TaskType::ExactMath);
        assert!(decision.profile.confidence >= 0.6);
        assert!(!decision.fallback_applied);
    }

    #[test]
    fn test_confidence_low() {
        let decision = analyze_prompt_semantics("Java");
        assert_eq!(decision.profile.semantic.primary_task, TaskType::General);
        assert!(decision.profile.confidence < 0.6);
    }

    #[test]
    fn test_fallback_trigger() {
        let decision = analyze_prompt_semantics("solve");
        assert!(decision.profile.confidence < 0.6);
        assert!(decision.fallback_applied);
        assert_eq!(decision.profile.semantic.primary_task, TaskType::General);
    }

    #[test]
    fn test_multi_intent_detection() {
        let decision = analyze_prompt_semantics("dame un ejemplo de E=mc2 y un ejercicio");
        assert_eq!(decision.profile.semantic.primary_task, TaskType::Conceptual);
        assert!(decision
            .profile
            .semantic
            .secondary_tasks
            .contains(&TaskType::Generative));
        assert!(decision
            .profile
            .semantic
            .secondary_tasks
            .contains(&TaskType::Math));
        assert_eq!(decision.profile.semantic.output_style, OutputStyle::Hybrid);
        assert_eq!(decision.profile.semantic.domain, Some(Domain::Physics));
    }

    #[test]
    fn test_hybrid_prompt_classification() {
        let decision = analyze_prompt_semantics("explica y calcula 2+2");
        assert_eq!(decision.profile.semantic.primary_task, TaskType::Conceptual);
        assert!(decision
            .profile
            .semantic
            .secondary_tasks
            .contains(&TaskType::ExactMath));
        assert_eq!(decision.profile.semantic.output_style, OutputStyle::Hybrid);
    }

    #[test]
    fn test_context_required_detection() {
        let decision = analyze_prompt_semantics("complementa la respuesta anterior");
        assert!(decision.profile.semantic.requires_context);
        assert!(decision.fallback_applied);
        assert_eq!(decision.profile.semantic.primary_task, TaskType::General);
    }

    #[test]
    fn test_ideation_classification() {
        let decision = analyze_prompt_semantics("dame 3 ideas de negocio de IA");
        assert_eq!(decision.profile.semantic.primary_task, TaskType::Generative);
        assert!(decision
            .profile
            .semantic
            .secondary_tasks
            .contains(&TaskType::StructuredList));
        assert_eq!(decision.profile.semantic.output_style, OutputStyle::Hybrid);
        assert_eq!(decision.profile.semantic.domain, Some(Domain::Business));
    }

    #[test]
    fn test_philosophical_prompt() {
        let decision = analyze_prompt_semantics("Filosoficamente hablando que es dios?");
        assert_eq!(decision.profile.semantic.primary_task, TaskType::Conceptual);
        assert_eq!(decision.profile.semantic.domain, Some(Domain::Philosophy));
    }

    #[test]
    fn prompt_analyzer_table_regression_current_outputs() {
        let cases = [
            (
                "6*9",
                TaskType::ExactMath,
                OutputStyle::Exact,
                Some(Domain::Math),
                false,
            ),
            (
                "Java",
                TaskType::General,
                OutputStyle::Explanatory,
                Some(Domain::General),
                false,
            ),
            (
                "dame 3 ideas de negocio de IA",
                TaskType::Generative,
                OutputStyle::Hybrid,
                Some(Domain::Business),
                false,
            ),
            (
                "complementa la respuesta anterior",
                TaskType::General,
                OutputStyle::Explanatory,
                Some(Domain::General),
                true,
            ),
        ];

        for (prompt, task_type, output_style, domain, fallback_applied) in cases {
            let decision = analyze_prompt_semantics(prompt);
            assert_eq!(
                decision.profile.semantic.primary_task, task_type,
                "{prompt}"
            );
            assert_eq!(
                decision.profile.semantic.output_style, output_style,
                "{prompt}"
            );
            assert_eq!(decision.profile.semantic.domain, domain, "{prompt}");
            assert_eq!(decision.fallback_applied, fallback_applied, "{prompt}");
        }
    }

    #[test]
    fn prompt_analyzer_no_runtime_side_effects() {
        let first = analyze_prompt_semantics("explica y calcula 2+2");
        let second = analyze_prompt_semantics("explica y calcula 2+2");

        assert_eq!(first, second);
    }
}
