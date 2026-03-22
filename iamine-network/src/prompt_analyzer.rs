use crate::expression_parser::normalize_expression;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptProfile {
    pub language: Language,
    pub complexity: Complexity,
    pub length: usize,
    pub task_type: TaskType,
    pub confidence: f32,
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
    let trimmed = prompt.trim();
    let lower = format!(" {} ", trimmed.to_lowercase());
    let words = trimmed.split_whitespace().count();
    let chars = trimmed.chars().count();

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
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    let language = if spanish_hits > english_hits && spanish_hits > 0 {
        Language::Spanish
    } else if english_hits > spanish_hits && english_hits > 0 {
        Language::English
    } else {
        Language::Unknown
    };

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
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    let complexity = if chars >= 80 || words >= 14 || conceptual_hits >= 2 {
        Complexity::High
    } else if chars >= 25 || words >= 5 || conceptual_hits >= 1 {
        Complexity::Medium
    } else {
        Complexity::Low
    };

    let original_task_type = detect_task_type(trimmed);
    let signals = collect_signals(trimmed, &lower, original_task_type, words, chars);
    let confidence = estimate_confidence(trimmed, &signals);
    let fallback_applied = confidence < CONFIDENCE_THRESHOLD && original_task_type != TaskType::General;
    let task_type = if fallback_applied {
        fallback_task_type(trimmed, &lower, original_task_type, complexity)
    } else {
        original_task_type
    };

    let profile = PromptProfile {
        language,
        complexity,
        length: chars,
        task_type,
        confidence,
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

fn fallback_task_type(
    trimmed: &str,
    lower: &str,
    original_task_type: TaskType,
    complexity: Complexity,
) -> TaskType {
    if trimmed.split_whitespace().count() <= 2 {
        return TaskType::General;
    }

    let reasoning_hint = contains_any(
        lower,
        &[
            "why",
            "how",
            "explica",
            "explain",
            "paso a paso",
            "step by step",
            "solve",
            "resuelve",
            "reason",
            "razona",
        ],
    );

    if matches!(complexity, Complexity::High)
        || reasoning_hint
        || matches!(original_task_type, TaskType::Reasoning | TaskType::SymbolicMath | TaskType::Conceptual)
        || (trimmed.chars().any(|c| c.is_ascii_digit()) && trimmed.split_whitespace().count() >= 4)
    {
        TaskType::Reasoning
    } else {
        TaskType::General
    }
}

fn collect_signals(
    trimmed: &str,
    lower: &str,
    task_type: TaskType,
    words: usize,
    chars: usize,
) -> Vec<Signal> {
    let raw_lower = lower.trim();
    let family_counts = [
        task_keyword_hits(raw_lower, TaskType::ExactMath),
        task_keyword_hits(raw_lower, TaskType::SymbolicMath),
        task_keyword_hits(raw_lower, TaskType::StructuredList),
        task_keyword_hits(raw_lower, TaskType::Deterministic),
        task_keyword_hits(raw_lower, TaskType::Code),
        task_keyword_hits(raw_lower, TaskType::Conceptual),
        task_keyword_hits(raw_lower, TaskType::Reasoning),
        task_keyword_hits(raw_lower, TaskType::Summarization),
        task_keyword_hits(raw_lower, TaskType::Math),
    ];

    let keyword_hits = task_keyword_hits(raw_lower, task_type);
    let pattern_strength = task_pattern_strength(trimmed, raw_lower, task_type);
    let competing_families = family_counts.into_iter().filter(|count| *count > 0).count();
    let ambiguity_strength =
        ambiguity_strength(trimmed, raw_lower, task_type, words, chars, keyword_hits, competing_families);
    let conflict_strength = conflict_strength(task_type, competing_families, keyword_hits, raw_lower);

    let mut signals = Vec::new();

    if keyword_hits > 0 {
        signals.push(Signal {
            kind: SignalKind::KeywordMatch,
            strength: (keyword_hits as f32 / 3.0).clamp(0.0, 1.0),
            note: format!("{} keyword hits for {:?}", keyword_hits, task_type),
        });
    }

    if pattern_strength > 0.0 {
        signals.push(Signal {
            kind: SignalKind::PatternMatch,
            strength: pattern_strength,
            note: format!("pattern support for {:?}", task_type),
        });
    }

    if ambiguity_strength > 0.0 {
        signals.push(Signal {
            kind: SignalKind::Ambiguity,
            strength: ambiguity_strength,
            note: "ambiguous or underspecified prompt".to_string(),
        });
    }

    if conflict_strength > 0.0 {
        signals.push(Signal {
            kind: SignalKind::Conflict,
            strength: conflict_strength,
            note: "conflicting task-family signals".to_string(),
        });
    }

    signals
}

fn task_keyword_hits(lower: &str, task_type: TaskType) -> usize {
    let keywords: &[&str] = match task_type {
        TaskType::ExactMath => &[
            "what is",
            "cuanto es",
            "cuánto es",
            "log",
            "sqrt",
            "square root",
            "pi",
            "digit",
            "digits",
            "digito",
            "digitos",
            "dígitos",
            "primeros",
            "first",
            "digits of pi",
            "digitos de pi",
            "dígitos de pi",
        ],
        TaskType::SymbolicMath => &[
            "derivada",
            "integral",
            "ecuación",
            "ecuacion",
            "matriz",
            "matrices",
            "f(x)",
            "dx",
            "dy/dx",
            "sin(",
            "cos(",
            "tan(",
            "derivative",
            "matrix",
            "function",
        ],
        TaskType::StructuredList => &[
            "abecedario",
            "alphabet",
            "enumera",
            "todas las letras",
            "all letters",
        ],
        TaskType::Deterministic => &[
            "dame",
            "escribe",
            "primeros",
            "first",
            "lista completa",
            "en orden",
            "in order",
        ],
        TaskType::Code => &["rust", "python", "javascript", "sql", "function", "script", "code"],
        TaskType::Conceptual => &[
            "explica",
            "explain",
            "por que",
            "porque",
            "why",
            "how",
            "describe",
            "teoria",
            "theory",
        ],
        TaskType::Reasoning => &["step by step", "paso a paso", "solve", "razona", "reason"],
        TaskType::Summarization => &["resume", "resumen", "haz un resumen", "genera un resumen", "summarize", "summary"],
        TaskType::Math => &["calculate", "calcula", "multiplica", "divide", "sum", "resta", "equation", "math"],
        TaskType::General => &[],
    };

    keywords.iter().filter(|needle| lower.contains(**needle)).count()
}

fn task_pattern_strength(trimmed: &str, lower: &str, task_type: TaskType) -> f32 {
    match task_type {
        TaskType::ExactMath => {
            let compact_expression = trimmed.len() <= 18
                && trimmed.chars().any(|c| c.is_ascii_digit())
                && trimmed
                    .chars()
                    .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | '^' | 'x' | 'X'));
            let normalized = normalize_expression(trimmed).is_some();
            let symbolic_function = lower.contains("sqrt(") || lower.contains("log");
            if compact_expression || normalized || symbolic_function {
                0.95
            } else {
                0.0
            }
        }
        TaskType::SymbolicMath => {
            if trimmed.contains("f(x)")
                || trimmed.contains("dx")
                || trimmed.contains("dy/dx")
                || trimmed.contains("[[")
                || lower.contains("sin(")
                || lower.contains("cos(")
                || lower.contains("tan(")
            {
                0.9
            } else {
                0.45
            }
        }
        TaskType::StructuredList => {
            if lower.contains("abecedario") || lower.contains("alphabet") {
                0.85
            } else {
                0.35
            }
        }
        TaskType::Summarization => {
            if contains_any(lower, &["resume", "resumen", "summarize", "summary"]) {
                0.8
            } else {
                0.0
            }
        }
        TaskType::Code => {
            if contains_any(lower, &["rust", "python", "javascript", "sql", "fn ", "def "]) {
                0.8
            } else {
                0.35
            }
        }
        TaskType::Reasoning => {
            if contains_any(lower, &["step by step", "paso a paso", "reason through"]) {
                0.7
            } else {
                0.25
            }
        }
        TaskType::Conceptual => {
            if contains_any(lower, &["why", "how", "explica", "explain"]) {
                0.55
            } else {
                0.2
            }
        }
        TaskType::Math | TaskType::Deterministic | TaskType::General => 0.0,
    }
}

fn ambiguity_strength(
    trimmed: &str,
    lower: &str,
    task_type: TaskType,
    words: usize,
    chars: usize,
    keyword_hits: usize,
    competing_families: usize,
) -> f32 {
    let compact_exact = matches!(task_type, TaskType::ExactMath)
        && trimmed.chars().any(|c| c.is_ascii_digit())
        && trimmed
            .chars()
            .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | '^' | 'x' | 'X' | '(' | ')'));
    let generic_only = contains_any(
        lower,
        &["solve", "resuelve", "explica", "explain", "resume", "summarize"],
    ) && words <= 2;

    let mut strength: f32 = 0.0;
    if words <= 2 && !compact_exact {
        strength += 0.35;
    }
    if chars <= 6 && !compact_exact {
        strength += 0.15;
    }
    if keyword_hits == 0 && !compact_exact {
        strength += 0.20;
    }
    if generic_only {
        strength += 0.25;
    }
    if competing_families >= 3 {
        strength += 0.10;
    }

    strength.clamp(0.0, 1.0)
}

fn conflict_strength(
    task_type: TaskType,
    competing_families: usize,
    keyword_hits: usize,
    lower: &str,
) -> f32 {
    let summarization_and_conceptual =
        contains_any(lower, &["resume", "resumen", "summarize"]) && contains_any(lower, &["explica", "explain", "why", "how"]);
    let exact_and_symbolic =
        matches!(task_type, TaskType::ExactMath) && contains_any(lower, &["f(x)", "dx", "integral", "derivada"]);

    let mut strength: f32 = 0.0;
    if competing_families >= 2 && keyword_hits <= 1 {
        strength += 0.30;
    }
    if competing_families >= 3 {
        strength += 0.20;
    }
    if summarization_and_conceptual || exact_and_symbolic {
        strength += 0.30;
    }

    strength.clamp(0.0, 1.0)
}

fn contains_any(prompt: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| prompt.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_detection_spanish() {
        let profile = analyze_prompt("explica la teoria de la relatividad");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.task_type, TaskType::Conceptual);
        assert!(profile.confidence >= 0.6);
    }

    #[test]
    fn test_summarization_detection() {
        let profile = analyze_prompt("genera un resumen de la relatividad");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.task_type, TaskType::Summarization);
        assert!(profile.confidence >= 0.6);
    }

    #[test]
    fn test_pi_digits_classification() {
        let profile = analyze_prompt("dame los primeros 100 digitos de pi");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.task_type, TaskType::ExactMath);
    }

    #[test]
    fn test_complexity_detection() {
        let low = analyze_prompt("What is 2+2?");
        let high = analyze_prompt("Explain the theory of relativity and why space-time bends around massive objects.");

        assert_eq!(low.complexity, Complexity::Low);
        assert_eq!(high.complexity, Complexity::High);
    }

    #[test]
    fn test_confidence_high() {
        let decision = analyze_prompt_semantics("6*9");
        assert_eq!(decision.profile.task_type, TaskType::ExactMath);
        assert!(decision.profile.confidence >= 0.6);
        assert!(!decision.fallback_applied);
    }

    #[test]
    fn test_confidence_low() {
        let decision = analyze_prompt_semantics("Java");
        assert_eq!(decision.profile.task_type, TaskType::General);
        assert!(decision.profile.confidence < 0.6);
    }

    #[test]
    fn test_fallback_trigger() {
        let decision = analyze_prompt_semantics("solve");
        assert_eq!(decision.original_task_type, TaskType::Reasoning);
        assert!(decision.profile.confidence < 0.6);
        assert!(decision.fallback_applied);
        assert_eq!(decision.profile.task_type, TaskType::General);
    }
}
