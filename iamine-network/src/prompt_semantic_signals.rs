use crate::expression_parser::normalize_expression;
use crate::prompt_analyzer::{Signal, SignalKind};
use crate::prompt_task_detection::{
    contains_any, has_math_domain_markers, has_symbolic_math_markers, is_conceptual_math_prompt,
    is_hybrid_example_prompt, is_ideation_prompt, is_multi_intent_prompt, is_philosophical_prompt,
    is_scientific_reasoning_prompt, is_structured_output_request,
};
use crate::task_analyzer::TaskType;

pub(crate) fn collect_signals(
    trimmed: &str,
    lower: &str,
    primary_task: TaskType,
    secondary_tasks: &[TaskType],
    requires_context: bool,
    has_context: bool,
    words: usize,
    chars: usize,
) -> Vec<Signal> {
    let raw_lower = lower.trim();
    let family_counts = [
        task_keyword_hits(raw_lower, TaskType::ExactMath),
        task_keyword_hits(raw_lower, TaskType::SymbolicMath),
        task_keyword_hits(raw_lower, TaskType::Generative),
        task_keyword_hits(raw_lower, TaskType::StructuredList),
        task_keyword_hits(raw_lower, TaskType::Deterministic),
        task_keyword_hits(raw_lower, TaskType::Code),
        task_keyword_hits(raw_lower, TaskType::Conceptual),
        task_keyword_hits(raw_lower, TaskType::Reasoning),
        task_keyword_hits(raw_lower, TaskType::Summarization),
        task_keyword_hits(raw_lower, TaskType::Math),
    ];

    let keyword_hits = task_keyword_hits(raw_lower, primary_task);
    let pattern_strength = task_pattern_strength(
        trimmed,
        raw_lower,
        primary_task,
        secondary_tasks,
        requires_context,
    );
    let competing_families = family_counts.into_iter().filter(|count| *count > 0).count();
    let ambiguity_strength = ambiguity_strength(
        trimmed,
        raw_lower,
        primary_task,
        words,
        chars,
        keyword_hits,
        competing_families,
        requires_context,
        has_context,
    );
    let conflict_strength = conflict_strength(
        primary_task,
        secondary_tasks,
        competing_families,
        keyword_hits,
        raw_lower,
    );

    let mut signals = Vec::new();

    if keyword_hits > 0 {
        signals.push(Signal {
            kind: SignalKind::KeywordMatch,
            strength: (keyword_hits as f32 / 3.0).clamp(0.0, 1.0),
            note: format!("{} keyword hits for {:?}", keyword_hits, primary_task),
        });
    }

    if pattern_strength > 0.0 {
        signals.push(Signal {
            kind: SignalKind::PatternMatch,
            strength: pattern_strength,
            note: format!("pattern support for {:?}", primary_task),
        });
    }

    if is_multi_intent_prompt(raw_lower) && !secondary_tasks.is_empty() {
        signals.push(Signal {
            kind: SignalKind::PatternMatch,
            strength: 0.55,
            note: "multi-intent prompt structure".to_string(),
        });
    }

    if requires_context {
        signals.push(Signal {
            kind: SignalKind::PatternMatch,
            strength: 0.80,
            note: "context-dependent follow-up prompt".to_string(),
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

    if requires_context && !has_context {
        signals.push(Signal {
            kind: SignalKind::Conflict,
            strength: 0.80,
            note: "context required but unavailable".to_string(),
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
            "limit",
            "límite",
            "limite",
        ],
        TaskType::Generative => &[
            "idea",
            "ideas",
            "brainstorm",
            "propon",
            "propón",
            "sugiere",
            "suggest",
            "creative",
            "creativo",
            "ejemplo",
            "example",
            "ejercicio",
            "exercise",
        ],
        TaskType::StructuredList => &[
            "abecedario",
            "alphabet",
            "enumera",
            "todas las letras",
            "all letters",
            "dame 3",
            "dame tres",
            "list",
            "lista",
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
        TaskType::Code => &[
            "rust",
            "python",
            "javascript",
            "sql",
            "function",
            "script",
            "code",
        ],
        TaskType::Conceptual => &[
            "explica", "explain", "por que", "porque", "why", "how", "describe", "teoria",
            "theory", "que es", "qué es", "filosof", "concepto",
        ],
        TaskType::Reasoning => &[
            "step by step",
            "paso a paso",
            "solve",
            "razona",
            "reason",
            "que pasaria",
            "qué pasaría",
            "what would happen",
        ],
        TaskType::Summarization => &[
            "resume",
            "resumen",
            "haz un resumen",
            "genera un resumen",
            "summarize",
            "summary",
        ],
        TaskType::Math => &[
            "calculate",
            "calcula",
            "multiplica",
            "divide",
            "sum",
            "resta",
            "equation",
            "math",
            "ecuacion",
            "ecuación",
        ],
        TaskType::General => &[],
    };

    keywords
        .iter()
        .filter(|needle| lower.contains(**needle))
        .count()
}

fn task_pattern_strength(
    trimmed: &str,
    lower: &str,
    task_type: TaskType,
    secondary_tasks: &[TaskType],
    requires_context: bool,
) -> f32 {
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
            if has_symbolic_math_markers(trimmed, lower) {
                0.90
            } else {
                0.40
            }
        }
        TaskType::Generative => {
            if is_ideation_prompt(lower) || is_hybrid_example_prompt(lower) {
                0.78
            } else {
                0.35
            }
        }
        TaskType::StructuredList => {
            if is_structured_output_request(trimmed, lower) {
                0.85
            } else {
                0.35
            }
        }
        TaskType::Summarization => {
            if contains_any(lower, &["resume", "resumen", "summarize", "summary"]) {
                0.80
            } else {
                0.0
            }
        }
        TaskType::Code => {
            if contains_any(
                lower,
                &["rust", "python", "javascript", "sql", "fn ", "def "],
            ) {
                0.80
            } else {
                0.35
            }
        }
        TaskType::Reasoning => {
            if is_scientific_reasoning_prompt(lower)
                || contains_any(lower, &["step by step", "paso a paso", "reason through"])
            {
                0.75
            } else {
                0.25
            }
        }
        TaskType::Conceptual => {
            if is_conceptual_math_prompt(trimmed, lower)
                || is_philosophical_prompt(lower)
                || contains_any(
                    lower,
                    &["why", "how", "explica", "explain", "que es", "qué es"],
                )
            {
                0.68
            } else {
                0.20
            }
        }
        TaskType::Math => {
            if has_math_domain_markers(trimmed, lower)
                || secondary_tasks.contains(&TaskType::SymbolicMath)
            {
                0.55
            } else {
                0.20
            }
        }
        TaskType::Deterministic | TaskType::General => {
            if requires_context {
                0.0
            } else {
                0.05
            }
        }
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
    requires_context: bool,
    has_context: bool,
) -> f32 {
    let compact_exact = matches!(task_type, TaskType::ExactMath)
        && trimmed.chars().any(|c| c.is_ascii_digit())
        && trimmed
            .chars()
            .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | '^' | 'x' | 'X' | '(' | ')'));
    let generic_only = contains_any(
        lower,
        &[
            "solve",
            "resuelve",
            "explica",
            "explain",
            "resume",
            "summarize",
        ],
    ) && words <= 2;

    let mut strength: f32 = 0.0;
    if words <= 2 && !compact_exact {
        strength += 0.35;
    }
    if chars <= 6 && !compact_exact {
        strength += 0.15;
    }
    if keyword_hits == 0
        && !compact_exact
        && !is_multi_intent_prompt(lower)
        && !is_hybrid_example_prompt(lower)
    {
        strength += 0.20;
    }
    if generic_only {
        strength += 0.25;
    }
    if competing_families >= 3 && !is_multi_intent_prompt(lower) {
        strength += 0.10;
    }
    if requires_context && !has_context {
        strength += 0.35;
    }

    strength.clamp(0.0, 1.0)
}

fn conflict_strength(
    task_type: TaskType,
    secondary_tasks: &[TaskType],
    competing_families: usize,
    keyword_hits: usize,
    lower: &str,
) -> f32 {
    let summarization_and_conceptual = contains_any(lower, &["resume", "resumen", "summarize"])
        && contains_any(lower, &["explica", "explain", "why", "how"]);
    let exact_and_symbolic = matches!(task_type, TaskType::ExactMath)
        && contains_any(lower, &["f(x)", "dx", "integral", "derivada"]);
    let composed_prompt = is_multi_intent_prompt(lower);

    let mut strength: f32 = 0.0;
    if competing_families >= 2 && keyword_hits <= 1 && !composed_prompt {
        strength += 0.30;
    }
    if competing_families >= 3 && !composed_prompt {
        strength += 0.20;
    }
    if summarization_and_conceptual || exact_and_symbolic {
        strength += 0.30;
    }
    if secondary_tasks.len() >= 2 && !composed_prompt {
        strength += 0.10;
    }

    strength.clamp(0.0, 1.0)
}
