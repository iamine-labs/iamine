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

fn determine_primary_task(
    trimmed: &str,
    lower: &str,
    detected_task_type: TaskType,
    requires_context: bool,
) -> TaskType {
    if requires_context {
        if contains_any(
            lower,
            &[
                "hazlo mas simple",
                "hazlo más simple",
                "simplifica",
                "explain it simply",
            ],
        ) {
            return TaskType::Conceptual;
        }
        return TaskType::General;
    }

    if is_hybrid_example_prompt(lower) {
        return TaskType::Conceptual;
    }

    if contains_any(lower, &["explica", "explain", "que es", "qué es"])
        && (has_exact_math_markers(trimmed, lower)
            || has_math_domain_markers(trimmed, lower)
            || matches!(
                detected_task_type,
                TaskType::Math | TaskType::ExactMath | TaskType::SymbolicMath
            ))
    {
        return TaskType::Conceptual;
    }

    if is_scientific_reasoning_prompt(lower) {
        return TaskType::Reasoning;
    }

    if is_ideation_prompt(lower) {
        return TaskType::Generative;
    }

    if is_philosophical_prompt(lower) {
        return TaskType::Conceptual;
    }

    if is_conceptual_math_prompt(trimmed, lower) {
        return TaskType::Conceptual;
    }

    detected_task_type
}

fn detect_secondary_tasks(
    trimmed: &str,
    lower: &str,
    primary_task: TaskType,
    requires_context: bool,
) -> Vec<TaskType> {
    let mut tasks = Vec::new();

    if requires_context {
        return tasks;
    }

    if contains_any(
        lower,
        &[
            "explica", "explain", "que es", "qué es", "como", "how", "por que", "why",
        ],
    ) && primary_task != TaskType::Conceptual
    {
        push_task(&mut tasks, TaskType::Conceptual);
    }

    if is_ideation_prompt(lower)
        || contains_any(lower, &["ejemplo", "example", "ejercicio", "exercise"])
    {
        push_task(&mut tasks, TaskType::Generative);
    }

    if is_structured_output_request(trimmed, lower) {
        push_task(&mut tasks, TaskType::StructuredList);
    }

    let symbolic_markers = has_symbolic_math_markers(trimmed, lower);
    if symbolic_markers && primary_task != TaskType::SymbolicMath {
        push_task(&mut tasks, TaskType::SymbolicMath);
    } else if has_math_domain_markers(trimmed, lower)
        && !matches!(
            primary_task,
            TaskType::Math | TaskType::ExactMath | TaskType::SymbolicMath
        )
    {
        push_task(&mut tasks, TaskType::Math);
    }

    if has_exact_math_markers(trimmed, lower)
        && !symbolic_markers
        && primary_task != TaskType::ExactMath
    {
        push_task(&mut tasks, TaskType::ExactMath);
    }

    if contains_any(
        lower,
        &[
            "step by step",
            "paso a paso",
            "que pasaria",
            "qué pasaría",
            "what would happen",
        ],
    ) && primary_task != TaskType::Reasoning
    {
        push_task(&mut tasks, TaskType::Reasoning);
    }

    if contains_any(lower, &["resume", "resumen", "summarize", "summary"])
        && primary_task != TaskType::Summarization
    {
        push_task(&mut tasks, TaskType::Summarization);
    }

    tasks.retain(|task| *task != primary_task);
    tasks
}

fn detect_domain(
    trimmed: &str,
    lower: &str,
    primary_task: TaskType,
    secondary_tasks: &[TaskType],
) -> Option<Domain> {
    if matches!(primary_task, TaskType::Code) || secondary_tasks.contains(&TaskType::Code) {
        return Some(Domain::Code);
    }

    if contains_any(
        lower,
        &[
            "agujero negro",
            "black hole",
            "gravedad",
            "gravity",
            "fisica",
            "física",
            "einstein",
            "e=mc2",
            "emc2",
            "energía",
            "energia",
        ],
    ) {
        return Some(Domain::Physics);
    }

    if contains_any(
        lower,
        &[
            "negocio",
            "negocios",
            "business",
            "startup",
            "mercado",
            "clientes",
            "modelo de negocio",
        ],
    ) {
        return Some(Domain::Business);
    }

    if is_philosophical_prompt(lower) {
        return Some(Domain::Philosophy);
    }

    if matches!(
        primary_task,
        TaskType::Math | TaskType::ExactMath | TaskType::SymbolicMath
    ) || secondary_tasks.iter().any(|task| {
        matches!(
            task,
            TaskType::Math | TaskType::ExactMath | TaskType::SymbolicMath
        )
    }) || has_math_domain_markers(trimmed, lower)
    {
        return Some(Domain::Math);
    }

    Some(Domain::General)
}

fn detect_output_style(primary_task: TaskType, secondary_tasks: &[TaskType]) -> OutputStyle {
    if !secondary_tasks.is_empty() {
        return OutputStyle::Hybrid;
    }

    match primary_task {
        TaskType::ExactMath | TaskType::Deterministic => OutputStyle::Exact,
        TaskType::StructuredList => OutputStyle::Structured,
        TaskType::Generative => OutputStyle::Generative,
        TaskType::Conceptual
        | TaskType::Reasoning
        | TaskType::SymbolicMath
        | TaskType::Summarization
        | TaskType::Math
        | TaskType::Code
        | TaskType::General => OutputStyle::Explanatory,
    }
}

fn detect_deterministic_level(
    primary_task: TaskType,
    secondary_tasks: &[TaskType],
    output_style: OutputStyle,
) -> DeterministicLevel {
    if matches!(
        primary_task,
        TaskType::ExactMath | TaskType::StructuredList | TaskType::Deterministic
    ) && secondary_tasks.is_empty()
    {
        return DeterministicLevel::High;
    }

    if matches!(output_style, OutputStyle::Generative)
        || matches!(
            primary_task,
            TaskType::Generative | TaskType::General | TaskType::Conceptual | TaskType::Reasoning
        )
    {
        return DeterministicLevel::Low;
    }

    DeterministicLevel::Medium
}

fn fallback_task_type(
    trimmed: &str,
    lower: &str,
    original_task_type: TaskType,
    secondary_tasks: &[TaskType],
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
            "que pasaria",
            "qué pasaría",
        ],
    );

    if matches!(complexity, Complexity::High)
        || reasoning_hint
        || matches!(
            original_task_type,
            TaskType::Reasoning | TaskType::SymbolicMath | TaskType::Conceptual
        )
        || secondary_tasks.iter().any(|task| {
            matches!(
                task,
                TaskType::Reasoning | TaskType::Conceptual | TaskType::SymbolicMath
            )
        })
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

fn is_structured_output_request(trimmed: &str, lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "lista",
            "enumera",
            "dame 3",
            "dame tres",
            "3 ideas",
            "tres ideas",
            "all letters",
            "abecedario",
        ],
    ) || (trimmed.chars().any(|c| c.is_ascii_digit())
        && contains_any(lower, &["ideas", "examples", "ejemplos"]))
}

fn is_hybrid_example_prompt(lower: &str) -> bool {
    contains_any(lower, &["ejemplo", "example"])
        && contains_any(lower, &["ejercicio", "exercise"])
        && contains_any(
            lower,
            &["ecuacion", "ecuación", "emc2", "e=mc2", "math", "matem"],
        )
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
        &[
            "calcula",
            "calculate",
            "ecuaciones",
            "equations",
            "explica",
            "explain",
        ],
    )
}

fn is_conceptual_math_prompt(trimmed: &str, lower: &str) -> bool {
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
    let math_concepts = contains_any(
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
    let strong_solving_markers = trimmed.contains("f(x)")
        || trimmed.contains("dx")
        || trimmed.contains("dy/dx")
        || trimmed.contains("[[")
        || trimmed.contains("]]")
        || (trimmed.chars().any(|c| c.is_ascii_digit())
            && trimmed
                .chars()
                .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | '^')));

    explain_cues && math_concepts && !strong_solving_markers
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

fn has_symbolic_math_markers(trimmed: &str, lower: &str) -> bool {
    trimmed.contains("f(x)")
        || trimmed.contains("dx")
        || trimmed.contains("dy/dx")
        || trimmed.contains("[[")
        || trimmed.contains("]]")
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

fn has_exact_math_markers(trimmed: &str, lower: &str) -> bool {
    (trimmed.chars().any(|c| c.is_ascii_digit())
        && trimmed
            .chars()
            .any(|c| matches!(c, '+' | '-' | '*' | '/' | '^' | 'x' | 'X')))
        || normalize_expression(trimmed).is_some()
        || contains_any(
            lower,
            &[
                "sqrt(",
                "log",
                "digits of pi",
                "digitos de pi",
                "dígitos de pi",
            ],
        )
}

fn has_math_domain_markers(trimmed: &str, lower: &str) -> bool {
    has_exact_math_markers(trimmed, lower)
        || has_symbolic_math_markers(trimmed, lower)
        || contains_any(
            lower,
            &[
                "matem",
                "math",
                "emc2",
                "e=mc2",
                "einstein",
                "ecuacion",
                "ecuación",
                "funcion",
                "función",
                "derivada",
                "integral",
                "limite",
                "límite",
            ],
        )
}

fn is_multi_intent_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[" y ", " and ", " luego ", " then ", " además ", " ademas "],
    )
}

fn push_task(tasks: &mut Vec<TaskType>, task: TaskType) {
    if !tasks.contains(&task) {
        tasks.push(task);
    }
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
}
