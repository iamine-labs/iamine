use crate::prompt_analyzer::{Complexity, DeterministicLevel, Domain, OutputStyle};
use crate::prompt_task_detection::{
    contains_any, has_math_domain_markers, is_philosophical_prompt,
};
use crate::task_analyzer::TaskType;

pub(crate) fn detect_domain(
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

pub(crate) fn detect_output_style(
    primary_task: TaskType,
    secondary_tasks: &[TaskType],
) -> OutputStyle {
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

pub(crate) fn detect_deterministic_level(
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

pub(crate) fn fallback_task_type(
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
