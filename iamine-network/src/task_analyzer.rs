use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskType {
    Math,
    ExactMath,
    StructuredList,
    Deterministic,
    Code,
    Conceptual,
    Reasoning,
    General,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProfile {
    pub task_type: TaskType,
}

pub fn detect_task_type(prompt: &str) -> TaskType {
    let trimmed = prompt.trim();
    let lower = trimmed.to_lowercase();

    if contains_any(&lower, &["function", "script", "code", "rust", "python", "javascript", "sql"]) {
        return TaskType::Code;
    }

    if is_exact_math_prompt(trimmed, &lower) {
        return TaskType::ExactMath;
    }

    if is_structured_list_prompt(&lower) {
        return TaskType::StructuredList;
    }

    if is_deterministic_prompt(trimmed, &lower) {
        return TaskType::Deterministic;
    }

    if contains_any(&lower, &["step by step", "paso a paso", "solve", "razona", "reason through"]) {
        return TaskType::Reasoning;
    }

    if is_math_prompt(trimmed, &lower) {
        return TaskType::Math;
    }

    if contains_any(&lower, &["explica", "describe", "why", "how", "por que", "porque", "como funciona"]) {
        return TaskType::Conceptual;
    }

    TaskType::General
}

fn contains_any(prompt: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| prompt.contains(needle))
}

fn is_math_prompt(trimmed: &str, lower: &str) -> bool {
    let short_math_expression = trimmed.len() <= 24
        && trimmed.chars().any(|c| c.is_ascii_digit())
        && trimmed.chars().any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | 'x' | 'X'));

    if short_math_expression {
        return true;
    }

    contains_any(
        lower,
        &[
            "solve",
            "equation",
            "calculate",
            "cuanto es",
            "cuánto es",
            "multiplica",
            "divide",
            "sum",
            "resta",
        ],
    ) && lower.chars().any(|c| c.is_ascii_digit())
}

fn is_exact_math_prompt(trimmed: &str, lower: &str) -> bool {
    let exact_expression = trimmed.len() <= 16
        && trimmed.chars().any(|c| c.is_ascii_digit())
        && trimmed.chars().any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | 'x' | 'X'));

    exact_expression
        || (contains_any(lower, &["pi", "π"]) && contains_any(lower, &["digit", "digits", "digito", "digitos", "dígitos", "primeros", "first"]))
        || (contains_any(lower, &["exact", "exacto", "exacta", "resultado exacto"]) && lower.chars().any(|c| c.is_ascii_digit()))
}

fn is_structured_list_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "abecedario",
            "alphabet",
            "lista",
            "listalos",
            "list them",
            "enumera",
            "todas las letras",
            "all letters",
        ],
    )
}

fn is_deterministic_prompt(trimmed: &str, lower: &str) -> bool {
    let has_number = trimmed.chars().any(|c| c.is_ascii_digit());
    contains_any(
        lower,
        &[
            "dame",
            "escribe",
            "todos",
            "todas",
            "primeros",
            "first",
            "lista completa",
            "in order",
            "en orden",
            "digits",
            "digitos",
            "dígitos",
        ],
    ) && (has_number || is_structured_list_prompt(lower))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_detection_math() {
        assert_eq!(detect_task_type("6x9"), TaskType::ExactMath);
        assert_eq!(detect_task_type("2+2"), TaskType::ExactMath);
    }

    #[test]
    fn test_task_detection_code() {
        assert_eq!(
            detect_task_type("write a python function to sort a list"),
            TaskType::Code
        );
    }

    #[test]
    fn test_task_detection_conceptual() {
        assert_eq!(
            detect_task_type("explica la teoria de la relatividad"),
            TaskType::Conceptual
        );
    }

    #[test]
    fn test_spanish_imperative_detection() {
        assert_eq!(
            detect_task_type("dame todas las letras del abecedario"),
            TaskType::StructuredList
        );
    }

    #[test]
    fn test_structured_list_detection() {
        assert_eq!(
            detect_task_type("enumera todas las letras del abecedario en orden"),
            TaskType::StructuredList
        );
    }

    #[test]
    fn test_exact_math_detection() {
        assert_eq!(
            detect_task_type("dame los primeros 100 digitos de pi"),
            TaskType::ExactMath
        );
    }
}
