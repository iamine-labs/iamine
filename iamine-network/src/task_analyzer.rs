use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskType {
    Math,
    Code,
    Factual,
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

    if is_math_prompt(trimmed, &lower) {
        return TaskType::Math;
    }

    if contains_any(&lower, &["function", "script", "code", "rust", "python", "javascript", "sql"]) {
        return TaskType::Code;
    }

    if contains_any(&lower, &["step by step", "paso a paso", "solve", "razona", "reason through"]) {
        return TaskType::Reasoning;
    }

    if contains_any(&lower, &["explica", "describe", "why", "how", "por que", "porque", "como funciona"]) {
        return TaskType::Conceptual;
    }

    if contains_any(&lower, &["what is", "que es", "qué es", "define"]) {
        return TaskType::Factual;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_detection_math() {
        assert_eq!(detect_task_type("6x9"), TaskType::Math);
        assert_eq!(detect_task_type("2+2"), TaskType::Math);
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
}
