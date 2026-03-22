use crate::expression_parser::normalize_expression;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskType {
    Math,
    ExactMath,
    SymbolicMath,
    Generative,
    StructuredList,
    Deterministic,
    Code,
    Conceptual,
    Reasoning,
    Summarization,
    General,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExactSubtype {
    Integer,
    DecimalSequence,
    Sequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProfile {
    pub task_type: TaskType,
}

pub fn detect_exact_subtype(prompt: &str, output: &str) -> ExactSubtype {
    let prompt_lower = prompt.to_lowercase();
    let output_lower = output.to_lowercase();
    let trimmed_output = output.trim();
    let arithmetic_prompt = prompt.len() <= 32
        && prompt.chars().any(|c| c.is_ascii_digit())
        && prompt
            .chars()
            .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | '^' | 'x' | 'X'));
    let verbal_exact_math = contains_any(&prompt_lower, &["square root of"])
        && prompt.chars().any(|c| c.is_ascii_digit());

    if contains_any(
        &prompt_lower,
        &["pi", "π", "digit", "digits", "digito", "digitos", "dígitos"],
    ) || (trimmed_output.contains('.')
        && trimmed_output
            .chars()
            .filter(|c| c.is_ascii_digit())
            .count()
            >= 4)
        || (trimmed_output.contains(". ") && trimmed_output.chars().any(|c| c.is_ascii_digit()))
    {
        return ExactSubtype::DecimalSequence;
    }

    if trimmed_output
        .chars()
        .all(|c| c.is_ascii_digit() || c.is_ascii_whitespace() || matches!(c, '-' | '+'))
        || arithmetic_prompt
        || verbal_exact_math
        || (prompt.chars().any(|c| c.is_ascii_digit())
            && !contains_any(&output_lower, &["=", " is "])
            && trimmed_output.chars().any(|c| c.is_ascii_digit()))
    {
        return ExactSubtype::Integer;
    }

    ExactSubtype::Sequence
}

pub fn detect_task_type(prompt: &str) -> TaskType {
    let trimmed = prompt.trim();
    let lower = trimmed.to_lowercase();

    if contains_any(
        &lower,
        &[
            "function",
            "script",
            "code",
            "rust",
            "python",
            "javascript",
            "sql",
        ],
    ) {
        return TaskType::Code;
    }

    if is_summarization_prompt(&lower) {
        return TaskType::Summarization;
    }

    if is_symbolic_math_prompt(trimmed, &lower) {
        return TaskType::SymbolicMath;
    }

    if is_exact_math_prompt(trimmed, &lower) {
        return TaskType::ExactMath;
    }

    if is_structured_list_prompt(&lower) {
        return TaskType::StructuredList;
    }

    if is_generative_prompt(&lower) {
        return TaskType::Generative;
    }

    if is_deterministic_prompt(trimmed, &lower) {
        return TaskType::Deterministic;
    }

    if contains_any(
        &lower,
        &[
            "step by step",
            "paso a paso",
            "solve",
            "razona",
            "reason through",
        ],
    ) {
        return TaskType::Reasoning;
    }

    if is_math_prompt(trimmed, &lower) {
        return TaskType::Math;
    }

    if contains_any(
        &lower,
        &[
            "explica",
            "describe",
            "why",
            "how",
            "por que",
            "porque",
            "como funciona",
        ],
    ) {
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
        && trimmed
            .chars()
            .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | 'x' | 'X'));

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

fn is_symbolic_math_prompt(trimmed: &str, lower: &str) -> bool {
    let has_math_markers = trimmed.contains("f(x)")
        || trimmed.contains("dx")
        || trimmed.contains("dy/dx")
        || trimmed.contains("lim")
        || trimmed.contains("[[")
        || trimmed.contains("]]");

    let symbolic_keywords = contains_any(
        lower,
        &[
            "derivada",
            "integral",
            "ecuación",
            "ecuacion",
            "matriz",
            "matrices",
            "función",
            "funcion",
            "f(x)",
            "dx",
            "dy/dx",
            "lim",
            "sin(",
            "cos(",
            "tan(",
            "derivative",
            "integral",
            "matrix",
            "limit",
        ],
    );

    symbolic_keywords || has_math_markers
}

fn is_exact_math_prompt(trimmed: &str, lower: &str) -> bool {
    let exact_expression = trimmed.len() <= 16
        && trimmed.chars().any(|c| c.is_ascii_digit())
        && trimmed
            .chars()
            .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | 'x' | 'X'));

    exact_expression
        || normalize_expression(trimmed).is_some()
        || compact_math_patterns(trimmed, lower)
        || (contains_any(lower, &["square root of"]) && lower.chars().any(|c| c.is_ascii_digit()))
        || (contains_any(lower, &["pi", "π"])
            && contains_any(
                lower,
                &[
                    "digit", "digits", "digito", "digitos", "dígitos", "primeros", "first",
                ],
            ))
        || (contains_any(lower, &["exact", "exacto", "exacta", "resultado exacto"])
            && lower.chars().any(|c| c.is_ascii_digit()))
}

fn compact_math_patterns(trimmed: &str, lower: &str) -> bool {
    let has_digits = trimmed.chars().any(|c| c.is_ascii_digit());
    let has_parentheses = trimmed.contains('(') && trimmed.contains(')');
    let has_power = trimmed.contains('^');
    let has_sqrt = lower.contains("sqrt(");

    has_digits && ((has_parentheses && trimmed.len() <= 32) || has_power || has_sqrt)
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

fn is_generative_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "idea",
            "ideas",
            "negocio",
            "negocios",
            "brainstorm",
            "propon",
            "propón",
            "sugiere",
            "suggest",
            "creative",
            "creativo",
            "nombres",
            "naming",
        ],
    )
}

fn is_summarization_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "resume",
            "resumen",
            "haz un resumen",
            "genera un resumen",
            "summarize",
            "summary",
        ],
    )
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
    fn test_symbolic_derivative() {
        assert_eq!(
            detect_task_type("Calcula la derivada de f(x)=4x^3+2x^2-7x+5"),
            TaskType::SymbolicMath
        );
    }

    #[test]
    fn test_integral_classification() {
        assert_eq!(
            detect_task_type("Evalua la integral definida de x^2 entre 0 y 3"),
            TaskType::SymbolicMath
        );
    }

    #[test]
    fn test_matrix_task() {
        assert_eq!(
            detect_task_type("Multiplica las matrices A=[[1,2],[3,4]] y B=[[2,0],[1,2]]"),
            TaskType::SymbolicMath
        );
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

    #[test]
    fn test_compact_math_detection() {
        assert_eq!(detect_task_type("6(9)"), TaskType::ExactMath);
        assert_eq!(detect_task_type("(2+2)"), TaskType::ExactMath);
    }

    #[test]
    fn test_power_expression_detection() {
        assert_eq!(detect_task_type("2^10"), TaskType::ExactMath);
        assert_eq!(detect_task_type("sqrt(16)"), TaskType::ExactMath);
    }

    #[test]
    fn test_exact_subtype_detection() {
        assert_eq!(detect_exact_subtype("6x9", "54"), ExactSubtype::Integer);
        assert_eq!(
            detect_exact_subtype("What is 2+2?", "2 + 2 = 4."),
            ExactSubtype::Integer
        );
        assert_eq!(
            detect_exact_subtype("square root of 16", "The square root of 16 is 4."),
            ExactSubtype::Integer
        );
        assert_eq!(
            detect_exact_subtype("dame los primeros 100 digitos de pi", "3. 14159"),
            ExactSubtype::DecimalSequence
        );
    }

    #[test]
    fn test_summarization_detection() {
        assert_eq!(
            detect_task_type("genera un resumen de la relatividad"),
            TaskType::Summarization
        );
    }

    #[test]
    fn test_symbolic_no_exact_fallback() {
        assert_eq!(
            detect_task_type("Calcula el valor de sin(45°) + cos(30°)"),
            TaskType::SymbolicMath
        );
    }

    #[test]
    fn test_generative_detection() {
        assert_eq!(
            detect_task_type("dame 3 ideas de negocios para este 2026 acerca de la ia"),
            TaskType::Generative
        );
    }
}
