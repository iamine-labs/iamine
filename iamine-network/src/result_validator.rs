use crate::task_analyzer::{detect_exact_subtype, TaskType};
use iamine_models::{normalize_output, output_validator};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultStatus {
    Valid,
    Invalid(String),
    Retryable(String),
}

pub fn validate_result(prompt: &str, task_type: TaskType, output: &str) -> ResultStatus {
    let trimmed = output.trim();

    if trimmed.is_empty() {
        return ResultStatus::Retryable("empty output".to_string());
    }

    if !trimmed.chars().any(|ch| ch.is_alphanumeric()) {
        return ResultStatus::Invalid("malformed output".to_string());
    }

    if matches!(
        task_type,
        TaskType::ExactMath | TaskType::StructuredList | TaskType::Deterministic
    ) {
        let task_label = task_label(task_type);
        let exact_subtype = if matches!(task_type, TaskType::ExactMath) {
            Some(detect_exact_subtype(prompt, trimmed))
        } else {
            None
        };
        let exact_subtype_label = exact_subtype.map(exact_subtype_label);
        let normalized = if matches!(task_type, TaskType::ExactMath) {
            normalize_output(task_label, exact_subtype_label, trimmed).0
        } else {
            trimmed.to_string()
        };

        let valid = output_validator::validate_structure(task_label, &normalized)
            && output_validator::validate_exactness(task_label, exact_subtype_label, &normalized);

        if valid {
            ResultStatus::Valid
        } else {
            ResultStatus::Retryable("strict output validation failed".to_string())
        }
    } else {
        let minimum_len = match task_type {
            TaskType::Generative | TaskType::Conceptual | TaskType::Reasoning => 12,
            TaskType::Summarization | TaskType::SymbolicMath | TaskType::Math | TaskType::Code => 8,
            TaskType::General => 4,
            _ => 1,
        };

        if trimmed.chars().count() < minimum_len {
            ResultStatus::Retryable("output too short".to_string())
        } else {
            ResultStatus::Valid
        }
    }
}

fn task_label(task_type: TaskType) -> &'static str {
    match task_type {
        TaskType::ExactMath => "ExactMath",
        TaskType::StructuredList => "StructuredList",
        TaskType::Deterministic => "Deterministic",
        TaskType::Math => "Math",
        TaskType::SymbolicMath => "SymbolicMath",
        TaskType::Generative => "Generative",
        TaskType::Code => "Code",
        TaskType::Conceptual => "Conceptual",
        TaskType::Reasoning => "Reasoning",
        TaskType::Summarization => "Summarization",
        TaskType::General => "General",
    }
}

fn exact_subtype_label(subtype: crate::task_analyzer::ExactSubtype) -> &'static str {
    match subtype {
        crate::task_analyzer::ExactSubtype::Integer => "Integer",
        crate::task_analyzer::ExactSubtype::DecimalSequence => "DecimalSequence",
        crate::task_analyzer::ExactSubtype::Sequence => "Sequence",
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_result, ResultStatus};
    use crate::task_analyzer::TaskType;

    #[test]
    fn test_empty_output_detection() {
        assert_eq!(
            validate_result("What is 2+2?", TaskType::ExactMath, "   "),
            ResultStatus::Retryable("empty output".to_string())
        );
    }

    #[test]
    fn test_short_exact_output_is_valid() {
        assert_eq!(
            validate_result("What is 2+2?", TaskType::ExactMath, "4"),
            ResultStatus::Valid
        );
    }

    #[test]
    fn test_too_short_explanatory_output_is_retryable() {
        assert_eq!(
            validate_result("Explain gravity", TaskType::Conceptual, "ok"),
            ResultStatus::Retryable("output too short".to_string())
        );
    }
}
