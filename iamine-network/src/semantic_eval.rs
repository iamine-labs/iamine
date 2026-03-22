use crate::prompt_analyzer::{analyze_prompt_semantics, CONFIDENCE_THRESHOLD};
use crate::task_analyzer::TaskType;
use serde::{Deserialize, Serialize};

const DEFAULT_DATASET_JSON: &str = include_str!("../../tests/semantic_dataset.json");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticDatasetEntry {
    pub prompt: String,
    pub expected_task: TaskType,
    pub should_normalize: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticEvalError {
    pub prompt: String,
    pub expected_task: TaskType,
    pub predicted_task: TaskType,
    pub expected_normalize: bool,
    pub predicted_normalize: bool,
    pub confidence: f32,
    pub fallback_applied: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticEvalReport {
    pub total: usize,
    pub correct: usize,
    pub accuracy: f32,
    pub fallback_rate: f32,
    pub low_confidence_rate: f32,
    pub error_cases: Vec<SemanticEvalError>,
}

pub fn should_use_strict_handling(task_type: TaskType) -> bool {
    matches!(
        task_type,
        TaskType::ExactMath | TaskType::StructuredList | TaskType::Deterministic
    )
}

pub fn load_default_dataset() -> Result<Vec<SemanticDatasetEntry>, serde_json::Error> {
    serde_json::from_str(DEFAULT_DATASET_JSON)
}

pub fn evaluate_dataset(entries: &[SemanticDatasetEntry]) -> SemanticEvalReport {
    let total = entries.len();
    let mut correct = 0usize;
    let mut fallback_count = 0usize;
    let mut low_confidence_count = 0usize;
    let mut error_cases = Vec::new();

    for entry in entries {
        let decision = analyze_prompt_semantics(&entry.prompt);
        let predicted_task = decision.profile.task_type;
        let predicted_normalize = should_use_strict_handling(predicted_task);
        let task_ok = predicted_task == entry.expected_task;
        let normalize_ok = predicted_normalize == entry.should_normalize;

        if decision.fallback_applied {
            fallback_count += 1;
        }
        if decision.profile.confidence < CONFIDENCE_THRESHOLD {
            low_confidence_count += 1;
        }

        if task_ok && normalize_ok {
            correct += 1;
        } else {
            error_cases.push(SemanticEvalError {
                prompt: entry.prompt.clone(),
                expected_task: entry.expected_task,
                predicted_task,
                expected_normalize: entry.should_normalize,
                predicted_normalize,
                confidence: decision.profile.confidence,
                fallback_applied: decision.fallback_applied,
            });
        }
    }

    let total_f32 = total.max(1) as f32;
    SemanticEvalReport {
        total,
        correct,
        accuracy: correct as f32 / total_f32,
        fallback_rate: fallback_count as f32 / total_f32,
        low_confidence_rate: low_confidence_count as f32 / total_f32,
        error_cases,
    }
}

pub fn evaluate_default_dataset() -> Result<SemanticEvalReport, serde_json::Error> {
    let entries = load_default_dataset()?;
    Ok(evaluate_dataset(&entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_accuracy() {
        let report = evaluate_default_dataset().unwrap();
        assert_eq!(report.accuracy, 1.0);
        assert!(report.error_cases.is_empty());
    }
}
