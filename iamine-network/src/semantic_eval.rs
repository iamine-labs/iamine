use crate::prompt_analyzer::{
    analyze_prompt_semantics, CONFIDENCE_THRESHOLD, DeterministicLevel, Domain, OutputStyle,
};
use crate::semantic_validator::validate_semantic_decision;
use crate::task_analyzer::TaskType;
use serde::{Deserialize, Serialize};

const DEFAULT_DATASET_JSON: &str = include_str!("../../tests/semantic_dataset.json");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticDatasetEntry {
    pub prompt: String,
    pub expected_task: TaskType,
    pub should_normalize: bool,
    #[serde(default)]
    pub expected_secondary: Vec<TaskType>,
    #[serde(default)]
    pub expected_domain: Option<Domain>,
    #[serde(default)]
    pub expected_output_style: Option<OutputStyle>,
    #[serde(default)]
    pub expected_requires_context: Option<bool>,
    #[serde(default)]
    pub expected_deterministic_level: Option<DeterministicLevel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticEvalError {
    pub prompt: String,
    pub expected_task: TaskType,
    pub predicted_task: TaskType,
    pub expected_normalize: bool,
    pub predicted_normalize: bool,
    pub expected_secondary: Vec<TaskType>,
    pub predicted_secondary: Vec<TaskType>,
    pub expected_domain: Option<Domain>,
    pub predicted_domain: Option<Domain>,
    pub expected_output_style: Option<OutputStyle>,
    pub predicted_output_style: OutputStyle,
    pub expected_requires_context: Option<bool>,
    pub predicted_requires_context: bool,
    pub expected_deterministic_level: Option<DeterministicLevel>,
    pub predicted_deterministic_level: DeterministicLevel,
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
        let analyzed = analyze_prompt_semantics(&entry.prompt);
        let validated = validate_semantic_decision(&entry.prompt, analyzed);
        let decision = validated.decision;
        let predicted_task = decision.profile.task_type;
        let predicted_normalize = should_use_strict_handling(predicted_task);
        let predicted_secondary = decision.profile.semantic.secondary_tasks.clone();
        let predicted_domain = decision.profile.semantic.domain;
        let predicted_output_style = decision.profile.semantic.output_style;
        let predicted_requires_context = decision.profile.semantic.requires_context;
        let predicted_deterministic_level = decision.profile.semantic.deterministic_level;
        let task_ok = predicted_task == entry.expected_task;
        let normalize_ok = predicted_normalize == entry.should_normalize;
        let secondary_ok = entry.expected_secondary.is_empty() || predicted_secondary == entry.expected_secondary;
        let domain_ok = entry
            .expected_domain
            .map(|expected| Some(expected) == predicted_domain)
            .unwrap_or(true);
        let output_style_ok = entry
            .expected_output_style
            .map(|expected| expected == predicted_output_style)
            .unwrap_or(true);
        let requires_context_ok = entry
            .expected_requires_context
            .map(|expected| expected == predicted_requires_context)
            .unwrap_or(true);
        let deterministic_ok = entry
            .expected_deterministic_level
            .map(|expected| expected == predicted_deterministic_level)
            .unwrap_or(true);

        if decision.fallback_applied {
            fallback_count += 1;
        }
        if decision.profile.confidence < CONFIDENCE_THRESHOLD {
            low_confidence_count += 1;
        }

        if task_ok && normalize_ok && secondary_ok && domain_ok && output_style_ok && requires_context_ok && deterministic_ok {
            correct += 1;
        } else {
            error_cases.push(SemanticEvalError {
                prompt: entry.prompt.clone(),
                expected_task: entry.expected_task,
                predicted_task,
                expected_normalize: entry.should_normalize,
                predicted_normalize,
                expected_secondary: entry.expected_secondary.clone(),
                predicted_secondary,
                expected_domain: entry.expected_domain,
                predicted_domain,
                expected_output_style: entry.expected_output_style,
                predicted_output_style,
                expected_requires_context: entry.expected_requires_context,
                predicted_requires_context,
                expected_deterministic_level: entry.expected_deterministic_level,
                predicted_deterministic_level,
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
