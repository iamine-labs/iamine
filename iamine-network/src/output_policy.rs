use crate::prompt_analyzer::{Complexity, PromptProfile};
use crate::task_analyzer::TaskType;

const MIN_TOKENS: usize = 64;
const MAX_TOKENS: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputPolicyDecision {
    pub max_tokens: usize,
    pub reason: String,
}

pub fn compute_max_tokens(profile: &PromptProfile, prompt: &str) -> usize {
    describe_output_policy(profile, prompt).max_tokens
}

pub fn describe_output_policy(profile: &PromptProfile, prompt: &str) -> OutputPolicyDecision {
    let lower = prompt.to_lowercase();
    let mut max_tokens = match profile.complexity {
        Complexity::Low => 128usize,
        Complexity::Medium => 512usize,
        Complexity::High => 768usize,
    };
    let mut reasons = vec![format!("complexity={}", complexity_label(profile.complexity))];

    if contains_any(&lower, &["explica", "describe", "why", "how", "detalle"]) {
        max_tokens += 200;
        reasons.push("explain keyword".to_string());
    }

    if contains_any(&lower, &["paso a paso", "step by step"]) {
        max_tokens += 300;
        reasons.push("step-by-step keyword".to_string());
    }

    if contains_any(&lower, &["define", "que es", "qué es"]) {
        max_tokens = max_tokens.saturating_sub(100);
        reasons.push("definition keyword".to_string());
    }

    if profile.task_type == TaskType::SymbolicMath {
        max_tokens += 256;
        reasons.push("symbolic math task".to_string());
    }

    if profile.task_type == TaskType::Summarization {
        max_tokens = max_tokens.saturating_sub(128);
        reasons.push("summarization task".to_string());
    }

    max_tokens = max_tokens.clamp(MIN_TOKENS, MAX_TOKENS);

    OutputPolicyDecision {
        max_tokens,
        reason: reasons.join(" + "),
    }
}

pub fn continue_inference(_previous_output: &str, _model_id: &str) -> Option<String> {
    // Future: implement continuation when output is truncated.
    None
}

fn contains_any(prompt: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| prompt.contains(needle))
}

fn complexity_label(complexity: Complexity) -> &'static str {
    match complexity {
        Complexity::Low => "low",
        Complexity::Medium => "medium",
        Complexity::High => "high",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_analyzer::{Language, PromptProfile};
    use crate::task_analyzer::TaskType;

    #[test]
    fn test_adaptive_token_policy() {
        let profile = PromptProfile {
            language: Language::English,
            complexity: Complexity::Low,
            length: 12,
            task_type: TaskType::General,
            confidence: 0.9,
        };

        assert_eq!(compute_max_tokens(&profile, "What is 2+2?"), 128);
    }

    #[test]
    fn test_token_scaling_explanatory_prompt() {
        let profile = PromptProfile {
            language: Language::Spanish,
            complexity: Complexity::Medium,
            length: 42,
            task_type: TaskType::Conceptual,
            confidence: 0.9,
        };

        let decision = describe_output_policy(&profile, "explica paso a paso la teoria de la relatividad");

        assert!(decision.max_tokens >= 1000);
        assert!(decision.reason.contains("complexity=medium"));
        assert!(decision.reason.contains("explain keyword"));
        assert!(decision.reason.contains("step-by-step keyword"));
    }

    #[test]
    fn test_summarization_reduces_budget() {
        let profile = PromptProfile {
            language: Language::Spanish,
            complexity: Complexity::Medium,
            length: 38,
            task_type: TaskType::Summarization,
            confidence: 0.9,
        };

        let decision = describe_output_policy(&profile, "genera un resumen de la relatividad");
        assert_eq!(decision.max_tokens, 384);
        assert!(decision.reason.contains("summarization task"));
    }

    #[test]
    fn test_symbolic_math_increases_budget() {
        let profile = PromptProfile {
            language: Language::Spanish,
            complexity: Complexity::Medium,
            length: 48,
            task_type: TaskType::SymbolicMath,
            confidence: 0.9,
        };

        let decision = describe_output_policy(&profile, "Calcula la derivada de f(x)=4x^3+2x^2-7x+5");
        assert_eq!(decision.max_tokens, 768);
        assert!(decision.reason.contains("symbolic math task"));
    }
}
