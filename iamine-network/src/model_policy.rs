use crate::prompt_analyzer::{Complexity, Language, PromptProfile};
use crate::task_analyzer::TaskType;

#[derive(Debug, Clone)]
pub struct ModelPolicyEngine {
    pub rules: Vec<PolicyRule>,
}

#[derive(Debug, Clone)]
pub struct PolicyRule {
    pub language: Option<Language>,
    pub complexity: Option<Complexity>,
    pub task_type: Option<TaskType>,
    pub model: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDecision {
    pub model: String,
    pub reason: String,
}

impl Default for ModelPolicyEngine {
    fn default() -> Self {
        Self {
            rules: vec![
                PolicyRule {
                    language: None,
                    complexity: None,
                    task_type: Some(TaskType::Math),
                    model: "llama3-3b".to_string(),
                    reason: "math task".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: None,
                    task_type: Some(TaskType::Code),
                    model: "llama3-3b".to_string(),
                    reason: "code task".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: None,
                    task_type: Some(TaskType::Reasoning),
                    model: "mistral-7b".to_string(),
                    reason: "reasoning task".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: None,
                    task_type: Some(TaskType::Conceptual),
                    model: "llama3-3b".to_string(),
                    reason: "conceptual task".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: None,
                    task_type: Some(TaskType::Factual),
                    model: "llama3-3b".to_string(),
                    reason: "factual task".to_string(),
                },
                PolicyRule {
                    language: Some(Language::Spanish),
                    complexity: Some(Complexity::High),
                    task_type: None,
                    model: "llama3-3b".to_string(),
                    reason: "spanish + high complexity".to_string(),
                },
                PolicyRule {
                    language: Some(Language::Spanish),
                    complexity: None,
                    task_type: None,
                    model: "llama3-3b".to_string(),
                    reason: "spanish language".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: Some(Complexity::High),
                    task_type: None,
                    model: "mistral-7b".to_string(),
                    reason: "high complexity".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: Some(Complexity::Medium),
                    task_type: None,
                    model: "llama3-3b".to_string(),
                    reason: "medium complexity".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: Some(Complexity::Low),
                    task_type: None,
                    model: "tinyllama-1b".to_string(),
                    reason: "low complexity".to_string(),
                },
            ],
        }
    }
}

impl ModelPolicyEngine {
    pub fn select_model(&self, profile: &PromptProfile) -> String {
        self.select_model_decision(profile).model
    }

    pub fn select_model_decision(&self, profile: &PromptProfile) -> PolicyDecision {
        self.select_model_decision_from_available(profile, &[])
    }

    pub fn candidate_models(&self, profile: &PromptProfile) -> Vec<String> {
        let mut ordered = Vec::new();
        for rule in self.matching_rules(profile) {
            if !ordered.contains(&rule.model) {
                ordered.push(rule.model.clone());
            }
        }

        for fallback in ["llama3-3b", "mistral-7b", "tinyllama-1b"] {
            if !ordered.iter().any(|model| model == fallback) {
                ordered.push(fallback.to_string());
            }
        }

        ordered
    }

    pub fn select_model_from_available(
        &self,
        profile: &PromptProfile,
        available_models: &[String],
    ) -> String {
        self.select_model_decision_from_available(profile, available_models).model
    }

    pub fn select_model_decision_from_available(
        &self,
        profile: &PromptProfile,
        available_models: &[String],
    ) -> PolicyDecision {
        if available_models.is_empty() {
            if let Some(rule) = self.matching_rules(profile).next() {
                return PolicyDecision {
                    model: rule.model.clone(),
                    reason: rule.reason.clone(),
                };
            }

            return PolicyDecision {
                model: "tinyllama-1b".to_string(),
                reason: "default fallback".to_string(),
            };
        }

        for rule in self.matching_rules(profile) {
            if available_models.iter().any(|available| available == &rule.model) {
                return PolicyDecision {
                    model: rule.model.clone(),
                    reason: rule.reason.clone(),
                };
            }
        }

        for fallback in ["llama3-3b", "mistral-7b", "tinyllama-1b"] {
            if available_models.iter().any(|available| available == fallback) {
                return PolicyDecision {
                    model: fallback.to_string(),
                    reason: "availability fallback".to_string(),
                };
            }
        }

        PolicyDecision {
            model: available_models
                .first()
                .cloned()
                .unwrap_or_else(|| "tinyllama-1b".to_string()),
            reason: "availability fallback".to_string(),
        }
    }

    fn matching_rules<'a>(&'a self, profile: &'a PromptProfile) -> impl Iterator<Item = &'a PolicyRule> {
        self.rules.iter().filter(|rule| {
            let task_ok = rule
                .task_type
                .map(|task| task == profile.task_type)
                .unwrap_or(true);
            let language_ok = rule
                .language
                .map(|lang| lang == profile.language)
                .unwrap_or(true);
            let complexity_ok = rule
                .complexity
                .map(|level| level == profile.complexity)
                .unwrap_or(true);

            task_ok && language_ok && complexity_ok
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spanish_high_conceptual() -> PromptProfile {
        PromptProfile {
            language: Language::Spanish,
            complexity: Complexity::High,
            length: 42,
            task_type: TaskType::Conceptual,
        }
    }

    #[test]
    fn test_model_selection_spanish() {
        let engine = ModelPolicyEngine::default();
        let selected = engine.select_model(&spanish_high_conceptual());
        assert_eq!(selected, "llama3-3b");
    }

    #[test]
    fn test_model_selection_task_math() {
        let engine = ModelPolicyEngine::default();
        let selected = engine.select_model(&PromptProfile {
            language: Language::Unknown,
            complexity: Complexity::Low,
            length: 3,
            task_type: TaskType::Math,
        });
        assert_eq!(selected, "llama3-3b");
    }

    #[test]
    fn test_model_selection_task_reasoning() {
        let engine = ModelPolicyEngine::default();
        let selected = engine.select_model(&PromptProfile {
            language: Language::English,
            complexity: Complexity::Medium,
            length: 32,
            task_type: TaskType::Reasoning,
        });
        assert_eq!(selected, "mistral-7b");
    }

    #[test]
    fn test_policy_priority_resolution() {
        let engine = ModelPolicyEngine::default();
        let decision = engine.select_model_decision(&PromptProfile {
            language: Language::Spanish,
            complexity: Complexity::High,
            length: 80,
            task_type: TaskType::Reasoning,
        });
        assert_eq!(decision.model, "mistral-7b");
        assert_eq!(decision.reason, "reasoning task");
    }

    #[test]
    fn test_model_fallback() {
        let engine = ModelPolicyEngine::default();
        let available = vec!["tinyllama-1b".to_string()];
        let decision = engine.select_model_decision_from_available(&spanish_high_conceptual(), &available);
        assert_eq!(decision.model, "tinyllama-1b");
        assert_eq!(decision.reason, "availability fallback");
    }
}
