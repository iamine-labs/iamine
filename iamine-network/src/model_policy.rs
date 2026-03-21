use crate::prompt_analyzer::{Complexity, Language, PromptProfile};

#[derive(Debug, Clone)]
pub struct ModelPolicyEngine {
    pub rules: Vec<PolicyRule>,
}

#[derive(Debug, Clone)]
pub struct PolicyRule {
    pub language: Option<Language>,
    pub complexity: Option<Complexity>,
    pub model: String,
}

impl Default for ModelPolicyEngine {
    fn default() -> Self {
        Self {
            rules: vec![
                PolicyRule {
                    language: Some(Language::Spanish),
                    complexity: Some(Complexity::High),
                    model: "llama3-3b".to_string(),
                },
                PolicyRule {
                    language: Some(Language::Spanish),
                    complexity: None,
                    model: "llama3-3b".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: Some(Complexity::High),
                    model: "mistral-7b".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: Some(Complexity::Medium),
                    model: "llama3-3b".to_string(),
                },
                PolicyRule {
                    language: None,
                    complexity: Some(Complexity::Low),
                    model: "tinyllama-1b".to_string(),
                },
            ],
        }
    }
}

impl ModelPolicyEngine {
    pub fn select_model(&self, profile: &PromptProfile) -> String {
        self.candidate_models(profile)
            .into_iter()
            .next()
            .unwrap_or_else(|| "tinyllama-1b".to_string())
    }

    pub fn candidate_models(&self, profile: &PromptProfile) -> Vec<String> {
        let mut ordered = Vec::new();
        for rule in &self.rules {
            let language_ok = rule.language.map(|lang| lang == profile.language).unwrap_or(true);
            let complexity_ok = rule
                .complexity
                .map(|level| level == profile.complexity)
                .unwrap_or(true);

            if language_ok && complexity_ok && !ordered.contains(&rule.model) {
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
        let candidates = self.candidate_models(profile);
        candidates
            .into_iter()
            .find(|candidate| available_models.iter().any(|available| available == candidate))
            .or_else(|| available_models.first().cloned())
            .unwrap_or_else(|| self.select_model(profile))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spanish_high() -> PromptProfile {
        PromptProfile {
            language: Language::Spanish,
            complexity: Complexity::High,
            length: 42,
        }
    }

    #[test]
    fn test_model_selection_spanish() {
        let engine = ModelPolicyEngine::default();
        let selected = engine.select_model(&spanish_high());
        assert_eq!(selected, "llama3-3b");
    }

    #[test]
    fn test_model_selection_complexity() {
        let engine = ModelPolicyEngine::default();
        let selected = engine.select_model(&PromptProfile {
            language: Language::English,
            complexity: Complexity::High,
            length: 120,
        });
        assert_eq!(selected, "mistral-7b");
    }

    #[test]
    fn test_model_fallback() {
        let engine = ModelPolicyEngine::default();
        let available = vec!["tinyllama-1b".to_string()];
        let selected = engine.select_model_from_available(&spanish_high(), &available);
        assert_eq!(selected, "tinyllama-1b");
    }
}
