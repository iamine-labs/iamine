use llama_cpp_2::model::{LlamaChatMessage, LlamaChatTemplate, LlamaModel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    English,
    Spanish,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateType {
    TinyLlama,
    Llama3,
    Mistral,
    Default,
}

#[derive(Debug, Clone)]
pub struct PromptBuilder {
    model_id: String,
}

impl PromptBuilder {
    pub fn new(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
        }
    }

    pub fn get_template(model_id: &str) -> TemplateType {
        let model_id = model_id.to_lowercase();
        if model_id.contains("tinyllama") {
            TemplateType::TinyLlama
        } else if model_id.contains("llama3") || model_id.contains("llama-3") {
            TemplateType::Llama3
        } else if model_id.contains("mistral") {
            TemplateType::Mistral
        } else {
            TemplateType::Default
        }
    }

    pub fn detect_language(prompt: &str) -> Language {
        let prompt = prompt.to_lowercase();
        let has_spanish_chars = ["á", "é", "í", "ó", "ú", "ñ", "¿", "¡"]
            .iter()
            .any(|needle| prompt.contains(needle));
        let spanish_hits = [
            " explica ",
            " teoria ",
            " relatividad",
            " por que",
            " que ",
            " como ",
            " una ",
            " el ",
            " la ",
            " los ",
            " las ",
            " en espanol",
            " español",
        ]
        .iter()
        .filter(|needle| prompt.contains(**needle))
        .count();

        if has_spanish_chars || spanish_hits >= 2 {
            Language::Spanish
        } else if prompt.is_ascii() {
            Language::English
        } else {
            Language::Unknown
        }
    }

    fn system_prompt(&self, system_prompt: &str, user_prompt: &str) -> String {
        let mut system = system_prompt.trim().to_string();
        if system.is_empty() {
            system = "You are a helpful assistant. Answer in the same language as the user. Stay factual, keep the exact original topic, and avoid repetition or drift. Do not replace the user's concept with a similar but different one.".to_string();
        } else if !system.to_lowercase().contains("same language as the user") {
            system.push_str(" Answer in the same language as the user. Stay factual, keep the exact original topic, and avoid repetition or drift. Do not replace the user's concept with a similar but different one.");
        }

        if matches!(Self::detect_language(user_prompt), Language::Spanish)
            && !system.to_lowercase().contains("espanol")
            && !system.to_lowercase().contains("español")
        {
            system.push_str(" Responde solo en espanol. No respondas en ingles. Si no sabes algo, dilo brevemente en espanol.");
        }

        system
    }

    fn fallback_template(&self, system_prompt: &str, user_prompt: &str) -> String {
        let system_prompt = self.system_prompt(system_prompt, user_prompt);
        let user_prompt = self.user_prompt(user_prompt);

        match Self::get_template(&self.model_id) {
            TemplateType::TinyLlama => format!(
                "<|system|>\n{}\n<|user|>\n{}\n<|assistant|>\n",
                system_prompt,
                user_prompt.trim()
            ),
            TemplateType::Llama3 => format!(
                "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n{}\n<|eot_id|><|start_header_id|>user<|end_header_id|>\n\n{}\n<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n",
                system_prompt,
                user_prompt.trim()
            ),
            TemplateType::Mistral => format!(
                "<s>[INST] {}\n\n{} [/INST]",
                system_prompt,
                user_prompt.trim()
            ),
            TemplateType::Default => format!(
                "<|system|>\n{}\n<|user|>\n{}\n<|assistant|>\n",
                system_prompt,
                user_prompt.trim()
            ),
        }
    }

    pub fn build_prompt(&self, system_prompt: &str, user_prompt: &str) -> String {
        self.fallback_template(system_prompt, user_prompt)
    }

    pub fn build_with_model(
        &self,
        model: &LlamaModel,
        system_prompt: &str,
        user_prompt: &str,
    ) -> String {
        let system_prompt = self.system_prompt(system_prompt, user_prompt);
        let user_prompt = self.user_prompt(user_prompt);
        let messages = match [
            LlamaChatMessage::new("system".to_string(), system_prompt.clone()),
            LlamaChatMessage::new("user".to_string(), user_prompt.trim().to_string()),
        ] {
            [Ok(system), Ok(user)] => vec![system, user],
            _ => return self.fallback_template(&system_prompt, &user_prompt),
        };

        let template = model
            .chat_template(None)
            .or_else(|_| LlamaChatTemplate::new(self.template_name()));

        match template {
            Ok(template) => model
                .apply_chat_template(&template, &messages, true)
                .unwrap_or_else(|_| self.fallback_template(&system_prompt, &user_prompt)),
            Err(_) => self.fallback_template(&system_prompt, &user_prompt),
        }
    }

    fn user_prompt<'a>(&self, user_prompt: &'a str) -> std::borrow::Cow<'a, str> {
        if matches!(Self::detect_language(user_prompt), Language::Spanish) {
            std::borrow::Cow::Owned(format!(
                "Pregunta en espanol. Responde en espanol de forma breve y clara.\n{}",
                user_prompt.trim()
            ))
        } else {
            std::borrow::Cow::Borrowed(user_prompt)
        }
    }

    fn template_name(&self) -> &'static str {
        match Self::get_template(&self.model_id) {
            TemplateType::TinyLlama => "chatml",
            TemplateType::Llama3 => "llama3",
            TemplateType::Mistral => "mistral",
            TemplateType::Default => "chatml",
        }
    }
}
