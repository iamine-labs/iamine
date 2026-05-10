use crate::model_requirements::{can_node_run_model, ModelRequirements};
use crate::node_capabilities::NodeCapabilities;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub model_id: String,
    pub size_gb: f64,
    pub quality_score: u32, // 1-100
    pub speed_score: u32,   // 1-100 (mayor = más rápido)
}

impl ModelInfo {
    pub fn registry() -> Vec<Self> {
        vec![
            Self {
                model_id: "tinyllama-1b".to_string(),
                size_gb: 0.6,
                quality_score: 30,
                speed_score: 95,
            },
            Self {
                model_id: "llama3-3b".to_string(),
                size_gb: 1.7,
                quality_score: 65,
                speed_score: 60,
            },
            Self {
                model_id: "mistral-7b".to_string(),
                size_gb: 3.8,
                quality_score: 90,
                speed_score: 30,
            },
        ]
    }
}

/// Estimar tokens en un prompt
pub fn estimate_tokens(prompt: &str) -> usize {
    // Heurística: ~4 caracteres por token (inglés promedio)
    (prompt.len() as f64 / 4.0).ceil() as usize
}

/// Clasificar complejidad del prompt
#[derive(Debug, Clone, PartialEq)]
pub enum PromptComplexity {
    Simple,  // < 20 tokens
    Medium,  // 20-100 tokens
    Complex, // > 100 tokens
}

pub fn classify_prompt(prompt: &str) -> PromptComplexity {
    let tokens = estimate_tokens(prompt);
    let words = prompt.split_whitespace().count();
    let chars = prompt.chars().count();

    // Heurística más robusta:
    // - complex si el prompt es claramente largo
    // - medium en rango intermedio
    // - simple para prompts cortos
    if tokens >= 80 || words >= 60 || chars >= 300 {
        PromptComplexity::Complex
    } else if tokens >= 20 || words >= 15 || chars >= 80 {
        PromptComplexity::Medium
    } else {
        PromptComplexity::Simple
    }
}

/// Seleccionar el mejor modelo para un prompt dado las capacidades del nodo
pub fn select_best_model(
    prompt: &str,
    available_models: &[String],
    capabilities: &NodeCapabilities,
) -> Option<String> {
    if available_models.is_empty() {
        return None;
    }

    let complexity = classify_prompt(prompt);
    let registry = ModelInfo::registry();

    // Filtrar modelos que el nodo puede ejecutar
    let runnable: Vec<&ModelInfo> = registry
        .iter()
        .filter(|m| available_models.contains(&m.model_id))
        .filter(|m| {
            ModelRequirements::for_model(&m.model_id)
                .map(|req| can_node_run_model(capabilities, &req))
                .unwrap_or(false)
        })
        .collect();

    if runnable.is_empty() {
        return available_models.first().cloned();
    }

    // Seleccionar según complejidad
    let selected = match complexity {
        PromptComplexity::Simple => {
            // Priorizar velocidad
            runnable.iter().max_by_key(|m| m.speed_score)
        }
        PromptComplexity::Medium => {
            // Balance calidad/velocidad
            runnable
                .iter()
                .max_by_key(|m| m.quality_score + m.speed_score)
        }
        PromptComplexity::Complex => {
            // Priorizar calidad fuertemente
            runnable
                .iter()
                .max_by_key(|m| m.quality_score * 3 + m.speed_score)
        }
    };

    selected.map(|m| m.model_id.clone())
}
