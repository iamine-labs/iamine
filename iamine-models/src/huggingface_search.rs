use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HFModel {
    #[serde(rename = "modelId")]
    pub id: String,
    #[serde(default)]
    pub downloads: u64,
    #[serde(default)]
    pub likes: u64,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub gated: serde_json::Value, // ← can be bool, string, or missing
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub private: bool,
    #[serde(flatten)]
    pub _extra: HashMap<String, serde_json::Value>,
}

impl HFModel {
    pub fn is_gated(&self) -> bool {
        match &self.gated {
            serde_json::Value::Bool(b) => *b,
            serde_json::Value::String(s) => s != "false" && !s.is_empty(),
            _ => false,
        }
    }
}

pub struct HuggingFaceSearch;

impl HuggingFaceSearch {
    /// Buscar modelos GGUF en HuggingFace (sin descargar catálogo completo)
    pub async fn search_gguf_models(query: &str, limit: usize) -> Result<Vec<HFModel>, String> {
        // API: https://huggingface.co/api/models?search=query&filter=gguf&sort=downloads
        let url = format!(
            "https://huggingface.co/api/models?search={}&filter=gguf&sort=downloads&limit={}",
            urlencoding::encode(query),
            limit
        );

        let client = reqwest::Client::new();
        let resp = client
            .get(&url)
            .header("User-Agent", "iamine-node/0.6")
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("HF request failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("HF API error: {}", resp.status()));
        }

        let models: Vec<HFModel> = resp
            .json()
            .await
            .map_err(|e| format!("Parse error: {}", e))?;

        Ok(models)
    }

    /// Lista modelos trending (sin búsqueda)
    pub async fn trending_models(limit: usize) -> Result<Vec<HFModel>, String> {
        Self::search_gguf_models("*", limit).await
    }

    /// Filtrar por criterios (tamaño estimado, licencia abierta, etc.)
    pub fn filter_suitable(models: Vec<HFModel>) -> Vec<HFModel> {
        let filtered: Vec<HFModel> = models
            .iter()
            .filter(|m| !m.is_gated() && !m.private && m.downloads >= 100)
            .cloned()
            .collect();

        // Si el filtro deja vacío, devolver todos los no-gated
        if filtered.is_empty() {
            models
                .into_iter()
                .filter(|m| !m.is_gated() && !m.private)
                .collect()
        } else {
            filtered
        }
    }
}
