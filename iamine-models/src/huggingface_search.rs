use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HFModel {
    pub id: String,                 // "mistralai/Mistral-7B-Instruct-v0.1"
    pub downloads: u64,
    pub likes: u64,
    pub tags: Vec<String>,
    pub gated: bool,
    pub description: Option<String>,
}

pub struct HuggingFaceSearch;

impl HuggingFaceSearch {
    /// Buscar modelos GGUF en HuggingFace (sin descargar catálogo completo)
    pub async fn search_gguf_models(
        query: &str,
        limit: usize,
    ) -> Result<Vec<HFModel>, String> {
        // API: https://huggingface.co/api/models?search=query&filter=gguf&sort=downloads
        let url = format!(
            "https://huggingface.co/api/models?search={}&filter=gguf&sort=downloads&limit={}",
            urlencoding::encode(query),
            limit
        );

        let client = reqwest::Client::new();
        let resp = client.get(&url)
            .header("User-Agent", "iamine-node/0.6")
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("HF request failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("HF API error: {}", resp.status()));
        }

        let models: Vec<HFModel> = resp.json().await
            .map_err(|e| format!("Parse error: {}", e))?;

        Ok(models)
    }

    /// Lista modelos trending (sin búsqueda)
    pub async fn trending_models(limit: usize) -> Result<Vec<HFModel>, String> {
        Self::search_gguf_models("*", limit).await
    }

    /// Filtrar por criterios (tamaño estimado, licencia abierta, etc.)
    pub fn filter_suitable(models: Vec<HFModel>) -> Vec<HFModel> {
        models.into_iter()
            .filter(|m| {
                // Excluir gated (requieren login)
                !m.gated
                // Solo LLMs populares (1K+ descargas)
                && m.downloads >= 1000
                // Validar tags
                && (m.tags.contains(&"llama".to_string())
                    || m.tags.contains(&"mistral".to_string())
                    || m.tags.contains(&"phi".to_string())
                    || m.tags.contains(&"qwen".to_string()))
            })
            .collect()
    }
}
