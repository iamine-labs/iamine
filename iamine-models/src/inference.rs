use crate::model_storage::ModelStorage;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct InferenceRequest {
    pub task_id: String,
    pub model_id: String,
    pub prompt: String,
    pub max_tokens: u32,
    pub temperature: f32,
}

#[derive(Debug, Clone)]
pub struct InferenceResult {
    pub task_id: String,
    pub model_id: String,
    pub output: String,
    pub tokens_generated: u32,
    pub truncated: bool,
    pub continuation_steps: usize,
    pub execution_ms: u64,
    pub success: bool,
    pub error: Option<String>,
}

impl InferenceResult {
    pub fn success(task_id: String, model_id: String, output: String, tokens: u32, ms: u64) -> Self {
        Self { task_id, model_id, output, tokens_generated: tokens, truncated: false, continuation_steps: 0, execution_ms: ms, success: true, error: None }
    }

    pub fn failure(task_id: String, model_id: String, error: String) -> Self {
        Self { task_id, model_id, output: String::new(), tokens_generated: 0, truncated: false, continuation_steps: 0, execution_ms: 0, success: false, error: Some(error) }
    }
}

pub struct InferenceEngine {
    storage: ModelStorage,
    loaded_models: HashMap<String, LoadedModel>,
}

struct LoadedModel {
    _model_id: String,
    _loaded_at: Instant,
}

impl InferenceEngine {
    pub fn new(storage: ModelStorage) -> Self {
        Self {
            storage,
            loaded_models: HashMap::new(),
        }
    }

    /// Carga modelo en memoria (placeholder — en v0.6 usa llama.cpp bindings)
    pub fn load_model(&mut self, model_id: &str) -> Result<(), String> {
        if self.loaded_models.contains_key(model_id) {
            println!("✅ Modelo {} ya cargado", model_id);
            return Ok(());
        }

        let path = self.storage.gguf_path(model_id);
        if !path.exists() {
            return Err(format!("Modelo {} no encontrado en disco", model_id));
        }

        println!("🧠 Cargando modelo {}...", model_id);
        // TODO v0.6: llama_cpp::LlamaModel::load_from_file(&path)

        self.loaded_models.insert(model_id.to_string(), LoadedModel {
            _model_id: model_id.to_string(),
            _loaded_at: Instant::now(),
        });

        println!("✅ Modelo {} cargado en memoria", model_id);
        Ok(())
    }

    /// Ejecuta inferencia
    pub async fn run_inference(&self, req: InferenceRequest) -> InferenceResult {
        let start = Instant::now();

        if !self.loaded_models.contains_key(&req.model_id) {
            return InferenceResult::failure(
                req.task_id,
                req.model_id,
                "Modelo no cargado".to_string(),
            );
        }

        println!("🤖 [Inference] {} | prompt={:.40}...", req.model_id, req.prompt);

        // TODO v0.6: llamada real a llama.cpp
        // Por ahora: mock que demuestra el flujo
        let mock_output = self.mock_inference(&req);
        let ms = start.elapsed().as_millis() as u64;
        let tokens = mock_output.split_whitespace().count() as u32;

        println!("✅ [Inference] {} tokens en {}ms", tokens, ms);

        InferenceResult::success(req.task_id, req.model_id, mock_output, tokens, ms)
    }

    fn mock_inference(&self, req: &InferenceRequest) -> String {
        // Mock determinista — respuesta basada en el prompt
        format!(
            "[{} mock] Respuesta al prompt: '{}' — \
            IaMine distributed inference placeholder. \
            Real llama.cpp execution coming in v0.6.",
            req.model_id,
            &req.prompt[..req.prompt.len().min(30)]
        )
    }

    /// Descarga modelo de memoria
    pub fn unload_model(&mut self, model_id: &str) {
        if self.loaded_models.remove(model_id).is_some() {
            println!("♻️  Modelo {} descargado de memoria", model_id);
        }
    }

    pub fn loaded_models(&self) -> Vec<&str> {
        self.loaded_models.keys().map(|s| s.as_str()).collect()
    }
}
