use crate::inference_engine::{
    InferenceEngine as RealInferenceEngine, InferenceRequest as RealInferenceRequest,
    InferenceResult as RealInferenceResult,
};
use crate::model_registry::ModelRegistry;
use crate::model_storage::ModelStorage;
use std::collections::HashSet;
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
    pub fn success(
        task_id: String,
        model_id: String,
        output: String,
        tokens: u32,
        ms: u64,
    ) -> Self {
        Self {
            task_id,
            model_id,
            output,
            tokens_generated: tokens,
            truncated: false,
            continuation_steps: 0,
            execution_ms: ms,
            success: true,
            error: None,
        }
    }

    pub fn failure(task_id: String, model_id: String, error: String) -> Self {
        Self {
            task_id,
            model_id,
            output: String::new(),
            tokens_generated: 0,
            truncated: false,
            continuation_steps: 0,
            execution_ms: 0,
            success: false,
            error: Some(error),
        }
    }
}

pub struct InferenceEngine {
    engine: RealInferenceEngine,
    registry: ModelRegistry,
    loaded_models: HashSet<String>,
}

impl InferenceEngine {
    pub fn new(storage: ModelStorage) -> Self {
        Self {
            engine: RealInferenceEngine::new(storage),
            registry: ModelRegistry::new(),
            loaded_models: HashSet::new(),
        }
    }

    /// Carga modelo en memoria usando el runtime real de llama.cpp.
    pub fn load_model(&mut self, model_id: &str) -> Result<(), String> {
        if self.loaded_models.contains(model_id) {
            println!("✅ Modelo {} ya cargado", model_id);
            return Ok(());
        }

        let expected_hash = self
            .registry
            .get(model_id)
            .map(|model| model.hash.as_str())
            .unwrap_or("skip");
        self.engine.load_model(model_id, expected_hash)?;
        self.loaded_models.insert(model_id.to_string());

        println!("✅ Modelo {} cargado en memoria", model_id);
        Ok(())
    }

    /// Ejecuta inferencia
    pub async fn run_inference(&self, req: InferenceRequest) -> InferenceResult {
        let start = Instant::now();

        if !self.loaded_models.contains(&req.model_id) {
            return InferenceResult::failure(
                req.task_id,
                req.model_id,
                "Modelo no cargado".to_string(),
            );
        }

        let task_id = req.task_id.clone();
        let model_id = req.model_id.clone();
        let real_req = RealInferenceRequest {
            task_id: req.task_id,
            model_id: req.model_id,
            prompt: req.prompt,
            max_tokens: req.max_tokens,
            temperature: req.temperature,
        };

        let real_result = self.engine.run_inference(real_req, None).await;
        let mut result = map_real_result(real_result);
        result.execution_ms = start.elapsed().as_millis() as u64;

        if result.success {
            println!(
                "✅ [Inference] {} tokens en {}ms",
                result.tokens_generated, result.execution_ms
            );
            return result;
        }

        let error = result
            .error
            .take()
            .unwrap_or_else(|| "Fallo de inferencia".to_string());
        InferenceResult::failure(task_id, model_id, error)
    }

    /// Descarga modelo de memoria
    pub fn unload_model(&mut self, model_id: &str) {
        if self.loaded_models.remove(model_id) {
            self.engine.unload_model(model_id);
            println!("♻️  Modelo {} descargado de memoria", model_id);
        }
    }

    pub fn loaded_models(&self) -> Vec<&str> {
        self.loaded_models.iter().map(|s| s.as_str()).collect()
    }
}

fn map_real_result(real: RealInferenceResult) -> InferenceResult {
    InferenceResult {
        task_id: real.task_id,
        model_id: real.model_id,
        output: real.output,
        tokens_generated: real.tokens_generated,
        truncated: real.truncated,
        continuation_steps: real.continuation_steps,
        execution_ms: real.execution_ms,
        success: real.success,
        error: real.error,
    }
}
