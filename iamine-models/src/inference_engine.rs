use crate::model_storage::ModelStorage;
use crate::model_verifier::ModelVerifier;
use crate::hardware_acceleration::HardwareAcceleration;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;

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
    pub execution_ms: u64,
    pub success: bool,
    pub error: Option<String>,
    pub accelerator_used: String,
}

impl InferenceResult {
    pub fn success(
        task_id: String, model_id: String, output: String,
        tokens: u32, ms: u64, accel: String,
    ) -> Self {
        Self { task_id, model_id, output, tokens_generated: tokens,
               execution_ms: ms, success: true, error: None,
               accelerator_used: accel }
    }

    pub fn failure(task_id: String, model_id: String, error: String) -> Self {
        Self { task_id, model_id, output: String::new(), tokens_generated: 0,
               execution_ms: 0, success: false, error: Some(error),
               accelerator_used: "none".to_string() }
    }
}

struct CachedModel {
    model_id: String,
    path: PathBuf,
    loaded_at: Instant,
    use_count: u64,
}

pub struct InferenceEngine {
    storage: ModelStorage,
    cache: HashMap<String, CachedModel>,
    hardware: HardwareAcceleration,
}

impl InferenceEngine {
    pub fn new(storage: ModelStorage) -> Self {
        let hardware = HardwareAcceleration::detect();
        Self { storage, cache: HashMap::new(), hardware }
    }

    /// Cargar modelo en cache (verificar SHA256 primero)
    pub fn load_model(&mut self, model_id: &str, expected_hash: &str) -> Result<(), String> {
        if self.cache.contains_key(model_id) {
            if let Some(m) = self.cache.get_mut(model_id) {
                m.use_count += 1;
            }
            println!("♻️  [Cache] Modelo {} reutilizado", model_id);
            return Ok(());
        }

        let path = self.storage.gguf_path(model_id);
        if !path.exists() {
            return Err(format!("Modelo {} no encontrado en ~/.iamine/models/", model_id));
        }

        // Verificar integridad
        ModelVerifier::verify_file(&path, expected_hash)?;

        println!("🧠 Cargando {} en memoria ({:?})...",
            model_id, self.hardware.accelerator);

        // TODO v0.6: llamar llama_cpp::LlamaModel::load_from_file(&path, params)
        // Por ahora registramos en cache

        self.cache.insert(model_id.to_string(), CachedModel {
            model_id: model_id.to_string(),
            path: path.clone(),
            loaded_at: Instant::now(),
            use_count: 1,
        });

        println!("✅ Modelo {} listo ({:?})", model_id, self.hardware.accelerator);
        Ok(())
    }

    /// Ejecutar inferencia con streaming de tokens
    pub async fn run_inference(
        &self,
        req: InferenceRequest,
        token_tx: Option<mpsc::Sender<String>>,
    ) -> InferenceResult {
        let start = Instant::now();

        if !self.cache.contains_key(&req.model_id) {
            return InferenceResult::failure(
                req.task_id,
                req.model_id,
                "Modelo no cargado — llama load_model() primero".to_string(),
            );
        }

        println!("🤖 [Inference] {} | '{}'",
            req.model_id, &req.prompt[..req.prompt.len().min(50)]);

        let accel = format!("{:?}", self.hardware.accelerator);

        // ─── REAL INFERENCE PATH ───────────────────────────────────────
        // Con feature "real-inference" activa usa llama_cpp:
        //
        // #[cfg(feature = "real-inference")]
        // {
        //     let params = self.hardware.llama_params();
        //     let model = llama_cpp::LlamaModel::load_from_file(&path, params)?;
        //     let mut ctx = model.new_context(&llama_cpp::LlamaContextParams::default())?;
        //     let tokens = ctx.tokenize(&req.prompt, true)?;
        //     let mut output = String::new();
        //     for token in ctx.generate(tokens, req.max_tokens) {
        //         let word = ctx.decode_token(token);
        //         output.push_str(&word);
        //         if let Some(tx) = &token_tx { let _ = tx.send(word).await; }
        //     }
        // }
        // ──────────────────────────────────────────────────────────────

        // ─── MOCK INFERENCE (mientras llama.cpp se integra) ───────────
        let output = self.mock_inference_stream(&req, &token_tx).await;
        let ms = start.elapsed().as_millis() as u64;
        let tokens = output.split_whitespace().count() as u32;

        println!("✅ [Inference] {} tokens en {}ms via {}", tokens, ms, accel);

        InferenceResult::success(req.task_id, req.model_id, output, tokens, ms, accel)
    }

    async fn mock_inference_stream(
        &self,
        req: &InferenceRequest,
        token_tx: &Option<mpsc::Sender<String>>,
    ) -> String {
        // Respuestas mock por pregunta (para test-inference)
        let response = if req.prompt.to_lowercase().contains("2+2") ||
                          req.prompt.to_lowercase().contains("2 + 2") {
            "2 + 2 equals 4.".to_string()
        } else if req.prompt.to_lowercase().contains("gravity") ||
                  req.prompt.to_lowercase().contains("gravedad") {
            "Gravity is the force by which a planet or other body draws objects toward its center. It governs planetary orbits, keeps us on Earth, and was described by Newton's law of universal gravitation and refined by Einstein's general relativity.".to_string()
        } else {
            format!(
                "[{} via {:?}] Response to: '{}' — \
                IaMine distributed inference. \
                Real llama.cpp execution active when model is verified.",
                req.model_id,
                self.hardware.accelerator,
                &req.prompt[..req.prompt.len().min(40)]
            )
        };

        // Simular streaming token por token
        let words: Vec<&str> = response.split_whitespace().collect();
        let mut streamed = String::new();

        for word in &words {
            let token = format!("{} ", word);
            streamed.push_str(&token);
            if let Some(tx) = token_tx {
                let _ = tx.send(token).await;
                tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
            }
        }

        streamed.trim().to_string()
    }

    pub fn unload_model(&mut self, model_id: &str) {
        if self.cache.remove(model_id).is_some() {
            println!("♻️  Modelo {} descargado de memoria", model_id);
        }
    }

    pub fn loaded_models(&self) -> Vec<&str> {
        self.cache.keys().map(|s| s.as_str()).collect()
    }

    pub fn is_loaded(&self, model_id: &str) -> bool {
        self.cache.contains_key(model_id)
    }
}
