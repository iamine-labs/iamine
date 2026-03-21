use crate::model_storage::ModelStorage;
use crate::model_verifier::ModelVerifier;
use crate::hardware_acceleration::HardwareAcceleration;
use encoding_rs::UTF_8;
use llama_cpp_2::context::params::LlamaContextParams;
use llama_cpp_2::llama_backend::LlamaBackend;
use llama_cpp_2::llama_batch::LlamaBatch;
use llama_cpp_2::model::{AddBos, LlamaModel};
use llama_cpp_2::model::params::LlamaModelParams;
use llama_cpp_2::sampling::LlamaSampler;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
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

#[allow(dead_code)]
struct CachedModel {
    model_id: String,
    path: PathBuf,
    model: Arc<LlamaModel>,
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

    fn backend() -> Result<&'static LlamaBackend, String> {
        static BACKEND: OnceLock<Result<LlamaBackend, String>> = OnceLock::new();
        BACKEND
            .get_or_init(|| LlamaBackend::init().map_err(|e| format!("No se pudo inicializar llama backend: {e}")))
            .as_ref()
            .map_err(Clone::clone)
    }

    fn model_params(&self) -> LlamaModelParams {
        let params = self.hardware.llama_params();
        LlamaModelParams::default()
            .with_n_gpu_layers(params.n_gpu_layers.max(0) as u32)
            .with_use_mmap(params.use_mmap)
            .with_use_mlock(params.use_mlock)
    }

    fn context_params(&self, max_tokens: u32) -> LlamaContextParams {
        let params = self.hardware.llama_params();
        let n_ctx = NonZeroU32::new((max_tokens.max(512) * 2).max(2048)).unwrap();
        LlamaContextParams::default()
            .with_n_ctx(Some(n_ctx))
            .with_n_threads(params.n_threads)
            .with_n_threads_batch(params.n_threads)
    }

    fn format_prompt(prompt: &str) -> String {
        format!(
            "<|system|>\nYou are a concise and helpful assistant.\n<|user|>\n{}\n<|assistant|>\n",
            prompt.trim()
        )
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

        // Verify integrity (skips if hash is empty/placeholder)
        ModelVerifier::verify_file(&path, expected_hash)?;

        // Check file size sanity
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if size < 1000 {
            return Err(format!("Model file {} too small ({} bytes) — likely a mock or corrupt file", model_id, size));
        }

        println!("🧠 Cargando {} en memoria ({:?}, {:.1} MB)...",
            model_id, self.hardware.accelerator, size as f64 / 1_048_576.0);

        let backend = Self::backend()?;
        let model = LlamaModel::load_from_file(backend, &path, &self.model_params())
            .map_err(|e| format!("No se pudo cargar GGUF {}: {}", model_id, e))?;

        self.cache.insert(model_id.to_string(), CachedModel {
            model_id: model_id.to_string(),
            path: path.clone(),
            model: Arc::new(model),
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
        let Some(cached) = self.cache.get(&req.model_id) else {
            return InferenceResult::failure(
                req.task_id,
                req.model_id,
                "Modelo no cargado — llama load_model() primero".to_string(),
            );
        };

        println!("🤖 [Inference REAL] {} | '{}'",
            req.model_id, &req.prompt[..req.prompt.len().min(50)]);

        let start = Instant::now();
        let accel = format!("{:?}", self.hardware.accelerator);
        let model = Arc::clone(&cached.model);
        let prompt = Self::format_prompt(&req.prompt);
        let max_tokens = req.max_tokens;
        let ctx_params = self.context_params(req.max_tokens);

        let inference = tokio::task::spawn_blocking(move || -> Result<(String, u32), String> {
            let backend = Self::backend()?;
            let mut ctx = model
                .new_context(backend, ctx_params)
                .map_err(|e| format!("No se pudo crear contexto llama: {}", e))?;

            let prompt_tokens = model
                .str_to_token(&prompt, AddBos::Always)
                .map_err(|e| format!("No se pudo tokenizar prompt: {}", e))?;

            if prompt_tokens.is_empty() {
                return Err("Prompt vacío tras tokenización".to_string());
            }

            let mut batch = LlamaBatch::new(512, 1);
            let last_index = (prompt_tokens.len() - 1) as i32;
            for (i, token) in (0_i32..).zip(prompt_tokens.into_iter()) {
                batch.add(token, i, &[0], i == last_index)
                    .map_err(|e| format!("No se pudo preparar batch inicial: {}", e))?;
            }

            ctx.decode(&mut batch)
                .map_err(|e| format!("Fallo evaluando prompt: {}", e))?;

            let mut sampler = LlamaSampler::chain_simple([
                LlamaSampler::dist(1234),
                LlamaSampler::greedy(),
            ]);
            let mut decoder = UTF_8.new_decoder();
            let mut output = String::new();
            let mut n_cur = batch.n_tokens();
            let mut generated = 0u32;

            while generated < max_tokens {
                let token = sampler.sample(&ctx, batch.n_tokens() - 1);
                sampler.accept(token);

                if model.is_eog_token(token) {
                    break;
                }

                let piece = model
                    .token_to_piece(token, &mut decoder, true, None)
                    .map_err(|e| format!("No se pudo decodificar token: {}", e))?;

                if let Some(tx) = &token_tx {
                    tx.blocking_send(piece.clone())
                        .map_err(|e| format!("No se pudo enviar token stream: {}", e))?;
                }

                output.push_str(&piece);
                batch.clear();
                batch.add(token, n_cur, &[0], true)
                    .map_err(|e| format!("No se pudo preparar batch de generación: {}", e))?;
                ctx.decode(&mut batch)
                    .map_err(|e| format!("Fallo evaluando token generado: {}", e))?;

                n_cur += 1;
                generated += 1;
            }

            Ok((output.trim().to_string(), generated))
        })
        .await;

        match inference {
            Ok(Ok((output, tokens))) => {
                let ms = start.elapsed().as_millis() as u64;
                println!("✅ [Inference REAL] {} tokens en {}ms via {}", tokens, ms, accel);
                InferenceResult::success(req.task_id, req.model_id, output, tokens, ms, accel)
            }
            Ok(Err(e)) => InferenceResult::failure(req.task_id, req.model_id, e),
            Err(e) => InferenceResult::failure(
                req.task_id,
                req.model_id,
                format!("Inference task failed: {}", e),
            ),
        }
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
