use crate::continuation_manager::ContinuationManager;
use crate::hardware_acceleration::{AcceleratorType, HardwareAcceleration};
use crate::inference_queue::InferenceQueue;
use crate::model_cache::{LoadedModel, ModelCache};
use crate::output_cleaner::clean_output;
use crate::prompt_builder::PromptBuilder;
use crate::model_storage::ModelStorage;
use crate::model_verifier::ModelVerifier;
use encoding_rs::UTF_8;
use llama_cpp_2::context::params::LlamaContextParams;
use llama_cpp_2::llama_backend::LlamaBackend;
use llama_cpp_2::llama_batch::LlamaBatch;
use llama_cpp_2::model::params::LlamaModelParams;
use llama_cpp_2::model::{AddBos, LlamaModel};
use llama_cpp_2::sampling::LlamaSampler;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::{mpsc, Semaphore};

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
    pub accelerator_used: String,
}

#[derive(Debug, Clone)]
pub struct SamplingConfig {
    pub temperature: f32,
    pub top_k: i32,
    pub top_p: f32,
    pub repeat_penalty: f32,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            temperature: 0.7,
            top_k: 40,
            top_p: 0.9,
            repeat_penalty: 1.1,
        }
    }
}

impl SamplingConfig {
    pub fn for_model_request(model_id: &str, prompt: &str, temperature: f32) -> Self {
        let mut config = Self::default();
        if temperature.is_finite() {
            config.temperature = temperature.clamp(0.0, 2.0);
        }
        let model_id = model_id.to_lowercase();
        let prompt = prompt.to_lowercase();

        if model_id.contains("tinyllama") {
            config.temperature = config.temperature.min(0.45);
            config.top_k = 24;
            config.top_p = 0.85;
            config.repeat_penalty = 1.15;
        }

        if prompt.contains("explica") || prompt.contains("teoria") || prompt.contains("relatividad") {
            config.temperature = config.temperature.min(0.35);
            config.top_p = config.top_p.min(0.8);
        }
        config
    }
}

impl InferenceResult {
    pub fn success(
        task_id: String,
        model_id: String,
        output: String,
        tokens: u32,
        truncated: bool,
        ms: u64,
        accel: String,
    ) -> Self {
        Self {
            task_id,
            model_id,
            output,
            tokens_generated: tokens,
            truncated,
            continuation_steps: 0,
            execution_ms: ms,
            success: true,
            error: None,
            accelerator_used: accel,
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
            accelerator_used: "none".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    Metal,
    Cuda,
    Cpu,
}

impl BackendType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metal => "metal",
            Self::Cuda => "cuda",
            Self::Cpu => "cpu",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InferenceContext {
    // Placeholder para futura reutilización de KV cache.
    // KV cache evita recomputar prefijos ya procesados y reduce
    // significativamente el costo por token en prompts repetidos.
    pub reuse_prefix: bool,
}

struct EngineInner {
    storage: ModelStorage,
    model_cache: Arc<ModelCache>,
    hardware: HardwareAcceleration,
    inference_limit: Arc<Semaphore>,
    queue: InferenceQueue,
    queue_started: AtomicBool,
    selected_backend: BackendType,
    active_inferences: AtomicUsize,
    max_active_observed: AtomicUsize,
}

#[derive(Clone)]
pub struct InferenceEngine {
    inner: Arc<EngineInner>,
}

struct GenerationChunk {
    output: String,
    tokens_generated: u32,
    truncated: bool,
    execution_ms: u64,
}

impl InferenceEngine {
    pub fn new(storage: ModelStorage) -> Self {
        Self::with_limits(storage, 1, 32)
    }

    pub fn with_limits(
        storage: ModelStorage,
        max_concurrent_inference: usize,
        queue_capacity: usize,
    ) -> Self {
        let hardware = HardwareAcceleration::detect();
        let selected_backend = Self::select_runtime_backend_for(&hardware);
        println!("[Backend] Selected: {}", selected_backend.as_str());

        Self {
            inner: Arc::new(EngineInner {
                storage,
                model_cache: Arc::new(ModelCache::new()),
                hardware,
                inference_limit: Arc::new(Semaphore::new(max_concurrent_inference.max(1))),
                queue: InferenceQueue::new(queue_capacity.max(1)),
                queue_started: AtomicBool::new(false),
                selected_backend,
                active_inferences: AtomicUsize::new(0),
                max_active_observed: AtomicUsize::new(0),
            }),
        }
    }

    fn backend() -> Result<&'static LlamaBackend, String> {
        static BACKEND: OnceLock<Result<LlamaBackend, String>> = OnceLock::new();
        BACKEND
            .get_or_init(|| {
                let mut backend = LlamaBackend::init()
                    .map_err(|e| format!("No se pudo inicializar llama backend: {e}"))?;
                backend.void_logs();
                Ok(backend)
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    pub fn select_runtime_backend() -> BackendType {
        let hardware = HardwareAcceleration::detect();
        Self::select_runtime_backend_for(&hardware)
    }

    fn select_runtime_backend_for(hardware: &HardwareAcceleration) -> BackendType {
        #[cfg(feature = "metal-backend")]
        if hardware.supports_metal() {
            return BackendType::Metal;
        }

        #[cfg(feature = "cuda-backend")]
        if hardware.supports_cuda() {
            return BackendType::Cuda;
        }

        BackendType::Cpu
    }

    pub fn runtime_backend_name() -> &'static str {
        Self::select_runtime_backend().as_str()
    }

    pub fn selected_backend_name(&self) -> &'static str {
        self.inner.selected_backend.as_str()
    }

    fn model_params(&self) -> LlamaModelParams {
        let params = self.inner.hardware.llama_params();
        let n_gpu_layers = match self.inner.selected_backend {
            BackendType::Metal | BackendType::Cuda => params.n_gpu_layers.max(0) as u32,
            BackendType::Cpu => 0,
        };

        LlamaModelParams::default()
            .with_n_gpu_layers(n_gpu_layers)
            .with_use_mmap(params.use_mmap)
            .with_use_mlock(params.use_mlock)
    }

    fn context_params(&self, max_tokens: u32) -> LlamaContextParams {
        let params = self.inner.hardware.llama_params();
        let n_ctx = NonZeroU32::new((max_tokens.max(512) * 2).max(2048)).unwrap();
        LlamaContextParams::default()
            .with_n_ctx(Some(n_ctx))
            .with_n_threads(params.n_threads)
            .with_n_threads_batch(params.n_threads)
    }

    fn inference_was_truncated(tokens_generated: u32, max_tokens: u32) -> bool {
        max_tokens > 0 && tokens_generated >= max_tokens
    }

    async fn generate_chunk(
        &self,
        model: Arc<LlamaModel>,
        model_id: String,
        user_prompt: String,
        max_tokens: u32,
        temperature: f32,
        token_tx: Option<mpsc::Sender<String>>,
    ) -> Result<GenerationChunk, String> {
        let prompt_builder = PromptBuilder::new(model_id.clone());
        let prompt = prompt_builder.build_with_model(
            model.as_ref(),
            "You are a helpful assistant. Answer clearly, stay factual, and avoid repetition.",
            &user_prompt,
        );
        let ctx_params = self.context_params(max_tokens);
        let sampling = SamplingConfig::for_model_request(&model_id, &user_prompt, temperature);
        let step_start = Instant::now();

        let inference = tokio::task::spawn_blocking(move || -> Result<GenerationChunk, String> {
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
                batch
                    .add(token, i, &[0], i == last_index)
                    .map_err(|e| format!("No se pudo preparar batch inicial: {}", e))?;
            }

            ctx.decode(&mut batch)
                .map_err(|e| format!("Fallo evaluando prompt: {}", e))?;

            let mut samplers = vec![LlamaSampler::penalties(64, sampling.repeat_penalty, 0.0, 0.0)];
            if sampling.top_k > 0 {
                samplers.push(LlamaSampler::top_k(sampling.top_k));
            }
            samplers.push(LlamaSampler::top_p(sampling.top_p, 1));
            if sampling.temperature <= 0.0 {
                samplers.push(LlamaSampler::greedy());
            } else {
                samplers.push(LlamaSampler::temp(sampling.temperature));
                samplers.push(LlamaSampler::dist(1234));
            }
            let mut sampler = LlamaSampler::chain_simple(samplers);
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
                batch
                    .add(token, n_cur, &[0], true)
                    .map_err(|e| format!("No se pudo preparar batch de generación: {}", e))?;
                ctx.decode(&mut batch)
                    .map_err(|e| format!("Fallo evaluando token generado: {}", e))?;

                n_cur += 1;
                generated += 1;
            }

            Ok(GenerationChunk {
                output: output.trim().to_string(),
                tokens_generated: generated,
                truncated: Self::inference_was_truncated(generated, max_tokens),
                execution_ms: 0,
            })
        })
        .await
        .map_err(|e| format!("Inference task failed: {}", e))?;

        let mut chunk = inference?;
        chunk.output = clean_output(chunk.output);
        chunk.execution_ms = step_start.elapsed().as_millis() as u64;
        Ok(chunk)
    }

    fn ensure_queue_worker(&self) {
        if self
            .inner
            .queue_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let Some(mut receiver) = self.inner.queue.take_receiver() else {
            return;
        };

        let engine = self.clone();
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                println!("[Inference] Processing request");
                let crate::inference_queue::InferenceRequest {
                    task_id,
                    prompt,
                    model_id,
                    max_tokens,
                    temperature,
                    token_tx,
                    response_tx,
                } = request;
                let request_id = task_id.clone();
                let model_id_for_log = model_id.clone();
                let result = engine
                    .process_inference(
                        InferenceRequest {
                            task_id,
                            model_id,
                            prompt,
                            max_tokens,
                            temperature,
                        },
                        token_tx,
                    )
                    .await;

                let _ = response_tx.send(result);
                println!(
                    "[Inference] Request completed: {} / {}",
                    request_id, model_id_for_log
                );
            }
        });
    }

    fn update_max_active(&self, current_active: usize) {
        let mut observed = self.inner.max_active_observed.load(Ordering::SeqCst);
        while current_active > observed {
            match self.inner.max_active_observed.compare_exchange(
                observed,
                current_active,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }
    }

    /// Cargar modelo en cache (verificar SHA256 primero)
    pub fn load_model(&self, model_id: &str, expected_hash: &str) -> Result<(), String> {
        if self.inner.model_cache.contains(model_id) {
            let _ = self.inner.model_cache.get(model_id);
            return Ok(());
        }

        let path = self.inner.storage.gguf_path(model_id);
        if !path.exists() {
            return Err(format!("Modelo {} no encontrado en ~/.iamine/models/", model_id));
        }

        ModelVerifier::verify_file(&path, expected_hash)?;

        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if size < 1000 {
            return Err(format!(
                "Model file {} too small ({} bytes) — likely a mock or corrupt file",
                model_id, size
            ));
        }

        println!(
            "[Inference] Loading {} in memory ({:?}, {:.1} MB)...",
            model_id,
            self.inner.hardware.accelerator,
            size as f64 / 1_048_576.0
        );
        println!("[Backend] Runtime: {}", self.selected_backend_name());

        let backend = Self::backend()?;
        self.inner.model_cache.get_or_load(model_id, || {
            let model = LlamaModel::load_from_file(backend, &path, &self.model_params())
                .map_err(|e| format!("No se pudo cargar GGUF {}: {}", model_id, e))?;

            Ok(LoadedModel::new(model_id.to_string(), path.clone(), model))
        })?;

        println!("[Inference] Model {} ready ({:?})", model_id, self.inner.hardware.accelerator);
        Ok(())
    }

    async fn process_inference(
        &self,
        req: InferenceRequest,
        token_tx: Option<mpsc::Sender<String>>,
    ) -> InferenceResult {
        let Some(cached) = self.inner.model_cache.get(&req.model_id) else {
            return InferenceResult::failure(
                req.task_id,
                req.model_id,
                "Modelo no cargado — llama load_model() primero".to_string(),
            );
        };

        let _context = InferenceContext { reuse_prefix: false };
        let permit = match Arc::clone(&self.inner.inference_limit).acquire_owned().await {
            Ok(permit) => permit,
            Err(e) => {
                return InferenceResult::failure(
                    req.task_id,
                    req.model_id,
                    format!("No se pudo adquirir permiso de inferencia: {}", e),
                );
            }
        };

        println!(
            "[Inference] {} | '{}'",
            req.model_id,
            &req.prompt[..req.prompt.len().min(50)]
        );

        let current_active = self.inner.active_inferences.fetch_add(1, Ordering::SeqCst) + 1;
        self.update_max_active(current_active);
        let accel = match self.inner.selected_backend {
            BackendType::Metal => "Metal".to_string(),
            BackendType::Cuda => "CUDA".to_string(),
            BackendType::Cpu => match self.inner.hardware.accelerator {
                AcceleratorType::CPU | AcceleratorType::None => "CPU".to_string(),
                _ => format!("{:?} (CPU fallback)", self.inner.hardware.accelerator),
            },
        };

        let model = Arc::clone(&cached.model);
        let continuation = ContinuationManager::default();
        let mut continuation_steps = 0usize;
        let mut current_prompt = req.prompt.clone();

        let initial_chunk = self
            .generate_chunk(
                Arc::clone(&model),
                req.model_id.clone(),
                current_prompt.clone(),
                req.max_tokens,
                req.temperature,
                token_tx.clone(),
            )
            .await;

        self.inner.active_inferences.fetch_sub(1, Ordering::SeqCst);
        drop(permit);

        match initial_chunk {
            Ok(chunk) => {
                let mut total_output = chunk.output.clone();
                let mut total_tokens = chunk.tokens_generated;
                let mut total_ms = chunk.execution_ms;
                let mut truncated = chunk.truncated;

                if truncated {
                    println!("[Continuation] triggered");
                }

                while truncated && continuation_steps < continuation.max_steps {
                    continuation_steps += 1;
                    println!("[Continuation] step {}", continuation_steps);
                    current_prompt = ContinuationManager::build_continuation_prompt(
                        &req.prompt,
                        &total_output,
                    );

                    let next_chunk = match self
                        .generate_chunk(
                            Arc::clone(&model),
                            req.model_id.clone(),
                            current_prompt.clone(),
                            req.max_tokens,
                            req.temperature,
                            token_tx.clone(),
                        )
                        .await
                    {
                        Ok(chunk) => chunk,
                        Err(e) => {
                            return InferenceResult::failure(req.task_id, req.model_id, e);
                        }
                    };

                    let cleaned = ContinuationManager::remove_overlap(&total_output, &next_chunk.output);
                    if !cleaned.trim().is_empty() {
                        if !total_output.is_empty() && !total_output.ends_with('\n') {
                            total_output.push('\n');
                        }
                        total_output.push_str(cleaned.trim());
                    }

                    total_tokens += next_chunk.tokens_generated;
                    total_ms += next_chunk.execution_ms;
                    truncated = next_chunk.truncated;
                }

                if continuation_steps > 0 {
                    println!("[Continuation] completed in {} steps", continuation_steps);
                }

                println!("[Inference] tokens_generated: {}", total_tokens);
                println!("[Inference] truncated: {}", truncated);
                if truncated {
                    println!("[Warning] Output truncated at token budget");
                }
                println!("[Inference] {} tokens in {}ms via {}", total_tokens, total_ms, accel);

                let mut result = InferenceResult::success(
                    req.task_id,
                    req.model_id,
                    clean_output(total_output),
                    total_tokens,
                    truncated,
                    total_ms,
                    accel,
                );
                result.continuation_steps = continuation_steps;
                result
            }
            Err(e) => InferenceResult::failure(req.task_id, req.model_id, e),
        }
    }

    /// Ejecutar inferencia con cola secuencial y streaming de tokens.
    pub async fn run_inference(
        &self,
        req: InferenceRequest,
        token_tx: Option<mpsc::Sender<String>>,
    ) -> InferenceResult {
        self.ensure_queue_worker();

        let fallback_task_id = req.task_id.clone();
        let fallback_model_id = req.model_id.clone();
        let response_rx = match self.inner.queue.enqueue(req, token_tx).await {
            Ok(rx) => rx,
            Err(e) => {
                return InferenceResult::failure(fallback_task_id, fallback_model_id, e);
            }
        };

        match response_rx.await {
            Ok(result) => result,
            Err(e) => InferenceResult::failure(
                fallback_task_id,
                fallback_model_id,
                format!("La cola de inferencia terminó antes de responder: {}", e),
            ),
        }
    }

    pub fn unload_model(&self, model_id: &str) {
        if self.inner.model_cache.remove(model_id) {
            println!("♻️  Modelo {} descargado de memoria", model_id);
        }
    }

    pub fn loaded_models(&self) -> Vec<String> {
        self.inner.model_cache.loaded_models()
    }

    pub fn is_loaded(&self, model_id: &str) -> bool {
        self.inner.model_cache.contains(model_id)
    }

    pub fn cache_size(&self) -> usize {
        self.inner.model_cache.len()
    }

    pub fn actual_model_loads(&self) -> usize {
        self.inner.model_cache.actual_loads()
    }

    pub fn model_cache_reuse_hits(&self) -> usize {
        self.inner.model_cache.reuse_hits()
    }

    pub fn max_active_inferences_observed(&self) -> usize {
        self.inner.max_active_observed.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::InferenceEngine;

    #[test]
    fn test_truncation_detection() {
        assert!(InferenceEngine::inference_was_truncated(100, 100));
        assert!(!InferenceEngine::inference_was_truncated(99, 100));
        assert!(!InferenceEngine::inference_was_truncated(0, 0));
    }
}
