use iamine_models::{
    ModelRegistry, ModelStorage, RealInferenceEngine, RealInferenceRequest, RealInferenceResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DaemonRequest {
    Ping,
    Shutdown,
    Infer {
        task_id: String,
        model_id: String,
        prompt: String,
        max_tokens: u32,
        temperature: f32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DaemonEvent {
    Pong,
    Ack {
        message: String,
    },
    Token {
        token: String,
        index: u32,
    },
    Result {
        result: DaemonInferenceResult,
        model_load_ms: u64,
        requests_handled: usize,
        actual_model_loads: usize,
        reuse_hits: usize,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct DaemonInferenceResponse {
    pub result: RealInferenceResult,
    pub model_load_ms: u64,
    pub requests_handled: usize,
    pub actual_model_loads: usize,
    pub reuse_hits: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonInferenceResult {
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

impl From<RealInferenceResult> for DaemonInferenceResult {
    fn from(value: RealInferenceResult) -> Self {
        Self {
            task_id: value.task_id,
            model_id: value.model_id,
            output: value.output,
            tokens_generated: value.tokens_generated,
            truncated: value.truncated,
            continuation_steps: value.continuation_steps,
            execution_ms: value.execution_ms,
            success: value.success,
            error: value.error,
            accelerator_used: value.accelerator_used,
        }
    }
}

impl From<DaemonInferenceResult> for RealInferenceResult {
    fn from(value: DaemonInferenceResult) -> Self {
        Self {
            task_id: value.task_id,
            model_id: value.model_id,
            output: value.output,
            tokens_generated: value.tokens_generated,
            truncated: value.truncated,
            continuation_steps: value.continuation_steps,
            execution_ms: value.execution_ms,
            success: value.success,
            error: value.error,
            accelerator_used: value.accelerator_used,
        }
    }
}

#[derive(Debug, Default)]
struct DaemonMetrics {
    requests_handled: AtomicUsize,
    last_inference_ms: AtomicU64,
    last_model_load_ms: AtomicU64,
}

#[derive(Debug, Default)]
struct WarmModelTracker {
    models: Mutex<HashSet<String>>,
}

impl WarmModelTracker {
    async fn mark_loaded(&self, model_id: &str) -> bool {
        self.models.lock().await.insert(model_id.to_string())
    }

    #[cfg(test)]
    async fn is_loaded(&self, model_id: &str) -> bool {
        self.models.lock().await.contains(model_id)
    }
}

#[derive(Clone)]
struct DaemonRuntime {
    engine: Arc<RealInferenceEngine>,
    registry: Arc<ModelRegistry>,
    metrics: Arc<DaemonMetrics>,
    warmed_models: Arc<WarmModelTracker>,
}

impl DaemonRuntime {
    fn new() -> Self {
        Self {
            engine: Arc::new(RealInferenceEngine::new(ModelStorage::new())),
            registry: Arc::new(ModelRegistry::new()),
            metrics: Arc::new(DaemonMetrics::default()),
            warmed_models: Arc::new(WarmModelTracker::default()),
        }
    }
}

pub fn daemon_socket_path() -> PathBuf {
    std::env::var("IAMINE_DAEMON_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("iamine-node-daemon.sock"))
}

pub async fn ping_daemon(socket_path: &Path) -> Result<(), String> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .map_err(|e| format!("No se pudo conectar al daemon: {}", e))?;
    write_message(&mut stream, &DaemonRequest::Ping).await?;
    let mut reader = BufReader::new(stream);
    match read_event(&mut reader).await? {
        Some(DaemonEvent::Pong) => Ok(()),
        Some(DaemonEvent::Error { message }) => Err(message),
        other => Err(format!("Respuesta invalida del daemon: {:?}", other)),
    }
}

#[cfg(test)]
pub async fn shutdown_daemon(socket_path: &Path) -> Result<(), String> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .map_err(|e| format!("No se pudo conectar al daemon: {}", e))?;
    write_message(&mut stream, &DaemonRequest::Shutdown).await?;
    let mut reader = BufReader::new(stream);
    match read_event(&mut reader).await? {
        Some(DaemonEvent::Ack { .. }) => Ok(()),
        Some(DaemonEvent::Error { message }) => Err(message),
        other => Err(format!("Respuesta invalida del daemon: {:?}", other)),
    }
}

pub async fn daemon_is_available(socket_path: &Path) -> bool {
    ping_daemon(socket_path).await.is_ok()
}

pub async fn infer_via_daemon<F>(
    socket_path: &Path,
    request: RealInferenceRequest,
    mut on_token: F,
) -> Result<DaemonInferenceResponse, String>
where
    F: FnMut(String),
{
    let mut stream = UnixStream::connect(socket_path)
        .await
        .map_err(|e| format!("No se pudo conectar al daemon: {}", e))?;
    write_message(
        &mut stream,
        &DaemonRequest::Infer {
            task_id: request.task_id,
            model_id: request.model_id,
            prompt: request.prompt,
            max_tokens: request.max_tokens,
            temperature: request.temperature,
        },
    )
    .await?;

    let mut reader = BufReader::new(stream);
    loop {
        match read_event(&mut reader).await? {
            Some(DaemonEvent::Token { token, .. }) => on_token(token),
            Some(DaemonEvent::Result {
                result,
                model_load_ms,
                requests_handled,
                actual_model_loads,
                reuse_hits,
            }) => {
                return Ok(DaemonInferenceResponse {
                    result: result.into(),
                    model_load_ms,
                    requests_handled,
                    actual_model_loads,
                    reuse_hits,
                });
            }
            Some(DaemonEvent::Error { message }) => return Err(message),
            Some(DaemonEvent::Ack { .. } | DaemonEvent::Pong) => {}
            None => return Err("El daemon cerro la conexion sin responder".to_string()),
        }
    }
}

pub async fn run_daemon(socket_path: PathBuf) -> Result<(), String> {
    run_daemon_with_runtime(socket_path, DaemonRuntime::new()).await
}

async fn run_daemon_with_runtime(
    socket_path: PathBuf,
    runtime: DaemonRuntime,
) -> Result<(), String> {
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }

    let listener = UnixListener::bind(&socket_path).map_err(|e| {
        format!(
            "No se pudo abrir socket del daemon {}: {}",
            socket_path.display(),
            e
        )
    })?;
    println!("[Daemon] Started");
    println!("[Daemon] Listening on {}", socket_path.display());

    let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _) = accept_result
                    .map_err(|e| format!("Error aceptando cliente daemon: {}", e))?;
                let runtime_clone = runtime.clone();
                let shutdown_tx_clone = shutdown_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, runtime_clone, shutdown_tx_clone).await {
                        eprintln!("[Daemon] Connection error: {}", e);
                    }
                });
            }
            _ = shutdown_rx.recv() => {
                println!("[Daemon] Stopping");
                break;
            }
        }
    }

    let _ = std::fs::remove_file(&socket_path);
    Ok(())
}

async fn handle_connection(
    mut stream: UnixStream,
    runtime: DaemonRuntime,
    shutdown_tx: broadcast::Sender<()>,
) -> Result<(), String> {
    let request = {
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .map_err(|e| format!("No se pudo leer request del daemon: {}", e))?;
        if read == 0 {
            return Ok(());
        }
        serde_json::from_str::<DaemonRequest>(line.trim_end())
            .map_err(|e| format!("Request daemon invalido: {}", e))?
    };

    match request {
        DaemonRequest::Ping => {
            write_event(&mut stream, &DaemonEvent::Pong).await?;
        }
        DaemonRequest::Shutdown => {
            write_event(
                &mut stream,
                &DaemonEvent::Ack {
                    message: "Daemon shutdown acknowledged".to_string(),
                },
            )
            .await?;
            let _ = shutdown_tx.send(());
        }
        DaemonRequest::Infer {
            task_id,
            model_id,
            prompt,
            max_tokens,
            temperature,
        } => {
            let already_loaded = runtime.engine.is_loaded(&model_id);
            let load_start = Instant::now();
            let hash = runtime
                .registry
                .get(&model_id)
                .map(|m| m.hash.clone())
                .unwrap_or_default();
            runtime.engine.load_model(&model_id, &hash)?;
            let model_load_ms = if already_loaded {
                0
            } else {
                load_start.elapsed().as_millis() as u64
            };
            if runtime.warmed_models.mark_loaded(&model_id).await && model_load_ms > 0 {
                println!(
                    "[Model] Loaded into memory: {} ({} ms)",
                    model_id, model_load_ms
                );
            }

            let (token_tx, mut token_rx) = mpsc::channel::<String>(100);
            let engine = Arc::clone(&runtime.engine);
            let request = RealInferenceRequest {
                task_id,
                model_id,
                prompt,
                max_tokens,
                temperature,
            };

            let inference_handle =
                tokio::spawn(async move { engine.run_inference(request, Some(token_tx)).await });

            let mut index = 0u32;
            while let Some(token) = token_rx.recv().await {
                write_event(&mut stream, &DaemonEvent::Token { token, index }).await?;
                index += 1;
            }

            let result = inference_handle
                .await
                .map_err(|e| format!("Daemon inference join error: {}", e))?;
            println!("[Inference] Executed in {} ms", result.execution_ms);
            runtime
                .metrics
                .requests_handled
                .fetch_add(1, Ordering::SeqCst);
            runtime
                .metrics
                .last_inference_ms
                .store(result.execution_ms, Ordering::SeqCst);
            runtime
                .metrics
                .last_model_load_ms
                .store(model_load_ms, Ordering::SeqCst);

            write_event(
                &mut stream,
                &DaemonEvent::Result {
                    model_load_ms,
                    requests_handled: runtime.metrics.requests_handled.load(Ordering::SeqCst),
                    actual_model_loads: runtime.engine.actual_model_loads(),
                    reuse_hits: runtime.engine.model_cache_reuse_hits(),
                    result: result.into(),
                },
            )
            .await?;
        }
    }

    Ok(())
}

async fn write_message(stream: &mut UnixStream, request: &DaemonRequest) -> Result<(), String> {
    let payload = serde_json::to_string(request)
        .map_err(|e| format!("No se pudo serializar request daemon: {}", e))?;
    stream
        .write_all(payload.as_bytes())
        .await
        .map_err(|e| format!("No se pudo escribir request daemon: {}", e))?;
    stream
        .write_all(b"\n")
        .await
        .map_err(|e| format!("No se pudo finalizar request daemon: {}", e))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("No se pudo flush request daemon: {}", e))
}

async fn write_event(stream: &mut UnixStream, event: &DaemonEvent) -> Result<(), String> {
    let payload = serde_json::to_string(event)
        .map_err(|e| format!("No se pudo serializar evento daemon: {}", e))?;
    stream
        .write_all(payload.as_bytes())
        .await
        .map_err(|e| format!("No se pudo escribir evento daemon: {}", e))?;
    stream
        .write_all(b"\n")
        .await
        .map_err(|e| format!("No se pudo finalizar evento daemon: {}", e))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("No se pudo flush evento daemon: {}", e))
}

async fn read_event(reader: &mut BufReader<UnixStream>) -> Result<Option<DaemonEvent>, String> {
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .await
        .map_err(|e| format!("No se pudo leer evento daemon: {}", e))?;
    if read == 0 {
        return Ok(None);
    }

    let event = serde_json::from_str::<DaemonEvent>(line.trim_end())
        .map_err(|e| format!("Evento daemon invalido: {}", e))?;
    Ok(Some(event))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_daemon_start_stop() {
        let tmp = tempdir().unwrap();
        let socket_path = tmp.path().join("iamine-daemon.sock");
        let handle = tokio::spawn(run_daemon(socket_path.clone()));

        timeout(Duration::from_secs(2), async {
            loop {
                if daemon_is_available(&socket_path).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();

        shutdown_daemon(&socket_path).await.unwrap();
        timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_model_loaded_once() {
        let tracker = WarmModelTracker::default();
        assert!(tracker.mark_loaded("llama3-3b").await);
        assert!(!tracker.mark_loaded("llama3-3b").await);
        assert!(tracker.is_loaded("llama3-3b").await);
    }

    #[tokio::test]
    async fn test_multiple_requests_no_reload() {
        let tracker = WarmModelTracker::default();
        assert!(tracker.mark_loaded("llama3-3b").await);
        assert!(!tracker.mark_loaded("llama3-3b").await);
        assert!(!tracker.mark_loaded("llama3-3b").await);
        assert!(tracker.mark_loaded("mistral-7b").await);
        assert!(tracker.is_loaded("mistral-7b").await);
    }
}
