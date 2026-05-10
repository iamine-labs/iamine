use crate::inference_engine::{InferenceRequest as EngineInferenceRequest, InferenceResult};
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

pub struct InferenceRequest {
    pub task_id: String,
    pub prompt: String,
    pub model_id: String,
    pub max_tokens: u32,
    pub temperature: f32,
    pub token_tx: Option<mpsc::Sender<String>>,
    pub response_tx: oneshot::Sender<InferenceResult>,
}

impl InferenceRequest {
    pub fn into_engine_request(self) -> EngineInferenceRequest {
        EngineInferenceRequest {
            task_id: self.task_id,
            model_id: self.model_id,
            prompt: self.prompt,
            max_tokens: self.max_tokens,
            temperature: self.temperature,
        }
    }
}

pub struct InferenceQueue {
    sender: mpsc::Sender<InferenceRequest>,
    receiver: Mutex<Option<mpsc::Receiver<InferenceRequest>>>,
}

impl InferenceQueue {
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }

    pub async fn enqueue(
        &self,
        request: EngineInferenceRequest,
        token_tx: Option<mpsc::Sender<String>>,
    ) -> Result<oneshot::Receiver<InferenceResult>, String> {
        let (response_tx, response_rx) = oneshot::channel();
        let queued = InferenceRequest {
            task_id: request.task_id,
            prompt: request.prompt,
            model_id: request.model_id,
            max_tokens: request.max_tokens,
            temperature: request.temperature,
            token_tx,
            response_tx,
        };

        println!("[Inference] Request queued");
        self.sender
            .send(queued)
            .await
            .map_err(|e| format!("No se pudo encolar inferencia: {}", e))?;

        Ok(response_rx)
    }

    pub fn take_receiver(&self) -> Option<mpsc::Receiver<InferenceRequest>> {
        self.receiver.lock().unwrap().take()
    }
}
