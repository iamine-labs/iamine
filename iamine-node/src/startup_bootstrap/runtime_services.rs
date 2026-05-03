use super::*;

pub(crate) struct RuntimeServicesState {
    pub(crate) pool: Arc<WorkerPool>,
    pub(crate) queue: Arc<TaskQueue>,
    pub(crate) scheduler: Arc<TaskScheduler>,
    pub(crate) task_manager: Arc<TaskManager>,
    pub(crate) task_response_tx: tokio::sync::mpsc::Sender<PendingTaskResponse>,
    pub(crate) task_response_rx: tokio::sync::mpsc::Receiver<PendingTaskResponse>,
    pub(crate) heartbeat: Arc<HeartbeatService>,
    pub(crate) metrics: Arc<RwLock<NodeMetrics>>,
    pub(crate) capabilities: WorkerCapabilities,
    pub(crate) task_cache: TaskCache,
    pub(crate) peer_tracker: PeerTracker,
    pub(crate) rate_limiter: RateLimiter,
}

pub(crate) async fn bootstrap_runtime_services(
    peer_id: PeerId,
    worker_slots: usize,
    wallet_reputation: f32,
    inference_backend_state: &InferenceBackendState,
) -> RuntimeServicesState {
    let pool = Arc::new(WorkerPool::with_slots(worker_slots));
    let queue = Arc::new(TaskQueue::new(peer_id.to_string()));
    let scheduler = Arc::new(TaskScheduler::new());
    let task_manager = Arc::new(TaskManager::new());
    let (task_response_tx, task_response_rx) =
        tokio::sync::mpsc::channel::<PendingTaskResponse>(64);
    let heartbeat = Arc::new(HeartbeatService::new());
    let metrics = Arc::new(RwLock::new(NodeMetrics::new()));
    let capabilities = WorkerCapabilities::detect(inference_backend_state);
    let task_cache = TaskCache::new(1000);
    let peer_tracker = PeerTracker::new();
    let rate_limiter = RateLimiter::new(100);

    {
        let mut runtime_metrics = metrics.write().await;
        runtime_metrics.reputation_score = wallet_reputation as u32;
    }

    RuntimeServicesState {
        pool,
        queue,
        scheduler,
        task_manager,
        task_response_tx,
        task_response_rx,
        heartbeat,
        metrics,
        capabilities,
        task_cache,
        peer_tracker,
        rate_limiter,
    }
}
