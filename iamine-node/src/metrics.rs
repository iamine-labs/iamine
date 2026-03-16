use std::sync::Arc;
use tokio::sync::RwLock;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct NodeMetrics {
    pub tasks_executed: u64,
    pub tasks_failed: u64,
    pub tasks_timed_out: u64,
    pub active_workers: usize,
    pub network_peers: usize,
    pub avg_execution_ms: f64,
    pub uptime_secs: u64,
    pub reputation_score: u32,
    pub mesh_peers: usize,
    pub direct_peers: usize,
    pub relay_peers: usize,
    pub avg_latency_ms: f64,
    pub msgs_rate_limited: u64,
    // ← v0.6 inference metrics
    pub inference_tasks_completed: u64,
    pub inference_tasks_failed: u64,
    pub avg_inference_ms: f64,
    pub total_tokens_generated: u64,
    pub routing_decisions_total: u64, // ← nuevo
    pub routing_latency_ms: f64,      // ← nuevo
}

impl NodeMetrics {
    pub fn new() -> Self {
        Self {
            tasks_executed: 0,
            tasks_failed: 0,
            tasks_timed_out: 0,
            active_workers: 0,
            network_peers: 0,
            avg_execution_ms: 0.0,
            uptime_secs: 0,
            reputation_score: 100,
            mesh_peers: 0,
            direct_peers: 0,
            relay_peers: 0,
            avg_latency_ms: 0.0,
            msgs_rate_limited: 0,
            inference_tasks_completed: 0,
            inference_tasks_failed: 0,
            avg_inference_ms: 0.0,
            total_tokens_generated: 0,
            routing_decisions_total: 0,
            routing_latency_ms: 0.0,
        }
    }

    pub fn task_success(&mut self, execution_ms: u64) {
        self.tasks_executed += 1;
        let n = self.tasks_executed as f64;
        self.avg_execution_ms = (self.avg_execution_ms * (n - 1.0) + execution_ms as f64) / n;
    }

    pub fn task_failed(&mut self) { self.tasks_failed += 1; }
    pub fn task_timed_out(&mut self) { self.tasks_timed_out += 1; }

    #[allow(dead_code)]
    pub fn success_rate(&self) -> f64 {
        let total = self.tasks_executed + self.tasks_failed + self.tasks_timed_out;
        if total == 0 { return 1.0; }
        self.tasks_executed as f64 / total as f64
    }

    pub fn inference_success(&mut self, execution_ms: u64, tokens: u64) {
        self.inference_tasks_completed += 1;
        self.total_tokens_generated += tokens;
        let n = self.inference_tasks_completed as f64;
        self.avg_inference_ms = (self.avg_inference_ms * (n - 1.0) + execution_ms as f64) / n;
    }

    pub fn inference_failed(&mut self) {
        self.inference_tasks_failed += 1;
    }

    pub fn routing_decision(&mut self, decision_ms: u64) {
        self.routing_decisions_total += 1;
        let n = self.routing_decisions_total as f64;
        self.routing_latency_ms = (self.routing_latency_ms * (n - 1.0) + decision_ms as f64) / n;
    }
}

/// Servidor HTTP de métricas en puerto 9090
pub async fn start_metrics_server(metrics: Arc<RwLock<NodeMetrics>>, port: u16) {
    use warp::Filter;

    let m = Arc::clone(&metrics);
    let metrics_route = warp::path("metrics")
        .and(warp::get())
        .and(warp::any().map(move || Arc::clone(&m)))
        .and_then(|m: Arc<RwLock<NodeMetrics>>| async move {
            let metrics = m.read().await;
            Ok::<_, warp::Rejection>(warp::reply::json(&*metrics))
        });

    let health_route = warp::path("health")
        .and(warp::get())
        .map(|| warp::reply::json(&serde_json::json!({"status": "ok"})));

    println!("📊 Metrics en http://localhost:{}/metrics", port);
    warp::serve(metrics_route.or(health_route))
        .run(([0, 0, 0, 0], port))
        .await;
}
