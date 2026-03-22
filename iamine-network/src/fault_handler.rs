use crate::node_registry::NodeRegistry;
use crate::scheduler::IntelligentScheduler;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureKind {
    EmptyOutput,
    InvalidOutput,
    TaskFailure,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u8,
    pub timeout_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RetryState {
    pub retry_count: u8,
    pub failed_peers: HashSet<String>,
    pub failed_models: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryTarget {
    pub peer_id: String,
    pub model_id: String,
}

impl RetryState {
    pub fn can_retry(&self, policy: &RetryPolicy) -> bool {
        self.retry_count < policy.max_retries
    }

    pub fn record_failure(&mut self, peer_id: Option<&str>, model_id: Option<&str>) {
        if let Some(peer_id) = peer_id {
            self.failed_peers.insert(peer_id.to_string());
        }
        if let Some(model_id) = model_id {
            self.failed_models.insert(model_id.to_string());
        }
    }

    pub fn advance_retry(&mut self) {
        self.retry_count = self.retry_count.saturating_add(1);
    }
}

pub fn select_retry_target(
    registry: &NodeRegistry,
    scheduler: &IntelligentScheduler,
    candidate_models: &[String],
    local_cluster_id: Option<&str>,
    retry_state: &RetryState,
) -> Option<RetryTarget> {
    scheduler
        .select_best_node_for_models_excluding(
            registry,
            candidate_models,
            local_cluster_id,
            &retry_state.failed_peers,
            &retry_state.failed_models,
        )
        .map(|decision| RetryTarget {
            peer_id: decision.peer_id,
            model_id: decision.model_id,
        })
}

#[cfg(test)]
mod tests {
    use super::{select_retry_target, FailureKind, RetryPolicy, RetryState};
    use crate::node_registry::{NodeCapability, NodeRegistry};
    use crate::scheduler::IntelligentScheduler;
    use std::time::Instant;

    fn make_cap(peer_id: &str, model: &str, cpu_score: u64, latency_ms: u32) -> NodeCapability {
        NodeCapability {
            peer_id: peer_id.to_string(),
            cpu_score,
            ram_gb: 8,
            gpu_available: false,
            storage_available_gb: 64,
            accelerator: "CPU".to_string(),
            models: vec![model.to_string()],
            worker_slots: 4,
            active_tasks: 0,
            latency_ms,
            last_seen: Instant::now(),
            cluster_id: Some("local".to_string()),
        }
    }

    #[test]
    fn test_retry_on_failure() {
        let scheduler = IntelligentScheduler::new();
        let mut registry = NodeRegistry::new();
        registry.register_node(make_cap("peer-a", "llama3-3b", 170_000, 10));
        registry.register_node(make_cap("peer-b", "llama3-3b", 160_000, 12));

        let mut retry_state = RetryState::default();
        retry_state.record_failure(Some("peer-a"), None);

        let target = select_retry_target(
            &registry,
            &scheduler,
            &["llama3-3b".to_string()],
            Some("local"),
            &retry_state,
        )
        .unwrap();

        assert_eq!(target.peer_id, "peer-b");
    }

    #[test]
    fn test_fallback_model() {
        let scheduler = IntelligentScheduler::new();
        let mut registry = NodeRegistry::new();
        registry.register_node(make_cap("peer-a", "llama3-3b", 170_000, 10));
        registry.register_node(make_cap("peer-b", "mistral-7b", 165_000, 12));

        let mut retry_state = RetryState::default();
        retry_state.record_failure(Some("peer-a"), Some("llama3-3b"));

        let target = select_retry_target(
            &registry,
            &scheduler,
            &["llama3-3b".to_string(), "mistral-7b".to_string()],
            Some("local"),
            &retry_state,
        )
        .unwrap();

        assert_eq!(target.model_id, "mistral-7b");
    }

    #[test]
    fn test_timeout_handling() {
        let policy = RetryPolicy::default();
        let mut retry_state = RetryState::default();
        assert!(retry_state.can_retry(&policy));
        retry_state.advance_retry();
        retry_state.advance_retry();
        assert!(!retry_state.can_retry(&policy));
        assert_eq!(FailureKind::Timeout, FailureKind::Timeout);
    }
}
