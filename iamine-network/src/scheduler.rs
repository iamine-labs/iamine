use crate::model_capability_matcher::{
    is_node_compatible_with_model, ModelHardwareRequirements, NodeHardwareProfile,
};
use crate::model_karma::model_karma;
use crate::node_registry::{NodeCapability, NodeRegistry};
use crate::node_scoring::{cluster_priority, free_slots, score_node, NodeScore};

#[derive(Debug, Clone, Default)]
pub struct IntelligentScheduler;

impl IntelligentScheduler {
    pub fn new() -> Self {
        Self
    }

    pub fn select_best_node(
        &self,
        registry: &NodeRegistry,
        model_id: &str,
        local_cluster_id: Option<&str>,
    ) -> Option<NodeScore> {
        println!("[Scheduler] Evaluating nodes for model {}", model_id);

        let requirements = ModelHardwareRequirements::for_model(model_id)?;
        let mut candidates = self.filter_nodes(registry.all_nodes(), model_id, &requirements);

        candidates.sort_by(|left, right| {
            let left_priority = cluster_priority(left, local_cluster_id);
            let right_priority = cluster_priority(right, local_cluster_id);
            let left_score = score_node(left, local_cluster_id);
            let right_score = score_node(right, local_cluster_id);
            right_priority
                .cmp(&left_priority)
                .then_with(|| {
                    right_score
                        .partial_cmp(&left_score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| left.peer_id.cmp(&right.peer_id))
        });

        let best = candidates.first()?;
        let score = score_node(best, local_cluster_id);
        println!(
            "[Scheduler] Selected node {} with score {:.3}",
            best.peer_id, score
        );

        Some(NodeScore {
            peer_id: best.peer_id.clone(),
            model_id: model_id.to_string(),
            score,
            free_slots: free_slots(best),
            model_karma_score: karma_score_for_model(model_id),
        })
    }

    pub fn select_best_node_for_models(
        &self,
        registry: &NodeRegistry,
        model_ids: &[String],
        local_cluster_id: Option<&str>,
    ) -> Option<NodeScore> {
        let mut best: Option<NodeScore> = None;

        for model_id in model_ids {
            if let Some(candidate) = self.select_best_node(registry, model_id, local_cluster_id) {
                let candidate_composite = composite_score(&candidate);
                let should_replace = match &best {
                    Some(current) => {
                        let current_composite = composite_score(current);
                        candidate_composite > current_composite
                            || (candidate_composite - current_composite).abs() < f64::EPSILON
                                && (candidate.model_karma_score > current.model_karma_score
                                    || (candidate.model_karma_score - current.model_karma_score)
                                        .abs()
                                        < f32::EPSILON
                                        && candidate.score > current.score)
                    }
                    None => true,
                };
                if should_replace {
                    best = Some(candidate);
                }
            }
        }

        best
    }

    fn filter_nodes(
        &self,
        nodes: Vec<(String, NodeCapability)>,
        model_id: &str,
        requirements: &ModelHardwareRequirements,
    ) -> Vec<NodeCapability> {
        nodes
            .into_iter()
            .map(|(_, node)| node)
            .filter(|node| node.models.iter().any(|model| model == model_id))
            .filter(|node| {
                let profile = NodeHardwareProfile {
                    ram_gb: node.ram_gb,
                    gpu_available: node.gpu_available,
                    storage_available_gb: node.storage_available_gb,
                };
                is_node_compatible_with_model(&profile, requirements)
            })
            .filter(|node| free_slots(node) > 0)
            .collect()
    }
}

fn karma_score_for_model(model_id: &str) -> f32 {
    model_karma(model_id)
        .map(|karma| karma.karma_score())
        .unwrap_or(0.5)
}

fn composite_score(score: &NodeScore) -> f64 {
    (score.score * 0.8) + (score.model_karma_score as f64 * 0.2)
}

#[cfg(test)]
mod tests {
    use super::IntelligentScheduler;
    use crate::model_karma::{clear_model_karma_store, record_model_metrics};
    use crate::model_metrics::ModelMetrics;
    use crate::node_registry::{NodeCapability, NodeRegistry};
    use std::sync::{Mutex, OnceLock};
    use std::time::Instant;

    fn karma_test_guard() -> &'static Mutex<()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| Mutex::new(()))
    }

    fn make_cap(
        peer_id: &str,
        cpu_score: u64,
        model: &str,
        ram_gb: u32,
        worker_slots: u32,
        active_tasks: u32,
        latency_ms: u32,
        cluster_id: Option<&str>,
    ) -> NodeCapability {
        NodeCapability {
            peer_id: peer_id.to_string(),
            cpu_score,
            ram_gb,
            gpu_available: false,
            storage_available_gb: 50,
            accelerator: "CPU".to_string(),
            models: vec![model.to_string()],
            worker_slots,
            active_tasks,
            latency_ms,
            last_seen: Instant::now(),
            cluster_id: cluster_id.map(|value| value.to_string()),
        }
    }

    #[test]
    fn test_scheduler_selects_best_node() {
        let scheduler = IntelligentScheduler::new();
        let mut registry = NodeRegistry::new();
        registry.register_node(make_cap(
            "peer1",
            120_000,
            "tinyllama-1b",
            8,
            8,
            6,
            20,
            None,
        ));
        registry.register_node(make_cap(
            "peer2",
            180_000,
            "tinyllama-1b",
            8,
            8,
            1,
            8,
            Some("local"),
        ));

        let best = scheduler
            .select_best_node(&registry, "tinyllama-1b", Some("local"))
            .unwrap();
        assert_eq!(best.peer_id, "peer2");
    }

    #[test]
    fn test_scheduler_respects_capacity() {
        let scheduler = IntelligentScheduler::new();
        let mut registry = NodeRegistry::new();
        registry.register_node(make_cap("peer1", 180_000, "tinyllama-1b", 8, 8, 8, 5, None));
        registry.register_node(make_cap(
            "peer2",
            100_000,
            "tinyllama-1b",
            8,
            8,
            4,
            20,
            None,
        ));

        let best = scheduler
            .select_best_node(&registry, "tinyllama-1b", None)
            .unwrap();
        assert_eq!(best.peer_id, "peer2");
    }

    #[test]
    fn test_scheduler_prefers_low_latency() {
        let scheduler = IntelligentScheduler::new();
        let mut registry = NodeRegistry::new();
        registry.register_node(make_cap("peer1", 160_000, "tinyllama-1b", 8, 8, 1, 5, None));
        registry.register_node(make_cap(
            "peer2",
            160_000,
            "tinyllama-1b",
            8,
            8,
            1,
            80,
            None,
        ));

        let best = scheduler
            .select_best_node(&registry, "tinyllama-1b", None)
            .unwrap();
        assert_eq!(best.peer_id, "peer1");
    }

    #[test]
    fn test_scheduler_filters_invalid_nodes() {
        let scheduler = IntelligentScheduler::new();
        let mut registry = NodeRegistry::new();
        registry.register_node(make_cap("peer1", 160_000, "llama3-3b", 2, 8, 1, 5, None));
        registry.register_node(make_cap(
            "peer2",
            160_000,
            "tinyllama-1b",
            16,
            8,
            8,
            10,
            None,
        ));

        assert!(scheduler
            .select_best_node(&registry, "llama3-3b", None)
            .is_none());
    }

    #[test]
    fn test_scheduler_prefers_high_karma_model() {
        let _lock = karma_test_guard().lock().unwrap();
        clear_model_karma_store();
        for _ in 0..20 {
            record_model_metrics("llama3-3b", ModelMetrics::new(true, 150, true, 0));
            record_model_metrics("tinyllama-1b", ModelMetrics::new(false, 2_500, false, 2));
        }

        let scheduler = IntelligentScheduler::new();
        let mut registry = NodeRegistry::new();
        registry.register_node(make_cap(
            "peer-a",
            160_000,
            "llama3-3b",
            8,
            8,
            2,
            12,
            Some("local"),
        ));
        registry.register_node(make_cap(
            "peer-b",
            160_000,
            "tinyllama-1b",
            8,
            8,
            2,
            10,
            Some("local"),
        ));

        let best = scheduler
            .select_best_node_for_models(
                &registry,
                &["tinyllama-1b".to_string(), "llama3-3b".to_string()],
                Some("local"),
            )
            .unwrap();

        assert_eq!(best.model_id, "llama3-3b");
        assert!(best.model_karma_score > 0.8);
    }
}
