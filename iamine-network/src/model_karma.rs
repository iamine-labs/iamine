use crate::model_karma_store::global_model_karma_manager;
use crate::model_metrics::ModelMetrics;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelKarma {
    pub model_id: String,
    pub accuracy_score: f32,
    pub latency_score: f32,
    pub reliability_score: f32,
    pub semantic_success_rate: f32,
    pub total_runs: u64,
}

impl ModelKarma {
    pub fn new(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            accuracy_score: 0.5,
            latency_score: 0.5,
            reliability_score: 0.5,
            semantic_success_rate: 0.5,
            total_runs: 0,
        }
    }

    pub fn karma_score(&self) -> f32 {
        (self.accuracy_score * 0.4)
            + (self.latency_score * 0.2)
            + (self.reliability_score * 0.2)
            + (self.semantic_success_rate * 0.2)
    }

    pub fn update(&mut self, metrics: ModelMetrics) {
        self.total_runs += 1;
        let n = self.total_runs as f32;
        self.accuracy_score = ((self.accuracy_score * (n - 1.0)) + metrics.accuracy_signal()) / n;
        self.latency_score = ((self.latency_score * (n - 1.0)) + metrics.latency_signal()) / n;
        self.reliability_score =
            ((self.reliability_score * (n - 1.0)) + metrics.reliability_signal()) / n;
        self.semantic_success_rate =
            ((self.semantic_success_rate * (n - 1.0)) + metrics.semantic_signal()) / n;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelKarmaStore {
    models: HashMap<String, ModelKarma>,
}

impl ModelKarmaStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_models(models: HashMap<String, ModelKarma>) -> Self {
        Self { models }
    }

    pub fn update_model(&mut self, model_id: &str, metrics: ModelMetrics) -> ModelKarma {
        let entry = self
            .models
            .entry(model_id.to_string())
            .or_insert_with(|| ModelKarma::new(model_id.to_string()));
        entry.update(metrics);
        entry.clone()
    }

    pub fn get(&self, model_id: &str) -> Option<ModelKarma> {
        self.models.get(model_id).cloned()
    }

    pub fn ranking(&self) -> Vec<ModelKarma> {
        let mut entries: Vec<ModelKarma> = self.models.values().cloned().collect();
        entries.sort_by(|left, right| {
            right
                .karma_score()
                .partial_cmp(&left.karma_score())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.model_id.cmp(&right.model_id))
        });
        entries
    }

    pub fn models(&self) -> &HashMap<String, ModelKarma> {
        &self.models
    }
}

pub fn record_model_metrics(model_id: &str, metrics: ModelMetrics) -> ModelKarma {
    global_model_karma_manager()
        .update_model(model_id, metrics)
        .unwrap_or_else(|error| {
            eprintln!(
                "[ModelKarma] Failed to persist metrics for {}: {}",
                model_id, error
            );
            ModelKarma::new(model_id.to_string())
        })
}

pub fn model_karma(model_id: &str) -> Option<ModelKarma> {
    global_model_karma_manager().get(model_id)
}

pub fn ranked_models() -> Vec<ModelKarma> {
    global_model_karma_manager().ranking()
}

#[cfg(test)]
pub fn clear_model_karma_store() {
    global_model_karma_manager().clear_for_tests();
}

#[cfg(test)]
mod tests {
    use super::{
        clear_model_karma_store, model_karma, ranked_models, record_model_metrics, ModelKarmaStore,
    };
    use crate::model_metrics::ModelMetrics;
    use std::sync::{Mutex, OnceLock};

    fn karma_test_guard() -> &'static Mutex<()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn test_model_karma_update() {
        let mut store = ModelKarmaStore::new();
        let karma = store.update_model("llama3-3b", ModelMetrics::new(true, 500, true, 0));
        assert_eq!(karma.total_runs, 1);
        assert!(karma.karma_score() > 0.5);
    }

    #[test]
    fn test_model_ranking() {
        let mut store = ModelKarmaStore::new();
        store.update_model("fast-good", ModelMetrics::new(true, 200, true, 0));
        store.update_model("slow-bad", ModelMetrics::new(false, 3_000, false, 2));
        let ranking = store.ranking();
        assert_eq!(ranking.first().unwrap().model_id, "fast-good");
    }

    #[test]
    fn test_global_model_karma_tracking() {
        let _lock = karma_test_guard().lock().unwrap();
        clear_model_karma_store();
        record_model_metrics("tinyllama-1b", ModelMetrics::new(true, 300, true, 0));
        assert!(model_karma("tinyllama-1b").is_some());
        assert!(!ranked_models().is_empty());
    }
}
