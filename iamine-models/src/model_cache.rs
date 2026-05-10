use llama_cpp_2::model::LlamaModel;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

#[derive(Debug)]
pub struct LoadedModel {
    pub model_id: String,
    pub path: PathBuf,
    pub model: Arc<LlamaModel>,
    pub loaded_at: Instant,
    use_count: AtomicU64,
}

impl LoadedModel {
    pub fn new(model_id: String, path: PathBuf, model: LlamaModel) -> Self {
        Self {
            model_id,
            path,
            model: Arc::new(model),
            loaded_at: Instant::now(),
            use_count: AtomicU64::new(1),
        }
    }

    pub fn mark_reused(&self) {
        self.use_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn use_count(&self) -> u64 {
        self.use_count.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Default)]
pub struct ModelCache {
    models: RwLock<HashMap<String, Arc<LoadedModel>>>,
    actual_loads: AtomicUsize,
    reuse_hits: AtomicUsize,
}

impl ModelCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, model_id: &str) -> Option<Arc<LoadedModel>> {
        let models = self.models.read().unwrap();
        let cached = models.get(model_id).cloned();
        drop(models);

        if let Some(model) = &cached {
            model.mark_reused();
            self.reuse_hits.fetch_add(1, Ordering::SeqCst);
            println!("[ModelCache] Reusing model {}", model_id);
        }

        cached
    }

    pub fn get_or_load<F>(&self, model_id: &str, loader: F) -> Result<Arc<LoadedModel>, String>
    where
        F: FnOnce() -> Result<LoadedModel, String>,
    {
        if let Some(cached) = self.get(model_id) {
            return Ok(cached);
        }

        let mut models = self.models.write().unwrap();
        if let Some(cached) = models.get(model_id).cloned() {
            cached.mark_reused();
            self.reuse_hits.fetch_add(1, Ordering::SeqCst);
            println!("[ModelCache] Reusing model {}", model_id);
            return Ok(cached);
        }

        let loaded = Arc::new(loader()?);
        println!("[ModelCache] Loaded model {}", model_id);
        self.actual_loads.fetch_add(1, Ordering::SeqCst);
        models.insert(model_id.to_string(), Arc::clone(&loaded));
        Ok(loaded)
    }

    pub fn remove(&self, model_id: &str) -> bool {
        self.models.write().unwrap().remove(model_id).is_some()
    }

    pub fn contains(&self, model_id: &str) -> bool {
        self.models.read().unwrap().contains_key(model_id)
    }

    pub fn loaded_models(&self) -> Vec<String> {
        self.models.read().unwrap().keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.models.read().unwrap().len()
    }

    pub fn actual_loads(&self) -> usize {
        self.actual_loads.load(Ordering::SeqCst)
    }

    pub fn reuse_hits(&self) -> usize {
        self.reuse_hits.load(Ordering::SeqCst)
    }
}
