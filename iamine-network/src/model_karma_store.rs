use crate::model_karma::{ModelKarma, ModelKarmaStore};
use crate::model_metrics::ModelMetrics;
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct ModelKarmaManager {
    path: PathBuf,
    store: Arc<RwLock<ModelKarmaStore>>,
}

impl ModelKarmaManager {
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let loaded = ModelKarmaStore::from_models(load_models_from_path(&path)?);
        Ok(Self {
            path,
            store: Arc::new(RwLock::new(loaded)),
        })
    }

    pub fn default() -> Self {
        Self::new(default_model_karma_path()).unwrap_or_else(|_| Self {
            path: default_model_karma_path(),
            store: Arc::new(RwLock::new(ModelKarmaStore::new())),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn reload(&self) -> io::Result<()> {
        let loaded = ModelKarmaStore::from_models(load_models_from_path(&self.path)?);
        let mut store = self.store.write().expect("model karma manager poisoned");
        *store = loaded;
        Ok(())
    }

    pub fn update_model(&self, model_id: &str, metrics: ModelMetrics) -> io::Result<ModelKarma> {
        let mut latest = ModelKarmaStore::from_models(load_models_from_path(&self.path)?);
        let karma = latest.update_model(model_id, metrics);
        save_models_to_path(&self.path, latest.models())?;

        let mut store = self.store.write().expect("model karma manager poisoned");
        *store = latest;
        Ok(karma)
    }

    pub fn get(&self, model_id: &str) -> Option<ModelKarma> {
        self.store
            .read()
            .expect("model karma manager poisoned")
            .get(model_id)
    }

    pub fn ranking(&self) -> Vec<ModelKarma> {
        self.store
            .read()
            .expect("model karma manager poisoned")
            .ranking()
    }

    #[cfg(test)]
    pub fn clear_for_tests(&self) {
        let _ = save_models_to_path(self.path(), &HashMap::new());
        let mut store = self.store.write().expect("model karma manager poisoned");
        *store = ModelKarmaStore::new();
    }
}

static GLOBAL_MODEL_KARMA_MANAGER: OnceLock<ModelKarmaManager> = OnceLock::new();

pub fn global_model_karma_manager() -> &'static ModelKarmaManager {
    GLOBAL_MODEL_KARMA_MANAGER.get_or_init(ModelKarmaManager::default)
}

pub fn default_model_karma_path() -> PathBuf {
    if let Some(path) = std::env::var_os("IAMINE_MODEL_KARMA_PATH") {
        return PathBuf::from(path);
    }

    default_model_karma_path_without_override()
}

#[cfg(test)]
fn default_model_karma_path_without_override() -> PathBuf {
    std::env::temp_dir().join("iamine-model-karma-tests.json")
}

#[cfg(not(test))]
fn default_model_karma_path_without_override() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".iamine")
        .join("model_karma.json")
}

fn load_models_from_path(path: &Path) -> io::Result<HashMap<String, ModelKarma>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let data = fs::read(path)?;
    if data.is_empty() {
        return Ok(HashMap::new());
    }

    serde_json::from_slice(&data).map_err(io::Error::other)
}

fn save_models_to_path(path: &Path, models: &HashMap<String, ModelKarma>) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let serialized = serde_json::to_vec_pretty(models).map_err(io::Error::other)?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_extension(format!("tmp-{}", suffix));
    fs::write(&tmp_path, serialized)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{default_model_karma_path, ModelKarmaManager};
    use crate::model_metrics::ModelMetrics;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-model-karma-{}.json", suffix))
    }

    #[test]
    fn test_karma_persistence() {
        let path = temp_path();
        let manager = ModelKarmaManager::new(&path).unwrap();
        manager
            .update_model("llama3-3b", ModelMetrics::new(true, 250, true, 0))
            .unwrap();

        assert!(path.exists());
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("llama3-3b"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_karma_reload() {
        let path = temp_path();
        let manager = ModelKarmaManager::new(&path).unwrap();
        manager
            .update_model("llama3-3b", ModelMetrics::new(true, 250, true, 0))
            .unwrap();

        let reloaded = ModelKarmaManager::new(&path).unwrap();
        let karma = reloaded.get("llama3-3b").unwrap();
        assert_eq!(karma.total_runs, 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_karma_update_persists() {
        let path = temp_path();
        let manager = ModelKarmaManager::new(&path).unwrap();
        manager
            .update_model("llama3-3b", ModelMetrics::new(true, 250, true, 0))
            .unwrap();
        manager
            .update_model("llama3-3b", ModelMetrics::new(true, 500, true, 1))
            .unwrap();

        let reloaded = ModelKarmaManager::new(&path).unwrap();
        let karma = reloaded.get("llama3-3b").unwrap();
        assert_eq!(karma.total_runs, 2);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_default_path_uses_iamine_location() {
        let path = default_model_karma_path();
        let rendered = path.to_string_lossy();
        assert!(rendered.contains("iamine-model-karma-tests.json"));
    }
}
