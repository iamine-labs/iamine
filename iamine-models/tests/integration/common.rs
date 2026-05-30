use iamine_models::model_validator::ModelValidator;
use iamine_models::node_models::{ModelId, NodeModels, PeerModelRegistry};
use iamine_models::storage_config::StorageConfig;
use iamine_models::*;
use std::sync::Arc;
use tempfile::TempDir;

fn temp_storage() -> (TempDir, ModelStorage) {
    let dir = TempDir::new().unwrap();
    let storage = ModelStorage::new_in(dir.path().to_path_buf());
    (dir, storage)
}
