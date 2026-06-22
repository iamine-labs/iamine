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

fn with_allowed_test_license(model: &ModelDescriptor) -> ModelDescriptor {
    let mut model = model.clone();
    model.hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        .to_string();
    model.license = LicenseMetadata {
        license_id: Some("MIT".to_string()),
        license_url: Some("https://opensource.org/license/mit".to_string()),
        policy_class: Some(LicenseClass::Allowed),
        requires_acceptance: false,
        revision: Some("test-fixture".to_string()),
    };
    model.network_policy = NetworkPolicyMetadata::distributed_allowed("test-fixture");
    model
}
