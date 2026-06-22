use crate::license_policy::{LicenseClass, LicenseOperation, LicensePolicyStatus};
use crate::{ModelDescriptor, ModelLicensePolicy};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const LICENSE_ACCEPTANCE_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicenseAcceptanceStatus {
    NotRequired,
    Accepted,
    Required,
    Unavailable,
}

impl LicenseAcceptanceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Accepted => "accepted",
            Self::Required => "required",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicenseAcceptanceReason {
    LicenseAcceptanceNotRequired,
    LicenseAcceptanceRecorded,
    LicenseAcceptanceRequired,
    LicenseIdMissing,
    LicenseRevisionMissing,
}

impl LicenseAcceptanceReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LicenseAcceptanceNotRequired => "license_acceptance_not_required",
            Self::LicenseAcceptanceRecorded => "license_acceptance_recorded",
            Self::LicenseAcceptanceRequired => "license_acceptance_required",
            Self::LicenseIdMissing => "license_id_missing",
            Self::LicenseRevisionMissing => "license_revision_missing",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseAcceptanceDecision {
    pub model_id: String,
    pub status: LicenseAcceptanceStatus,
    pub reason: LicenseAcceptanceReason,
    pub permits_operation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicenseAcceptanceRecord {
    pub model_id: String,
    pub license_id: String,
    pub revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LicenseAcceptanceStoreData {
    schema_version: String,
    records: Vec<LicenseAcceptanceRecord>,
}

impl Default for LicenseAcceptanceStoreData {
    fn default() -> Self {
        Self {
            schema_version: LICENSE_ACCEPTANCE_SCHEMA_VERSION.to_string(),
            records: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LicenseAcceptanceStore {
    path: PathBuf,
}

impl Default for LicenseAcceptanceStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LicenseAcceptanceStore {
    pub fn new() -> Self {
        Self::new_in(default_license_acceptance_path())
    }

    pub fn new_in(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn has_accepted_descriptor(&self, model: &ModelDescriptor) -> bool {
        let Ok(record) = acceptance_record_for_model(model) else {
            return false;
        };
        self.load_data()
            .map(|data| {
                data.records.iter().any(|stored| {
                    stored.model_id == record.model_id
                        && stored.license_id == record.license_id
                        && stored.revision == record.revision
                })
            })
            .unwrap_or(false)
    }

    pub fn accept_descriptor(
        &self,
        model: &ModelDescriptor,
    ) -> Result<LicenseAcceptanceRecord, String> {
        let license_decision =
            ModelLicensePolicy.evaluate_descriptor(model, LicenseOperation::Download, false);
        if license_decision.status != LicensePolicyStatus::RequiresAcceptance {
            return Err(format!(
                "model license acceptance unavailable: {}",
                license_decision.reason.as_str()
            ));
        }

        let record = acceptance_record_for_model(model).map_err(|reason| {
            format!("model license acceptance unavailable: {}", reason.as_str())
        })?;
        let mut data = self.load_data()?;
        data.records.retain(|stored| {
            !(stored.model_id == record.model_id
                && stored.license_id == record.license_id
                && stored.revision == record.revision)
        });
        data.records.push(record.clone());
        data.records.sort_by(|a, b| {
            (&a.model_id, &a.license_id, &a.revision).cmp(&(
                &b.model_id,
                &b.license_id,
                &b.revision,
            ))
        });
        self.save_data(&data)?;
        Ok(record)
    }

    fn load_data(&self) -> Result<LicenseAcceptanceStoreData, String> {
        if !self.path.exists() {
            return Ok(LicenseAcceptanceStoreData::default());
        }
        let raw = fs::read_to_string(&self.path).map_err(|e| {
            format!(
                "could not read license acceptance store {}: {}",
                self.path.display(),
                e
            )
        })?;
        let data: LicenseAcceptanceStoreData = serde_json::from_str(&raw).map_err(|e| {
            format!(
                "could not parse license acceptance store {}: {}",
                self.path.display(),
                e
            )
        })?;
        if data.schema_version != LICENSE_ACCEPTANCE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported license acceptance schema {}",
                data.schema_version
            ));
        }
        Ok(data)
    }

    fn save_data(&self, data: &LicenseAcceptanceStoreData) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "could not create license acceptance directory {}: {}",
                    parent.display(),
                    e
                )
            })?;
        }
        let serialized = serde_json::to_string_pretty(data)
            .map_err(|e| format!("could not serialize license acceptance store: {}", e))?;
        fs::write(&self.path, serialized).map_err(|e| {
            format!(
                "could not write license acceptance store {}: {}",
                self.path.display(),
                e
            )
        })?;
        restrict_owner_access(&self.path);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelLicenseAcceptancePolicy;

impl ModelLicenseAcceptancePolicy {
    pub fn evaluate_descriptor(
        &self,
        model: &ModelDescriptor,
        operation: LicenseOperation,
        accepted: bool,
    ) -> LicenseAcceptanceDecision {
        match acceptance_record_for_model(model) {
            Ok(_) if accepted => decision(
                model,
                LicenseAcceptanceStatus::Accepted,
                LicenseAcceptanceReason::LicenseAcceptanceRecorded,
                true,
            ),
            Ok(_) if operation == LicenseOperation::List => decision(
                model,
                LicenseAcceptanceStatus::Required,
                LicenseAcceptanceReason::LicenseAcceptanceRequired,
                true,
            ),
            Ok(_) => decision(
                model,
                LicenseAcceptanceStatus::Required,
                LicenseAcceptanceReason::LicenseAcceptanceRequired,
                false,
            ),
            Err(LicenseAcceptanceReason::LicenseAcceptanceNotRequired) => decision(
                model,
                LicenseAcceptanceStatus::NotRequired,
                LicenseAcceptanceReason::LicenseAcceptanceNotRequired,
                true,
            ),
            Err(reason) if operation == LicenseOperation::List => {
                decision(model, LicenseAcceptanceStatus::Unavailable, reason, true)
            }
            Err(reason) => decision(model, LicenseAcceptanceStatus::Unavailable, reason, false),
        }
    }
}

fn decision(
    model: &ModelDescriptor,
    status: LicenseAcceptanceStatus,
    reason: LicenseAcceptanceReason,
    permits_operation: bool,
) -> LicenseAcceptanceDecision {
    LicenseAcceptanceDecision {
        model_id: model.id.clone(),
        status,
        reason,
        permits_operation,
    }
}

fn acceptance_record_for_model(
    model: &ModelDescriptor,
) -> Result<LicenseAcceptanceRecord, LicenseAcceptanceReason> {
    if model.license.policy_class != Some(LicenseClass::RequiresAcceptance) {
        return Err(LicenseAcceptanceReason::LicenseAcceptanceNotRequired);
    }
    let license_id = model
        .license
        .license_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(LicenseAcceptanceReason::LicenseIdMissing)?;
    let revision = model
        .license
        .revision
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(LicenseAcceptanceReason::LicenseRevisionMissing)?;
    Ok(LicenseAcceptanceRecord {
        model_id: model.id.clone(),
        license_id: license_id.to_string(),
        revision: revision.to_string(),
    })
}

fn default_license_acceptance_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".iamine")
        .join("license_acceptance.json")
}

#[cfg(unix)]
fn restrict_owner_access(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600);
        let _ = fs::set_permissions(path, permissions);
    }
}

#[cfg(not(unix))]
fn restrict_owner_access(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LicenseMetadata, ModelDescriptor, NetworkPolicyMetadata};
    use tempfile::TempDir;

    fn model(policy_class: LicenseClass, revision: Option<&str>) -> ModelDescriptor {
        ModelDescriptor {
            id: "acceptance-model".to_string(),
            version: "1.0".to_string(),
            architecture: "llama".to_string(),
            size_bytes: 1_048_576,
            required_ram_gb: 1,
            required_vram_gb: 0,
            shards: 1,
            hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            download_url: "https://huggingface.co/iamine/test/resolve/main/test.gguf".to_string(),
            quantization: "q4_k_m".to_string(),
            license: LicenseMetadata {
                license_id: Some("custom-requires-acceptance".to_string()),
                license_url: Some("https://example.com/license".to_string()),
                policy_class: Some(policy_class),
                requires_acceptance: policy_class == LicenseClass::RequiresAcceptance,
                revision: revision.map(str::to_string),
            },
            network_policy: NetworkPolicyMetadata::distributed_allowed("test-fixture"),
        }
    }

    #[test]
    fn requires_acceptance_blocks_download_without_record() {
        let model = model(LicenseClass::RequiresAcceptance, Some("2026-06-21"));

        let decision = ModelLicenseAcceptancePolicy.evaluate_descriptor(
            &model,
            LicenseOperation::Download,
            false,
        );

        assert_eq!(decision.status, LicenseAcceptanceStatus::Required);
        assert_eq!(
            decision.reason,
            LicenseAcceptanceReason::LicenseAcceptanceRequired
        );
        assert!(!decision.permits_operation);
    }

    #[test]
    fn accepted_record_permits_download() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let store = LicenseAcceptanceStore::new_in(dir.path().join("license_acceptance.json"));
        let model = model(LicenseClass::RequiresAcceptance, Some("2026-06-21"));

        store.accept_descriptor(&model)?;
        let decision = ModelLicenseAcceptancePolicy.evaluate_descriptor(
            &model,
            LicenseOperation::Download,
            store.has_accepted_descriptor(&model),
        );

        assert_eq!(decision.status, LicenseAcceptanceStatus::Accepted);
        assert!(decision.permits_operation);
        Ok(())
    }

    #[test]
    fn list_reports_required_without_blocking() {
        let model = model(LicenseClass::RequiresAcceptance, Some("2026-06-21"));

        let decision =
            ModelLicenseAcceptancePolicy.evaluate_descriptor(&model, LicenseOperation::List, false);

        assert_eq!(decision.status, LicenseAcceptanceStatus::Required);
        assert!(decision.permits_operation);
    }

    #[test]
    fn allowed_license_does_not_require_acceptance() {
        let model = model(LicenseClass::Allowed, Some("2026-06-21"));

        let decision = ModelLicenseAcceptancePolicy.evaluate_descriptor(
            &model,
            LicenseOperation::Download,
            false,
        );

        assert_eq!(decision.status, LicenseAcceptanceStatus::NotRequired);
        assert!(decision.permits_operation);
    }

    #[test]
    fn revision_change_invalidates_previous_acceptance() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let store = LicenseAcceptanceStore::new_in(dir.path().join("license_acceptance.json"));
        let old_model = model(LicenseClass::RequiresAcceptance, Some("2026-06-21"));
        let new_model = model(LicenseClass::RequiresAcceptance, Some("2026-06-22"));

        store.accept_descriptor(&old_model)?;

        assert!(store.has_accepted_descriptor(&old_model));
        assert!(!store.has_accepted_descriptor(&new_model));
        Ok(())
    }

    #[test]
    fn missing_revision_makes_acceptance_unavailable() {
        let model = model(LicenseClass::RequiresAcceptance, None);

        let decision = ModelLicenseAcceptancePolicy.evaluate_descriptor(
            &model,
            LicenseOperation::Download,
            false,
        );

        assert_eq!(decision.status, LicenseAcceptanceStatus::Unavailable);
        assert_eq!(
            decision.reason,
            LicenseAcceptanceReason::LicenseRevisionMissing
        );
        assert!(!decision.permits_operation);
    }
}
