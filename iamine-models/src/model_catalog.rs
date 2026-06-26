use crate::license_acceptance::{LicenseAcceptanceStatus, LicenseAcceptanceStore};
use crate::license_policy::LicenseOperation;
use crate::model_compatibility::{
    evaluate_node_model_compatibility, ModelCompatibilityReason, ModelCompatibilityStatus,
};
use crate::model_registry::ModelRegistry;
use crate::model_registry_admission::evaluate_model_registry_admission_with_license_acceptance_store;
use crate::model_requirements::ModelRequirements;
use crate::model_storage::ModelStorage;
use crate::node_capabilities::NodeCapabilities;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelCatalogDownloadAction {
    AlreadyInstalled,
    Ready,
    LicenseAcceptanceRequired,
    Incompatible,
    Blocked,
}

impl ModelCatalogDownloadAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AlreadyInstalled => "already_installed",
            Self::Ready => "ready",
            Self::LicenseAcceptanceRequired => "license_acceptance_required",
            Self::Incompatible => "incompatible",
            Self::Blocked => "blocked",
        }
    }

    fn sort_rank(&self) -> u8 {
        match self {
            Self::AlreadyInstalled => 0,
            Self::Ready => 1,
            Self::LicenseAcceptanceRequired => 2,
            Self::Incompatible => 3,
            Self::Blocked => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCatalogGateStatus {
    pub gate: String,
    pub status: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelCatalogEntry {
    pub id: String,
    pub version: String,
    pub architecture: String,
    pub size_gb: f64,
    pub size_bytes: u64,
    pub required_ram_gb: u32,
    pub required_storage_gb: u32,
    pub installed: bool,
    pub compatible: bool,
    pub compatibility_status: String,
    pub compatibility_reasons: Vec<String>,
    pub download_action: ModelCatalogDownloadAction,
    pub download_reason: String,
    pub gates: Vec<ModelCatalogGateStatus>,
}

impl ModelCatalogEntry {
    pub fn is_download_ready(&self) -> bool {
        self.download_action == ModelCatalogDownloadAction::Ready
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelCatalogSelection {
    pub model_id: String,
    pub reason: String,
    pub entry: ModelCatalogEntry,
}

pub fn build_model_catalog_entries(
    registry: &ModelRegistry,
    storage: &ModelStorage,
    license_acceptance_store: &LicenseAcceptanceStore,
    capabilities: &NodeCapabilities,
) -> Vec<ModelCatalogEntry> {
    let mut entries: Vec<ModelCatalogEntry> = registry
        .list()
        .into_iter()
        .map(|model| {
            let installed = storage.has_model(&model.id);
            let operation = if installed {
                LicenseOperation::ExistingExecution
            } else {
                LicenseOperation::Download
            };
            let admission = evaluate_model_registry_admission_with_license_acceptance_store(
                model,
                operation,
                installed,
                license_acceptance_store,
            );
            let compatibility = evaluate_node_model_compatibility(&model.id, capabilities);
            let compatible = compatibility.status == ModelCompatibilityStatus::Compatible;
            let compatibility_reasons = compatibility
                .reasons
                .iter()
                .map(compatibility_reason_code)
                .collect::<Vec<_>>();
            let required_storage_gb = ModelRequirements::for_model(&model.id)
                .map(|req| req.min_storage_gb)
                .unwrap_or_else(|| model.size_bytes.div_ceil(1_073_741_824) as u32);

            let (download_action, download_reason) = if installed {
                (
                    ModelCatalogDownloadAction::AlreadyInstalled,
                    "model_already_installed".to_string(),
                )
            } else if !compatible {
                (
                    ModelCatalogDownloadAction::Incompatible,
                    non_empty_reason(&compatibility_reasons, "hardware_incompatible"),
                )
            } else if admission.license_acceptance.status == LicenseAcceptanceStatus::Required {
                (
                    ModelCatalogDownloadAction::LicenseAcceptanceRequired,
                    admission.license_acceptance.reason.as_str().to_string(),
                )
            } else if let Some(error) = admission.first_blocking_error() {
                (ModelCatalogDownloadAction::Blocked, error)
            } else {
                (
                    ModelCatalogDownloadAction::Ready,
                    "all_catalog_gates_permit_download".to_string(),
                )
            };

            ModelCatalogEntry {
                id: model.id.clone(),
                version: model.version.clone(),
                architecture: model.architecture.clone(),
                size_gb: model.size_gb(),
                size_bytes: model.size_bytes,
                required_ram_gb: model.required_ram_gb,
                required_storage_gb,
                installed,
                compatible,
                compatibility_status: compatibility_status_code(compatibility.status).to_string(),
                compatibility_reasons,
                download_action,
                download_reason,
                gates: vec![
                    ModelCatalogGateStatus {
                        gate: "hardware_compatibility".to_string(),
                        status: compatibility_status_code(compatibility.status).to_string(),
                        reason: compatibility_gate_reason(&compatibility.reasons),
                    },
                    ModelCatalogGateStatus {
                        gate: "download_policy".to_string(),
                        status: admission.download.status.as_str().to_string(),
                        reason: admission.download.policy_reason(),
                    },
                    ModelCatalogGateStatus {
                        gate: "registry_integrity".to_string(),
                        status: admission.registry_integrity.status.as_str().to_string(),
                        reason: admission.registry_integrity.policy_reason(),
                    },
                    ModelCatalogGateStatus {
                        gate: "license_policy".to_string(),
                        status: admission.license.status.as_str().to_string(),
                        reason: admission.license.reason.as_str().to_string(),
                    },
                    ModelCatalogGateStatus {
                        gate: "license_acceptance".to_string(),
                        status: admission.license_acceptance.status.as_str().to_string(),
                        reason: admission.license_acceptance.reason.as_str().to_string(),
                    },
                    ModelCatalogGateStatus {
                        gate: "network_policy".to_string(),
                        status: admission.network_policy.status.as_str().to_string(),
                        reason: admission.network_policy.policy_reason().to_string(),
                    },
                ],
            }
        })
        .collect();

    entries.sort_by(|left, right| {
        left.download_action
            .sort_rank()
            .cmp(&right.download_action.sort_rank())
            .then(left.required_ram_gb.cmp(&right.required_ram_gb))
            .then(left.size_bytes.cmp(&right.size_bytes))
            .then(left.id.cmp(&right.id))
    });
    entries
}

pub fn select_model_catalog_download_candidate(
    entries: &[ModelCatalogEntry],
    requested_model_id: Option<&str>,
) -> Result<Option<ModelCatalogSelection>, String> {
    if let Some(model_id) = requested_model_id {
        let Some(entry) = entries.iter().find(|entry| entry.id == model_id) else {
            return Err(format!("model '{model_id}' not found in catalog"));
        };

        return match entry.download_action {
            ModelCatalogDownloadAction::Ready => Ok(Some(ModelCatalogSelection {
                model_id: entry.id.clone(),
                reason: "requested_model_ready".to_string(),
                entry: entry.clone(),
            })),
            ModelCatalogDownloadAction::AlreadyInstalled => Ok(Some(ModelCatalogSelection {
                model_id: entry.id.clone(),
                reason: "requested_model_already_installed".to_string(),
                entry: entry.clone(),
            })),
            _ => Err(format!(
                "model '{}' is not eligible for download: {} ({})",
                entry.id,
                entry.download_action.as_str(),
                entry.download_reason
            )),
        };
    }

    Ok(entries
        .iter()
        .filter(|entry| entry.is_download_ready())
        .min_by(|left, right| {
            left.required_ram_gb
                .cmp(&right.required_ram_gb)
                .then(left.size_bytes.cmp(&right.size_bytes))
                .then(left.id.cmp(&right.id))
        })
        .map(|entry| ModelCatalogSelection {
            model_id: entry.id.clone(),
            reason: "smallest_compatible_ready_model".to_string(),
            entry: entry.clone(),
        }))
}

fn compatibility_status_code(status: ModelCompatibilityStatus) -> &'static str {
    match status {
        ModelCompatibilityStatus::Compatible => "compatible",
        ModelCompatibilityStatus::Incompatible => "incompatible",
        ModelCompatibilityStatus::UnknownModel => "unknown_model",
        ModelCompatibilityStatus::UnknownHardware => "unknown_hardware",
    }
}

fn compatibility_gate_reason(reasons: &[ModelCompatibilityReason]) -> String {
    if reasons.is_empty() {
        "compatible".to_string()
    } else {
        reasons
            .iter()
            .map(compatibility_reason_code)
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn compatibility_reason_code(reason: &ModelCompatibilityReason) -> String {
    match reason {
        ModelCompatibilityReason::UnknownModel { model_id } => {
            format!("unknown_model:{model_id}")
        }
        ModelCompatibilityReason::MissingHardwareField { field } => {
            format!("missing_hardware_field:{field}")
        }
        ModelCompatibilityReason::InsufficientRam {
            required_gb,
            available_gb,
        } => format!("insufficient_ram:required={required_gb},available={available_gb}"),
        ModelCompatibilityReason::InsufficientStorage {
            required_gb,
            available_gb,
        } => {
            format!("insufficient_storage:required={required_gb},available={available_gb}")
        }
        ModelCompatibilityReason::GpuRequired { accelerator } => format!(
            "gpu_required:accelerator={}",
            accelerator.as_deref().unwrap_or("unknown")
        ),
        ModelCompatibilityReason::MissingCpuFeature { feature } => {
            format!("missing_cpu_feature:{feature}")
        }
    }
}

fn non_empty_reason(reasons: &[String], fallback: &str) -> String {
    if reasons.is_empty() {
        fallback.to_string()
    } else {
        reasons.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LicenseAcceptanceStore, ModelRegistry, ModelStorage};
    use tempfile::TempDir;

    fn capabilities(ram_gb: u32, storage_available_gb: u32) -> NodeCapabilities {
        NodeCapabilities {
            node_id: "test-node".to_string(),
            cpu_cores: 8,
            ram_gb,
            gpu_type: None,
            npu_type: None,
            storage_available_gb,
            worker_slots: 4,
            supported_models: Vec::new(),
            cpu_features: vec!["avx2".to_string()],
            accelerator: "CPU".to_string(),
        }
    }

    fn catalog_for(
        capabilities: &NodeCapabilities,
    ) -> Result<(TempDir, TempDir, Vec<ModelCatalogEntry>), Box<dyn std::error::Error>> {
        let storage_dir = TempDir::new()?;
        let acceptance_dir = TempDir::new()?;
        let storage = ModelStorage::new_in(storage_dir.path().to_path_buf());
        let acceptance =
            LicenseAcceptanceStore::new_in(acceptance_dir.path().join("license_acceptance.json"));
        let entries =
            build_model_catalog_entries(&ModelRegistry::new(), &storage, &acceptance, capabilities);
        Ok((storage_dir, acceptance_dir, entries))
    }

    #[test]
    fn catalog_marks_llama_license_acceptance_required_before_download(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_storage_dir, _acceptance_dir, entries) = catalog_for(&capabilities(16, 64))?;
        let llama = entries
            .iter()
            .find(|entry| entry.id == "llama3-3b")
            .ok_or("missing llama catalog entry")?;

        assert_eq!(
            llama.download_action,
            ModelCatalogDownloadAction::LicenseAcceptanceRequired
        );
        assert_eq!(llama.compatibility_status, "compatible");
        assert!(llama
            .gates
            .iter()
            .any(|gate| { gate.gate == "license_acceptance" && gate.status == "required" }));
        Ok(())
    }

    #[test]
    fn catalog_selection_prefers_smallest_ready_compatible_model(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_storage_dir, _acceptance_dir, entries) = catalog_for(&capabilities(16, 64))?;
        let selection = select_model_catalog_download_candidate(&entries, None)?
            .ok_or("expected one ready model")?;

        assert_eq!(selection.model_id, "tinyllama-1b");
        assert_eq!(selection.reason, "smallest_compatible_ready_model");
        assert!(selection.entry.is_download_ready());
        Ok(())
    }

    #[test]
    fn requested_incompatible_model_is_rejected_without_fallback(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_storage_dir, _acceptance_dir, entries) = catalog_for(&capabilities(2, 64))?;
        let error = select_model_catalog_download_candidate(&entries, Some("mistral-7b"))
            .expect_err("mistral should not be selected on a 2GB node");

        assert!(error.contains("mistral-7b"));
        assert!(error.contains("incompatible"));
        assert!(!error.contains("tinyllama-1b"));
        Ok(())
    }

    #[test]
    fn accepted_llama_license_makes_requested_download_ready(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage_dir = TempDir::new()?;
        let acceptance_dir = TempDir::new()?;
        let storage = ModelStorage::new_in(storage_dir.path().to_path_buf());
        let acceptance =
            LicenseAcceptanceStore::new_in(acceptance_dir.path().join("license_acceptance.json"));
        let registry = ModelRegistry::new();
        let llama = registry
            .get("llama3-3b")
            .ok_or("missing llama descriptor")?;
        acceptance.accept_descriptor(llama)?;

        let entries =
            build_model_catalog_entries(&registry, &storage, &acceptance, &capabilities(16, 64));
        let selection = select_model_catalog_download_candidate(&entries, Some("llama3-3b"))?
            .ok_or("expected requested llama selection")?;

        assert_eq!(selection.model_id, "llama3-3b");
        assert_eq!(
            selection.entry.download_action,
            ModelCatalogDownloadAction::Ready
        );
        Ok(())
    }
}
