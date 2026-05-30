use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ModelExecutionStatus {
    Executable,
    MetadataOnly,
    MissingFromStorage,
    BackendNotReal,
    DisabledByMock,
    UnknownModel,
}

impl ModelExecutionStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Executable => "executable",
            Self::MetadataOnly => "metadata_only",
            Self::MissingFromStorage => "missing_from_storage",
            Self::BackendNotReal => "backend_not_real",
            Self::DisabledByMock => "disabled_by_mock",
            Self::UnknownModel => "unknown_model",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ModelCapabilityStatus {
    pub(crate) model_id: String,
    pub(crate) known_by_registry: bool,
    pub(crate) present_in_storage: bool,
    pub(crate) executable: bool,
    pub(crate) execution_status: ModelExecutionStatus,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelCapabilitySnapshot {
    pub(crate) models_in_registry: Vec<String>,
    pub(crate) models_in_storage: Vec<String>,
    pub(crate) executable_models: Vec<String>,
    pub(crate) metadata_only_models: Vec<String>,
    pub(crate) unavailable_models: Vec<ModelCapabilityStatus>,
    pub(crate) model_statuses: Vec<ModelCapabilityStatus>,
}

pub(crate) struct ModelCapabilityConsistencyInput<'a> {
    pub(crate) models_in_registry: &'a [String],
    pub(crate) models_in_storage: &'a [String],
    pub(crate) advertised_executable_models: &'a [String],
    pub(crate) backend_is_mock: bool,
    pub(crate) real_inference_available: bool,
}

pub(crate) fn normalize_model_capabilities(
    input: &ModelCapabilityConsistencyInput<'_>,
) -> ModelCapabilitySnapshot {
    let models_in_registry = sorted_unique(input.models_in_registry);
    let models_in_storage = sorted_unique(input.models_in_storage);
    let advertised_executable_models = sorted_unique(input.advertised_executable_models);
    let registry_set = as_set(&models_in_registry);
    let storage_set = as_set(&models_in_storage);
    let advertised_set = as_set(&advertised_executable_models);
    let all_models: BTreeSet<_> = registry_set
        .union(&storage_set)
        .copied()
        .chain(advertised_set.iter().copied())
        .collect();

    let model_statuses: Vec<_> = all_models
        .into_iter()
        .map(|model_id| {
            let known_by_registry = registry_set.contains(model_id);
            let present_in_storage = storage_set.contains(model_id);
            let advertised_executable = advertised_set.contains(model_id);
            let executable = known_by_registry
                && present_in_storage
                && advertised_executable
                && !input.backend_is_mock
                && input.real_inference_available;
            let execution_status = if executable {
                ModelExecutionStatus::Executable
            } else if !known_by_registry {
                ModelExecutionStatus::UnknownModel
            } else if !present_in_storage {
                ModelExecutionStatus::MissingFromStorage
            } else if input.backend_is_mock {
                ModelExecutionStatus::DisabledByMock
            } else if !input.real_inference_available {
                ModelExecutionStatus::BackendNotReal
            } else {
                ModelExecutionStatus::MetadataOnly
            };

            ModelCapabilityStatus {
                model_id: model_id.to_string(),
                known_by_registry,
                present_in_storage,
                executable,
                execution_status,
                reason: execution_status.as_str().to_string(),
            }
        })
        .collect();

    ModelCapabilitySnapshot {
        models_in_registry,
        models_in_storage,
        executable_models: model_statuses
            .iter()
            .filter(|model| model.executable)
            .map(|model| model.model_id.clone())
            .collect(),
        metadata_only_models: model_statuses
            .iter()
            .filter(|model| !model.executable)
            .map(|model| model.model_id.clone())
            .collect(),
        unavailable_models: model_statuses
            .iter()
            .filter(|model| !model.executable)
            .cloned()
            .collect(),
        model_statuses,
    }
}

fn sorted_unique(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn as_set(values: &[String]) -> BTreeSet<&str> {
    values.iter().map(String::as_str).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(values: &[&str]) -> Vec<String> {
        values.iter().map(ToString::to_string).collect()
    }

    fn snapshot(
        models_in_registry: &[&str],
        models_in_storage: &[&str],
        advertised_executable_models: &[&str],
        backend_is_mock: bool,
        real_inference_available: bool,
    ) -> ModelCapabilitySnapshot {
        normalize_model_capabilities(&ModelCapabilityConsistencyInput {
            models_in_registry: &values(models_in_registry),
            models_in_storage: &values(models_in_storage),
            advertised_executable_models: &values(advertised_executable_models),
            backend_is_mock,
            real_inference_available,
        })
    }

    #[test]
    fn mock_skip_does_not_advertise_real_executable_models() {
        let snapshot = snapshot(
            &["tinyllama-1b"],
            &["tinyllama-1b"],
            &["tinyllama-1b"],
            true,
            false,
        );

        assert!(snapshot.executable_models.is_empty());
        assert_eq!(snapshot.metadata_only_models, values(&["tinyllama-1b"]));
        assert_eq!(
            snapshot.model_statuses[0].execution_status,
            ModelExecutionStatus::DisabledByMock
        );
    }

    #[test]
    fn real_backend_executable_models_preserved() {
        let snapshot = snapshot(
            &["tinyllama-1b"],
            &["tinyllama-1b"],
            &["tinyllama-1b"],
            false,
            true,
        );

        assert_eq!(snapshot.executable_models, values(&["tinyllama-1b"]));
        assert!(snapshot.metadata_only_models.is_empty());
    }

    #[test]
    fn registry_model_missing_from_storage_not_executable() {
        let snapshot = snapshot(&["mistral-7b"], &[], &[], false, true);

        assert!(snapshot.executable_models.is_empty());
        assert_eq!(
            snapshot.model_statuses[0].execution_status,
            ModelExecutionStatus::MissingFromStorage
        );
    }

    #[test]
    fn unknown_model_not_executable() {
        let snapshot = snapshot(&[], &["local-custom"], &[], false, true);

        assert!(snapshot.executable_models.is_empty());
        assert_eq!(
            snapshot.model_statuses[0].execution_status,
            ModelExecutionStatus::UnknownModel
        );
    }

    #[test]
    fn unvalidated_storage_model_is_metadata_only() {
        let snapshot = snapshot(&["tinyllama-1b"], &["tinyllama-1b"], &[], false, true);

        assert!(snapshot.executable_models.is_empty());
        assert_eq!(
            snapshot.model_statuses[0].execution_status,
            ModelExecutionStatus::MetadataOnly
        );
    }
}
