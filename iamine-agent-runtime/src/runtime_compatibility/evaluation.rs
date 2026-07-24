use iamine_agents::{
    parse_resource_requirements_yaml, ExecutionMode, NetworkMode, ResourceOperatingMode,
    ResourceRequirementsMetadata,
};

use crate::{PackageReferenceKind, PackageReviewSubject};

use super::{
    RuntimeCompatibilityError, RuntimeCompatibilityErrorCode, RuntimeCompatibilityRequirement,
    RuntimeLanguageAvailability, RuntimeLanguageDecision, RuntimeLanguageMode,
    RuntimeNetworkAvailability, RuntimeResourceEnvelope,
};

pub(super) struct CompatibilityResult {
    pub operating_mode: ResourceOperatingMode,
}

pub(super) fn evaluate_subject(
    subject: PackageReviewSubject<'_>,
    language: RuntimeLanguageDecision,
    resources: RuntimeResourceEnvelope,
) -> Result<CompatibilityResult, RuntimeCompatibilityError> {
    validate_language(language)?;
    let metadata = parse_resource_metadata(subject)?;
    if metadata.package_id != subject.package().manifest().package_id {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::ResourcePackageMismatch,
            RuntimeCompatibilityRequirement::ResourceMetadata,
        ));
    }

    let operating_mode = map_operating_mode(subject.package().manifest().agent.earliest_mode);
    validate_resources(&metadata, operating_mode, resources)?;
    Ok(CompatibilityResult { operating_mode })
}

fn validate_language(decision: RuntimeLanguageDecision) -> Result<(), RuntimeCompatibilityError> {
    if decision.mode() != RuntimeLanguageMode::RustNativeOfficial {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::RuntimeModeUnsupported,
            RuntimeCompatibilityRequirement::RuntimeLanguage,
        ));
    }

    let code = match decision.availability() {
        RuntimeLanguageAvailability::Available => return Ok(()),
        RuntimeLanguageAvailability::Unavailable => {
            RuntimeCompatibilityErrorCode::RuntimeUnavailable
        }
        RuntimeLanguageAvailability::Deferred => RuntimeCompatibilityErrorCode::RuntimeDeferred,
        RuntimeLanguageAvailability::Blocked => RuntimeCompatibilityErrorCode::RuntimeBlocked,
    };
    Err(RuntimeCompatibilityError::new(
        code,
        RuntimeCompatibilityRequirement::RuntimeLanguage,
    ))
}

fn parse_resource_metadata(
    subject: PackageReviewSubject<'_>,
) -> Result<ResourceRequirementsMetadata, RuntimeCompatibilityError> {
    let reference = subject
        .references()
        .get(PackageReferenceKind::ResourceRequirements)
        .ok_or_else(|| {
            RuntimeCompatibilityError::new(
                RuntimeCompatibilityErrorCode::ResourceMetadataMissing,
                RuntimeCompatibilityRequirement::ResourceMetadata,
            )
        })?;
    let content = std::str::from_utf8(reference.content()).map_err(|_| {
        RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::ResourceMetadataInvalid,
            RuntimeCompatibilityRequirement::ResourceMetadata,
        )
    })?;
    parse_resource_requirements_yaml(content).map_err(|_| {
        RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::ResourceMetadataInvalid,
            RuntimeCompatibilityRequirement::ResourceMetadata,
        )
    })
}

fn map_operating_mode(mode: ExecutionMode) -> ResourceOperatingMode {
    match mode {
        ExecutionMode::LocalReadonly => ResourceOperatingMode::LocalReadonly,
        ExecutionMode::LocalPlanning => ResourceOperatingMode::LocalPlanning,
        ExecutionMode::LanReadonly => ResourceOperatingMode::LanReadonly,
    }
}

fn validate_resources(
    metadata: &ResourceRequirementsMetadata,
    operating_mode: ResourceOperatingMode,
    resources: RuntimeResourceEnvelope,
) -> Result<(), RuntimeCompatibilityError> {
    if !metadata.operating_modes.contains(&operating_mode) {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::OperatingModeMissing,
            RuntimeCompatibilityRequirement::OperatingMode,
        ));
    }
    let key = operating_mode.as_str();
    let cpu = metadata
        .cpu
        .get(key)
        .ok_or_else(invalid_resource_metadata)?;
    let memory = metadata
        .memory
        .get(key)
        .ok_or_else(invalid_resource_metadata)?;
    let storage = metadata
        .storage
        .get(key)
        .ok_or_else(invalid_resource_metadata)?;
    let network = metadata
        .network
        .get(key)
        .ok_or_else(invalid_resource_metadata)?;

    if resources.logical_cores() < cpu.min_logical_cores {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::CpuInsufficient,
            RuntimeCompatibilityRequirement::Cpu,
        ));
    }
    if resources.memory_limit_mb() < memory.max_working_set_mb {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::MemoryInsufficient,
            RuntimeCompatibilityRequirement::Memory,
        ));
    }
    let required_storage = storage
        .package_size_mb
        .checked_add(storage.temp_workspace_mb)
        .and_then(|value| value.checked_add(storage.cache_budget_mb))
        .ok_or_else(invalid_resource_metadata)?;
    if resources.storage_limit_mb() < required_storage {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::StorageInsufficient,
            RuntimeCompatibilityRequirement::Storage,
        ));
    }
    if !network_available(resources.network(), network.mode) {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::NetworkInsufficient,
            RuntimeCompatibilityRequirement::Network,
        ));
    }
    Ok(())
}

fn network_available(available: RuntimeNetworkAvailability, required: NetworkMode) -> bool {
    match required {
        NetworkMode::None => true,
        NetworkMode::LocalOnly => matches!(
            available,
            RuntimeNetworkAvailability::LocalOnly | RuntimeNetworkAvailability::LanReadonly
        ),
        NetworkMode::LanReadonly => available == RuntimeNetworkAvailability::LanReadonly,
    }
}

fn invalid_resource_metadata() -> RuntimeCompatibilityError {
    RuntimeCompatibilityError::new(
        RuntimeCompatibilityErrorCode::ResourceMetadataInvalid,
        RuntimeCompatibilityRequirement::ResourceMetadata,
    )
}
