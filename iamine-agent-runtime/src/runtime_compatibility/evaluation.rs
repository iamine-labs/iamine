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

pub(crate) struct CompatibleResourceProfile {
    minimum_logical_cores: u16,
    recommended_logical_cores: u16,
    max_background_threads: u16,
    max_working_set_mb: u64,
    total_storage_mb: u64,
    writable_storage_mb: u64,
    network: NetworkMode,
}

impl CompatibleResourceProfile {
    pub(crate) const fn minimum_logical_cores(&self) -> u16 {
        self.minimum_logical_cores
    }

    pub(crate) const fn recommended_logical_cores(&self) -> u16 {
        self.recommended_logical_cores
    }

    pub(crate) const fn max_background_threads(&self) -> u16 {
        self.max_background_threads
    }

    pub(crate) const fn max_working_set_mb(&self) -> u64 {
        self.max_working_set_mb
    }

    pub(crate) const fn total_storage_mb(&self) -> u64 {
        self.total_storage_mb
    }

    pub(crate) const fn writable_storage_mb(&self) -> u64 {
        self.writable_storage_mb
    }

    pub(crate) const fn network(&self) -> NetworkMode {
        self.network
    }
}

pub(super) fn evaluate_subject(
    subject: PackageReviewSubject<'_>,
    language: RuntimeLanguageDecision,
    resources: RuntimeResourceEnvelope,
) -> Result<CompatibilityResult, RuntimeCompatibilityError> {
    validate_language(language)?;
    let operating_mode = map_operating_mode(subject.package().manifest().agent.earliest_mode);
    let profile = resolve_compatible_resource_profile(subject, operating_mode)?;
    validate_resources(&profile, resources)?;
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

pub(crate) fn resolve_compatible_resource_profile(
    subject: PackageReviewSubject<'_>,
    operating_mode: ResourceOperatingMode,
) -> Result<CompatibleResourceProfile, RuntimeCompatibilityError> {
    let metadata = parse_resource_metadata(subject)?;
    if metadata.package_id != subject.package().manifest().package_id {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::ResourcePackageMismatch,
            RuntimeCompatibilityRequirement::ResourceMetadata,
        ));
    }
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
    let writable_storage_mb = storage
        .temp_workspace_mb
        .checked_add(storage.cache_budget_mb)
        .ok_or_else(invalid_resource_metadata)?;
    let total_storage_mb = storage
        .package_size_mb
        .checked_add(writable_storage_mb)
        .ok_or_else(invalid_resource_metadata)?;

    Ok(CompatibleResourceProfile {
        minimum_logical_cores: cpu.min_logical_cores,
        recommended_logical_cores: cpu.recommended_logical_cores,
        max_background_threads: cpu.max_background_threads,
        max_working_set_mb: memory.max_working_set_mb,
        total_storage_mb,
        writable_storage_mb,
        network: network.mode,
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
    profile: &CompatibleResourceProfile,
    resources: RuntimeResourceEnvelope,
) -> Result<(), RuntimeCompatibilityError> {
    if resources.logical_cores() < profile.minimum_logical_cores() {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::CpuInsufficient,
            RuntimeCompatibilityRequirement::Cpu,
        ));
    }
    if resources.memory_limit_mb() < profile.max_working_set_mb() {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::MemoryInsufficient,
            RuntimeCompatibilityRequirement::Memory,
        ));
    }
    if resources.storage_limit_mb() < profile.total_storage_mb() {
        return Err(RuntimeCompatibilityError::new(
            RuntimeCompatibilityErrorCode::StorageInsufficient,
            RuntimeCompatibilityRequirement::Storage,
        ));
    }
    if !network_available(resources.network(), profile.network()) {
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
