use iamine_agents::{NetworkMode, ResourceOperatingMode};

use crate::{
    runtime_compatibility::resolve_compatible_resource_profile, PackageReviewSubject,
    RuntimeCompatibilityEvidence, RuntimeLanguageMode,
};

use super::{
    SandboxEnforcementError, SandboxEnforcementErrorCode, SandboxEnforcementPolicy,
    SandboxEnforcementRequirement, SandboxResourceLimits, SandboxRestrictionProfile,
};

pub(super) struct SandboxEvaluation {
    pub limits: SandboxResourceLimits,
    pub restrictions: SandboxRestrictionProfile,
}

pub(super) fn evaluate_subject(
    subject: PackageReviewSubject<'_>,
    compatibility: &RuntimeCompatibilityEvidence<'_>,
    policy: SandboxEnforcementPolicy,
) -> Result<SandboxEvaluation, SandboxEnforcementError> {
    if compatibility.runtime_mode() != RuntimeLanguageMode::RustNativeOfficial {
        return Err(SandboxEnforcementError::new(
            SandboxEnforcementErrorCode::RuntimeModeUnsupported,
            SandboxEnforcementRequirement::RuntimeMode,
        ));
    }
    if compatibility.operating_mode() != ResourceOperatingMode::LocalReadonly {
        return Err(SandboxEnforcementError::new(
            SandboxEnforcementErrorCode::OperatingModeUnsupported,
            SandboxEnforcementRequirement::OperatingMode,
        ));
    }

    validate_security_policy(subject)?;
    let profile = resolve_compatible_resource_profile(subject, compatibility.operating_mode())
        .map_err(|_| {
            SandboxEnforcementError::new(
                SandboxEnforcementErrorCode::ResourceMetadataInvalid,
                SandboxEnforcementRequirement::ResourceMetadata,
            )
        })?;
    if profile.network() != NetworkMode::None {
        return Err(SandboxEnforcementError::new(
            SandboxEnforcementErrorCode::NetworkAccessUnsupported,
            SandboxEnforcementRequirement::NetworkIsolation,
        ));
    }

    let available = compatibility.resources();
    let logical_cores = profile
        .recommended_logical_cores()
        .min(available.logical_cores());
    let max_background_threads = profile.max_background_threads().min(logical_cores);
    let limits = SandboxResourceLimits::new(
        logical_cores,
        max_background_threads,
        profile.max_working_set_mb(),
        profile.writable_storage_mb(),
        policy.max_wall_time_ms(),
        policy.max_open_files(),
    );
    Ok(SandboxEvaluation {
        limits,
        restrictions: SandboxRestrictionProfile::local_readonly(),
    })
}

fn validate_security_policy(
    subject: PackageReviewSubject<'_>,
) -> Result<(), SandboxEnforcementError> {
    let security = &subject.package().manifest().security;
    if security.collects_credentials || security.collects_host_identifiers {
        return Err(SandboxEnforcementError::new(
            SandboxEnforcementErrorCode::PrivateDataRequested,
            SandboxEnforcementRequirement::SecurityPolicy,
        ));
    }
    if security.allows_destructive_actions
        || security.allows_arbitrary_shell
        || security.allows_unrestricted_filesystem
    {
        return Err(SandboxEnforcementError::new(
            SandboxEnforcementErrorCode::UnsafeSecurityPolicy,
            SandboxEnforcementRequirement::FilesystemIsolation,
        ));
    }
    if security.requires_network {
        return Err(SandboxEnforcementError::new(
            SandboxEnforcementErrorCode::NetworkAccessUnsupported,
            SandboxEnforcementRequirement::NetworkIsolation,
        ));
    }
    Ok(())
}
