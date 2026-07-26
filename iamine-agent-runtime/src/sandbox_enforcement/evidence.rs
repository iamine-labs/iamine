use std::{fmt, sync::Arc};

use crate::PackageReviewSubject;

use super::{
    SandboxCleanupOwner, SandboxCleanupTrigger, SandboxEnforcementRequirement, SandboxPlatform,
    SandboxResourceLimits, SandboxRestrictionProfile,
};

#[derive(Debug)]
pub(crate) struct SandboxAuthorityIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxEnforcementEvidenceStatus {
    Prepared,
}

pub const SANDBOX_ENFORCEMENT_SCHEMA_VERSION: &str = "iamine.agent.sandbox_enforcement.plan-0.1";

const ESTABLISHED_REQUIREMENTS: [SandboxEnforcementRequirement; 12] = [
    SandboxEnforcementRequirement::RuntimeCompatibilityEvidence,
    SandboxEnforcementRequirement::InputOutputEnforcementEvidence,
    SandboxEnforcementRequirement::EvidenceChain,
    SandboxEnforcementRequirement::CurrentPlatform,
    SandboxEnforcementRequirement::RuntimeMode,
    SandboxEnforcementRequirement::OperatingMode,
    SandboxEnforcementRequirement::SecurityPolicy,
    SandboxEnforcementRequirement::ResourceMetadata,
    SandboxEnforcementRequirement::FilesystemIsolation,
    SandboxEnforcementRequirement::NetworkIsolation,
    SandboxEnforcementRequirement::ResourceLimits,
    SandboxEnforcementRequirement::CleanupOwnership,
];

#[must_use]
pub struct SandboxEnforcementEvidence<'a> {
    authority: Arc<SandboxAuthorityIdentity>,
    subject: PackageReviewSubject<'a>,
    platform: SandboxPlatform,
    limits: SandboxResourceLimits,
    restrictions: SandboxRestrictionProfile,
}

impl<'a> SandboxEnforcementEvidence<'a> {
    pub(crate) fn new(
        authority: Arc<SandboxAuthorityIdentity>,
        subject: PackageReviewSubject<'a>,
        platform: SandboxPlatform,
        limits: SandboxResourceLimits,
        restrictions: SandboxRestrictionProfile,
    ) -> Self {
        Self {
            authority,
            subject,
            platform,
            limits,
            restrictions,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        SANDBOX_ENFORCEMENT_SCHEMA_VERSION
    }

    pub const fn status(&self) -> SandboxEnforcementEvidenceStatus {
        SandboxEnforcementEvidenceStatus::Prepared
    }

    pub const fn requirements(&self) -> &'static [SandboxEnforcementRequirement] {
        &ESTABLISHED_REQUIREMENTS
    }

    pub const fn platform(&self) -> SandboxPlatform {
        self.platform
    }

    pub const fn resource_limits(&self) -> SandboxResourceLimits {
        self.limits
    }

    pub const fn restrictions(&self) -> SandboxRestrictionProfile {
        self.restrictions
    }

    pub const fn cleanup_owner(&self) -> SandboxCleanupOwner {
        self.restrictions.cleanup_owner()
    }

    pub const fn cleanup_triggers(&self) -> &'static [SandboxCleanupTrigger] {
        self.restrictions.cleanup_triggers()
    }

    pub const fn sandbox_active(&self) -> bool {
        false
    }

    pub const fn cleanup_registered(&self) -> bool {
        false
    }

    pub const fn load_allowed(&self) -> bool {
        false
    }

    pub const fn execution_allowed(&self) -> bool {
        false
    }

    pub const fn persistence_allowed(&self) -> bool {
        false
    }

    pub const fn transport_allowed(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<SandboxAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'a> {
        self.subject
    }
}

impl fmt::Debug for SandboxEnforcementEvidence<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SandboxEnforcementEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("platform", &self.platform.as_str())
            .field("limits", &"[redacted]")
            .field("restrictions", &"[redacted]")
            .field("sandbox_active", &false)
            .field("cleanup_registered", &false)
            .field("load_allowed", &false)
            .field("execution_allowed", &false)
            .finish()
    }
}
