use std::{fmt, sync::Arc};

use iamine_agents::ResourceOperatingMode;

use crate::PackageReviewSubject;

use super::{RuntimeCompatibilityRequirement, RuntimeLanguageMode, RuntimeResourceEnvelope};

#[derive(Debug)]
pub(crate) struct RuntimeCompatibilityAuthorityIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeCompatibilityEvidenceStatus {
    Established,
}

const ESTABLISHED_REQUIREMENTS: [RuntimeCompatibilityRequirement; 8] = [
    RuntimeCompatibilityRequirement::PackageReviewEvidence,
    RuntimeCompatibilityRequirement::RuntimeLanguage,
    RuntimeCompatibilityRequirement::ResourceMetadata,
    RuntimeCompatibilityRequirement::OperatingMode,
    RuntimeCompatibilityRequirement::Cpu,
    RuntimeCompatibilityRequirement::Memory,
    RuntimeCompatibilityRequirement::Storage,
    RuntimeCompatibilityRequirement::Network,
];

#[must_use]
pub struct RuntimeCompatibilityEvidence<'a> {
    authority: Arc<RuntimeCompatibilityAuthorityIdentity>,
    subject: PackageReviewSubject<'a>,
    runtime_mode: RuntimeLanguageMode,
    operating_mode: ResourceOperatingMode,
    resources: RuntimeResourceEnvelope,
}

impl<'a> RuntimeCompatibilityEvidence<'a> {
    pub(crate) fn new(
        authority: Arc<RuntimeCompatibilityAuthorityIdentity>,
        subject: PackageReviewSubject<'a>,
        runtime_mode: RuntimeLanguageMode,
        operating_mode: ResourceOperatingMode,
        resources: RuntimeResourceEnvelope,
    ) -> Self {
        Self {
            authority,
            subject,
            runtime_mode,
            operating_mode,
            resources,
        }
    }

    pub const fn status(&self) -> RuntimeCompatibilityEvidenceStatus {
        RuntimeCompatibilityEvidenceStatus::Established
    }

    pub const fn requirements(&self) -> &'static [RuntimeCompatibilityRequirement] {
        &ESTABLISHED_REQUIREMENTS
    }

    pub const fn runtime_mode(&self) -> RuntimeLanguageMode {
        self.runtime_mode
    }

    pub const fn operating_mode(&self) -> ResourceOperatingMode {
        self.operating_mode
    }

    pub const fn load_allowed(&self) -> bool {
        false
    }

    pub const fn execution_allowed(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<RuntimeCompatibilityAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'a> {
        self.subject
    }

    pub(crate) const fn resources(&self) -> RuntimeResourceEnvelope {
        self.resources
    }
}

impl fmt::Debug for RuntimeCompatibilityEvidence<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeCompatibilityEvidence")
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("runtime_mode", &self.runtime_mode)
            .field("operating_mode", &self.operating_mode.as_str())
            .field("resources", &"[redacted]")
            .field("authority", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("load_allowed", &false)
            .field("execution_allowed", &false)
            .finish()
    }
}
