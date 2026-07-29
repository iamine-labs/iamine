use std::{fmt, sync::Arc};

use crate::package_load_evidence_integration::PackageLoadEvidenceIdentity;
use crate::{PackageLoadEvidence, PackageReviewSubject};

use super::PackageLoaderRequirement;

pub const LOADED_AGENT_PACKAGE_SCHEMA_VERSION: &str =
    "iamine.agent.package_loader.loaded_package-0.1";

#[derive(Debug)]
pub(crate) struct PackageLoaderAuthorityIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LoadedAgentPackageStatus {
    Loaded,
}

const LOAD_REQUIREMENTS: [PackageLoaderRequirement; 3] = [
    PackageLoaderRequirement::PackageLoadEvidence,
    PackageLoaderRequirement::BoundedReferenceSnapshot,
    PackageLoaderRequirement::ValidatedReferenceContract,
];

#[must_use]
pub struct LoadedAgentPackage<'subject> {
    authority: Arc<PackageLoaderAuthorityIdentity>,
    evidence: Arc<PackageLoadEvidenceIdentity>,
    subject: PackageReviewSubject<'subject>,
    lifecycle_revision: u8,
}

impl<'subject> LoadedAgentPackage<'subject> {
    pub(super) fn new(
        authority: Arc<PackageLoaderAuthorityIdentity>,
        evidence: &PackageLoadEvidence<'subject>,
    ) -> Self {
        Self {
            authority,
            evidence: Arc::clone(evidence.identity()),
            subject: evidence.subject(),
            lifecycle_revision: evidence.lifecycle_revision(),
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        LOADED_AGENT_PACKAGE_SCHEMA_VERSION
    }

    pub const fn status(&self) -> LoadedAgentPackageStatus {
        LoadedAgentPackageStatus::Loaded
    }

    pub const fn requirements(&self) -> &'static [PackageLoaderRequirement] {
        &LOAD_REQUIREMENTS
    }

    pub fn reference_count(&self) -> usize {
        self.subject.reference_count()
    }

    pub const fn total_reference_bytes(&self) -> u64 {
        self.subject.total_reference_bytes()
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn package_load_evidence_verified(&self) -> bool {
        true
    }

    pub const fn package_loaded(&self) -> bool {
        true
    }

    pub const fn execution_allowed(&self) -> bool {
        false
    }

    pub const fn execution_started(&self) -> bool {
        false
    }

    pub const fn runtime_active(&self) -> bool {
        false
    }

    pub const fn sandbox_active(&self) -> bool {
        false
    }

    pub const fn scheduler_mutated(&self) -> bool {
        false
    }

    pub const fn transport_started(&self) -> bool {
        false
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn external_event_emitted(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<PackageLoaderAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn evidence(&self) -> &Arc<PackageLoadEvidenceIdentity> {
        &self.evidence
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'subject> {
        self.subject
    }

    pub(super) fn matches_evidence(&self, evidence: &PackageLoadEvidence<'subject>) -> bool {
        Arc::ptr_eq(&self.evidence, evidence.identity())
            && self.subject.same_as(evidence.subject())
            && self.lifecycle_revision == evidence.lifecycle_revision()
    }
}

impl fmt::Debug for LoadedAgentPackage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoadedAgentPackage")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("evidence", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("reference_count", &self.reference_count())
            .field("total_reference_bytes", &self.total_reference_bytes())
            .field("package_load_evidence_verified", &true)
            .field("package_loaded", &true)
            .field("execution_allowed", &false)
            .field("execution_started", &false)
            .field("runtime_active", &false)
            .field("sandbox_active", &false)
            .field("scheduler_mutated", &false)
            .field("transport_started", &false)
            .field("persisted", &false)
            .field("external_event_emitted", &false)
            .finish()
    }
}
