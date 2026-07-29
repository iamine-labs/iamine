use std::{fmt, sync::Arc};

use crate::execution_authorization::ExecutionAuthorizationEvidenceIdentity;
use crate::{ExecutionAuthorizationEvidence, PackageReviewSubject};

use super::PackageLoadEvidenceRequirement;

pub const PACKAGE_LOAD_EVIDENCE_SCHEMA_VERSION: &str =
    "iamine.agent.package_load_evidence.decision-0.1";

#[derive(Debug)]
pub(crate) struct PackageLoadEvidenceAuthorityIdentity;

#[derive(Debug)]
pub(crate) struct PackageLoadEvidenceIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PackageLoadEvidenceStatus {
    Eligible,
}

const PACKAGE_LOAD_REQUIREMENTS: [PackageLoadEvidenceRequirement; 9] = [
    PackageLoadEvidenceRequirement::ScopeManifestValidation,
    PackageLoadEvidenceRequirement::CapabilityMetadataValidation,
    PackageLoadEvidenceRequirement::ExpertiseMetadataValidation,
    PackageLoadEvidenceRequirement::ResourceRequirementsValidation,
    PackageLoadEvidenceRequirement::PermissionModelValidation,
    PackageLoadEvidenceRequirement::AuditPolicyValidation,
    PackageLoadEvidenceRequirement::BoundaryEvalValidation,
    PackageLoadEvidenceRequirement::ReferenceContract,
    PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence,
];

#[must_use]
pub struct PackageLoadEvidence<'subject> {
    authority: Arc<PackageLoadEvidenceAuthorityIdentity>,
    identity: Arc<PackageLoadEvidenceIdentity>,
    authorization: Arc<ExecutionAuthorizationEvidenceIdentity>,
    subject: PackageReviewSubject<'subject>,
    lifecycle_revision: u8,
}

impl<'subject> PackageLoadEvidence<'subject> {
    pub(crate) fn new(
        authority: Arc<PackageLoadEvidenceAuthorityIdentity>,
        authorization: &ExecutionAuthorizationEvidence<'subject>,
    ) -> Self {
        Self {
            authority,
            identity: Arc::new(PackageLoadEvidenceIdentity),
            authorization: Arc::clone(authorization.identity()),
            subject: authorization.subject(),
            lifecycle_revision: authorization.lifecycle_revision(),
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        PACKAGE_LOAD_EVIDENCE_SCHEMA_VERSION
    }

    pub const fn status(&self) -> PackageLoadEvidenceStatus {
        PackageLoadEvidenceStatus::Eligible
    }

    pub const fn requirements(&self) -> &'static [PackageLoadEvidenceRequirement] {
        &PACKAGE_LOAD_REQUIREMENTS
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn evidence_integrated(&self) -> bool {
        true
    }

    pub const fn package_load_allowed(&self) -> bool {
        true
    }

    pub const fn package_loaded(&self) -> bool {
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

    pub(crate) const fn authority(&self) -> &Arc<PackageLoadEvidenceAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn identity(&self) -> &Arc<PackageLoadEvidenceIdentity> {
        &self.identity
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'subject> {
        self.subject
    }

    pub(crate) fn matches_authorization(
        &self,
        authorization: &ExecutionAuthorizationEvidence<'subject>,
    ) -> bool {
        Arc::ptr_eq(&self.authorization, authorization.identity())
            && self.subject.same_as(authorization.subject())
            && self.lifecycle_revision == authorization.lifecycle_revision()
    }
}

impl fmt::Debug for PackageLoadEvidence<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackageLoadEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("authorization", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("evidence_integrated", &true)
            .field("package_load_allowed", &true)
            .field("package_loaded", &false)
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
