use std::{fmt, sync::Arc};

use crate::{
    execution_lifecycle::ExecutionIdentity, sandbox_enforcement::SandboxEvidenceIdentity,
    ExecutionLifecycleState, PackageReviewSubject,
};

use super::evaluation::AuthorizationFacts;
use super::ExecutionAuthorizationRequirement;

pub const EXECUTION_AUTHORIZATION_SCHEMA_VERSION: &str =
    "iamine.agent.execution_authorization.decision-0.1";

#[derive(Debug)]
pub(crate) struct ExecutionAuthorizationAuthorityIdentity;

#[derive(Debug)]
pub(crate) struct ExecutionAuthorizationEvidenceIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionAuthorizationEvidenceStatus {
    Authorized,
}

const AUTHORIZATION_REQUIREMENTS: [ExecutionAuthorizationRequirement; 14] = [
    ExecutionAuthorizationRequirement::PackageIdentity,
    ExecutionAuthorizationRequirement::PackageReviewEvidence,
    ExecutionAuthorizationRequirement::RuntimeCompatibilityEvidence,
    ExecutionAuthorizationRequirement::InputOutputEnforcementEvidence,
    ExecutionAuthorizationRequirement::SandboxEnforcementEvidence,
    ExecutionAuthorizationRequirement::LifecycleRecord,
    ExecutionAuthorizationRequirement::LifecycleState,
    ExecutionAuthorizationRequirement::TimeoutCancelControl,
    ExecutionAuthorizationRequirement::ScopeEvaluation,
    ExecutionAuthorizationRequirement::PermissionEvaluation,
    ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
    ExecutionAuthorizationRequirement::AuditScopeEvidence,
    ExecutionAuthorizationRequirement::AuditPermissionEvidence,
    ExecutionAuthorizationRequirement::AuditLifecycleEvidence,
];

#[must_use]
pub struct ExecutionAuthorizationEvidence<'subject> {
    authority: Arc<ExecutionAuthorizationAuthorityIdentity>,
    identity: Arc<ExecutionAuthorizationEvidenceIdentity>,
    subject: PackageReviewSubject<'subject>,
    execution: Arc<ExecutionIdentity>,
    sandbox: Arc<SandboxEvidenceIdentity>,
    selected_candidate_id: String,
    lifecycle_revision: u8,
}

impl<'subject> ExecutionAuthorizationEvidence<'subject> {
    pub(crate) fn new(
        authority: Arc<ExecutionAuthorizationAuthorityIdentity>,
        facts: AuthorizationFacts<'subject>,
    ) -> Self {
        Self {
            authority,
            identity: Arc::new(ExecutionAuthorizationEvidenceIdentity),
            subject: facts.subject,
            execution: facts.execution,
            sandbox: facts.sandbox,
            selected_candidate_id: facts.selected_candidate_id,
            lifecycle_revision: facts.lifecycle_revision,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        EXECUTION_AUTHORIZATION_SCHEMA_VERSION
    }

    pub const fn status(&self) -> ExecutionAuthorizationEvidenceStatus {
        ExecutionAuthorizationEvidenceStatus::Authorized
    }

    pub const fn requirements(&self) -> &'static [ExecutionAuthorizationRequirement] {
        &AUTHORIZATION_REQUIREMENTS
    }

    pub fn selected_candidate_id(&self) -> &str {
        &self.selected_candidate_id
    }

    pub const fn lifecycle_state(&self) -> ExecutionLifecycleState {
        ExecutionLifecycleState::ScopeCheck
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn authorization_recorded(&self) -> bool {
        true
    }

    pub const fn execution_authorized(&self) -> bool {
        true
    }

    pub const fn package_load_allowed(&self) -> bool {
        false
    }

    pub const fn package_loaded(&self) -> bool {
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

    pub(crate) const fn authority(&self) -> &Arc<ExecutionAuthorizationAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn identity(&self) -> &Arc<ExecutionAuthorizationEvidenceIdentity> {
        &self.identity
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'subject> {
        self.subject
    }

    pub(crate) fn matches(&self, facts: &AuthorizationFacts<'subject>) -> bool {
        self.subject.same_as(facts.subject)
            && Arc::ptr_eq(&self.execution, &facts.execution)
            && Arc::ptr_eq(&self.sandbox, &facts.sandbox)
            && self.selected_candidate_id == facts.selected_candidate_id
            && self.lifecycle_revision == facts.lifecycle_revision
    }
}

impl fmt::Debug for ExecutionAuthorizationEvidence<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionAuthorizationEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("sandbox", &"[redacted]")
            .field("selected_candidate_id", &"[redacted]")
            .field("lifecycle_state", &self.lifecycle_state().as_str())
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("authorization_recorded", &true)
            .field("execution_authorized", &true)
            .field("package_load_allowed", &false)
            .field("package_loaded", &false)
            .field("runtime_active", &false)
            .field("sandbox_active", &false)
            .field("scheduler_mutated", &false)
            .field("transport_started", &false)
            .field("persisted", &false)
            .field("external_event_emitted", &false)
            .finish()
    }
}
