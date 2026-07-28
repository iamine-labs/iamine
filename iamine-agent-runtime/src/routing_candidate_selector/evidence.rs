use std::{fmt, sync::Arc};

use super::{
    RoutingCandidateExclusionReason, RoutingCandidateSelectionAuthorityIdentity,
    RoutingCandidateSelectionOutcome, RoutingCandidateSelectorRequirement,
    RoutingSelectionBlockedAction,
};
use crate::sandbox_enforcement::SandboxEvidenceIdentity;

pub const ROUTING_CANDIDATE_SELECTION_SCHEMA_VERSION: &str =
    "iamine.agent.routing_candidate_selection.enforced-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoutingCandidateSelectionEvidenceStatus {
    Established,
}

const ESTABLISHED_REQUIREMENTS: [RoutingCandidateSelectorRequirement; 9] = [
    RoutingCandidateSelectorRequirement::TaskType,
    RoutingCandidateSelectorRequirement::ResourceRequirements,
    RoutingCandidateSelectorRequirement::CandidateCount,
    RoutingCandidateSelectorRequirement::CandidateIdentity,
    RoutingCandidateSelectorRequirement::ScopeEvaluation,
    RoutingCandidateSelectorRequirement::PermissionEvaluation,
    RoutingCandidateSelectorRequirement::RuntimeCompatibilityEvidence,
    RoutingCandidateSelectorRequirement::SandboxEnforcementEvidence,
    RoutingCandidateSelectorRequirement::DeterministicSelection,
];

#[must_use]
pub struct RoutingCandidateSelectionEvidence {
    authority: Arc<RoutingCandidateSelectionAuthorityIdentity>,
    outcome: RoutingCandidateSelectionOutcome,
    selected_candidate_id: Option<String>,
    selected_sandbox: Option<Arc<SandboxEvidenceIdentity>>,
    candidate_count: u16,
    eligible_candidate_count: u16,
    excluded_candidate_count: u16,
    exclusion_counts: [u16; 8],
}

impl RoutingCandidateSelectionEvidence {
    pub(crate) fn new(
        authority: Arc<RoutingCandidateSelectionAuthorityIdentity>,
        result: super::evaluation::RoutingSelectionResult,
    ) -> Self {
        Self {
            authority,
            outcome: result.outcome,
            selected_candidate_id: result.selected_candidate_id,
            selected_sandbox: result.selected_sandbox,
            candidate_count: result.candidate_count,
            eligible_candidate_count: result.eligible_candidate_count,
            excluded_candidate_count: result.excluded_candidate_count,
            exclusion_counts: result.exclusion_counts,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        ROUTING_CANDIDATE_SELECTION_SCHEMA_VERSION
    }

    pub const fn status(&self) -> RoutingCandidateSelectionEvidenceStatus {
        RoutingCandidateSelectionEvidenceStatus::Established
    }

    pub const fn requirements(&self) -> &'static [RoutingCandidateSelectorRequirement] {
        &ESTABLISHED_REQUIREMENTS
    }

    pub const fn outcome(&self) -> RoutingCandidateSelectionOutcome {
        self.outcome
    }

    pub fn selected_candidate_id(&self) -> Option<&str> {
        self.selected_candidate_id.as_deref()
    }

    pub const fn candidate_count(&self) -> u16 {
        self.candidate_count
    }

    pub const fn eligible_candidate_count(&self) -> u16 {
        self.eligible_candidate_count
    }

    pub const fn excluded_candidate_count(&self) -> u16 {
        self.excluded_candidate_count
    }

    pub const fn exclusion_count(&self, reason: RoutingCandidateExclusionReason) -> u16 {
        self.exclusion_counts[reason.index()]
    }

    pub const fn blocked_action(&self) -> RoutingSelectionBlockedAction {
        RoutingSelectionBlockedAction::ContinueLocalExecution
    }

    pub const fn selection_recorded(&self) -> bool {
        true
    }

    pub const fn execution_authorized(&self) -> bool {
        false
    }

    pub const fn concrete_route_created(&self) -> bool {
        false
    }

    pub const fn scheduler_mutated(&self) -> bool {
        false
    }

    pub const fn model_selected(&self) -> bool {
        false
    }

    pub const fn distributed_moe_used(&self) -> bool {
        false
    }

    pub const fn transport_started(&self) -> bool {
        false
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn audit_event_emitted(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<RoutingCandidateSelectionAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn selected_sandbox(&self) -> Option<&Arc<SandboxEvidenceIdentity>> {
        self.selected_sandbox.as_ref()
    }
}

impl fmt::Debug for RoutingCandidateSelectionEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutingCandidateSelectionEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("outcome", &self.outcome.as_str())
            .field(
                "selected_candidate_id",
                &self.selected_candidate_id.as_ref().map(|_| "[redacted]"),
            )
            .field(
                "selected_sandbox",
                &self.selected_sandbox.as_ref().map(|_| "[redacted]"),
            )
            .field("candidate_count", &self.candidate_count)
            .field("eligible_candidate_count", &self.eligible_candidate_count)
            .field("excluded_candidate_count", &self.excluded_candidate_count)
            .field("exclusion_counts", &self.exclusion_counts)
            .field("blocked_action", &self.blocked_action().as_str())
            .field("execution_authorized", &false)
            .field("concrete_route_created", &false)
            .field("scheduler_mutated", &false)
            .field("model_selected", &false)
            .field("distributed_moe_used", &false)
            .field("transport_started", &false)
            .field("persisted", &false)
            .field("audit_event_emitted", &false)
            .finish()
    }
}
