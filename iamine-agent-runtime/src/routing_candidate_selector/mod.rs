mod authority;
mod error;
mod evaluation;
mod evidence;
mod policy;

pub use authority::RoutingCandidateSelectionAuthority;
pub(crate) use authority::RoutingCandidateSelectionAuthorityIdentity;
pub use error::{
    RoutingCandidateSelectorError, RoutingCandidateSelectorErrorCode,
    RoutingCandidateSelectorRequirement,
};
pub use evidence::{
    RoutingCandidateSelectionEvidence, RoutingCandidateSelectionEvidenceStatus,
    ROUTING_CANDIDATE_SELECTION_SCHEMA_VERSION,
};
pub use policy::{
    RoutingCandidateAvailability, RoutingCandidateCompatibility, RoutingCandidateExclusionReason,
    RoutingCandidateRef, RoutingCandidateRiskClass, RoutingCandidateSandbox,
    RoutingCandidateSelectionOutcome, RoutingResourceRequirements, RoutingSelectionBlockedAction,
    RoutingSelectionRequestRef, MAX_ROUTING_CANDIDATES, MAX_ROUTING_CANDIDATE_ID_BYTES,
    MAX_ROUTING_TASK_TYPE_BYTES, ROUTING_CANDIDATE_EXCLUSION_REASONS,
    ROUTING_CANDIDATE_SELECTION_OUTCOMES,
};
