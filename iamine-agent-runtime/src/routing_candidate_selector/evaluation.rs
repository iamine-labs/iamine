use std::{collections::HashSet, sync::Arc};

use iamine_agents::{PermissionDecision, ScopeDecision};

use crate::{
    sandbox_enforcement::SandboxEvidenceIdentity, RuntimeCompatibilityAuthority,
    RuntimeNetworkAvailability, SandboxEnforcementAuthority,
};

use super::{
    RoutingCandidateAvailability, RoutingCandidateCompatibility, RoutingCandidateExclusionReason,
    RoutingCandidateRef, RoutingCandidateRiskClass, RoutingCandidateSandbox,
    RoutingCandidateSelectionOutcome, RoutingCandidateSelectorError,
    RoutingCandidateSelectorErrorCode, RoutingCandidateSelectorRequirement,
    RoutingSelectionRequestRef, MAX_ROUTING_CANDIDATES, MAX_ROUTING_CANDIDATE_ID_BYTES,
    MAX_ROUTING_TASK_TYPE_BYTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExclusionDisposition {
    NoCandidate,
    Handoff,
    Blocked,
}

pub(crate) struct RoutingSelectionResult {
    pub(crate) outcome: RoutingCandidateSelectionOutcome,
    pub(crate) selected_candidate_id: Option<String>,
    pub(crate) selected_sandbox: Option<Arc<SandboxEvidenceIdentity>>,
    pub(crate) candidate_count: u16,
    pub(crate) eligible_candidate_count: u16,
    pub(crate) excluded_candidate_count: u16,
    pub(crate) exclusion_counts: [u16; 8],
}

pub(crate) fn evaluate_candidates(
    request: RoutingSelectionRequestRef<'_>,
    candidates: &[RoutingCandidateRef<'_>],
    compatibility_authority: &RuntimeCompatibilityAuthority,
    sandbox_authority: &SandboxEnforcementAuthority,
) -> Result<RoutingSelectionResult, RoutingCandidateSelectorError> {
    validate_request(request)?;
    if candidates.len() > MAX_ROUTING_CANDIDATES {
        return Err(error(
            RoutingCandidateSelectorErrorCode::TooManyCandidates,
            RoutingCandidateSelectorRequirement::CandidateCount,
        ));
    }

    let mut candidate_ids = HashSet::with_capacity(candidates.len());
    for candidate in candidates {
        validate_candidate(*candidate, &mut candidate_ids)?;
        validate_evidence(*candidate, compatibility_authority, sandbox_authority)?;
    }

    let mut selected_candidate_id = None;
    let mut selected_sandbox = None;
    let mut eligible_candidate_count = 0_u16;
    let mut excluded_candidate_count = 0_u16;
    let mut exclusion_counts = [0_u16; 8];
    let mut handoff_seen = false;
    let mut blocked_seen = false;

    for candidate in candidates {
        match exclusion_for(request, *candidate) {
            Some((reason, disposition)) => {
                excluded_candidate_count += 1;
                exclusion_counts[reason.index()] += 1;
                handoff_seen |= disposition == ExclusionDisposition::Handoff;
                blocked_seen |= disposition == ExclusionDisposition::Blocked;
            }
            None => {
                eligible_candidate_count += 1;
                if eligible_candidate_count == 1 {
                    selected_candidate_id = Some(candidate.candidate_id().to_string());
                    selected_sandbox = match candidate.sandbox() {
                        RoutingCandidateSandbox::Prepared(evidence) => {
                            Some(Arc::clone(evidence.identity()))
                        }
                        RoutingCandidateSandbox::Unavailable | RoutingCandidateSandbox::Unknown => {
                            None
                        }
                    };
                } else {
                    selected_candidate_id = None;
                    selected_sandbox = None;
                }
            }
        }
    }

    let outcome = match eligible_candidate_count {
        1 => RoutingCandidateSelectionOutcome::CandidateSelected,
        count if count > 1 => RoutingCandidateSelectionOutcome::MultipleCandidates,
        _ if blocked_seen => RoutingCandidateSelectionOutcome::Blocked,
        _ if handoff_seen => RoutingCandidateSelectionOutcome::HandoffRequired,
        _ => RoutingCandidateSelectionOutcome::NoCandidate,
    };

    Ok(RoutingSelectionResult {
        outcome,
        selected_candidate_id,
        selected_sandbox,
        candidate_count: candidates.len() as u16,
        eligible_candidate_count,
        excluded_candidate_count,
        exclusion_counts,
    })
}

fn validate_request(
    request: RoutingSelectionRequestRef<'_>,
) -> Result<(), RoutingCandidateSelectorError> {
    if request.task_type().is_empty() {
        return Err(error(
            RoutingCandidateSelectorErrorCode::EmptyTaskType,
            RoutingCandidateSelectorRequirement::TaskType,
        ));
    }
    if !is_bounded_identifier(request.task_type(), MAX_ROUTING_TASK_TYPE_BYTES) {
        return Err(error(
            RoutingCandidateSelectorErrorCode::InvalidTaskType,
            RoutingCandidateSelectorRequirement::TaskType,
        ));
    }
    Ok(())
}

fn validate_candidate<'a>(
    candidate: RoutingCandidateRef<'a>,
    candidate_ids: &mut HashSet<&'a str>,
) -> Result<(), RoutingCandidateSelectorError> {
    if candidate.candidate_id().is_empty() {
        return Err(error(
            RoutingCandidateSelectorErrorCode::EmptyCandidateId,
            RoutingCandidateSelectorRequirement::CandidateIdentity,
        ));
    }
    if !is_bounded_identifier(candidate.candidate_id(), MAX_ROUTING_CANDIDATE_ID_BYTES) {
        return Err(error(
            RoutingCandidateSelectorErrorCode::InvalidCandidateId,
            RoutingCandidateSelectorRequirement::CandidateIdentity,
        ));
    }
    if !is_bounded_identifier(candidate.task_type(), MAX_ROUTING_TASK_TYPE_BYTES) {
        return Err(error(
            RoutingCandidateSelectorErrorCode::InvalidCandidateTaskType,
            RoutingCandidateSelectorRequirement::TaskType,
        ));
    }
    if !candidate_ids.insert(candidate.candidate_id()) {
        return Err(error(
            RoutingCandidateSelectorErrorCode::DuplicateCandidateId,
            RoutingCandidateSelectorRequirement::DeterministicSelection,
        ));
    }
    Ok(())
}

fn validate_evidence(
    candidate: RoutingCandidateRef<'_>,
    compatibility_authority: &RuntimeCompatibilityAuthority,
    sandbox_authority: &SandboxEnforcementAuthority,
) -> Result<(), RoutingCandidateSelectorError> {
    if let RoutingCandidateCompatibility::Compatible(evidence) = candidate.compatibility() {
        if !compatibility_authority.verifies(evidence, candidate.subject()) {
            return Err(error(
                RoutingCandidateSelectorErrorCode::RuntimeCompatibilityNotVerified,
                RoutingCandidateSelectorRequirement::RuntimeCompatibilityEvidence,
            ));
        }
    }
    if let RoutingCandidateSandbox::Prepared(evidence) = candidate.sandbox() {
        if !sandbox_authority.verifies(evidence, candidate.subject()) {
            return Err(error(
                RoutingCandidateSelectorErrorCode::SandboxEnforcementNotVerified,
                RoutingCandidateSelectorRequirement::SandboxEnforcementEvidence,
            ));
        }
    }
    Ok(())
}

fn exclusion_for(
    request: RoutingSelectionRequestRef<'_>,
    candidate: RoutingCandidateRef<'_>,
) -> Option<(RoutingCandidateExclusionReason, ExclusionDisposition)> {
    if candidate.task_type() != request.task_type() {
        return Some((
            RoutingCandidateExclusionReason::ScopeMismatch,
            ExclusionDisposition::NoCandidate,
        ));
    }

    match candidate.scope().decision() {
        ScopeDecision::Allow => {}
        ScopeDecision::Clarify | ScopeDecision::HandoffToOrchestrator => {
            return Some((
                RoutingCandidateExclusionReason::ScopeMismatch,
                ExclusionDisposition::Handoff,
            ));
        }
        ScopeDecision::Refuse => {
            return Some((
                RoutingCandidateExclusionReason::ScopeMismatch,
                ExclusionDisposition::Blocked,
            ));
        }
        _ => {
            return Some((
                RoutingCandidateExclusionReason::MetadataUnknown,
                ExclusionDisposition::Blocked,
            ));
        }
    }

    match candidate.permission().decision() {
        PermissionDecision::Allow => {}
        PermissionDecision::RequireConfirmation | PermissionDecision::HandoffToOrchestrator => {
            return Some((
                RoutingCandidateExclusionReason::PermissionMismatch,
                ExclusionDisposition::Handoff,
            ));
        }
        PermissionDecision::Refuse => {
            return Some((
                RoutingCandidateExclusionReason::PermissionMismatch,
                ExclusionDisposition::Blocked,
            ));
        }
        _ => {
            return Some((
                RoutingCandidateExclusionReason::MetadataUnknown,
                ExclusionDisposition::Blocked,
            ));
        }
    }

    if candidate.risk_class() == RoutingCandidateRiskClass::Prohibited
        || candidate.risk_class().rank() > request.maximum_risk().rank()
    {
        return Some((
            RoutingCandidateExclusionReason::RiskTooHigh,
            ExclusionDisposition::Blocked,
        ));
    }

    match candidate.availability() {
        RoutingCandidateAvailability::Available => {}
        RoutingCandidateAvailability::Busy | RoutingCandidateAvailability::Unavailable => {
            return Some((
                RoutingCandidateExclusionReason::ResourceMismatch,
                ExclusionDisposition::NoCandidate,
            ));
        }
        RoutingCandidateAvailability::Unknown => {
            return Some((
                RoutingCandidateExclusionReason::MetadataUnknown,
                ExclusionDisposition::Blocked,
            ));
        }
    }

    let compatibility = match candidate.compatibility() {
        RoutingCandidateCompatibility::Compatible(evidence) => evidence,
        RoutingCandidateCompatibility::Incompatible => {
            return Some((
                RoutingCandidateExclusionReason::NodeIncompatible,
                ExclusionDisposition::NoCandidate,
            ));
        }
        RoutingCandidateCompatibility::Unknown => {
            return Some((
                RoutingCandidateExclusionReason::MetadataUnknown,
                ExclusionDisposition::Blocked,
            ));
        }
    };

    if compatibility.operating_mode() != request.operating_mode() {
        return Some((
            RoutingCandidateExclusionReason::PolicyConflict,
            ExclusionDisposition::Blocked,
        ));
    }
    if !resources_satisfy(compatibility.resources(), request.resources()) {
        return Some((
            RoutingCandidateExclusionReason::ResourceMismatch,
            ExclusionDisposition::NoCandidate,
        ));
    }

    match candidate.sandbox() {
        RoutingCandidateSandbox::Prepared(_) => None,
        RoutingCandidateSandbox::Unavailable => Some((
            RoutingCandidateExclusionReason::SandboxUnavailable,
            ExclusionDisposition::Blocked,
        )),
        RoutingCandidateSandbox::Unknown => Some((
            RoutingCandidateExclusionReason::MetadataUnknown,
            ExclusionDisposition::Blocked,
        )),
    }
}

fn resources_satisfy(
    available: crate::RuntimeResourceEnvelope,
    required: super::RoutingResourceRequirements,
) -> bool {
    available.logical_cores() >= required.logical_cores()
        && available.memory_limit_mb() >= required.memory_mb()
        && available.storage_limit_mb() >= required.storage_mb()
        && network_satisfies(available.network(), required.network())
}

fn network_satisfies(
    available: RuntimeNetworkAvailability,
    required: RuntimeNetworkAvailability,
) -> bool {
    match required {
        RuntimeNetworkAvailability::None => true,
        RuntimeNetworkAvailability::LocalOnly => matches!(
            available,
            RuntimeNetworkAvailability::LocalOnly | RuntimeNetworkAvailability::LanReadonly
        ),
        RuntimeNetworkAvailability::LanReadonly => {
            matches!(available, RuntimeNetworkAvailability::LanReadonly)
        }
    }
}

fn is_bounded_identifier(value: &str, maximum_bytes: usize) -> bool {
    value.len() <= maximum_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

const fn error(
    code: RoutingCandidateSelectorErrorCode,
    requirement: RoutingCandidateSelectorRequirement,
) -> RoutingCandidateSelectorError {
    RoutingCandidateSelectorError::new(code, requirement)
}
