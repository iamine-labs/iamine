#[path = "support/routing_candidate_chain.rs"]
mod routing_candidate_chain;
#[path = "support/routing_policy.rs"]
mod routing_policy;
#[allow(dead_code)]
#[path = "support/sandbox_chain.rs"]
mod sandbox_chain;

use std::{collections::HashSet, error::Error};

use iamine_agent_runtime::{
    PackageReviewSubject, RoutingCandidateAvailability, RoutingCandidateCompatibility,
    RoutingCandidateExclusionReason, RoutingCandidateRef, RoutingCandidateRiskClass,
    RoutingCandidateSandbox, RoutingCandidateSelectionAuthority,
    RoutingCandidateSelectionEvidenceStatus, RoutingCandidateSelectionOutcome,
    RoutingCandidateSelectorErrorCode, RoutingCandidateSelectorRequirement,
    RoutingResourceRequirements, RoutingSelectionRequestRef, RuntimeNetworkAvailability,
    MAX_ROUTING_CANDIDATES, ROUTING_CANDIDATE_EXCLUSION_REASONS,
    ROUTING_CANDIDATE_SELECTION_OUTCOMES, ROUTING_CANDIDATE_SELECTION_SCHEMA_VERSION,
};
use iamine_agents::{PermissionEvaluation, ResourceOperatingMode, ScopeEvaluation};

use routing_candidate_chain::{prepare_routing_candidate, PreparedRoutingCandidate};
use routing_policy::{
    allowed_permission, allowed_scope, clarify_scope, confirmation_permission, refused_permission,
    refused_scope, TASK_TYPE,
};
use sandbox_chain::PackageFixture;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn one_eligible_candidate_is_selected_without_authorizing_execution() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let permission = allowed_permission(&scope)?;
    let candidate = eligible_candidate("candidate-alpha", subject, scope, permission, &prepared);
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let evidence = authority.select(
        request(RoutingCandidateRiskClass::Moderate)?,
        &[candidate],
        &prepared.compatibility_authority,
        &prepared.sandbox_authority,
    )?;

    assert_eq!(
        evidence.schema_version(),
        ROUTING_CANDIDATE_SELECTION_SCHEMA_VERSION
    );
    assert_eq!(
        evidence.status(),
        RoutingCandidateSelectionEvidenceStatus::Established
    );
    assert_eq!(
        evidence.outcome(),
        RoutingCandidateSelectionOutcome::CandidateSelected
    );
    assert_eq!(evidence.selected_candidate_id(), Some("candidate-alpha"));
    assert_eq!(evidence.candidate_count(), 1);
    assert_eq!(evidence.eligible_candidate_count(), 1);
    assert_eq!(evidence.excluded_candidate_count(), 0);
    assert!(authority.verifies(&evidence));
    assert!(evidence.selection_recorded());
    assert_no_side_effects(&evidence);
    Ok(())
}

#[test]
fn multiple_candidates_are_reported_without_arbitrary_tie_breaking() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let permission = allowed_permission(&scope)?;
    let first = eligible_candidate("candidate-alpha", subject, scope, permission, &prepared);
    let second = eligible_candidate("candidate-beta", subject, scope, permission, &prepared);
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    for candidates in [[first, second], [second, first]] {
        let evidence = authority.select(
            request(RoutingCandidateRiskClass::Moderate)?,
            &candidates,
            &prepared.compatibility_authority,
            &prepared.sandbox_authority,
        )?;
        assert_eq!(
            evidence.outcome(),
            RoutingCandidateSelectionOutcome::MultipleCandidates
        );
        assert_eq!(evidence.selected_candidate_id(), None);
        assert_eq!(evidence.eligible_candidate_count(), 2);
        assert_no_side_effects(&evidence);
    }
    Ok(())
}

#[test]
fn empty_candidate_set_returns_no_candidate() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let evidence = authority.select(
        request(RoutingCandidateRiskClass::Low)?,
        &[],
        &prepared.compatibility_authority,
        &prepared.sandbox_authority,
    )?;

    assert_eq!(
        evidence.outcome(),
        RoutingCandidateSelectionOutcome::NoCandidate
    );
    assert_eq!(evidence.candidate_count(), 0);
    assert_eq!(evidence.selected_candidate_id(), None);
    Ok(())
}

#[test]
fn scope_decisions_map_to_handoff_or_fail_closed_blocking() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let permission_scope = allowed_scope()?;
    let permission = allowed_permission(&permission_scope)?;
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let clarify = eligible_candidate(
        "candidate-clarify",
        subject,
        clarify_scope()?,
        permission,
        &prepared,
    );
    let evidence = select_one(&authority, &prepared, clarify)?;
    assert_eq!(
        evidence.outcome(),
        RoutingCandidateSelectionOutcome::HandoffRequired
    );
    assert_eq!(
        evidence.exclusion_count(RoutingCandidateExclusionReason::ScopeMismatch),
        1
    );

    let refused = eligible_candidate(
        "candidate-refused",
        subject,
        refused_scope()?,
        permission,
        &prepared,
    );
    let evidence = select_one(&authority, &prepared, refused)?;
    assert_eq!(
        evidence.outcome(),
        RoutingCandidateSelectionOutcome::Blocked
    );
    assert_no_side_effects(&evidence);
    Ok(())
}

#[test]
fn permission_decisions_map_to_handoff_or_fail_closed_blocking() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let confirmation = eligible_candidate(
        "candidate-confirm",
        subject,
        scope,
        confirmation_permission(&scope)?,
        &prepared,
    );
    let evidence = select_one(&authority, &prepared, confirmation)?;
    assert_eq!(
        evidence.outcome(),
        RoutingCandidateSelectionOutcome::HandoffRequired
    );
    assert_eq!(
        evidence.exclusion_count(RoutingCandidateExclusionReason::PermissionMismatch),
        1
    );

    let refused = eligible_candidate(
        "candidate-refused",
        subject,
        scope,
        refused_permission(&scope)?,
        &prepared,
    );
    let evidence = select_one(&authority, &prepared, refused)?;
    assert_eq!(
        evidence.outcome(),
        RoutingCandidateSelectionOutcome::Blocked
    );
    Ok(())
}

#[test]
fn resource_risk_and_policy_mismatches_are_independent() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let permission = allowed_permission(&scope)?;
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let busy = RoutingCandidateRef::new(
        "candidate-busy",
        TASK_TYPE,
        RoutingCandidateRiskClass::Low,
        RoutingCandidateAvailability::Busy,
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Compatible(&prepared.compatibility_evidence),
        RoutingCandidateSandbox::Prepared(&prepared.sandbox_evidence),
    );
    let evidence = select_one(&authority, &prepared, busy)?;
    assert_reason(
        &evidence,
        RoutingCandidateSelectionOutcome::NoCandidate,
        RoutingCandidateExclusionReason::ResourceMismatch,
    );

    let high_risk = RoutingCandidateRef::new(
        "candidate-risk",
        TASK_TYPE,
        RoutingCandidateRiskClass::High,
        RoutingCandidateAvailability::Available,
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Compatible(&prepared.compatibility_evidence),
        RoutingCandidateSandbox::Prepared(&prepared.sandbox_evidence),
    );
    let evidence = authority.select(
        request(RoutingCandidateRiskClass::Low)?,
        &[high_risk],
        &prepared.compatibility_authority,
        &prepared.sandbox_authority,
    )?;
    assert_reason(
        &evidence,
        RoutingCandidateSelectionOutcome::Blocked,
        RoutingCandidateExclusionReason::RiskTooHigh,
    );

    let candidate =
        eligible_candidate("candidate-resources", subject, scope, permission, &prepared);
    let evidence = authority.select(
        RoutingSelectionRequestRef::new(
            TASK_TYPE,
            ResourceOperatingMode::LocalReadonly,
            RoutingResourceRequirements::new(3, 256, 20, RuntimeNetworkAvailability::None)?,
            RoutingCandidateRiskClass::Moderate,
        ),
        &[candidate],
        &prepared.compatibility_authority,
        &prepared.sandbox_authority,
    )?;
    assert_reason(
        &evidence,
        RoutingCandidateSelectionOutcome::NoCandidate,
        RoutingCandidateExclusionReason::ResourceMismatch,
    );

    let candidate = eligible_candidate("candidate-policy", subject, scope, permission, &prepared);
    let evidence = authority.select(
        RoutingSelectionRequestRef::new(
            TASK_TYPE,
            ResourceOperatingMode::LocalPlanning,
            RoutingResourceRequirements::new(1, 256, 20, RuntimeNetworkAvailability::None)?,
            RoutingCandidateRiskClass::Moderate,
        ),
        &[candidate],
        &prepared.compatibility_authority,
        &prepared.sandbox_authority,
    )?;
    assert_reason(
        &evidence,
        RoutingCandidateSelectionOutcome::Blocked,
        RoutingCandidateExclusionReason::PolicyConflict,
    );
    Ok(())
}

#[test]
fn compatibility_sandbox_and_unknown_metadata_fail_closed() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let permission = allowed_permission(&scope)?;
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let incompatible = candidate_with_state(
        "candidate-incompatible",
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Incompatible,
        RoutingCandidateSandbox::Prepared(&prepared.sandbox_evidence),
    );
    assert_reason(
        &select_one(&authority, &prepared, incompatible)?,
        RoutingCandidateSelectionOutcome::NoCandidate,
        RoutingCandidateExclusionReason::NodeIncompatible,
    );

    let no_sandbox = candidate_with_state(
        "candidate-no-sandbox",
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Compatible(&prepared.compatibility_evidence),
        RoutingCandidateSandbox::Unavailable,
    );
    assert_reason(
        &select_one(&authority, &prepared, no_sandbox)?,
        RoutingCandidateSelectionOutcome::Blocked,
        RoutingCandidateExclusionReason::SandboxUnavailable,
    );

    let unknown = candidate_with_state(
        "candidate-unknown",
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Unknown,
        RoutingCandidateSandbox::Unknown,
    );
    assert_reason(
        &select_one(&authority, &prepared, unknown)?,
        RoutingCandidateSelectionOutcome::Blocked,
        RoutingCandidateExclusionReason::MetadataUnknown,
    );
    Ok(())
}

#[test]
fn evidence_from_other_authorities_is_rejected() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let expected = prepare_routing_candidate(subject)?;
    let foreign = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let permission = allowed_permission(&scope)?;
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let foreign_compatibility = candidate_with_state(
        "candidate-foreign-compatibility",
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Compatible(&foreign.compatibility_evidence),
        RoutingCandidateSandbox::Prepared(&expected.sandbox_evidence),
    );
    let error = authority
        .select(
            request(RoutingCandidateRiskClass::Moderate)?,
            &[foreign_compatibility],
            &expected.compatibility_authority,
            &expected.sandbox_authority,
        )
        .err()
        .ok_or("foreign compatibility evidence unexpectedly accepted")?;
    assert_eq!(
        error.code(),
        RoutingCandidateSelectorErrorCode::RuntimeCompatibilityNotVerified
    );

    let foreign_sandbox = candidate_with_state(
        "candidate-foreign-sandbox",
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Compatible(&expected.compatibility_evidence),
        RoutingCandidateSandbox::Prepared(&foreign.sandbox_evidence),
    );
    let error = authority
        .select(
            request(RoutingCandidateRiskClass::Moderate)?,
            &[foreign_sandbox],
            &expected.compatibility_authority,
            &expected.sandbox_authority,
        )
        .err()
        .ok_or("foreign sandbox evidence unexpectedly accepted")?;
    assert_eq!(
        error.code(),
        RoutingCandidateSelectorErrorCode::SandboxEnforcementNotVerified
    );
    Ok(())
}

#[test]
fn inputs_are_bounded_and_candidate_ids_are_unique() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let permission = allowed_permission(&scope)?;
    let candidate = eligible_candidate("candidate-alpha", subject, scope, permission, &prepared);
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();

    let duplicate_error = authority
        .select(
            request(RoutingCandidateRiskClass::Moderate)?,
            &[candidate, candidate],
            &prepared.compatibility_authority,
            &prepared.sandbox_authority,
        )
        .err()
        .ok_or("duplicate candidate ids unexpectedly accepted")?;
    assert_eq!(
        duplicate_error.code(),
        RoutingCandidateSelectorErrorCode::DuplicateCandidateId
    );
    assert_eq!(
        duplicate_error.requirement(),
        RoutingCandidateSelectorRequirement::DeterministicSelection
    );

    let oversized = [candidate; MAX_ROUTING_CANDIDATES + 1];
    let count_error = authority
        .select(
            request(RoutingCandidateRiskClass::Moderate)?,
            &oversized,
            &prepared.compatibility_authority,
            &prepared.sandbox_authority,
        )
        .err()
        .ok_or("oversized candidate set unexpectedly accepted")?;
    assert_eq!(
        count_error.code(),
        RoutingCandidateSelectorErrorCode::TooManyCandidates
    );

    let invalid = eligible_candidate("../private-path", subject, scope, permission, &prepared);
    let id_error = authority
        .select(
            request(RoutingCandidateRiskClass::Moderate)?,
            &[invalid],
            &prepared.compatibility_authority,
            &prepared.sandbox_authority,
        )
        .err()
        .ok_or("unsafe candidate id unexpectedly accepted")?;
    assert_eq!(
        id_error.code(),
        RoutingCandidateSelectorErrorCode::InvalidCandidateId
    );
    Ok(())
}

#[test]
fn public_contract_is_stable_and_debug_output_is_redacted() -> TestResult {
    assert_eq!(
        ROUTING_CANDIDATE_SELECTION_OUTCOMES
            .iter()
            .map(|value| value.as_str())
            .collect::<HashSet<_>>()
            .len(),
        5
    );
    assert_eq!(
        ROUTING_CANDIDATE_EXCLUSION_REASONS
            .iter()
            .map(|value| value.as_str())
            .collect::<HashSet<_>>()
            .len(),
        8
    );

    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let prepared = prepare_routing_candidate(subject)?;
    let scope = allowed_scope()?;
    let permission = allowed_permission(&scope)?;
    let candidate = eligible_candidate(
        "private-candidate-id",
        subject,
        scope,
        permission,
        &prepared,
    );
    let authority = RoutingCandidateSelectionAuthority::new_operator_local();
    let evidence = authority.select(
        request(RoutingCandidateRiskClass::Moderate)?,
        &[candidate],
        &prepared.compatibility_authority,
        &prepared.sandbox_authority,
    )?;

    for debug in [
        format!("{candidate:?}"),
        format!("{:?}", request(RoutingCandidateRiskClass::Moderate)?),
        format!("{evidence:?}"),
        format!("{authority:?}"),
    ] {
        assert!(!debug.contains("private-candidate-id"));
        assert!(!debug.contains(TASK_TYPE));
        assert!(!debug.contains(fixture.package_id()));
    }
    Ok(())
}

fn request(
    maximum_risk: RoutingCandidateRiskClass,
) -> Result<RoutingSelectionRequestRef<'static>, Box<dyn Error>> {
    Ok(RoutingSelectionRequestRef::new(
        TASK_TYPE,
        ResourceOperatingMode::LocalReadonly,
        RoutingResourceRequirements::new(1, 256, 20, RuntimeNetworkAvailability::None)?,
        maximum_risk,
    ))
}

fn eligible_candidate<'a>(
    candidate_id: &'a str,
    subject: PackageReviewSubject<'a>,
    scope: ScopeEvaluation,
    permission: PermissionEvaluation,
    prepared: &'a PreparedRoutingCandidate<'a>,
) -> RoutingCandidateRef<'a> {
    candidate_with_state(
        candidate_id,
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Compatible(&prepared.compatibility_evidence),
        RoutingCandidateSandbox::Prepared(&prepared.sandbox_evidence),
    )
}

fn candidate_with_state<'a>(
    candidate_id: &'a str,
    subject: PackageReviewSubject<'a>,
    scope: ScopeEvaluation,
    permission: PermissionEvaluation,
    compatibility: RoutingCandidateCompatibility<'a>,
    sandbox: RoutingCandidateSandbox<'a>,
) -> RoutingCandidateRef<'a> {
    RoutingCandidateRef::new(
        candidate_id,
        TASK_TYPE,
        RoutingCandidateRiskClass::Low,
        RoutingCandidateAvailability::Available,
        subject,
        scope,
        permission,
        compatibility,
        sandbox,
    )
}

fn select_one(
    authority: &RoutingCandidateSelectionAuthority,
    prepared: &PreparedRoutingCandidate<'_>,
    candidate: RoutingCandidateRef<'_>,
) -> Result<iamine_agent_runtime::RoutingCandidateSelectionEvidence, Box<dyn Error>> {
    Ok(authority.select(
        request(RoutingCandidateRiskClass::Moderate)?,
        &[candidate],
        &prepared.compatibility_authority,
        &prepared.sandbox_authority,
    )?)
}

fn assert_reason(
    evidence: &iamine_agent_runtime::RoutingCandidateSelectionEvidence,
    outcome: RoutingCandidateSelectionOutcome,
    reason: RoutingCandidateExclusionReason,
) {
    assert_eq!(evidence.outcome(), outcome);
    assert_eq!(evidence.exclusion_count(reason), 1);
    assert_eq!(evidence.excluded_candidate_count(), 1);
}

fn assert_no_side_effects(evidence: &iamine_agent_runtime::RoutingCandidateSelectionEvidence) {
    assert!(!evidence.execution_authorized());
    assert!(!evidence.concrete_route_created());
    assert!(!evidence.scheduler_mutated());
    assert!(!evidence.model_selected());
    assert!(!evidence.distributed_moe_used());
    assert!(!evidence.transport_started());
    assert!(!evidence.persisted());
    assert!(!evidence.audit_event_emitted());
}
