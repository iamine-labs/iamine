#[allow(dead_code)]
#[path = "support/routing_policy.rs"]
mod routing_policy;
#[allow(dead_code)]
mod support;

use std::{collections::HashSet, error::Error};

use iamine_agent_runtime::{
    inspect_runtime_foundation, AuditEventEnforcementAuthority, AuditEventEnforcementBlockedAction,
    AuditEventEnforcementError, AuditEventEnforcementErrorCode, AuditEventEnforcementEvidence,
    AuditEventEnforcementEvidenceStatus, AuditEventEnforcementRequirement, DeclaredAgentPackage,
    ExecutionLifecycleAuthority, ExecutionLifecycleState, RuntimeFoundationStatus, RuntimeOwner,
    RuntimeOwnerState, AUDIT_EVENT_ENFORCEMENT_SCHEMA_VERSION,
};
use iamine_agents::{
    assess_package_load_yaml, parse_and_validate_yaml, AuditEventClass, AuditEventSource,
    AuditLifecycleState, AuditOutcome, PackageLoadBlockerCode, PackageLoadStatus,
    MAX_AUDIT_EVENTS_PER_PROJECTION,
};

use routing_policy::{
    allowed_permission, allowed_scope, confirmation_permission, refused_permission, refused_scope,
    PACKAGE_ID,
};
use support::sandbox_chain::{prepare_sandbox, PackageFixture, VALID_MANIFEST};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn typed_scope_and_permission_projections_remain_bounded_and_unbound() -> TestResult {
    let authority = AuditEventEnforcementAuthority::new_operator_local();
    let allowed_scope = allowed_scope()?;
    let refused_scope = refused_scope()?;
    let allowed_permission = allowed_permission(&allowed_scope)?;
    let confirmation_permission = confirmation_permission(&allowed_scope)?;
    let refused_permission = refused_permission(&allowed_scope)?;

    let cases = [
        (
            authority.enforce_scope(&allowed_scope),
            AuditEventSource::Scope,
            AuditOutcome::Allowed,
            1,
        ),
        (
            authority.enforce_scope(&refused_scope),
            AuditEventSource::Scope,
            AuditOutcome::Refused,
            2,
        ),
        (
            authority.enforce_permission(&allowed_permission),
            AuditEventSource::Permission,
            AuditOutcome::Allowed,
            1,
        ),
        (
            authority.enforce_permission(&confirmation_permission),
            AuditEventSource::Permission,
            AuditOutcome::ConfirmationRequired,
            1,
        ),
        (
            authority.enforce_permission(&refused_permission),
            AuditEventSource::Permission,
            AuditOutcome::Refused,
            2,
        ),
    ];

    for (evidence, source, outcome, event_count) in cases {
        assert_eq!(evidence.source(), source);
        assert_eq!(evidence.outcome(), outcome);
        assert_eq!(evidence.event_count(), event_count);
        assert!(authority.verifies(&evidence));
        assert!(!evidence.upstream_authority_bound());
        assert_eq!(evidence.lifecycle_revision(), None);
        assert!(evidence.event_count() <= MAX_AUDIT_EVENTS_PER_PROJECTION);
        assert_safe_evidence(&evidence);
    }
    Ok(())
}

#[test]
fn refusal_projection_preserves_check_before_refusal() -> TestResult {
    let authority = AuditEventEnforcementAuthority::new_operator_local();
    let evidence = authority.enforce_scope(&refused_scope()?);
    let events = evidence.events();

    assert_eq!(events.len(), 2);
    assert_eq!(events.primary().class(), AuditEventClass::ScopeChecked);
    assert_eq!(
        events.secondary().map(|event| event.class()),
        Some(AuditEventClass::RefusalRecorded)
    );
    assert_eq!(events.primary().outcome(), AuditOutcome::Refused);
    assert_eq!(
        events.secondary().map(|event| event.outcome()),
        Some(AuditOutcome::Refused)
    );
    Ok(())
}

#[test]
fn lifecycle_projection_is_bound_to_authority_execution_and_revision() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let audit = AuditEventEnforcementAuthority::new_operator_local();

    let evidence = audit.enforce_lifecycle(&lifecycle, &record)?;

    assert_eq!(
        evidence.schema_version(),
        AUDIT_EVENT_ENFORCEMENT_SCHEMA_VERSION
    );
    assert_eq!(
        evidence.status(),
        AuditEventEnforcementEvidenceStatus::Established
    );
    assert_eq!(evidence.source(), AuditEventSource::Lifecycle);
    assert_eq!(evidence.outcome(), AuditOutcome::Observed);
    assert_eq!(evidence.event_count(), 1);
    assert_eq!(
        evidence.events().primary().lifecycle_state(),
        Some(AuditLifecycleState::Queued)
    );
    assert_eq!(evidence.lifecycle_revision(), Some(0));
    assert!(evidence.upstream_authority_bound());
    assert!(audit.verifies_lifecycle(&evidence, &lifecycle, &record));
    assert_safe_evidence(&evidence);
    Ok(())
}

#[test]
fn lifecycle_handoff_preserves_the_bounded_secondary_event() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let _ = lifecycle.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    let _ = lifecycle.transition(&mut record, 1, ExecutionLifecycleState::ScopeCheck)?;
    let _ = lifecycle.transition(&mut record, 2, ExecutionLifecycleState::HandoffRequired)?;
    let audit = AuditEventEnforcementAuthority::new_operator_local();

    let evidence = audit.enforce_lifecycle(&lifecycle, &record)?;

    assert_eq!(evidence.event_count(), 2);
    assert_eq!(evidence.lifecycle_revision(), Some(3));
    assert_eq!(
        evidence.events().primary().lifecycle_state(),
        Some(AuditLifecycleState::HandoffRequired)
    );
    assert_eq!(
        evidence.events().secondary().map(|event| event.class()),
        Some(AuditEventClass::HandoffRequired)
    );
    assert!(audit.verifies_lifecycle(&evidence, &lifecycle, &record));
    Ok(())
}

#[test]
fn foreign_lifecycle_authority_is_rejected_without_evidence() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let foreign = ExecutionLifecycleAuthority::new_operator_local();
    let audit = AuditEventEnforcementAuthority::new_operator_local();

    assert_error(
        audit.enforce_lifecycle(&foreign, &record),
        AuditEventEnforcementErrorCode::LifecycleRecordNotVerified,
        AuditEventEnforcementRequirement::LifecycleAuthority,
    )
}

#[test]
fn lifecycle_evidence_cannot_be_replayed_after_revision_or_execution_changes() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let other_record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let audit = AuditEventEnforcementAuthority::new_operator_local();
    let evidence = audit.enforce_lifecycle(&lifecycle, &record)?;

    assert!(!audit.verifies_lifecycle(&evidence, &lifecycle, &other_record));
    let _ = lifecycle.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    assert!(audit.verifies(&evidence));
    assert!(!audit.verifies_lifecycle(&evidence, &lifecycle, &record));
    Ok(())
}

#[test]
fn audit_authorities_are_isolated_and_debug_output_is_redacted() -> TestResult {
    let authority = AuditEventEnforcementAuthority::new_operator_local();
    let foreign = AuditEventEnforcementAuthority::new_operator_local();
    let evidence = authority.enforce_scope(&allowed_scope()?);
    let debug = format!("{evidence:?}");

    assert!(authority.verifies(&evidence));
    assert!(!foreign.verifies(&evidence));
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains(PACKAGE_ID));
    for forbidden in [
        "username",
        "hostname",
        "home/",
        "private_key",
        "wallet",
        "prompt",
        "output",
    ] {
        assert!(!debug.contains(forbidden));
    }
    Ok(())
}

#[test]
fn requirements_distinguish_typed_and_authority_bound_sources() -> TestResult {
    let audit = AuditEventEnforcementAuthority::new_operator_local();
    let typed = audit.enforce_scope(&allowed_scope()?);

    assert_eq!(
        typed.requirements(),
        [
            AuditEventEnforcementRequirement::AuditAuthority,
            AuditEventEnforcementRequirement::TypedProjection,
            AuditEventEnforcementRequirement::BoundedEventSet,
        ]
    );

    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let bound = audit.enforce_lifecycle(&lifecycle, &record)?;

    assert_eq!(
        bound.requirements(),
        [
            AuditEventEnforcementRequirement::AuditAuthority,
            AuditEventEnforcementRequirement::TypedProjection,
            AuditEventEnforcementRequirement::BoundedEventSet,
            AuditEventEnforcementRequirement::LifecycleAuthority,
            AuditEventEnforcementRequirement::ExecutionIdentity,
        ]
    );
    Ok(())
}

#[test]
fn audit_evidence_never_claims_authorization_persistence_or_side_effects() -> TestResult {
    let audit = AuditEventEnforcementAuthority::new_operator_local();
    let scope = audit.enforce_scope(&allowed_scope()?);
    let permission = audit.enforce_permission(&allowed_permission(&allowed_scope()?)?);

    for evidence in [&scope, &permission] {
        assert_safe_evidence(evidence);
        assert_eq!(
            evidence.blocked_action(),
            AuditEventEnforcementBlockedAction::TreatAsExecutionAuthorization
        );
    }
    Ok(())
}

#[test]
fn runtime_and_package_load_boundaries_remain_closed() -> TestResult {
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let runtime = inspect_runtime_foundation(DeclaredAgentPackage::from_manifest(&manifest));
    let audit_owner = runtime
        .owner_statuses()
        .iter()
        .find(|status| status.owner() == RuntimeOwner::AuditEventEnforcement)
        .ok_or("audit event enforcement owner must remain explicit")?;
    let package = assess_package_load_yaml(VALID_MANIFEST)?;
    let blockers = package.blockers().iter().copied().collect::<HashSet<_>>();

    assert_eq!(runtime.status(), RuntimeFoundationStatus::Blocked);
    assert_eq!(audit_owner.state(), RuntimeOwnerState::Unavailable);
    assert_eq!(package.status(), PackageLoadStatus::Blocked);
    assert!(!package.load_allowed());
    assert!(blockers.contains(&PackageLoadBlockerCode::AuditEventEnforcementUnavailable));
    assert!(blockers.contains(&PackageLoadBlockerCode::ExecutionAuthorizationUnavailable));
    Ok(())
}

fn assert_safe_evidence(evidence: &AuditEventEnforcementEvidence) {
    assert!(evidence.event_recorded());
    assert!(!evidence.execution_authorized());
    assert!(!evidence.side_effect_verified());
    assert!(!evidence.package_loaded());
    assert!(!evidence.runtime_active());
    assert!(!evidence.transport_started());
    assert!(!evidence.persisted());
    assert!(!evidence.external_event_emitted());
}

fn assert_error(
    result: Result<AuditEventEnforcementEvidence, AuditEventEnforcementError>,
    code: AuditEventEnforcementErrorCode,
    requirement: AuditEventEnforcementRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("expected audit event enforcement error")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    Ok(())
}
