mod support;

use std::{collections::HashSet, error::Error};

use iamine_agent_runtime::{
    inspect_runtime_foundation, DeclaredAgentPackage, ExecutionLifecycleAuthority,
    ExecutionLifecycleRecord, ExecutionLifecycleState, ExecutionLifecycleTransitionEvidence,
    HandoffBlockedAction, HandoffDispatchEvidenceStatus, HandoffEnforcementAuthority, HandoffError,
    HandoffErrorCode, HandoffReason, HandoffRequest, HandoffRequirement, HandoffTarget,
    RuntimeFoundationStatus, RuntimeOwner, RuntimeOwnerState, HANDOFF_DISPATCH_SCHEMA_VERSION,
    HANDOFF_REASONS, HANDOFF_TARGETS,
};
use iamine_agents::{assess_package_load_yaml, parse_and_validate_yaml, PackageLoadStatus};

use support::sandbox_chain::{prepare_sandbox, PackageFixture, VALID_MANIFEST};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn advance_to_handoff(
    lifecycle: &ExecutionLifecycleAuthority,
    record: &mut ExecutionLifecycleRecord<'_>,
) -> TestResult<ExecutionLifecycleTransitionEvidence> {
    let _ = lifecycle.transition(record, 0, ExecutionLifecycleState::PermissionPending)?;
    let _ = lifecycle.transition(record, 1, ExecutionLifecycleState::ScopeCheck)?;
    Ok(lifecycle.transition(record, 2, ExecutionLifecycleState::HandoffRequired)?)
}

#[test]
fn canonical_targets_reasons_and_safe_summaries_are_exact() {
    assert_eq!(
        HANDOFF_TARGETS.map(HandoffTarget::as_str),
        [
            "operator",
            "orchestrator",
            "specialized_agent",
            "architecture_review",
            "security_review",
            "qa_review",
            "blocked_state",
        ]
    );
    assert_eq!(
        HANDOFF_REASONS.map(HandoffReason::as_str),
        [
            "out_of_scope",
            "permission_missing",
            "risk_too_high",
            "input_ambiguous",
            "output_requires_review",
            "sandbox_unavailable",
            "timeout_or_cancelled",
            "policy_conflict",
        ]
    );

    let summaries = HANDOFF_REASONS
        .map(HandoffReason::operator_summary)
        .map(|summary| summary.as_str())
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(summaries.len(), HANDOFF_REASONS.len());
    assert!(summaries.iter().all(|summary| !summary.is_empty()));

    let request = HandoffRequest::new(HandoffTarget::Orchestrator, HandoffReason::OutOfScope);
    assert_eq!(
        request.blocked_action(),
        HandoffBlockedAction::ContinueLocalExecution
    );
    assert_eq!(
        request.blocked_action().as_str(),
        "continue_local_execution"
    );
}

#[test]
fn prepare_requires_the_exact_handoff_transition_and_execution() -> TestResult {
    let fixture_a = PackageFixture::valid()?;
    let references_a = fixture_a.resolve()?;
    let subject_a = fixture_a.subject(&references_a);
    let sandbox_a = prepare_sandbox(subject_a)?;
    let fixture_b = PackageFixture::valid()?;
    let references_b = fixture_b.resolve()?;
    let subject_b = fixture_b.subject(&references_b);
    let sandbox_b = prepare_sandbox(subject_b)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record_a = lifecycle.queue(&sandbox_a.authority, &sandbox_a.evidence, subject_a)?;
    let mut record_b = lifecycle.queue(&sandbox_b.authority, &sandbox_b.evidence, subject_b)?;
    let handoff_a = advance_to_handoff(&lifecycle, &mut record_a)?;
    let handoff_b = advance_to_handoff(&lifecycle, &mut record_b)?;
    let authority = HandoffEnforcementAuthority::new_operator_local();
    let request = HandoffRequest::new(HandoffTarget::Orchestrator, HandoffReason::OutOfScope);

    assert_handoff_error(
        authority.prepare(&lifecycle, &record_b, &handoff_a, request),
        HandoffErrorCode::HandoffTransitionNotVerified,
        HandoffRequirement::HandoffTransitionEvidence,
    )?;
    let control = authority.prepare(&lifecycle, &record_b, &handoff_b, request)?;
    assert!(authority.verifies_control(&control, &lifecycle, &record_b));
    assert!(!authority.verifies_control(
        &control,
        &ExecutionLifecycleAuthority::new_operator_local(),
        &record_b
    ));
    Ok(())
}

#[test]
fn prepare_requires_the_current_handoff_state_without_mutation() -> TestResult {
    let source_fixture = PackageFixture::valid()?;
    let source_references = source_fixture.resolve()?;
    let source_subject = source_fixture.subject(&source_references);
    let source_sandbox = prepare_sandbox(source_subject)?;
    let target_fixture = PackageFixture::valid()?;
    let target_references = target_fixture.resolve()?;
    let target_subject = target_fixture.subject(&target_references);
    let target_sandbox = prepare_sandbox(target_subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut source_record = lifecycle.queue(
        &source_sandbox.authority,
        &source_sandbox.evidence,
        source_subject,
    )?;
    let handoff = advance_to_handoff(&lifecycle, &mut source_record)?;
    let target_record = lifecycle.queue(
        &target_sandbox.authority,
        &target_sandbox.evidence,
        target_subject,
    )?;
    let authority = HandoffEnforcementAuthority::new_operator_local();

    assert_handoff_error(
        authority.prepare(
            &lifecycle,
            &target_record,
            &handoff,
            HandoffRequest::new(HandoffTarget::Operator, HandoffReason::InputAmbiguous),
        ),
        HandoffErrorCode::HandoffStateRequired,
        HandoffRequirement::HandoffRequiredState,
    )?;
    assert_eq!(target_record.state(), ExecutionLifecycleState::Queued);
    assert_eq!(target_record.revision(), 0);
    Ok(())
}

#[test]
fn elevated_risk_and_output_review_require_explicit_review_targets() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let handoff = advance_to_handoff(&lifecycle, &mut record)?;
    let authority = HandoffEnforcementAuthority::new_operator_local();

    for reason in [
        HandoffReason::RiskTooHigh,
        HandoffReason::OutputRequiresReview,
    ] {
        assert_handoff_error(
            authority.prepare(
                &lifecycle,
                &record,
                &handoff,
                HandoffRequest::new(HandoffTarget::Orchestrator, reason),
            ),
            HandoffErrorCode::TargetReasonMismatch,
            HandoffRequirement::CompatibleTargetReason,
        )?;
        assert_eq!(record.state(), ExecutionLifecycleState::HandoffRequired);
        assert_eq!(record.revision(), 3);
    }

    let review = authority.prepare(
        &lifecycle,
        &record,
        &handoff,
        HandoffRequest::new(HandoffTarget::SecurityReview, HandoffReason::RiskTooHigh),
    )?;
    assert_eq!(review.target(), HandoffTarget::SecurityReview);
    Ok(())
}

#[test]
fn dispatch_records_cancelled_without_transport_or_permission_expansion() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let handoff = advance_to_handoff(&lifecycle, &mut record)?;
    let authority = HandoffEnforcementAuthority::new_operator_local();
    let control = authority.prepare(
        &lifecycle,
        &record,
        &handoff,
        HandoffRequest::new(HandoffTarget::Orchestrator, HandoffReason::OutOfScope),
    )?;

    assert!(control.prepared());
    assert!(!control.dispatch_recorded());
    assert!(!control.transport_performed());
    assert!(!control.concrete_target_selected());
    assert!(!control.scope_expanded());
    assert!(!control.permissions_expanded());
    assert!(!control.execution_authorized());
    assert!(!control.runtime_active());
    assert!(!control.persisted());

    let evidence = authority.dispatch(&control, &lifecycle, &mut record)?;
    assert_eq!(evidence.schema_version(), HANDOFF_DISPATCH_SCHEMA_VERSION);
    assert_eq!(evidence.status(), HandoffDispatchEvidenceStatus::Recorded);
    assert_eq!(evidence.target(), HandoffTarget::Orchestrator);
    assert_eq!(evidence.reason(), HandoffReason::OutOfScope);
    assert_eq!(
        evidence.blocked_action(),
        HandoffBlockedAction::ContinueLocalExecution
    );
    assert_eq!(
        evidence.terminal_state(),
        ExecutionLifecycleState::Cancelled
    );
    assert_eq!(record.state(), ExecutionLifecycleState::Cancelled);
    assert_eq!(record.revision(), 4);
    assert!(evidence.dispatch_recorded());
    assert!(evidence.local_execution_cancelled());
    assert!(!evidence.transport_performed());
    assert!(!evidence.concrete_target_selected());
    assert!(!evidence.target_execution_started());
    assert!(!evidence.human_approval_completed());
    assert!(!evidence.scope_expanded());
    assert!(!evidence.permissions_expanded());
    assert!(!evidence.execution_authorized());
    assert!(!evidence.runtime_active());
    assert!(!evidence.persisted());
    assert!(!evidence.audit_emitted());
    assert!(authority.verifies_dispatch(&evidence, &control, &lifecycle, &record));
    Ok(())
}

#[test]
fn every_target_class_remains_non_executable_and_non_routing() -> TestResult {
    for target in HANDOFF_TARGETS {
        let fixture = PackageFixture::valid()?;
        let references = fixture.resolve()?;
        let subject = fixture.subject(&references);
        let sandbox = prepare_sandbox(subject)?;
        let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
        let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
        let handoff = advance_to_handoff(&lifecycle, &mut record)?;
        let authority = HandoffEnforcementAuthority::new_operator_local();
        let control = authority.prepare(
            &lifecycle,
            &record,
            &handoff,
            HandoffRequest::new(target, HandoffReason::PolicyConflict),
        )?;
        let evidence = authority.dispatch(&control, &lifecycle, &mut record)?;

        assert_eq!(evidence.target(), target);
        assert!(!evidence.concrete_target_selected());
        assert!(!evidence.target_execution_started());
        assert!(!evidence.transport_performed());
        assert!(!evidence.execution_authorized());
    }
    Ok(())
}

#[test]
fn foreign_authority_and_replayed_control_fail_without_second_dispatch() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let handoff = advance_to_handoff(&lifecycle, &mut record)?;
    let authority = HandoffEnforcementAuthority::new_operator_local();
    let foreign = HandoffEnforcementAuthority::new_operator_local();
    let control = authority.prepare(
        &lifecycle,
        &record,
        &handoff,
        HandoffRequest::new(HandoffTarget::Operator, HandoffReason::InputAmbiguous),
    )?;

    assert_handoff_error(
        foreign.dispatch(&control, &lifecycle, &mut record),
        HandoffErrorCode::ForeignAuthority,
        HandoffRequirement::HandoffAuthority,
    )?;
    assert_eq!(record.state(), ExecutionLifecycleState::HandoffRequired);
    assert_eq!(record.revision(), 3);

    let _ = authority.dispatch(&control, &lifecycle, &mut record)?;
    assert_handoff_error(
        authority.dispatch(&control, &lifecycle, &mut record),
        HandoffErrorCode::StaleRevision,
        HandoffRequirement::CurrentRevision,
    )?;
    assert_eq!(record.state(), ExecutionLifecycleState::Cancelled);
    assert_eq!(record.revision(), 4);
    Ok(())
}

#[test]
fn debug_and_errors_do_not_expose_package_or_execution_identity() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let package_id = fixture.package_id().to_owned();
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let handoff = advance_to_handoff(&lifecycle, &mut record)?;
    let authority = HandoffEnforcementAuthority::new_operator_local();
    let control = authority.prepare(
        &lifecycle,
        &record,
        &handoff,
        HandoffRequest::new(HandoffTarget::QaReview, HandoffReason::OutputRequiresReview),
    )?;
    let evidence = authority.dispatch(&control, &lifecycle, &mut record)?;

    for output in [
        format!("{authority:?}"),
        format!("{control:?}"),
        format!("{evidence:?}"),
    ] {
        assert!(output.contains("[redacted]"));
        assert!(!output.contains(&package_id));
        assert!(!output.contains("node-doctor"));
    }

    let error = HandoffEnforcementAuthority::new_operator_local()
        .dispatch(&control, &lifecycle, &mut record)
        .err()
        .ok_or("foreign handoff authority unexpectedly accepted")?;
    assert!(!error.to_string().contains(&package_id));
    assert!(!format!("{error:?}").contains(&package_id));
    Ok(())
}

#[test]
fn package_load_and_runtime_foundation_remain_fail_closed() -> TestResult {
    let package_report = assess_package_load_yaml(VALID_MANIFEST)?;
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let runtime_report = inspect_runtime_foundation(DeclaredAgentPackage::from_manifest(&manifest));
    let handoff_owner = runtime_report
        .owner_statuses()
        .iter()
        .find(|status| status.owner() == RuntimeOwner::HandoffEnforcement)
        .ok_or("handoff owner is missing")?;

    assert_eq!(package_report.status(), PackageLoadStatus::Blocked);
    assert!(!package_report.load_allowed());
    assert_eq!(runtime_report.status(), RuntimeFoundationStatus::Blocked);
    assert!(!runtime_report.package_access_available());
    assert!(!runtime_report.execution_available());
    assert_eq!(handoff_owner.state(), RuntimeOwnerState::Unavailable);
    Ok(())
}

fn assert_handoff_error<T>(
    result: Result<T, HandoffError>,
    code: HandoffErrorCode,
    requirement: HandoffRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("handoff operation unexpectedly succeeded")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    assert!(!error.to_string().is_empty());
    Ok(())
}
