mod support;

use std::{collections::HashSet, error::Error};

use iamine_agent_runtime::{
    inspect_runtime_foundation, DeclaredAgentPackage, ExecutionLifecycleAuthority,
    ExecutionLifecycleError, ExecutionLifecycleErrorCode, ExecutionLifecycleRequirement,
    ExecutionLifecycleState, ExecutionLifecycleTransitionEvidenceStatus, RuntimeOwner,
    RuntimeOwnerState, SandboxEnforcementAuthority, SandboxEnforcementPolicy,
    EXECUTION_LIFECYCLE_RECORD_SCHEMA_VERSION, EXECUTION_LIFECYCLE_STATES,
    EXECUTION_LIFECYCLE_TRANSITION_SCHEMA_VERSION, MAX_EXECUTION_LIFECYCLE_TRANSITIONS,
};
use iamine_agents::{
    assess_package_load_yaml, parse_and_validate_yaml, AuditLifecycleState, PackageLoadBlockerCode,
    PackageLoadStatus,
};

use support::sandbox_chain::{
    prepare_sandbox, prepare_sandbox_evidence, PackageFixture, VALID_MANIFEST,
};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn canonical_states_match_the_observational_audit_vocabulary() {
    let audit_states = [
        AuditLifecycleState::Queued,
        AuditLifecycleState::PermissionPending,
        AuditLifecycleState::ScopeCheck,
        AuditLifecycleState::HandoffRequired,
        AuditLifecycleState::Running,
        AuditLifecycleState::Completed,
        AuditLifecycleState::Failed,
        AuditLifecycleState::Cancelled,
        AuditLifecycleState::Timeout,
        AuditLifecycleState::Blocked,
    ];

    assert_eq!(EXECUTION_LIFECYCLE_STATES.len(), audit_states.len());
    for (runtime, audit) in EXECUTION_LIFECYCLE_STATES.into_iter().zip(audit_states) {
        assert_eq!(runtime.as_str(), audit.as_str());
    }
}

#[test]
fn transition_shape_is_exact_and_terminal_states_have_no_exits() {
    let expected = HashSet::from([
        (
            ExecutionLifecycleState::Queued,
            ExecutionLifecycleState::PermissionPending,
        ),
        (
            ExecutionLifecycleState::Queued,
            ExecutionLifecycleState::Blocked,
        ),
        (
            ExecutionLifecycleState::PermissionPending,
            ExecutionLifecycleState::ScopeCheck,
        ),
        (
            ExecutionLifecycleState::PermissionPending,
            ExecutionLifecycleState::Blocked,
        ),
        (
            ExecutionLifecycleState::ScopeCheck,
            ExecutionLifecycleState::HandoffRequired,
        ),
        (
            ExecutionLifecycleState::ScopeCheck,
            ExecutionLifecycleState::Running,
        ),
        (
            ExecutionLifecycleState::ScopeCheck,
            ExecutionLifecycleState::Blocked,
        ),
        (
            ExecutionLifecycleState::HandoffRequired,
            ExecutionLifecycleState::Cancelled,
        ),
        (
            ExecutionLifecycleState::Running,
            ExecutionLifecycleState::Completed,
        ),
        (
            ExecutionLifecycleState::Running,
            ExecutionLifecycleState::Failed,
        ),
        (
            ExecutionLifecycleState::Running,
            ExecutionLifecycleState::Cancelled,
        ),
        (
            ExecutionLifecycleState::Running,
            ExecutionLifecycleState::Timeout,
        ),
    ]);

    let mut observed = HashSet::new();
    for from in EXECUTION_LIFECYCLE_STATES {
        for to in EXECUTION_LIFECYCLE_STATES {
            if from.has_canonical_transition_to(to) {
                observed.insert((from, to));
            }
        }
    }
    assert_eq!(observed, expected);

    for terminal in [
        ExecutionLifecycleState::Completed,
        ExecutionLifecycleState::Failed,
        ExecutionLifecycleState::Cancelled,
        ExecutionLifecycleState::Timeout,
        ExecutionLifecycleState::Blocked,
    ] {
        assert!(terminal.is_terminal());
        assert!(EXECUTION_LIFECYCLE_STATES
            .iter()
            .all(|target| !terminal.has_canonical_transition_to(*target)));
    }
}

#[test]
fn queue_requires_the_exact_sandbox_authority_evidence_and_subject() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let cloned_references = references.clone();
    let subject = fixture.subject(&references);
    let cloned_subject = fixture.subject(&cloned_references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();

    assert_lifecycle_error(
        lifecycle.queue(&sandbox.authority, &sandbox.evidence, cloned_subject),
        ExecutionLifecycleErrorCode::SandboxEvidenceNotVerified,
        ExecutionLifecycleRequirement::SandboxEnforcementEvidence,
    )?;

    let other_sandbox = SandboxEnforcementAuthority::new_operator_local(
        SandboxEnforcementPolicy::new(30_000, 128)?,
    )?;
    assert_lifecycle_error(
        lifecycle.queue(&other_sandbox, &sandbox.evidence, subject),
        ExecutionLifecycleErrorCode::SandboxEvidenceNotVerified,
        ExecutionLifecycleRequirement::SandboxEnforcementEvidence,
    )?;

    let record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let replacement_evidence = prepare_sandbox_evidence(&sandbox.authority, subject)?;
    assert_eq!(
        record.schema_version(),
        EXECUTION_LIFECYCLE_RECORD_SCHEMA_VERSION
    );
    assert_eq!(record.state(), ExecutionLifecycleState::Queued);
    assert_eq!(record.revision(), 0);
    assert!(!record.is_terminal());
    assert!(!record.execution_authorized());
    assert!(!record.runtime_active());
    assert!(!record.persisted());
    assert!(lifecycle.verifies_record(&record, &sandbox.authority, &sandbox.evidence, subject));
    assert!(!lifecycle.verifies_record(
        &record,
        &sandbox.authority,
        &replacement_evidence,
        subject
    ));
    assert!(
        !ExecutionLifecycleAuthority::new_operator_local().verifies_record(
            &record,
            &sandbox.authority,
            &sandbox.evidence,
            subject
        )
    );
    Ok(())
}

#[test]
fn valid_non_running_path_records_bounded_authority_evidence() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let authority = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = authority.queue(&sandbox.authority, &sandbox.evidence, subject)?;

    let permission =
        authority.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    assert_transition(
        &permission,
        ExecutionLifecycleState::Queued,
        ExecutionLifecycleState::PermissionPending,
        1,
    );
    assert!(authority.verifies_transition(&permission, &record));

    let scope = authority.transition(&mut record, 1, ExecutionLifecycleState::ScopeCheck)?;
    let handoff = authority.transition(&mut record, 2, ExecutionLifecycleState::HandoffRequired)?;
    let cancelled = authority.transition(&mut record, 3, ExecutionLifecycleState::Cancelled)?;

    assert_eq!(record.state(), ExecutionLifecycleState::Cancelled);
    assert_eq!(record.revision(), MAX_EXECUTION_LIFECYCLE_TRANSITIONS);
    assert!(record.is_terminal());
    assert!(authority.verifies_transition(&permission, &record));
    assert!(authority.verifies_transition(&scope, &record));
    assert!(authority.verifies_transition(&handoff, &record));
    assert!(authority.verifies_transition(&cancelled, &record));
    assert_transition(
        &cancelled,
        ExecutionLifecycleState::HandoffRequired,
        ExecutionLifecycleState::Cancelled,
        4,
    );
    Ok(())
}

#[test]
fn running_requires_independent_authorization_without_mutation() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let authority = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = authority.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let _ = authority.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    let _ = authority.transition(&mut record, 1, ExecutionLifecycleState::ScopeCheck)?;

    assert!(record
        .state()
        .has_canonical_transition_to(ExecutionLifecycleState::Running));
    assert_lifecycle_error(
        authority.transition(&mut record, 2, ExecutionLifecycleState::Running),
        ExecutionLifecycleErrorCode::ExecutionAuthorizationRequired,
        ExecutionLifecycleRequirement::ExecutionAuthorizationEvidence,
    )?;
    assert_eq!(record.state(), ExecutionLifecycleState::ScopeCheck);
    assert_eq!(record.revision(), 2);

    assert_lifecycle_error(
        authority.transition(&mut record, 2, ExecutionLifecycleState::Completed),
        ExecutionLifecycleErrorCode::InvalidTransition,
        ExecutionLifecycleRequirement::CanonicalTransition,
    )?;
    assert_eq!(record.state(), ExecutionLifecycleState::ScopeCheck);
    assert_eq!(record.revision(), 2);
    Ok(())
}

#[test]
fn stale_foreign_invalid_and_terminal_transitions_fail_closed() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let authority = ExecutionLifecycleAuthority::new_operator_local();
    let foreign = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = authority.queue(&sandbox.authority, &sandbox.evidence, subject)?;

    assert_lifecycle_error(
        authority.transition(&mut record, 0, ExecutionLifecycleState::Queued),
        ExecutionLifecycleErrorCode::InvalidTransition,
        ExecutionLifecycleRequirement::CanonicalTransition,
    )?;
    assert_lifecycle_error(
        authority.transition(&mut record, 0, ExecutionLifecycleState::Completed),
        ExecutionLifecycleErrorCode::InvalidTransition,
        ExecutionLifecycleRequirement::CanonicalTransition,
    )?;
    assert_lifecycle_error(
        foreign.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending),
        ExecutionLifecycleErrorCode::ForeignLifecycleAuthority,
        ExecutionLifecycleRequirement::LifecycleAuthority,
    )?;

    let _ = authority.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    assert_lifecycle_error(
        authority.transition(&mut record, 0, ExecutionLifecycleState::ScopeCheck),
        ExecutionLifecycleErrorCode::StaleRevision,
        ExecutionLifecycleRequirement::CurrentRevision,
    )?;
    let _ = authority.transition(&mut record, 1, ExecutionLifecycleState::Blocked)?;
    assert_lifecycle_error(
        authority.transition(&mut record, 2, ExecutionLifecycleState::Failed),
        ExecutionLifecycleErrorCode::TerminalState,
        ExecutionLifecycleRequirement::NonTerminalState,
    )?;
    assert_eq!(record.state(), ExecutionLifecycleState::Blocked);
    assert_eq!(record.revision(), 2);
    Ok(())
}

#[test]
fn transition_evidence_is_bound_to_one_execution_record() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let authority = ExecutionLifecycleAuthority::new_operator_local();
    let mut first = authority.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let second = authority.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let evidence = authority.transition(&mut first, 0, ExecutionLifecycleState::Blocked)?;

    assert!(authority.verifies_transition(&evidence, &first));
    assert!(!authority.verifies_transition(&evidence, &second));
    assert!(
        !ExecutionLifecycleAuthority::new_operator_local().verifies_transition(&evidence, &first)
    );
    assert!(!evidence.execution_authorized());
    assert!(!evidence.runtime_active());
    assert!(!evidence.persisted());
    assert!(!evidence.audit_emitted());
    assert!(!evidence.cleanup_completed());
    assert!(!evidence.transport_allowed());
    assert!(!evidence.package_loaded());
    Ok(())
}

#[test]
fn debug_and_errors_do_not_expose_package_or_execution_identity() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let authority = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = authority.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let evidence =
        authority.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    let error = authority
        .transition(&mut record, 0, ExecutionLifecycleState::ScopeCheck)
        .err()
        .ok_or("stale transition unexpectedly succeeded")?;
    let combined = format!("{authority:?} {record:?} {evidence:?} {error:?} {error}");

    assert!(combined.contains("[redacted]"));
    assert!(!combined.contains(fixture.package_id()));
    assert!(!combined.contains("node_readiness_diagnostic_report"));
    assert!(!combined.contains("metadata/"));
    assert!(!combined.contains("iamine-agent-runtime"));
    Ok(())
}

#[test]
fn foundation_and_package_load_blockers_remain_fail_closed() -> TestResult {
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let foundation = inspect_runtime_foundation(DeclaredAgentPackage::from_manifest(&manifest));
    let lifecycle = foundation
        .owner_statuses()
        .iter()
        .find(|status| status.owner() == RuntimeOwner::ExecutionLifecycle)
        .ok_or("execution lifecycle owner missing")?;

    assert_eq!(lifecycle.state(), RuntimeOwnerState::Unavailable);
    assert!(!foundation.execution_available());
    assert!(!foundation.package_access_available());

    let load = assess_package_load_yaml(VALID_MANIFEST)?;
    assert_eq!(load.status(), PackageLoadStatus::Blocked);
    assert!(!load.load_allowed());
    assert!(load
        .blockers()
        .contains(&PackageLoadBlockerCode::SandboxEnforcementUnavailable));
    assert!(load
        .blockers()
        .contains(&PackageLoadBlockerCode::ExecutionAuthorizationUnavailable));
    Ok(())
}

fn assert_transition(
    evidence: &iamine_agent_runtime::ExecutionLifecycleTransitionEvidence,
    from: ExecutionLifecycleState,
    to: ExecutionLifecycleState,
    revision: u8,
) {
    assert_eq!(
        evidence.schema_version(),
        EXECUTION_LIFECYCLE_TRANSITION_SCHEMA_VERSION
    );
    assert_eq!(
        evidence.status(),
        ExecutionLifecycleTransitionEvidenceStatus::Recorded
    );
    assert_eq!(evidence.from(), from);
    assert_eq!(evidence.to(), to);
    assert_eq!(evidence.revision(), revision);
    assert!(evidence.transition_recorded());
    assert!(!evidence.execution_authorized());
    assert!(!evidence.runtime_active());
}

fn assert_lifecycle_error<T>(
    result: Result<T, ExecutionLifecycleError>,
    code: ExecutionLifecycleErrorCode,
    requirement: ExecutionLifecycleRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("unsafe lifecycle operation unexpectedly succeeded")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    Ok(())
}
