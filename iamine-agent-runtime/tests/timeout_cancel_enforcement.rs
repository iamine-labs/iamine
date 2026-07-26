mod support;

use std::{
    error::Error,
    time::{Duration, Instant},
};

use iamine_agent_runtime::{
    AgentTimeoutClass, CancellationSource, ExecutionLifecycleAuthority, ExecutionLifecycleState,
    SandboxCleanupOwner, SandboxCleanupResult, SandboxCleanupTrigger, TimeoutCancelAuthority,
    TimeoutCancelConfigurationErrorCode, TimeoutCancelError, TimeoutCancelErrorCode,
    TimeoutCancelEvent, TimeoutCancelPolicy, TimeoutCancelRequirement,
    TimeoutCancelTerminalEvidenceStatus, AGENT_TIMEOUT_CLASSES,
    CANCELLATION_REQUEST_SCHEMA_VERSION, CANCELLATION_SOURCES, CLEANUP_TIMEOUT_SCHEMA_VERSION,
    MAX_AGENT_TIMEOUT_MS, TIMEOUT_CANCEL_TERMINAL_SCHEMA_VERSION,
};

use support::sandbox_chain::{prepare_sandbox, PackageFixture};

type TestResult = Result<(), Box<dyn Error>>;

fn policy() -> Result<TimeoutCancelPolicy, iamine_agent_runtime::TimeoutCancelConfigurationError> {
    TimeoutCancelPolicy::new(100, 100, 100, 30_000, 100, 100)
}

#[test]
fn policy_requires_every_canonical_timeout_to_be_bounded() -> TestResult {
    let configured = policy()?;
    let expected = [
        (AgentTimeoutClass::PermissionWait, 100),
        (AgentTimeoutClass::ScopeCheck, 100),
        (AgentTimeoutClass::SandboxStart, 100),
        (AgentTimeoutClass::Execution, 30_000),
        (AgentTimeoutClass::Handoff, 100),
        (AgentTimeoutClass::Cleanup, 100),
    ];
    assert_eq!(AGENT_TIMEOUT_CLASSES.len(), expected.len());
    for (timeout_class, timeout_ms) in expected {
        assert_eq!(configured.timeout_ms(timeout_class), timeout_ms);
        assert!(timeout_class.as_str().ends_with("_timeout"));
    }
    assert_eq!(
        CANCELLATION_SOURCES.map(CancellationSource::as_str),
        [
            "operator_cancelled",
            "orchestrator_cancelled",
            "permission_revoked",
            "scope_violation_cancelled",
            "sandbox_failure_cancelled",
            "timeout_cancelled",
            "shutdown_cancelled",
        ]
    );

    let zero = TimeoutCancelPolicy::new(100, 100, 0, 100, 100, 100)
        .err()
        .ok_or("zero timeout unexpectedly accepted")?;
    assert_eq!(
        zero.code(),
        TimeoutCancelConfigurationErrorCode::ZeroTimeout
    );
    assert_eq!(zero.timeout_class(), AgentTimeoutClass::SandboxStart);

    let unbounded = TimeoutCancelPolicy::new(100, 100, 100, MAX_AGENT_TIMEOUT_MS + 1, 100, 100)
        .err()
        .ok_or("unbounded timeout unexpectedly accepted")?;
    assert_eq!(
        unbounded.code(),
        TimeoutCancelConfigurationErrorCode::TimeoutTooLarge
    );
    assert_eq!(unbounded.timeout_class(), AgentTimeoutClass::Execution);
    Ok(())
}

#[test]
fn establish_requires_the_exact_lifecycle_chain_and_sandbox_bound() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let cloned_references = references.clone();
    let subject = fixture.subject(&references);
    let cloned_subject = fixture.subject(&cloned_references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();

    let control = authority.establish(
        &lifecycle,
        &record,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    assert!(authority.verifies_control(&control, &lifecycle, &record));
    assert!(!control.execution_authorized());
    assert!(!control.runtime_active());
    assert!(!control.persisted());

    assert_timeout_error(
        authority.establish(
            &lifecycle,
            &record,
            &sandbox.authority,
            &sandbox.evidence,
            cloned_subject,
            policy()?,
        ),
        TimeoutCancelErrorCode::LifecycleRecordNotVerified,
        TimeoutCancelRequirement::LifecycleRecord,
    )?;

    let too_long = TimeoutCancelPolicy::new(100, 100, 100, 30_001, 100, 100)?;
    assert_timeout_error(
        authority.establish(
            &lifecycle,
            &record,
            &sandbox.authority,
            &sandbox.evidence,
            subject,
            too_long,
        ),
        TimeoutCancelErrorCode::TimeoutPolicyExceedsSandbox,
        TimeoutCancelRequirement::SandboxWallTime,
    )?;
    Ok(())
}

#[test]
fn timeout_does_not_mutate_before_deadline_then_blocks_queued_work() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();
    let control = authority.establish(
        &lifecycle,
        &record,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    let started_at = Instant::now();
    let timeout = authority.arm_timeout_at(
        &control,
        &lifecycle,
        &record,
        AgentTimeoutClass::SandboxStart,
        started_at,
    )?;

    assert!(!timeout.expired_at(started_at + Duration::from_millis(99)));
    assert_timeout_error(
        authority.enforce_timeout_at(
            &control,
            &lifecycle,
            &mut record,
            &timeout,
            started_at + Duration::from_millis(99),
        ),
        TimeoutCancelErrorCode::TimeoutNotExpired,
        TimeoutCancelRequirement::ExpiredDeadline,
    )?;
    assert_eq!(record.state(), ExecutionLifecycleState::Queued);
    assert_eq!(record.revision(), 0);

    let evidence = authority.enforce_timeout_at(
        &control,
        &lifecycle,
        &mut record,
        &timeout,
        started_at + Duration::from_millis(100),
    )?;
    assert_eq!(
        evidence.status(),
        TimeoutCancelTerminalEvidenceStatus::TerminalRecordedCleanupPending
    );
    assert_eq!(
        evidence.event(),
        TimeoutCancelEvent::Timeout(AgentTimeoutClass::SandboxStart)
    );
    assert_eq!(evidence.terminal_state(), ExecutionLifecycleState::Blocked);
    assert_eq!(evidence.lifecycle_revision(), 1);
    assert_eq!(
        evidence.cleanup_owner(),
        SandboxCleanupOwner::RuntimeSandboxAdapter
    );
    assert_eq!(evidence.cleanup_trigger(), SandboxCleanupTrigger::Timeout);
    assert_eq!(evidence.cleanup_result(), SandboxCleanupResult::Pending);
    assert!(evidence.terminal_state_recorded());
    assert!(!evidence.cleanup_completed());
    assert!(!evidence.execution_authorized());
    assert!(!evidence.runtime_active());
    assert!(!evidence.persisted());
    assert!(!evidence.audit_emitted());
    assert!(authority.verifies_terminal(&evidence, &control, &lifecycle, &record));
    Ok(())
}

#[test]
fn timeout_class_must_match_the_current_lifecycle_phase() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();
    let control = authority.establish(
        &lifecycle,
        &record,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;

    assert_timeout_error(
        authority.arm_timeout(&control, &lifecycle, &record, AgentTimeoutClass::Execution),
        TimeoutCancelErrorCode::TimeoutClassStateMismatch,
        TimeoutCancelRequirement::StateTimeoutClass,
    )?;

    let queued_timeout = authority.arm_timeout(
        &control,
        &lifecycle,
        &record,
        AgentTimeoutClass::SandboxStart,
    )?;
    let _ = lifecycle.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    assert_timeout_error(
        authority.enforce_timeout_at(
            &control,
            &lifecycle,
            &mut record,
            &queued_timeout,
            queued_timeout.deadline(),
        ),
        TimeoutCancelErrorCode::StaleRevision,
        TimeoutCancelRequirement::CurrentRevision,
    )?;
    assert_eq!(record.state(), ExecutionLifecycleState::PermissionPending);
    assert_eq!(record.revision(), 1);
    Ok(())
}

#[test]
fn handoff_timeout_records_cancelled_without_reclassifying_it_as_success() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();
    let control = authority.establish(
        &lifecycle,
        &record,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    let _ = lifecycle.transition(&mut record, 0, ExecutionLifecycleState::PermissionPending)?;
    let _ = lifecycle.transition(&mut record, 1, ExecutionLifecycleState::ScopeCheck)?;
    let _ = lifecycle.transition(&mut record, 2, ExecutionLifecycleState::HandoffRequired)?;
    let started_at = Instant::now();
    let timeout = authority.arm_timeout_at(
        &control,
        &lifecycle,
        &record,
        AgentTimeoutClass::Handoff,
        started_at,
    )?;
    let evidence = authority.enforce_timeout_at(
        &control,
        &lifecycle,
        &mut record,
        &timeout,
        timeout.deadline(),
    )?;

    assert_eq!(
        evidence.event(),
        TimeoutCancelEvent::Timeout(AgentTimeoutClass::Handoff)
    );
    assert_eq!(
        evidence.terminal_state(),
        ExecutionLifecycleState::Cancelled
    );
    assert_ne!(
        evidence.terminal_state(),
        ExecutionLifecycleState::Completed
    );
    Ok(())
}

#[test]
fn cancellation_is_one_shot_shared_and_records_cleanup_pending() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();
    let control = authority.establish(
        &lifecycle,
        &record,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    let handle = control.cancellation_handle();
    let observer = handle.clone();

    assert!(!handle.cancellation_requested());
    let request = authority.request_cancellation(
        &control,
        &lifecycle,
        &record,
        &handle,
        0,
        CancellationSource::Operator,
    )?;
    assert_eq!(
        request.schema_version(),
        CANCELLATION_REQUEST_SCHEMA_VERSION
    );
    assert_eq!(request.source(), CancellationSource::Operator);
    assert!(!request.terminal_state_recorded());
    assert!(!request.cleanup_completed());
    assert_eq!(
        observer.requested_source(),
        Some(CancellationSource::Operator)
    );
    assert!(!observer.cancellation_enforced());

    assert_timeout_error(
        authority.request_cancellation(
            &control,
            &lifecycle,
            &record,
            &handle,
            0,
            CancellationSource::Shutdown,
        ),
        TimeoutCancelErrorCode::CancellationAlreadyRequested,
        TimeoutCancelRequirement::SingleCancellationRequest,
    )?;
    assert_eq!(
        observer.requested_source(),
        Some(CancellationSource::Operator)
    );

    let terminal =
        authority.enforce_cancellation(&control, &lifecycle, &mut record, &handle, &request)?;
    assert_eq!(
        terminal.schema_version(),
        TIMEOUT_CANCEL_TERMINAL_SCHEMA_VERSION
    );
    assert_eq!(
        terminal.event(),
        TimeoutCancelEvent::Cancellation(CancellationSource::Operator)
    );
    assert_eq!(terminal.terminal_state(), ExecutionLifecycleState::Blocked);
    assert_eq!(
        terminal.cleanup_trigger(),
        SandboxCleanupTrigger::Cancellation
    );
    assert_eq!(terminal.cleanup_result(), SandboxCleanupResult::Pending);
    assert!(observer.cancellation_enforced());
    assert!(authority.verifies_terminal(&terminal, &control, &lifecycle, &record));
    Ok(())
}

#[test]
fn stale_request_and_foreign_handles_fail_without_mutation() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut first = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let second = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();
    let first_control = authority.establish(
        &lifecycle,
        &first,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    let second_control = authority.establish(
        &lifecycle,
        &second,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    let first_handle = first_control.cancellation_handle();
    let second_handle = second_control.cancellation_handle();

    assert_timeout_error(
        authority.request_cancellation(
            &first_control,
            &lifecycle,
            &first,
            &first_handle,
            1,
            CancellationSource::Orchestrator,
        ),
        TimeoutCancelErrorCode::StaleRevision,
        TimeoutCancelRequirement::CurrentRevision,
    )?;
    assert!(!first_handle.cancellation_requested());

    assert_timeout_error(
        authority.request_cancellation(
            &first_control,
            &lifecycle,
            &first,
            &second_handle,
            0,
            CancellationSource::Orchestrator,
        ),
        TimeoutCancelErrorCode::CancellationHandleNotVerified,
        TimeoutCancelRequirement::CancellationHandle,
    )?;
    assert_eq!(first.state(), ExecutionLifecycleState::Queued);

    let request = authority.request_cancellation(
        &first_control,
        &lifecycle,
        &first,
        &first_handle,
        0,
        CancellationSource::Orchestrator,
    )?;
    let _ = lifecycle.transition(&mut first, 0, ExecutionLifecycleState::PermissionPending)?;
    assert_timeout_error(
        authority.enforce_cancellation(
            &first_control,
            &lifecycle,
            &mut first,
            &first_handle,
            &request,
        ),
        TimeoutCancelErrorCode::CancellationRequestNotVerified,
        TimeoutCancelRequirement::CancellationRequest,
    )?;
    assert_eq!(first.state(), ExecutionLifecycleState::PermissionPending);
    Ok(())
}

#[test]
fn cleanup_timeout_records_adapter_result_without_reclassifying_terminal_state() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();
    let control = authority.establish(
        &lifecycle,
        &record,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    let started_at = Instant::now();
    let termination_timeout = authority.arm_timeout_at(
        &control,
        &lifecycle,
        &record,
        AgentTimeoutClass::SandboxStart,
        started_at,
    )?;
    let terminal = authority.enforce_timeout_at(
        &control,
        &lifecycle,
        &mut record,
        &termination_timeout,
        termination_timeout.deadline(),
    )?;
    assert_timeout_error(
        authority.arm_timeout_at(
            &control,
            &lifecycle,
            &record,
            AgentTimeoutClass::Cleanup,
            started_at,
        ),
        TimeoutCancelErrorCode::TimeoutClassStateMismatch,
        TimeoutCancelRequirement::StateTimeoutClass,
    )?;
    let timeout =
        authority.arm_cleanup_timeout_at(&control, &lifecycle, &record, &terminal, started_at)?;
    let evidence = authority.record_cleanup_timeout_at(
        &control,
        &lifecycle,
        &record,
        &timeout,
        timeout.deadline(),
    )?;

    assert_eq!(evidence.schema_version(), CLEANUP_TIMEOUT_SCHEMA_VERSION);
    assert_eq!(evidence.terminal_state(), ExecutionLifecycleState::Blocked);
    assert_eq!(evidence.lifecycle_revision(), 1);
    assert_eq!(
        evidence.cleanup_owner(),
        SandboxCleanupOwner::RuntimeSandboxAdapter
    );
    assert_eq!(evidence.cleanup_result(), SandboxCleanupResult::TimedOut);
    assert!(!evidence.lifecycle_state_changed());
    assert!(!evidence.cleanup_completed());
    assert_eq!(record.state(), ExecutionLifecycleState::Blocked);
    assert_eq!(record.revision(), 1);
    assert!(authority.verifies_cleanup_timeout(&evidence, &control, &lifecycle, &record));
    Ok(())
}

#[test]
fn foreign_authority_and_debug_output_do_not_expose_identity() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let authority = TimeoutCancelAuthority::new_operator_local();
    let foreign = TimeoutCancelAuthority::new_operator_local();
    let control = authority.establish(
        &lifecycle,
        &record,
        &sandbox.authority,
        &sandbox.evidence,
        subject,
        policy()?,
    )?;
    let handle = control.cancellation_handle();

    assert_timeout_error(
        foreign.arm_timeout(
            &control,
            &lifecycle,
            &record,
            AgentTimeoutClass::SandboxStart,
        ),
        TimeoutCancelErrorCode::ForeignAuthority,
        TimeoutCancelRequirement::TimeoutCancelAuthority,
    )?;
    let combined = format!("{authority:?} {control:?} {handle:?}");
    assert!(combined.contains("[redacted]"));
    assert!(!combined.contains(fixture.package_id()));
    assert!(!combined.contains("node_readiness_diagnostic_report"));
    assert!(!combined.contains("metadata/"));
    assert!(!combined.contains("iamine-agent-runtime"));
    Ok(())
}

fn assert_timeout_error<T>(
    result: Result<T, TimeoutCancelError>,
    code: TimeoutCancelErrorCode,
    requirement: TimeoutCancelRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("unsafe timeout/cancel operation unexpectedly succeeded")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    Ok(())
}
