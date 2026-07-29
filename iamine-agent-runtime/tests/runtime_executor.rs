#[allow(dead_code)]
#[path = "support/execution_authorization_chain.rs"]
mod execution_authorization_chain;
#[allow(dead_code)]
#[path = "support/routing_policy.rs"]
mod routing_policy;
#[allow(dead_code)]
#[path = "support/sandbox_chain.rs"]
mod sandbox_chain;

use std::{collections::HashSet, error::Error, thread, time::Duration};

use execution_authorization_chain::PreparedAuthorizationChain;
use iamine_agent_runtime::{
    CancellationSource, EnforcedInputRecord, ExecutionAuthorizationAuthority,
    ExecutionLifecycleState, InputClassification, LoadedAgentPackage, OfficialRustProgram,
    OfficialRustProgramFailure, OfficialRustProgramFailureCode, OfficialRustProgramHandler,
    OfficialRustProgramOutput, OfficialRustProgramRegistry, OutputClassification,
    PackageLoadEvidenceAuthority, PackageLoaderAuthority, RuntimeExecutionContext,
    RuntimeExecutionPermit, RuntimeExecutionPreparation, RuntimeExecutionRequest,
    RuntimeExecutionStatus, RuntimeExecutionVerification, RuntimeExecutorAuthority,
    RuntimeExecutorError, RuntimeExecutorErrorCode, RuntimeExecutorRequirement,
    TimeoutCancelPolicy, RUNTIME_EXECUTION_RESULT_SCHEMA_VERSION,
};
use iamine_agents::{assess_package_load_yaml, PackageLoadStatus};
use routing_policy::PACKAGE_ID;
use sandbox_chain::{PackageFixture, VALID_MANIFEST};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct PreparedRuntime<'subject> {
    loaded: LoadedAgentPackage<'subject>,
    registry: OfficialRustProgramRegistry,
    program: OfficialRustProgram<'subject>,
    permit: Option<RuntimeExecutionPermit<'subject>>,
}

impl<'subject> PreparedRuntime<'subject> {
    fn new(
        chain: &PreparedAuthorizationChain<'subject>,
        executor: &RuntimeExecutorAuthority,
        handler: OfficialRustProgramHandler,
    ) -> TestResult<Self> {
        let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
        let authorization = {
            let request = chain.request();
            authorization_authority.authorize(&request)?
        };
        let evidence_authority = PackageLoadEvidenceAuthority::new_operator_local();
        let evidence = {
            let request = chain.request();
            evidence_authority.integrate(&authorization_authority, &authorization, &request)?
        };
        let loader = PackageLoaderAuthority::new_operator_local();
        let loaded = {
            let request = chain.request();
            loader.load(
                &evidence_authority,
                &evidence,
                &authorization_authority,
                &authorization,
                &request,
            )?
        };
        let registry = OfficialRustProgramRegistry::new_operator_local();
        let program = registry.register(chain.subject, handler);
        let permit = {
            let request = chain.request();
            executor.prepare(
                RuntimeExecutionPreparation::new(&loaded, &request)
                    .with_loader(&loader, &evidence_authority, &evidence)
                    .with_authorization(&authorization_authority, &authorization)
                    .with_program(&registry, &program),
            )?
        };
        Ok(Self {
            loaded,
            registry,
            program,
            permit: Some(permit),
        })
    }

    fn request<'context>(
        &'context mut self,
        chain: &'context mut PreparedAuthorizationChain<'subject>,
        input: &'context EnforcedInputRecord,
    ) -> RuntimeExecutionRequest<'context, 'subject> {
        RuntimeExecutionRequest::new(
            self.permit.take().expect("test permit is one-shot"),
            &chain.lifecycle_authority,
            &mut chain.lifecycle_record,
            input,
        )
        .with_program(&self.registry, &self.program)
        .with_sandbox(&chain.sandbox_authority, &chain.sandbox_evidence)
        .with_timeout_cancel(&chain.timeout_authority, &chain.timeout_control)
        .with_input_output(&chain.input_output_authority, &chain.input_output_evidence)
        .with_audit(&chain.audit_authority)
    }
}

#[test]
fn exact_loaded_authorized_package_executes_registered_official_program() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, successful_program)?;

    let result = executor.execute(runtime.request(&mut chain, &input))?;

    assert_eq!(
        result.schema_version(),
        RUNTIME_EXECUTION_RESULT_SCHEMA_VERSION
    );
    assert_eq!(result.status(), RuntimeExecutionStatus::Completed);
    assert_eq!(result.started_revision(), 3);
    assert_eq!(result.completed_revision(), 4);
    assert_eq!(
        chain.lifecycle_record.state(),
        ExecutionLifecycleState::Completed
    );
    assert_eq!(result.output().redacted_content(), "status unavailable");
    assert_eq!(
        result.output().classification(),
        OutputClassification::DiagnosticReport
    );
    assert!(result.execution_authorized());
    assert!(result.package_loaded());
    assert!(result.execution_started());
    assert!(result.runtime_was_active());
    assert!(result.sandbox_adapter_was_active());
    assert!(!result.os_isolation_claimed());
    assert!(result.cleanup_completed());
    assert!(result.audit_recorded());
    assert!(!result.scheduler_mutated());
    assert!(!result.transport_started());
    assert!(!result.persisted());
    assert!(!result.external_event_emitted());
    assert!(executor.verifies_result(
        RuntimeExecutionVerification::new(
            &result,
            &runtime.loaded,
            &chain.lifecycle_authority,
            &chain.lifecycle_record,
        )
        .with_program(&runtime.registry, &runtime.program)
        .with_audit(&chain.audit_authority),
    ));
    Ok(())
}

#[test]
fn foreign_executor_cannot_consume_permit() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, successful_program)?;
    let foreign = RuntimeExecutorAuthority::new_operator_local();

    assert_executor_error(
        foreign.execute(runtime.request(&mut chain, &input)),
        RuntimeExecutorErrorCode::ForeignExecutorAuthority,
        RuntimeExecutorRequirement::ExecutorAuthority,
    )?;
    assert_eq!(
        chain.lifecycle_record.state(),
        ExecutionLifecycleState::ScopeCheck
    );
    Ok(())
}

#[test]
fn registered_program_cannot_be_substituted_after_permit() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, successful_program)?;
    let foreign_registry = OfficialRustProgramRegistry::new_operator_local();
    let foreign_program = foreign_registry.register(chain.subject, successful_program);
    let request = RuntimeExecutionRequest::new(
        runtime.permit.take().ok_or("missing permit")?,
        &chain.lifecycle_authority,
        &mut chain.lifecycle_record,
        &input,
    )
    .with_program(&foreign_registry, &foreign_program)
    .with_sandbox(&chain.sandbox_authority, &chain.sandbox_evidence)
    .with_timeout_cancel(&chain.timeout_authority, &chain.timeout_control)
    .with_input_output(&chain.input_output_authority, &chain.input_output_evidence)
    .with_audit(&chain.audit_authority);

    assert_executor_error(
        executor.execute(request),
        RuntimeExecutorErrorCode::OfficialProgramNotVerified,
        RuntimeExecutorRequirement::OfficialProgram,
    )?;
    Ok(())
}

#[test]
fn stale_lifecycle_revision_invalidates_permit() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, successful_program)?;
    let _ = chain.lifecycle_authority.transition(
        &mut chain.lifecycle_record,
        2,
        ExecutionLifecycleState::HandoffRequired,
    )?;

    assert_executor_error(
        executor.execute(runtime.request(&mut chain, &input)),
        RuntimeExecutorErrorCode::StaleExecutionPermit,
        RuntimeExecutorRequirement::ExecutionPermit,
    )?;
    Ok(())
}

#[test]
fn pending_cancellation_blocks_before_running() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, successful_program)?;
    let cancellation = chain.timeout_control.cancellation_handle();
    let _request = chain.timeout_authority.request_cancellation(
        &chain.timeout_control,
        &chain.lifecycle_authority,
        &chain.lifecycle_record,
        &cancellation,
        2,
        CancellationSource::Operator,
    )?;

    assert_executor_error(
        executor.execute(runtime.request(&mut chain, &input)),
        RuntimeExecutorErrorCode::CancellationPending,
        RuntimeExecutorRequirement::TimeoutCancelControl,
    )?;
    assert_eq!(
        chain.lifecycle_record.state(),
        ExecutionLifecycleState::ScopeCheck
    );
    Ok(())
}

#[test]
fn input_from_another_evidence_chain_is_rejected() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let mut chain = PreparedAuthorizationChain::new(subject)?;
    let foreign_chain = PreparedAuthorizationChain::new(subject)?;
    let foreign_input = enforced_input(&foreign_chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, successful_program)?;

    assert_executor_error(
        executor.execute(runtime.request(&mut chain, &foreign_input)),
        RuntimeExecutorErrorCode::EnforcedInputNotVerified,
        RuntimeExecutorRequirement::EnforcedInput,
    )?;
    Ok(())
}

#[test]
fn program_failure_records_failed_terminal_state() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, failing_program)?;

    assert_executor_error(
        executor.execute(runtime.request(&mut chain, &input)),
        RuntimeExecutorErrorCode::RuntimeProgramFailed,
        RuntimeExecutorRequirement::RuntimeProgram,
    )?;
    assert_eq!(
        chain.lifecycle_record.state(),
        ExecutionLifecycleState::Failed
    );
    Ok(())
}

#[test]
fn invalid_program_output_records_failed_terminal_state() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, oversized_output_program)?;

    assert_executor_error(
        executor.execute(runtime.request(&mut chain, &input)),
        RuntimeExecutorErrorCode::RuntimeOutputRejected,
        RuntimeExecutorRequirement::EnforcedOutput,
    )?;
    assert_eq!(
        chain.lifecycle_record.state(),
        ExecutionLifecycleState::Failed
    );
    Ok(())
}

#[test]
fn execution_timeout_is_enforced_as_timeout_terminal_state() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let policy = TimeoutCancelPolicy::new(1_000, 1_000, 1_000, 1, 1_000, 1_000)?;
    let mut chain =
        PreparedAuthorizationChain::new_with_timeout_policy(fixture.subject(&references), policy)?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, slow_program)?;

    assert_executor_error(
        executor.execute(runtime.request(&mut chain, &input)),
        RuntimeExecutorErrorCode::RuntimeTimedOut,
        RuntimeExecutorRequirement::RuntimeProgram,
    )?;
    assert_eq!(
        chain.lifecycle_record.state(),
        ExecutionLifecycleState::Timeout
    );
    Ok(())
}

#[test]
fn public_lifecycle_api_still_cannot_bypass_executor() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;

    let error = chain
        .lifecycle_authority
        .transition(
            &mut chain.lifecycle_record,
            2,
            ExecutionLifecycleState::Running,
        )
        .expect_err("public transition must not start execution");
    assert_eq!(
        error.code(),
        iamine_agent_runtime::ExecutionLifecycleErrorCode::ExecutionAuthorizationRequired
    );
    Ok(())
}

#[test]
fn debug_errors_and_results_do_not_expose_private_values() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let input = enforced_input(&chain, "redacted status request")?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, successful_program)?;
    let result = executor.execute(runtime.request(&mut chain, &input))?;

    for debug in [
        format!("{executor:?}"),
        format!("{:?}", runtime.registry),
        format!("{:?}", runtime.program),
        format!("{result:?}"),
    ] {
        assert!(debug.contains("[redacted]"));
        for forbidden in [
            PACKAGE_ID,
            "redacted status request",
            "status unavailable",
            "username",
            "hostname",
            "home/",
            "private_key",
            "wallet",
        ] {
            assert!(!debug.contains(forbidden));
        }
    }
    Ok(())
}

#[test]
fn codes_are_unique_stable_and_static_package_gate_remains_blocked() -> TestResult {
    let requirements = [
        RuntimeExecutorRequirement::ExecutorAuthority,
        RuntimeExecutorRequirement::ExecutionPermit,
        RuntimeExecutorRequirement::LoadedPackage,
        RuntimeExecutorRequirement::ExecutionAuthorization,
        RuntimeExecutorRequirement::OfficialProgram,
        RuntimeExecutorRequirement::LifecycleRecord,
        RuntimeExecutorRequirement::SandboxEvidence,
        RuntimeExecutorRequirement::SandboxRestrictions,
        RuntimeExecutorRequirement::TimeoutCancelControl,
        RuntimeExecutorRequirement::EnforcedInput,
        RuntimeExecutorRequirement::RuntimeProgram,
        RuntimeExecutorRequirement::EnforcedOutput,
        RuntimeExecutorRequirement::AuditEvidence,
    ]
    .into_iter()
    .map(RuntimeExecutorRequirement::as_str)
    .collect::<HashSet<_>>();
    let errors = [
        RuntimeExecutorErrorCode::LoadedPackageNotVerified,
        RuntimeExecutorErrorCode::OfficialProgramNotVerified,
        RuntimeExecutorErrorCode::ForeignExecutorAuthority,
        RuntimeExecutorErrorCode::StaleExecutionPermit,
        RuntimeExecutorErrorCode::LifecycleRecordNotVerified,
        RuntimeExecutorErrorCode::SandboxEvidenceNotVerified,
        RuntimeExecutorErrorCode::SandboxRestrictionsUnsupported,
        RuntimeExecutorErrorCode::TimeoutControlNotVerified,
        RuntimeExecutorErrorCode::CancellationPending,
        RuntimeExecutorErrorCode::EnforcedInputNotVerified,
        RuntimeExecutorErrorCode::LifecycleTransitionRejected,
        RuntimeExecutorErrorCode::TimeoutArmFailed,
        RuntimeExecutorErrorCode::RuntimeProgramFailed,
        RuntimeExecutorErrorCode::RuntimeTimedOut,
        RuntimeExecutorErrorCode::RuntimeOutputRejected,
        RuntimeExecutorErrorCode::AuditProjectionFailed,
    ]
    .into_iter()
    .map(RuntimeExecutorErrorCode::as_str)
    .collect::<HashSet<_>>();

    assert_eq!(requirements.len(), 13);
    assert_eq!(errors.len(), 16);
    let static_gate = assess_package_load_yaml(VALID_MANIFEST)?;
    assert_eq!(static_gate.status(), PackageLoadStatus::Blocked);
    assert!(!static_gate.load_allowed());
    Ok(())
}

fn enforced_input(
    chain: &PreparedAuthorizationChain<'_>,
    content: &str,
) -> TestResult<EnforcedInputRecord> {
    let attested = chain.input_output_authority.attest_redacted_input(
        &chain.input_output_evidence,
        chain.subject,
        content,
    )?;
    Ok(chain.input_output_authority.enforce_input(
        &chain.input_output_evidence,
        chain.subject,
        InputClassification::TaskDescriptor,
        attested,
    )?)
}

fn successful_program(
    context: &RuntimeExecutionContext<'_>,
    _: &str,
) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure> {
    context.checkpoint()?;
    Ok(OfficialRustProgramOutput::operator_reviewed(
        OutputClassification::DiagnosticReport,
        "status unavailable",
    ))
}

fn failing_program(
    _: &RuntimeExecutionContext<'_>,
    _: &str,
) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure> {
    Err(OfficialRustProgramFailure::new(
        OfficialRustProgramFailureCode::ExecutionFailed,
    ))
}

fn oversized_output_program(
    _: &RuntimeExecutionContext<'_>,
    _: &str,
) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure> {
    Ok(OfficialRustProgramOutput::operator_reviewed(
        OutputClassification::DiagnosticReport,
        "x".repeat(256),
    ))
}

fn slow_program(
    context: &RuntimeExecutionContext<'_>,
    _: &str,
) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure> {
    thread::sleep(Duration::from_millis(5));
    context.checkpoint()?;
    Ok(OfficialRustProgramOutput::operator_reviewed(
        OutputClassification::DiagnosticReport,
        "late",
    ))
}

fn assert_executor_error<T>(
    result: Result<T, RuntimeExecutorError>,
    expected_code: RuntimeExecutorErrorCode,
    expected_requirement: RuntimeExecutorRequirement,
) -> TestResult {
    let error = result.err().ok_or("expected runtime executor error")?;
    assert_eq!(error.code(), expected_code);
    assert_eq!(error.requirement(), expected_requirement);
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}
