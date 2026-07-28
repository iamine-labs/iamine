#[path = "support/execution_authorization_chain.rs"]
mod execution_authorization_chain;
#[allow(dead_code)]
#[path = "support/routing_policy.rs"]
mod routing_policy;
#[allow(dead_code)]
#[path = "support/sandbox_chain.rs"]
mod sandbox_chain;

use std::{collections::HashSet, error::Error};

use execution_authorization_chain::PreparedAuthorizationChain;
use iamine_agent_runtime::{
    inspect_runtime_foundation, AuditEventEnforcementAuthority, CancellationSource,
    ExecutionAuthorizationAuthority, ExecutionAuthorizationError, ExecutionAuthorizationErrorCode,
    ExecutionAuthorizationEvidence, ExecutionAuthorizationEvidenceStatus,
    ExecutionAuthorizationRequest, ExecutionAuthorizationRequirement, ExecutionLifecycleAuthority,
    ExecutionLifecycleState, InputOutputEnforcementAuthority, InputOutputPolicy,
    PackageReviewAuthority, RoutingCandidateAvailability, RoutingCandidateCompatibility,
    RoutingCandidateRef, RoutingCandidateRiskClass, RoutingCandidateSandbox,
    RoutingCandidateSelectionAuthority, RoutingCandidateSelectionOutcome,
    RoutingResourceRequirements, RoutingSelectionRequestRef, RuntimeCompatibilityAuthority,
    RuntimeFoundationStatus, RuntimeLanguageAvailability, RuntimeLanguageDecision,
    RuntimeLanguageMode, RuntimeNetworkAvailability, RuntimeOwner, RuntimeOwnerState,
    RuntimeResourceEnvelope, SandboxEnforcementAuthority, SandboxEnforcementPolicy,
    EXECUTION_AUTHORIZATION_SCHEMA_VERSION,
};
use iamine_agents::{
    assess_package_load_yaml, PackageLoadBlockerCode, PackageLoadStatus, PermissionConfirmation,
    ResourceOperatingMode, ScopeRequestClassification,
};

use routing_policy::{
    permission_request, refused_permission, scope_request, PACKAGE_ID, TASK_TYPE,
};
use sandbox_chain::{PackageFixture, VALID_MANIFEST};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const FOREIGN_PACKAGE_ID: &str = "iamine.beta.other-agent";
const LOCAL_READONLY_CATEGORIES: [&str; 1] = ["local_readonly"];
const SHELL_CATEGORIES: [&str; 1] = ["arbitrary_shell"];

#[test]
fn exact_owner_chain_emits_passive_authorization_evidence() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();

    let evidence = authority.authorize(&request)?;

    assert_eq!(
        evidence.schema_version(),
        EXECUTION_AUTHORIZATION_SCHEMA_VERSION
    );
    assert_eq!(
        evidence.status(),
        ExecutionAuthorizationEvidenceStatus::Authorized
    );
    assert_eq!(evidence.requirements().len(), 14);
    assert_eq!(evidence.selected_candidate_id(), "candidate-local");
    assert_eq!(
        evidence.lifecycle_state(),
        ExecutionLifecycleState::ScopeCheck
    );
    assert_eq!(evidence.lifecycle_revision(), 2);
    assert!(evidence.authorization_recorded());
    assert!(evidence.execution_authorized());
    assert!(authority.verifies(&evidence, &request));
    assert_no_side_effects(&evidence);
    Ok(())
}

#[test]
fn missing_or_foreign_package_review_fails_closed() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let bare = ExecutionAuthorizationRequest::new(
        chain.subject,
        &chain.scope_policy,
        scope_request(PACKAGE_ID, ScopeRequestClassification::InScopeCandidate),
        &chain.permission_policy,
        permission_request(
            PACKAGE_ID,
            "inspect_status",
            &LOCAL_READONLY_CATEGORIES,
            PermissionConfirmation::NotProvided,
        ),
    );
    assert_error(
        authority.authorize(&bare),
        ExecutionAuthorizationErrorCode::PackageReviewNotVerified,
        ExecutionAuthorizationRequirement::PackageReviewEvidence,
    )?;

    let foreign = PackageReviewAuthority::new_operator_local();
    let request = chain
        .request()
        .with_package_review(&foreign, &chain.review_evidence);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::PackageReviewNotVerified,
        ExecutionAuthorizationRequirement::PackageReviewEvidence,
    )
}

#[test]
fn foreign_compatibility_input_output_and_sandbox_authorities_are_rejected() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();

    let foreign_compatibility = RuntimeCompatibilityAuthority::new_operator_local(
        RuntimeLanguageDecision::new(
            RuntimeLanguageMode::RustNativeOfficial,
            RuntimeLanguageAvailability::Available,
        ),
        RuntimeResourceEnvelope::new(2, 512, 84, RuntimeNetworkAvailability::None)?,
    );
    let request = chain
        .request()
        .with_runtime_compatibility(&foreign_compatibility, &chain.compatibility_evidence);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::RuntimeCompatibilityNotVerified,
        ExecutionAuthorizationRequirement::RuntimeCompatibilityEvidence,
    )?;

    let foreign_input_output = InputOutputEnforcementAuthority::new_operator_local(
        InputOutputPolicy::new(128, 128, false)?,
    );
    let request = chain
        .request()
        .with_input_output(&foreign_input_output, &chain.input_output_evidence);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::InputOutputEnforcementNotVerified,
        ExecutionAuthorizationRequirement::InputOutputEnforcementEvidence,
    )?;

    let foreign_sandbox = SandboxEnforcementAuthority::new_operator_local(
        SandboxEnforcementPolicy::new(30_000, 128)?,
    )?;
    let request = chain
        .request()
        .with_sandbox(&foreign_sandbox, &chain.sandbox_evidence);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::SandboxEnforcementNotVerified,
        ExecutionAuthorizationRequirement::SandboxEnforcementEvidence,
    )
}

#[test]
fn lifecycle_must_be_exact_and_at_scope_check() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();

    let foreign = ExecutionLifecycleAuthority::new_operator_local();
    let request = chain
        .request()
        .with_lifecycle(&foreign, &chain.lifecycle_record);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::LifecycleRecordNotVerified,
        ExecutionAuthorizationRequirement::LifecycleRecord,
    )?;

    let queued = chain.lifecycle_authority.queue(
        &chain.sandbox_authority,
        &chain.sandbox_evidence,
        chain.subject,
    )?;
    let request = chain
        .request()
        .with_lifecycle(&chain.lifecycle_authority, &queued);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::LifecycleNotReady,
        ExecutionAuthorizationRequirement::LifecycleState,
    )
}

#[test]
fn timeout_control_must_match_and_have_no_pending_cancellation() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let foreign = iamine_agent_runtime::TimeoutCancelAuthority::new_operator_local();
    let request = chain
        .request()
        .with_timeout_cancel(&foreign, &chain.timeout_control);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::TimeoutCancelControlNotVerified,
        ExecutionAuthorizationRequirement::TimeoutCancelControl,
    )?;

    let cancellation = chain.timeout_control.cancellation_handle();
    let _ = chain.timeout_authority.request_cancellation(
        &chain.timeout_control,
        &chain.lifecycle_authority,
        &chain.lifecycle_record,
        &cancellation,
        chain.lifecycle_record.revision(),
        CancellationSource::Operator,
    )?;
    assert_error(
        authority.authorize(&chain.request()),
        ExecutionAuthorizationErrorCode::CancellationAlreadyRequested,
        ExecutionAuthorizationRequirement::TimeoutCancelControl,
    )
}

#[test]
fn policy_inputs_are_recomputed_and_bound_to_the_reviewed_package() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let package_mismatch = chain.request_with(
        scope_request(
            FOREIGN_PACKAGE_ID,
            ScopeRequestClassification::InScopeCandidate,
        ),
        permission_request(
            FOREIGN_PACKAGE_ID,
            "inspect_status",
            &LOCAL_READONLY_CATEGORIES,
            PermissionConfirmation::NotProvided,
        ),
    );
    assert_error(
        authority.authorize(&package_mismatch),
        ExecutionAuthorizationErrorCode::PackageIdentityMismatch,
        ExecutionAuthorizationRequirement::PackageIdentity,
    )?;

    let denied_scope = chain.request_with(
        scope_request(PACKAGE_ID, ScopeRequestClassification::PromptInjection),
        permission_request(
            PACKAGE_ID,
            "inspect_status",
            &LOCAL_READONLY_CATEGORIES,
            PermissionConfirmation::NotProvided,
        ),
    );
    assert_error(
        authority.authorize(&denied_scope),
        ExecutionAuthorizationErrorCode::ScopeNotAllowed,
        ExecutionAuthorizationRequirement::ScopeEvaluation,
    )?;

    let denied_permission = chain.request_with(
        scope_request(PACKAGE_ID, ScopeRequestClassification::InScopeCandidate),
        permission_request(
            PACKAGE_ID,
            "run_shell",
            &SHELL_CATEGORIES,
            PermissionConfirmation::TrustedOrchestratorConfirmed,
        ),
    );
    assert_error(
        authority.authorize(&denied_permission),
        ExecutionAuthorizationErrorCode::PermissionNotAllowed,
        ExecutionAuthorizationRequirement::PermissionEvaluation,
    )
}

#[test]
fn routing_requires_one_candidate_bound_to_the_exact_sandbox() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let empty = chain.routing_authority.select(
        routing_request()?,
        &[],
        &chain.compatibility_authority,
        &chain.sandbox_authority,
    )?;
    assert_eq!(
        empty.outcome(),
        RoutingCandidateSelectionOutcome::NoCandidate
    );
    let request = chain
        .request()
        .with_routing(&chain.routing_authority, &empty);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::CandidateNotSelected,
        ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
    )?;

    let alternate_sandbox = chain.sandbox_authority.establish(
        &chain.compatibility_authority,
        &chain.compatibility_evidence,
        &chain.input_output_authority,
        &chain.input_output_evidence,
        chain.subject,
    )?;
    let alternate_route = chain.routing_authority.select(
        routing_request()?,
        &[eligible_candidate(&chain, &alternate_sandbox)],
        &chain.compatibility_authority,
        &chain.sandbox_authority,
    )?;
    let request = chain
        .request()
        .with_routing(&chain.routing_authority, &alternate_route);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::RoutingSubjectNotVerified,
        ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
    )
}

#[test]
fn routing_authority_and_audit_projections_must_match_exactly() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let foreign_routing = RoutingCandidateSelectionAuthority::new_operator_local();
    let request = chain
        .request()
        .with_routing(&foreign_routing, &chain.routing_evidence);
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::RoutingSelectionNotVerified,
        ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
    )?;

    let foreign_audit = AuditEventEnforcementAuthority::new_operator_local();
    let foreign_scope = foreign_audit.enforce_scope(&chain.scope);
    let request = chain.request().with_audit(
        &chain.audit_authority,
        &foreign_scope,
        &chain.permission_audit,
        &chain.lifecycle_audit,
    );
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::ScopeAuditNotVerified,
        ExecutionAuthorizationRequirement::AuditScopeEvidence,
    )?;

    let refused = refused_permission(&chain.scope)?;
    let refused_audit = chain.audit_authority.enforce_permission(&refused);
    let request = chain.request().with_audit(
        &chain.audit_authority,
        &chain.scope_audit,
        &refused_audit,
        &chain.lifecycle_audit,
    );
    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::PermissionAuditNotVerified,
        ExecutionAuthorizationRequirement::AuditPermissionEvidence,
    )
}

#[test]
fn lifecycle_audit_cannot_be_replayed_from_another_execution() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let mut other_record = chain.lifecycle_authority.queue(
        &chain.sandbox_authority,
        &chain.sandbox_evidence,
        chain.subject,
    )?;
    let _ = chain.lifecycle_authority.transition(
        &mut other_record,
        0,
        ExecutionLifecycleState::PermissionPending,
    )?;
    let _ = chain.lifecycle_authority.transition(
        &mut other_record,
        1,
        ExecutionLifecycleState::ScopeCheck,
    )?;
    let other_audit = chain
        .audit_authority
        .enforce_lifecycle(&chain.lifecycle_authority, &other_record)?;
    let request = chain.request().with_audit(
        &chain.audit_authority,
        &chain.scope_audit,
        &chain.permission_audit,
        &other_audit,
    );

    assert_error(
        authority.authorize(&request),
        ExecutionAuthorizationErrorCode::LifecycleAuditNotVerified,
        ExecutionAuthorizationRequirement::AuditLifecycleEvidence,
    )
}

#[test]
fn authorization_authorities_are_isolated_and_replay_fails_after_cancellation() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let foreign = ExecutionAuthorizationAuthority::new_operator_local();
    let evidence = authority.authorize(&chain.request())?;

    assert!(authority.verifies(&evidence, &chain.request()));
    assert!(!foreign.verifies(&evidence, &chain.request()));

    let cancellation = chain.timeout_control.cancellation_handle();
    let _ = chain.timeout_authority.request_cancellation(
        &chain.timeout_control,
        &chain.lifecycle_authority,
        &chain.lifecycle_record,
        &cancellation,
        chain.lifecycle_record.revision(),
        CancellationSource::ScopeViolation,
    )?;
    assert!(!authority.verifies(&evidence, &chain.request()));
    Ok(())
}

#[test]
fn authorization_replay_fails_after_lifecycle_revision_changes() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let mut chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let evidence = authority.authorize(&chain.request())?;

    let _ = chain.lifecycle_authority.transition(
        &mut chain.lifecycle_record,
        2,
        ExecutionLifecycleState::HandoffRequired,
    )?;

    assert!(!authority.verifies(&evidence, &chain.request()));
    assert_error(
        authority.authorize(&chain.request()),
        ExecutionAuthorizationErrorCode::LifecycleNotReady,
        ExecutionAuthorizationRequirement::LifecycleState,
    )
}

#[test]
fn debug_errors_and_evidence_do_not_expose_private_or_package_values() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let evidence = authority.authorize(&request)?;

    for debug in [
        format!("{authority:?}"),
        format!("{request:?}"),
        format!("{evidence:?}"),
    ] {
        assert!(debug.contains("[redacted]"));
        for forbidden in [
            PACKAGE_ID,
            "candidate-local",
            "inspect_status",
            "username",
            "hostname",
            "home/",
            "private_key",
            "wallet",
            "sensitive-prompt-payload",
            "sensitive-output-payload",
        ] {
            assert!(
                !debug.contains(forbidden),
                "debug output exposed forbidden value: {forbidden}"
            );
        }
    }

    let denied = chain.request_with(
        scope_request(PACKAGE_ID, ScopeRequestClassification::Dangerous),
        permission_request(
            PACKAGE_ID,
            "inspect_status",
            &LOCAL_READONLY_CATEGORIES,
            PermissionConfirmation::NotProvided,
        ),
    );
    let error = authority.authorize(&denied).expect_err("must fail closed");
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}

#[test]
fn package_load_and_runtime_owners_remain_unavailable() -> TestResult {
    let manifest = iamine_agents::parse_and_validate_yaml(VALID_MANIFEST)?;
    let declared = iamine_agent_runtime::DeclaredAgentPackage::from_manifest(&manifest);
    let report = inspect_runtime_foundation(declared);
    assert_eq!(report.status(), RuntimeFoundationStatus::Blocked);
    assert!(!report.package_access_available());
    assert!(!report.execution_available());
    assert!(report.owner_statuses().iter().any(|status| {
        status.owner() == RuntimeOwner::ExecutionAuthorization
            && status.state() == RuntimeOwnerState::Unavailable
    }));

    let load = assess_package_load_yaml(VALID_MANIFEST)?;
    assert_eq!(load.status(), PackageLoadStatus::Blocked);
    assert!(load
        .blockers()
        .contains(&PackageLoadBlockerCode::ExecutionAuthorizationUnavailable));
    Ok(())
}

fn routing_request() -> TestResult<RoutingSelectionRequestRef<'static>> {
    Ok(RoutingSelectionRequestRef::new(
        TASK_TYPE,
        ResourceOperatingMode::LocalReadonly,
        RoutingResourceRequirements::new(1, 256, 20, RuntimeNetworkAvailability::None)?,
        RoutingCandidateRiskClass::Moderate,
    ))
}

fn eligible_candidate<'a>(
    chain: &'a PreparedAuthorizationChain<'a>,
    sandbox: &'a iamine_agent_runtime::SandboxEnforcementEvidence<'a>,
) -> RoutingCandidateRef<'a> {
    RoutingCandidateRef::new(
        "candidate-alternate",
        TASK_TYPE,
        RoutingCandidateRiskClass::Low,
        RoutingCandidateAvailability::Available,
        chain.subject,
        chain.scope,
        chain.permission,
        RoutingCandidateCompatibility::Compatible(&chain.compatibility_evidence),
        RoutingCandidateSandbox::Prepared(sandbox),
    )
}

fn assert_no_side_effects(evidence: &ExecutionAuthorizationEvidence<'_>) {
    assert!(!evidence.package_load_allowed());
    assert!(!evidence.package_loaded());
    assert!(!evidence.runtime_active());
    assert!(!evidence.sandbox_active());
    assert!(!evidence.scheduler_mutated());
    assert!(!evidence.transport_started());
    assert!(!evidence.persisted());
    assert!(!evidence.external_event_emitted());
}

fn assert_error<T>(
    result: Result<T, ExecutionAuthorizationError>,
    code: ExecutionAuthorizationErrorCode,
    requirement: ExecutionAuthorizationRequirement,
) -> TestResult {
    let error = result.err().ok_or("expected authorization error")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}

#[test]
fn requirement_codes_are_unique_and_stable() {
    let requirements = [
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
    let unique = requirements
        .iter()
        .map(|requirement| requirement.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(unique.len(), requirements.len());
}
