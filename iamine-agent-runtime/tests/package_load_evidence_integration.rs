#[allow(dead_code)]
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
    inspect_runtime_foundation, CancellationSource, ExecutionAuthorizationAuthority,
    PackageLoadEvidence, PackageLoadEvidenceAuthority, PackageLoadEvidenceError,
    PackageLoadEvidenceErrorCode, PackageLoadEvidenceRequirement, PackageLoadEvidenceStatus,
    PackageReferenceKind, RuntimeFoundationStatus, RuntimeOwner, RuntimeOwnerState,
    PACKAGE_LOAD_EVIDENCE_SCHEMA_VERSION,
};
use iamine_agents::{assess_package_load_yaml, PackageLoadBlockerCode, PackageLoadStatus};
use routing_policy::PACKAGE_ID;
use sandbox_chain::{PackageFixture, VALID_MANIFEST};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const VALID_CAPABILITY: &str = include_str!(
    "../../iamine-agents/tests/fixtures/descriptive_metadata/valid/capability-metadata.yaml"
);
const VALID_BOUNDARY: &str = include_str!(
    "../../iamine-agents/tests/fixtures/boundary_eval/valid/agent-boundary-tests.yaml"
);

#[test]
fn exact_authorization_emits_passive_package_load_evidence() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();

    let evidence = authority.integrate(&authorization_authority, &authorization, &request)?;

    assert_eq!(
        evidence.schema_version(),
        PACKAGE_LOAD_EVIDENCE_SCHEMA_VERSION
    );
    assert_eq!(evidence.status(), PackageLoadEvidenceStatus::Eligible);
    assert_eq!(evidence.requirements().len(), 9);
    assert!(evidence
        .requirements()
        .contains(&PackageLoadEvidenceRequirement::ReferenceContract));
    assert!(evidence
        .requirements()
        .contains(&PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence));
    assert_eq!(evidence.lifecycle_revision(), 2);
    assert!(evidence.evidence_integrated());
    assert!(evidence.package_load_allowed());
    assert!(authority.verifies(
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    ));
    assert_no_side_effects(&evidence);
    Ok(())
}

#[test]
fn foreign_or_missing_authorization_provenance_fails_closed() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let foreign = ExecutionAuthorizationAuthority::new_operator_local();
    let authority = PackageLoadEvidenceAuthority::new_operator_local();

    assert_error(
        authority.integrate(&foreign, &authorization, &request),
        PackageLoadEvidenceErrorCode::ExecutionAuthorizationNotVerified,
        PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence,
    )
}

#[test]
fn evidence_is_bound_to_the_exact_package_and_authorization_instance() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let second_authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();
    let evidence = authority.integrate(&authorization_authority, &authorization, &request)?;

    assert!(!authority.verifies(
        &evidence,
        &authorization_authority,
        &second_authorization,
        &request,
    ));

    let other_fixture = PackageFixture::valid()?;
    let other_references = other_fixture.resolve()?;
    let other_chain = PreparedAuthorizationChain::new(other_fixture.subject(&other_references))?;
    assert_error(
        authority.integrate(
            &authorization_authority,
            &authorization,
            &other_chain.request(),
        ),
        PackageLoadEvidenceErrorCode::ExecutionAuthorizationNotVerified,
        PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence,
    )
}

#[test]
fn stale_authorization_cannot_be_replayed_after_cancellation() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();
    let evidence = authority.integrate(&authorization_authority, &authorization, &request)?;

    let cancellation = chain.timeout_control.cancellation_handle();
    let _ = chain.timeout_authority.request_cancellation(
        &chain.timeout_control,
        &chain.lifecycle_authority,
        &chain.lifecycle_record,
        &cancellation,
        chain.lifecycle_record.revision(),
        CancellationSource::Operator,
    )?;
    let current = chain.request();

    assert!(!authority.verifies(
        &evidence,
        &authorization_authority,
        &authorization,
        &current,
    ));
    assert_error(
        authority.integrate(&authorization_authority, &authorization, &current),
        PackageLoadEvidenceErrorCode::ExecutionAuthorizationNotVerified,
        PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence,
    )
}

#[test]
fn integration_authorities_are_isolated() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();
    let evidence = authority.integrate(&authorization_authority, &authorization, &request)?;
    let foreign = PackageLoadEvidenceAuthority::new_operator_local();

    assert!(!foreign.verifies(
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    ));
    Ok(())
}

#[test]
fn every_reviewed_reference_must_pass_its_canonical_validator() -> TestResult {
    let fixture = PackageFixture::valid()?;
    fixture.overwrite_reference(
        PackageReferenceKind::CapabilityMetadata,
        b"not-valid-capability-metadata",
    )?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();

    assert_error(
        authority.integrate(&authorization_authority, &authorization, &request),
        PackageLoadEvidenceErrorCode::ReferenceValidationFailed,
        PackageLoadEvidenceRequirement::CapabilityMetadataValidation,
    )
}

#[test]
fn validated_references_must_target_the_exact_reviewed_package() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let foreign = VALID_CAPABILITY.replace(PACKAGE_ID, "iamine.beta.other-agent");
    fixture.overwrite_reference(PackageReferenceKind::CapabilityMetadata, foreign.as_bytes())?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();

    assert_error(
        authority.integrate(&authorization_authority, &authorization, &request),
        PackageLoadEvidenceErrorCode::PackageIdentityMismatch,
        PackageLoadEvidenceRequirement::ReferenceContract,
    )
}

#[test]
fn cross_reference_contract_mismatches_fail_closed() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let mismatched = VALID_BOUNDARY.replace("scope_ref: agent-scope.yaml", "scope_ref: other.yaml");
    fixture.overwrite_reference(PackageReferenceKind::BoundaryTests, mismatched.as_bytes())?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();

    assert_error(
        authority.integrate(&authorization_authority, &authorization, &request),
        PackageLoadEvidenceErrorCode::ReferenceContractMismatch,
        PackageLoadEvidenceRequirement::ReferenceContract,
    )
}

#[test]
fn requirement_and_error_codes_are_unique_and_stable() {
    let requirements = [
        PackageLoadEvidenceRequirement::ScopeManifestValidation,
        PackageLoadEvidenceRequirement::CapabilityMetadataValidation,
        PackageLoadEvidenceRequirement::ExpertiseMetadataValidation,
        PackageLoadEvidenceRequirement::ResourceRequirementsValidation,
        PackageLoadEvidenceRequirement::PermissionModelValidation,
        PackageLoadEvidenceRequirement::AuditPolicyValidation,
        PackageLoadEvidenceRequirement::BoundaryEvalValidation,
        PackageLoadEvidenceRequirement::ReferenceContract,
        PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence,
    ]
    .into_iter()
    .map(PackageLoadEvidenceRequirement::as_str)
    .collect::<HashSet<_>>();
    let errors = [
        PackageLoadEvidenceErrorCode::ReferenceMissing,
        PackageLoadEvidenceErrorCode::ReferenceEncodingInvalid,
        PackageLoadEvidenceErrorCode::ReferenceValidationFailed,
        PackageLoadEvidenceErrorCode::PackageIdentityMismatch,
        PackageLoadEvidenceErrorCode::ReferenceContractMismatch,
        PackageLoadEvidenceErrorCode::ExecutionAuthorizationNotVerified,
    ]
    .into_iter()
    .map(PackageLoadEvidenceErrorCode::as_str)
    .collect::<HashSet<_>>();

    assert_eq!(requirements.len(), 9);
    assert_eq!(errors.len(), 6);
    assert!(requirements.contains("scope_manifest_validation"));
    assert!(requirements.contains("execution_authorization_evidence"));
    assert!(errors.contains("reference_validation_failed"));
    assert!(errors.contains("execution_authorization_not_verified"));
}

#[test]
fn debug_and_errors_do_not_expose_package_or_private_values() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let authority = PackageLoadEvidenceAuthority::new_operator_local();
    let evidence = authority.integrate(&authorization_authority, &authorization, &request)?;

    for debug in [format!("{authority:?}"), format!("{evidence:?}")] {
        assert!(debug.contains("[redacted]"));
        for forbidden in [
            PACKAGE_ID,
            "candidate-local",
            "username",
            "hostname",
            "home/",
            "private_key",
            "wallet",
        ] {
            assert!(!debug.contains(forbidden));
        }
    }

    let foreign = ExecutionAuthorizationAuthority::new_operator_local();
    let error = authority
        .integrate(&foreign, &authorization, &request)
        .expect_err("foreign authorization must fail");
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}

#[test]
fn static_gate_and_runtime_owners_remain_closed() -> TestResult {
    let load = assess_package_load_yaml(VALID_MANIFEST)?;
    assert_eq!(load.status(), PackageLoadStatus::Blocked);
    assert!(!load.load_allowed());
    assert_eq!(load.blockers().len(), 19);
    assert!(load
        .blockers()
        .contains(&PackageLoadBlockerCode::ExecutionAuthorizationUnavailable));

    let manifest = iamine_agents::parse_and_validate_yaml(VALID_MANIFEST)?;
    let declared = iamine_agent_runtime::DeclaredAgentPackage::from_manifest(&manifest);
    let foundation = inspect_runtime_foundation(declared);
    assert_eq!(foundation.status(), RuntimeFoundationStatus::Blocked);
    assert!(!foundation.package_access_available());
    assert!(!foundation.execution_available());
    for owner in [
        RuntimeOwner::PackageLoadEvidenceIntegration,
        RuntimeOwner::PackageLoader,
        RuntimeOwner::RuntimeExecutor,
    ] {
        assert!(foundation.owner_statuses().iter().any(|status| {
            status.owner() == owner && status.state() == RuntimeOwnerState::Unavailable
        }));
    }
    Ok(())
}

fn assert_no_side_effects(evidence: &PackageLoadEvidence<'_>) {
    assert!(!evidence.package_loaded());
    assert!(!evidence.execution_started());
    assert!(!evidence.runtime_active());
    assert!(!evidence.sandbox_active());
    assert!(!evidence.scheduler_mutated());
    assert!(!evidence.transport_started());
    assert!(!evidence.persisted());
    assert!(!evidence.external_event_emitted());
}

fn assert_error<T>(
    result: Result<T, PackageLoadEvidenceError>,
    code: PackageLoadEvidenceErrorCode,
    requirement: PackageLoadEvidenceRequirement,
) -> TestResult {
    let error = result.err().ok_or("expected package-load evidence error")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}
