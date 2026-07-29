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
    RuntimeFoundationStatus, RuntimeOwner, RuntimeOwnerState, PACKAGE_LOAD_EVIDENCE_SCHEMA_VERSION,
};
use iamine_agents::{assess_package_load_yaml, PackageLoadBlockerCode, PackageLoadStatus};
use routing_policy::PACKAGE_ID;
use sandbox_chain::{PackageFixture, VALID_MANIFEST};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

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
    assert_eq!(
        evidence.requirements(),
        [PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence]
    );
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

    assert_error(authority.integrate(&foreign, &authorization, &request))
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
    assert_error(authority.integrate(
        &authorization_authority,
        &authorization,
        &other_chain.request(),
    ))
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
    assert_error(authority.integrate(&authorization_authority, &authorization, &current))
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
fn requirement_and_error_codes_are_unique_and_stable() {
    let requirements = [PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence]
        .into_iter()
        .map(PackageLoadEvidenceRequirement::as_str)
        .collect::<HashSet<_>>();
    let errors = [PackageLoadEvidenceErrorCode::ExecutionAuthorizationNotVerified]
        .into_iter()
        .map(PackageLoadEvidenceErrorCode::as_str)
        .collect::<HashSet<_>>();

    assert_eq!(
        requirements,
        HashSet::from(["execution_authorization_evidence"])
    );
    assert_eq!(
        errors,
        HashSet::from(["execution_authorization_not_verified"])
    );
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

fn assert_error<T>(result: Result<T, PackageLoadEvidenceError>) -> TestResult {
    let error = result.err().ok_or("expected package-load evidence error")?;
    assert_eq!(
        error.code(),
        PackageLoadEvidenceErrorCode::ExecutionAuthorizationNotVerified
    );
    assert_eq!(
        error.requirement(),
        PackageLoadEvidenceRequirement::ExecutionAuthorizationEvidence
    );
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}
