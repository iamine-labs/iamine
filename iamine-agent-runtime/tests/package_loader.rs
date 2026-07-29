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
    ExecutionAuthorizationEvidence, LoadedAgentPackage, LoadedAgentPackageStatus,
    PackageLoadEvidence, PackageLoadEvidenceAuthority, PackageLoaderAuthority, PackageLoaderError,
    PackageLoaderErrorCode, PackageLoaderRequirement, PackageReferenceKind,
    RuntimeFoundationStatus, RuntimeOwner, RuntimeOwnerState, LOADED_AGENT_PACKAGE_SCHEMA_VERSION,
};
use iamine_agents::{assess_package_load_yaml, PackageLoadStatus};
use routing_policy::PACKAGE_ID;
use sandbox_chain::{PackageFixture, VALID_MANIFEST};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn exact_eligibility_evidence_loads_bounded_package_snapshot() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, evidence_authority, evidence) =
        integrated_evidence(&chain)?;
    let request = chain.request();
    let authority = PackageLoaderAuthority::new_operator_local();

    let loaded = authority.load(
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    )?;

    assert_eq!(loaded.schema_version(), LOADED_AGENT_PACKAGE_SCHEMA_VERSION);
    assert_eq!(loaded.status(), LoadedAgentPackageStatus::Loaded);
    assert_eq!(loaded.requirements().len(), 3);
    assert_eq!(loaded.reference_count(), 7);
    assert_eq!(loaded.total_reference_bytes(), references.total_bytes());
    assert_eq!(loaded.lifecycle_revision(), 2);
    assert!(loaded.package_load_evidence_verified());
    assert!(loaded.package_loaded());
    assert!(authority.verifies(
        &loaded,
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    ));
    assert_no_execution_side_effects(&loaded);
    Ok(())
}

#[test]
fn foreign_package_load_evidence_authority_fails_closed() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, _evidence_authority, evidence) =
        integrated_evidence(&chain)?;
    let request = chain.request();
    let foreign = PackageLoadEvidenceAuthority::new_operator_local();
    let authority = PackageLoaderAuthority::new_operator_local();

    assert_loader_error(authority.load(
        &foreign,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    ))
}

#[test]
fn stale_evidence_cannot_load_after_cancellation() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, evidence_authority, evidence) =
        integrated_evidence(&chain)?;
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
    let authority = PackageLoaderAuthority::new_operator_local();

    assert_loader_error(authority.load(
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &current,
    ))
}

#[test]
fn loaded_package_is_bound_to_exact_evidence_instance() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, evidence_authority, evidence) =
        integrated_evidence(&chain)?;
    let request = chain.request();
    let second_evidence =
        evidence_authority.integrate(&authorization_authority, &authorization, &request)?;
    let authority = PackageLoaderAuthority::new_operator_local();
    let loaded = authority.load(
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    )?;

    assert!(!authority.verifies(
        &loaded,
        &evidence_authority,
        &second_evidence,
        &authorization_authority,
        &authorization,
        &request,
    ));
    Ok(())
}

#[test]
fn loader_authorities_are_isolated() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, evidence_authority, evidence) =
        integrated_evidence(&chain)?;
    let request = chain.request();
    let authority = PackageLoaderAuthority::new_operator_local();
    let loaded = authority.load(
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    )?;
    let foreign = PackageLoaderAuthority::new_operator_local();

    assert!(!foreign.verifies(
        &loaded,
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    ));
    Ok(())
}

#[test]
fn loading_uses_the_exact_resolved_snapshot_without_reopening_paths() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, evidence_authority, evidence) =
        integrated_evidence(&chain)?;
    fixture.overwrite_reference(
        PackageReferenceKind::CapabilityMetadata,
        b"changed-after-bounded-resolution",
    )?;
    let request = chain.request();
    let authority = PackageLoaderAuthority::new_operator_local();

    let loaded = authority.load(
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    )?;

    assert_eq!(loaded.reference_count(), 7);
    assert_eq!(loaded.total_reference_bytes(), references.total_bytes());
    assert!(authority.verifies(
        &loaded,
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    ));
    Ok(())
}

#[test]
fn requirement_and_error_codes_are_unique_and_stable() {
    let requirements = [
        PackageLoaderRequirement::PackageLoadEvidence,
        PackageLoaderRequirement::BoundedReferenceSnapshot,
        PackageLoaderRequirement::ValidatedReferenceContract,
    ]
    .into_iter()
    .map(PackageLoaderRequirement::as_str)
    .collect::<HashSet<_>>();
    let errors = [PackageLoaderErrorCode::PackageLoadEvidenceNotVerified]
        .into_iter()
        .map(PackageLoaderErrorCode::as_str)
        .collect::<HashSet<_>>();

    assert_eq!(requirements.len(), 3);
    assert_eq!(errors.len(), 1);
    assert!(requirements.contains("package_load_evidence"));
    assert!(requirements.contains("bounded_reference_snapshot"));
    assert!(requirements.contains("validated_reference_contract"));
    assert!(errors.contains("package_load_evidence_not_verified"));
}

#[test]
fn debug_and_errors_do_not_expose_package_or_private_values() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, evidence_authority, evidence) =
        integrated_evidence(&chain)?;
    let request = chain.request();
    let authority = PackageLoaderAuthority::new_operator_local();
    let loaded = authority.load(
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    )?;

    for debug in [format!("{authority:?}"), format!("{loaded:?}")] {
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

    let foreign = PackageLoadEvidenceAuthority::new_operator_local();
    let error = authority
        .load(
            &foreign,
            &evidence,
            &authorization_authority,
            &authorization,
            &request,
        )
        .expect_err("foreign package-load evidence authority must fail");
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}

#[test]
fn static_gate_and_runtime_executor_remain_closed_after_loading() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let chain = PreparedAuthorizationChain::new(fixture.subject(&references))?;
    let (authorization_authority, authorization, evidence_authority, evidence) =
        integrated_evidence(&chain)?;
    let request = chain.request();
    let authority = PackageLoaderAuthority::new_operator_local();
    let loaded = authority.load(
        &evidence_authority,
        &evidence,
        &authorization_authority,
        &authorization,
        &request,
    )?;

    let static_load = assess_package_load_yaml(VALID_MANIFEST)?;
    assert_eq!(static_load.status(), PackageLoadStatus::Blocked);
    assert!(!static_load.load_allowed());

    let foundation =
        inspect_runtime_foundation(iamine_agent_runtime::DeclaredAgentPackage::from_manifest(
            &iamine_agents::parse_and_validate_yaml(VALID_MANIFEST)?,
        ));
    assert_eq!(foundation.status(), RuntimeFoundationStatus::Blocked);
    assert!(!foundation.package_access_available());
    assert!(!foundation.execution_available());
    for owner in [RuntimeOwner::PackageLoader, RuntimeOwner::RuntimeExecutor] {
        assert!(foundation.owner_statuses().iter().any(|status| {
            status.owner() == owner && status.state() == RuntimeOwnerState::Unavailable
        }));
    }
    assert!(loaded.package_loaded());
    assert_no_execution_side_effects(&loaded);
    Ok(())
}

fn integrated_evidence<'subject>(
    chain: &PreparedAuthorizationChain<'subject>,
) -> TestResult<(
    ExecutionAuthorizationAuthority,
    ExecutionAuthorizationEvidence<'subject>,
    PackageLoadEvidenceAuthority,
    PackageLoadEvidence<'subject>,
)> {
    let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
    let request = chain.request();
    let authorization = authorization_authority.authorize(&request)?;
    let evidence_authority = PackageLoadEvidenceAuthority::new_operator_local();
    let evidence =
        evidence_authority.integrate(&authorization_authority, &authorization, &request)?;
    Ok((
        authorization_authority,
        authorization,
        evidence_authority,
        evidence,
    ))
}

fn assert_no_execution_side_effects(loaded: &LoadedAgentPackage<'_>) {
    assert!(!loaded.execution_allowed());
    assert!(!loaded.execution_started());
    assert!(!loaded.runtime_active());
    assert!(!loaded.sandbox_active());
    assert!(!loaded.scheduler_mutated());
    assert!(!loaded.transport_started());
    assert!(!loaded.persisted());
    assert!(!loaded.external_event_emitted());
}

fn assert_loader_error<T>(result: Result<T, PackageLoaderError>) -> TestResult {
    let error = result.err().ok_or("expected package-loader error")?;
    assert_eq!(
        error.code(),
        PackageLoaderErrorCode::PackageLoadEvidenceNotVerified
    );
    assert_eq!(
        error.requirement(),
        PackageLoaderRequirement::PackageLoadEvidence
    );
    assert!(!error.to_string().contains(PACKAGE_ID));
    Ok(())
}
