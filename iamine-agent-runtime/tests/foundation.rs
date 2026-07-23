use std::{collections::HashSet, error::Error};

use iamine_agent_runtime::{
    inspect_runtime_foundation, DeclaredAgentPackage, RuntimeFoundationStatus, RuntimeOwner,
    RuntimeOwnerState,
};
use iamine_agents::{parse_and_validate_yaml, PackageLoadStatus};

type TestResult = Result<(), Box<dyn Error>>;

const VALID_MANIFEST: &str =
    include_str!("../../iamine-agents/tests/fixtures/valid/node-doctor-agent.yaml");

#[test]
fn typed_manifest_remains_blocked_at_the_runtime_foundation() -> TestResult {
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let declared_package = DeclaredAgentPackage::from_manifest(&manifest);
    let report = inspect_runtime_foundation(declared_package);

    assert_eq!(report.status(), RuntimeFoundationStatus::Blocked);
    assert!(!report.package_access_available());
    assert!(!report.execution_available());

    Ok(())
}

#[test]
fn every_future_runtime_owner_is_explicit_and_unavailable() -> TestResult {
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let report = inspect_runtime_foundation(DeclaredAgentPackage::from_manifest(&manifest));
    let mut names = HashSet::new();
    let expected_order = [
        RuntimeOwner::PackageReferenceResolver,
        RuntimeOwner::PackageReviewEvidence,
        RuntimeOwner::RuntimeCompatibility,
        RuntimeOwner::InputOutputEnforcement,
        RuntimeOwner::SandboxEnforcement,
        RuntimeOwner::ExecutionLifecycle,
        RuntimeOwner::TimeoutCancelEnforcement,
        RuntimeOwner::HandoffEnforcement,
        RuntimeOwner::OutOfScopeResponseEnforcement,
        RuntimeOwner::RoutingCandidateSelector,
        RuntimeOwner::AuditEventEnforcement,
        RuntimeOwner::ExecutionAuthorization,
        RuntimeOwner::PackageLoadEvidenceIntegration,
        RuntimeOwner::PackageLoader,
        RuntimeOwner::RuntimeExecutor,
    ];
    let actual_order = report
        .owner_statuses()
        .iter()
        .map(|status| status.owner())
        .collect::<Vec<_>>();

    assert_eq!(actual_order, expected_order);
    for status in report.owner_statuses() {
        assert_eq!(status.state(), RuntimeOwnerState::Unavailable);
        assert!(names.insert(status.owner().as_str()));
    }

    Ok(())
}

#[test]
fn declared_package_debug_output_redacts_manifest_values() -> TestResult {
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let package_id = manifest.package_id.clone();
    let declared_package = DeclaredAgentPackage::from_manifest(&manifest);
    let debug_output = format!("{declared_package:?}");

    assert!(debug_output.contains("[redacted]"));
    assert!(!debug_output.contains(&package_id));

    Ok(())
}

#[test]
fn runtime_foundation_does_not_change_the_package_load_gate() -> TestResult {
    let package_report = iamine_agents::assess_package_load_yaml(VALID_MANIFEST)?;
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let runtime_report = inspect_runtime_foundation(DeclaredAgentPackage::from_manifest(&manifest));

    assert_eq!(package_report.status(), PackageLoadStatus::Blocked);
    assert!(!package_report.load_allowed());
    assert_eq!(runtime_report.status(), RuntimeFoundationStatus::Blocked);
    assert!(!runtime_report.execution_available());

    Ok(())
}
