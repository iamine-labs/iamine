use std::collections::HashSet;
use std::error::Error;
use std::io;

use iamine_agents::{
    assess_package_load_yaml, ManifestError, ManifestErrorCode, PackageLoadBlockerCode,
    PackageLoadStatus, MAX_MANIFEST_BYTES,
};

const VALID_MANIFEST: &str = include_str!("fixtures/valid/node-doctor-agent.yaml");
const EXPECTED_BLOCKER_COUNT: usize = 19;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn valid_root_manifest_remains_blocked_for_package_load() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;

    assert_eq!(report.status(), PackageLoadStatus::Blocked);
    assert!(!report.load_allowed());
    assert_eq!(report.blockers().len(), EXPECTED_BLOCKER_COUNT);
    Ok(())
}

#[test]
fn report_lists_every_current_downstream_gate() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;
    let expected = [
        PackageLoadBlockerCode::ScopeManifestValidatorUnavailable,
        PackageLoadBlockerCode::CapabilityMetadataValidatorUnavailable,
        PackageLoadBlockerCode::ExpertiseMetadataValidatorUnavailable,
        PackageLoadBlockerCode::ResourceRequirementsValidatorUnavailable,
        PackageLoadBlockerCode::PermissionModelValidatorUnavailable,
        PackageLoadBlockerCode::AuditPolicyValidatorUnavailable,
        PackageLoadBlockerCode::BoundaryEvalValidatorUnavailable,
        PackageLoadBlockerCode::LocalRegistryReviewUnavailable,
        PackageLoadBlockerCode::LanguagePolicyReviewUnavailable,
        PackageLoadBlockerCode::DependencyPolicyReviewUnavailable,
        PackageLoadBlockerCode::RuntimeLanguageCompatibilityUnavailable,
        PackageLoadBlockerCode::ResourceCompatibilityUnavailable,
        PackageLoadBlockerCode::HumanReviewEvidenceUnavailable,
        PackageLoadBlockerCode::InputOutputEnforcementUnavailable,
        PackageLoadBlockerCode::SandboxEnforcementUnavailable,
        PackageLoadBlockerCode::ScopeEnforcementUnavailable,
        PackageLoadBlockerCode::PermissionEnforcementUnavailable,
        PackageLoadBlockerCode::AuditEventEnforcementUnavailable,
        PackageLoadBlockerCode::ExecutionAuthorizationUnavailable,
    ];

    assert_eq!(report.blockers(), expected);
    Ok(())
}

#[test]
fn blocker_codes_are_unique_bounded_and_stable() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;
    let codes = report
        .blockers()
        .iter()
        .copied()
        .map(PackageLoadBlockerCode::as_str)
        .collect::<HashSet<_>>();

    assert_eq!(codes.len(), EXPECTED_BLOCKER_COUNT);
    assert!(codes.len() <= 32);
    assert!(codes.contains("scope_manifest_validator_unavailable"));
    assert!(codes.contains("execution_authorization_unavailable"));
    Ok(())
}

#[test]
fn invalid_root_manifest_is_rejected_before_load_assessment() -> TestResult {
    let invalid = VALID_MANIFEST.replace("schema:", "unknown_field: blocked\nschema:");
    let error = require_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    Ok(())
}

#[test]
fn execution_claim_is_rejected_before_load_assessment() -> TestResult {
    let invalid =
        VALID_MANIFEST.replace("execution_authorized: false", "execution_authorized: true");
    let error = require_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SemanticValidation);
    Ok(())
}

#[test]
fn oversized_input_reuses_manifest_size_bound() -> TestResult {
    let input = "x".repeat(MAX_MANIFEST_BYTES + 1);
    let error = require_error(&input)?;

    assert_eq!(error.code(), ManifestErrorCode::InputTooLarge);
    Ok(())
}

#[test]
fn path_shaped_input_is_not_opened_as_a_package() -> TestResult {
    let fixture_path = "tests/fixtures/valid/node-doctor-agent.yaml";
    let error = require_error(fixture_path)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    Ok(())
}

#[test]
fn report_does_not_retain_or_echo_manifest_values() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;
    let rendered = format!("{report:?}");

    assert!(!rendered.contains("iamine.beta.node-doctor"));
    assert!(!rendered.contains("agent-scope.yaml"));
    Ok(())
}

#[test]
fn assessment_is_deterministic() -> TestResult {
    let first = assess_package_load_yaml(VALID_MANIFEST)?;
    let second = assess_package_load_yaml(VALID_MANIFEST)?;

    assert_eq!(first, second);
    Ok(())
}

fn require_error(input: &str) -> Result<ManifestError, Box<dyn Error>> {
    assess_package_load_yaml(input)
        .err()
        .ok_or_else(|| io::Error::other("package load assessment was expected to fail").into())
}
