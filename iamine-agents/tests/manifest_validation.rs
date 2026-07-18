use std::error::Error;
use std::io;

use iamine_agents::{
    manifest_json_schema, parse_and_validate_yaml, validate_manifest, AgentPackageManifest,
    ManifestError, ManifestErrorCode, ManifestViolationCode, ManifestViolations,
    MAX_MANIFEST_BYTES,
};

const VALID_MANIFEST: &str = include_str!("fixtures/valid/node-doctor-agent.yaml");
const UNKNOWN_FIELD_MANIFEST: &str = include_str!("fixtures/invalid/unknown-field.yaml");

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn valid_manifest_parses_and_passes_semantic_validation() -> TestResult {
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;

    assert_eq!(manifest.package_id, "iamine.beta.node-doctor");
    assert!(!manifest.execution_authorized);
    assert!(validate_manifest(&manifest).is_ok());
    Ok(())
}

#[test]
fn generated_schema_rejects_unknown_fields() -> TestResult {
    let error = require_parse_error(UNKNOWN_FIELD_MANIFEST)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    Ok(())
}

#[test]
fn generated_schema_rejects_unknown_nested_fields() -> TestResult {
    let invalid = VALID_MANIFEST.replace(
        "  task_class: diagnostic_report",
        "  task_class: diagnostic_report\n  private_extension: blocked",
    );
    let error = require_parse_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    Ok(())
}

#[test]
fn generated_schema_requires_all_declared_fields() -> TestResult {
    let invalid = VALID_MANIFEST.replace("execution_authorized: false\n", "");
    let error = require_parse_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    Ok(())
}

#[test]
fn generated_schema_rejects_blocked_execution_modes() -> TestResult {
    let invalid = VALID_MANIFEST.replace("local_readonly", "remote_execution");
    let error = require_parse_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    Ok(())
}

#[test]
fn generated_schema_is_derived_from_canonical_types() -> TestResult {
    let schema = manifest_json_schema()?;
    let object = schema
        .as_object()
        .ok_or_else(|| test_failure("root schema must be an object"))?;
    let required = schema["required"]
        .as_array()
        .ok_or_else(|| test_failure("required fields must be listed"))?;

    assert!(object.contains_key("definitions"));
    assert_eq!(schema["additionalProperties"], false);
    assert!(required.iter().any(|field| field == "execution_authorized"));
    assert_eq!(
        schema["definitions"]["SecurityPolicy"]["additionalProperties"],
        false
    );
    Ok(())
}

#[test]
fn execution_authorization_is_semantically_blocked() -> TestResult {
    let invalid =
        VALID_MANIFEST.replace("execution_authorized: false", "execution_authorized: true");
    let error = require_parse_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SemanticValidation);
    assert!(error.violations().is_some_and(
        |violations| violations.contains_code(ManifestViolationCode::ExecutionNotAllowed)
    ));
    Ok(())
}

#[test]
fn unsafe_security_claim_is_semantically_blocked() -> TestResult {
    let invalid = VALID_MANIFEST.replace(
        "allows_arbitrary_shell: false",
        "allows_arbitrary_shell: true",
    );
    let error = require_parse_error(&invalid)?;

    assert!(error
        .violations()
        .is_some_and(|violations| violations.contains_code(ManifestViolationCode::UnsafeSecurity)));
    Ok(())
}

#[test]
fn public_distribution_claim_is_semantically_blocked() -> TestResult {
    let invalid = VALID_MANIFEST.replace("public_beta: false", "public_beta: true");
    let error = require_parse_error(&invalid)?;

    assert!(error.violations().is_some_and(
        |violations| violations.contains_code(ManifestViolationCode::UnsafeDistribution)
    ));
    Ok(())
}

#[test]
fn generated_schema_rejects_unavailable_distribution_values() -> TestResult {
    let invalid = VALID_MANIFEST.replace("local_dev", "public_marketplace");
    let error = require_parse_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    Ok(())
}

#[test]
fn missing_review_gate_is_semantically_blocked() -> TestResult {
    let invalid = VALID_MANIFEST.replace(
        "requires_human_review: true",
        "requires_human_review: false",
    );
    let error = require_parse_error(&invalid)?;

    assert!(error.violations().is_some_and(
        |violations| violations.contains_code(ManifestViolationCode::MissingReviewGate)
    ));
    Ok(())
}

#[test]
fn private_or_absolute_reference_is_blocked_without_echoing_value() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let private_reference = "/restricted/agent-scope.yaml";
    manifest.references.scope_manifest = private_reference.to_owned();

    let violations = require_violations(&manifest)?;
    assert!(violations.contains_code(ManifestViolationCode::InvalidReference));
    assert!(!violations.to_string().contains(private_reference));
    Ok(())
}

#[test]
fn traversal_and_platform_absolute_references_are_blocked() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    manifest.references.scope_manifest = "../agent-scope.yaml".to_owned();
    manifest.references.audit_policy = "C:\\private\\agent-audit.yaml".to_owned();

    let violations = require_violations(&manifest)?;
    assert_eq!(
        violations
            .iter()
            .filter(|violation| violation.code == ManifestViolationCode::InvalidReference)
            .count(),
        2
    );
    Ok(())
}

#[test]
fn referenced_metadata_format_remains_outside_root_parser_ownership() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    manifest.references.audit_policy = "metadata/agent-audit.toml".to_owned();

    assert!(validate_manifest(&manifest).is_ok());
    Ok(())
}

#[test]
fn duplicate_references_are_rejected() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    manifest.references.audit_policy = manifest.references.scope_manifest.clone();

    let violations = require_violations(&manifest)?;
    assert!(violations.contains_code(ManifestViolationCode::DuplicateValue));
    Ok(())
}

#[test]
fn duplicate_personas_are_rejected() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    manifest
        .agent
        .personas
        .push("home_troubleshooter".to_owned());

    let violations = require_violations(&manifest)?;
    assert!(violations.contains_code(ManifestViolationCode::DuplicateValue));
    Ok(())
}

#[test]
fn empty_persona_list_is_rejected() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    manifest.agent.personas.clear();

    let violations = require_violations(&manifest)?;
    assert!(violations.contains_code(ManifestViolationCode::InvalidCollection));
    Ok(())
}

#[test]
fn invalid_package_identity_and_version_are_rejected() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    manifest.package_id = "Other Product/private host".to_owned();
    manifest.package_version = "latest".to_owned();

    let violations = require_violations(&manifest)?;
    assert!(violations.contains_code(ManifestViolationCode::InvalidIdentifier));
    assert!(violations.contains_code(ManifestViolationCode::InvalidVersion));
    Ok(())
}

#[test]
fn unsupported_schema_is_rejected_without_echoing_identifier() -> TestResult {
    let mut manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let unsupported = "private.schema.value";
    manifest.schema = unsupported.to_owned();

    let violations = require_violations(&manifest)?;
    assert!(violations.contains_code(ManifestViolationCode::UnsupportedSchema));
    assert!(!violations.to_string().contains(unsupported));
    Ok(())
}

#[test]
fn oversized_input_is_rejected_before_yaml_parsing() -> TestResult {
    let oversized = "x".repeat(MAX_MANIFEST_BYTES + 1);
    let error = require_parse_error(&oversized)?;

    assert_eq!(error.code(), ManifestErrorCode::InputTooLarge);
    Ok(())
}

#[test]
fn syntax_errors_report_location_without_echoing_source_text() -> TestResult {
    let private_text = "sensitive-input-value";
    let malformed = format!("schema: [\n{private_text}");
    let error = require_parse_error(&malformed)?;

    assert_eq!(error.code(), ManifestErrorCode::InvalidYaml);
    assert!(!error.to_string().contains(private_text));
    Ok(())
}

#[test]
fn schema_errors_do_not_echo_unknown_private_values() -> TestResult {
    let private_text = "sensitive-input-value";
    let invalid = VALID_MANIFEST.replace(
        "display_name: Node Doctor",
        &format!("display_name: Node Doctor\nprivate_field: {private_text}"),
    );
    let error = require_parse_error(&invalid)?;

    assert_eq!(error.code(), ManifestErrorCode::SchemaValidation);
    assert!(!error.to_string().contains(private_text));
    Ok(())
}

fn require_parse_error(input: &str) -> Result<ManifestError, Box<dyn Error>> {
    parse_and_validate_yaml(input)
        .err()
        .ok_or_else(|| test_failure("manifest was expected to fail"))
}

fn require_violations(
    manifest: &AgentPackageManifest,
) -> Result<ManifestViolations, Box<dyn Error>> {
    validate_manifest(manifest)
        .err()
        .ok_or_else(|| test_failure("manifest was expected to have semantic violations"))
}

fn test_failure(message: &'static str) -> Box<dyn Error> {
    io::Error::other(message).into()
}
