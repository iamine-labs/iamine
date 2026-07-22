use iamine_agents::{
    assess_package_load_yaml, audit_policy_json_schema, parse_audit_policy_yaml,
    parse_permission_policy_yaml, parse_scope_policy_yaml, permission_policy_json_schema,
    scope_policy_json_schema, PackageLoadStatus, PolicyMetadataError, PolicyMetadataErrorCode,
    PolicyMetadataViolationCode, MAX_POLICY_METADATA_BYTES,
};

const ROOT_MANIFEST: &str = include_str!("fixtures/valid/node-doctor-agent.yaml");
const SCOPE_POLICY: &str = include_str!("fixtures/policy_metadata/valid/scope-policy.yaml");
const PERMISSION_POLICY: &str =
    include_str!("fixtures/policy_metadata/valid/permission-policy.yaml");
const AUDIT_POLICY: &str = include_str!("fixtures/policy_metadata/valid/audit-policy.yaml");

fn expect_error<T>(result: Result<T, PolicyMetadataError>, message: &str) -> PolicyMetadataError {
    match result {
        Ok(_) => panic!("{message}"),
        Err(error) => error,
    }
}

#[test]
fn valid_policy_metadata_parses_through_each_typed_boundary() {
    let scope = parse_scope_policy_yaml(SCOPE_POLICY).expect("scope policy should parse");
    let permission =
        parse_permission_policy_yaml(PERMISSION_POLICY).expect("permission policy should parse");
    let audit = parse_audit_policy_yaml(AUDIT_POLICY).expect("audit policy should parse");

    assert_eq!(scope.package_id, "iamine.beta.node-doctor");
    assert_eq!(permission.package_id, "iamine.beta.node-doctor");
    assert_eq!(audit.package_id, "iamine.beta.node-doctor");
}

#[test]
fn generated_policy_schemas_are_available_without_runtime_or_package_io() {
    for schema in [
        scope_policy_json_schema(),
        permission_policy_json_schema(),
        audit_policy_json_schema(),
    ] {
        let schema = schema.expect("schema generation should succeed");
        assert!(schema.get("$schema").is_some());
        assert!(schema.get("definitions").is_some());
    }
}

#[test]
fn unknown_policy_fields_are_rejected_by_each_schema_before_semantic_validation() {
    let scope_input = SCOPE_POLICY.replacen(
        "scope_can_self_approve: false",
        "scope_can_self_approve: false\nunknown_claim: true",
        1,
    );
    let permission_input = format!("{PERMISSION_POLICY}\nunknown_claim: true\n");
    let audit_input = format!("{AUDIT_POLICY}\nunknown_claim: true\n");

    for error in [
        expect_error(
            parse_scope_policy_yaml(&scope_input),
            "unknown scope field must be rejected",
        ),
        expect_error(
            parse_permission_policy_yaml(&permission_input),
            "unknown permission field must be rejected",
        ),
        expect_error(
            parse_audit_policy_yaml(&audit_input),
            "unknown audit field must be rejected",
        ),
    ] {
        assert_eq!(error.code(), PolicyMetadataErrorCode::SchemaValidation);
    }
}

#[test]
fn missing_scope_safety_boundary_fails_closed() {
    let input = SCOPE_POLICY.replacen("    - wallet_keys\n", "", 1);

    let error = expect_error(parse_scope_policy_yaml(&input), "missing deny must block");
    assert_eq!(error.code(), PolicyMetadataErrorCode::SemanticValidation);
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(PolicyMetadataViolationCode::MissingSafetyBoundary));
}

#[test]
fn permissive_permission_policy_fails_closed() {
    let input = PERMISSION_POLICY.replacen("default_policy: deny", "default_policy: allow", 1);

    let error = expect_error(parse_permission_policy_yaml(&input), "allow must block");
    assert_eq!(error.code(), PolicyMetadataErrorCode::SemanticValidation);
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(PolicyMetadataViolationCode::UnsafePolicy));
}

#[test]
fn unsupported_permission_category_fails_closed() {
    let input = PERMISSION_POLICY.replacen("  - local_readonly", "  - unrestricted_filesystem", 1);

    let error = expect_error(
        parse_permission_policy_yaml(&input),
        "unsafe category must block",
    );
    assert_eq!(error.code(), PolicyMetadataErrorCode::SemanticValidation);
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(PolicyMetadataViolationCode::UnsafePolicy));
}

#[test]
fn unredacted_audit_evidence_fails_closed() {
    let input = AUDIT_POLICY.replacen("blocks_raw_prompts: true", "blocks_raw_prompts: false", 1);

    let error = expect_error(
        parse_audit_policy_yaml(&input),
        "unredacted policy must block",
    );
    assert_eq!(error.code(), PolicyMetadataErrorCode::SemanticValidation);
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(PolicyMetadataViolationCode::UnsafePolicy));
}

#[test]
fn unsafe_audit_evidence_reference_fails_without_echoing_the_value() {
    let input = AUDIT_POLICY.replacen(
        "review/human-review.md",
        "/Users/secret/private-audit.log",
        1,
    );

    let error = expect_error(parse_audit_policy_yaml(&input), "private path must block");
    assert_eq!(error.code(), PolicyMetadataErrorCode::SemanticValidation);
    assert!(!error.to_string().contains("private-audit.log"));
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(PolicyMetadataViolationCode::InvalidReference));
}

#[test]
fn oversized_policy_input_is_rejected_before_yaml_parsing() {
    let input = "x".repeat(MAX_POLICY_METADATA_BYTES + 1);

    let error = expect_error(
        parse_scope_policy_yaml(&input),
        "oversized input must block",
    );
    assert_eq!(error.code(), PolicyMetadataErrorCode::InputTooLarge);
}

#[test]
fn policy_metadata_does_not_change_the_always_blocked_package_load_gate() {
    parse_scope_policy_yaml(SCOPE_POLICY).expect("scope policy should parse");
    parse_permission_policy_yaml(PERMISSION_POLICY).expect("permission policy should parse");
    parse_audit_policy_yaml(AUDIT_POLICY).expect("audit policy should parse");

    let report = assess_package_load_yaml(ROOT_MANIFEST).expect("root manifest should parse");
    assert_eq!(report.status(), PackageLoadStatus::Blocked);
    assert!(!report.load_allowed());
}
