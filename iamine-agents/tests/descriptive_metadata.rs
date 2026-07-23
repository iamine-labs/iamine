use iamine_agents::{
    assess_package_load_yaml, capability_metadata_json_schema, expertise_metadata_json_schema,
    parse_capability_metadata_yaml, parse_expertise_metadata_yaml,
    parse_resource_requirements_yaml, resource_requirements_json_schema, DescriptiveMetadataError,
    DescriptiveMetadataErrorCode, DescriptiveMetadataViolationCode, PackageLoadStatus,
    MAX_DESCRIPTIVE_METADATA_BYTES,
};

const ROOT_MANIFEST: &str = include_str!("fixtures/valid/node-doctor-agent.yaml");
const CAPABILITY_METADATA: &str =
    include_str!("fixtures/descriptive_metadata/valid/capability-metadata.yaml");
const EXPERTISE_METADATA: &str =
    include_str!("fixtures/descriptive_metadata/valid/expertise-metadata.yaml");
const RESOURCE_REQUIREMENTS: &str =
    include_str!("fixtures/descriptive_metadata/valid/resource-requirements.yaml");

fn expect_error<T>(
    result: Result<T, DescriptiveMetadataError>,
    message: &str,
) -> DescriptiveMetadataError {
    match result {
        Ok(_) => panic!("{message}"),
        Err(error) => error,
    }
}

#[test]
fn valid_descriptive_metadata_parses_through_each_typed_boundary() {
    let capability = parse_capability_metadata_yaml(CAPABILITY_METADATA)
        .expect("capability metadata should parse");
    let expertise =
        parse_expertise_metadata_yaml(EXPERTISE_METADATA).expect("expertise metadata should parse");
    let resources = parse_resource_requirements_yaml(RESOURCE_REQUIREMENTS)
        .expect("resource requirements should parse");

    assert_eq!(capability.package_id, "iamine.beta.node-doctor");
    assert_eq!(expertise.package_id, "iamine.beta.node-doctor");
    assert_eq!(resources.package_id, "iamine.beta.node-doctor");
}

#[test]
fn generated_descriptive_schemas_are_available_without_runtime_or_package_io() {
    for schema in [
        capability_metadata_json_schema(),
        expertise_metadata_json_schema(),
        resource_requirements_json_schema(),
    ] {
        let schema = schema.expect("schema generation should succeed");
        assert!(schema.get("$schema").is_some());
        assert!(schema.get("definitions").is_some());
    }
}

#[test]
fn unknown_descriptive_fields_are_rejected_before_semantic_validation() {
    let capability_input = format!("{CAPABILITY_METADATA}\nunknown_claim: true\n");
    let expertise_input = format!("{EXPERTISE_METADATA}\nunknown_claim: true\n");
    let resource_input = format!("{RESOURCE_REQUIREMENTS}\nunknown_claim: true\n");

    for error in [
        expect_error(
            parse_capability_metadata_yaml(&capability_input),
            "unknown capability field must be rejected",
        ),
        expect_error(
            parse_expertise_metadata_yaml(&expertise_input),
            "unknown expertise field must be rejected",
        ),
        expect_error(
            parse_resource_requirements_yaml(&resource_input),
            "unknown resource field must be rejected",
        ),
    ] {
        assert_eq!(error.code(), DescriptiveMetadataErrorCode::SchemaValidation);
    }
}

#[test]
fn unsafe_capability_operation_fails_closed() {
    let input = CAPABILITY_METADATA.replacen("  - classify_status", "  - run_shell", 1);

    let error = expect_error(
        parse_capability_metadata_yaml(&input),
        "unsafe operation must block",
    );
    assert_eq!(
        error.code(),
        DescriptiveMetadataErrorCode::SemanticValidation
    );
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::UnsafeClaim));
}

#[test]
fn privacy_sensitive_capability_input_fails_closed() {
    let input = CAPABILITY_METADATA.replacen("  - user_provided_error_text", "  - credentials", 1);

    let error = expect_error(
        parse_capability_metadata_yaml(&input),
        "private input class must block",
    );
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::UnsafeClaim));
}

#[test]
fn promissory_expertise_claim_fails_closed() {
    let input = EXPERTISE_METADATA.replacen(
        "  - can_explain_readiness_status",
        "  - guarantees_correct_answer",
        1,
    );

    let error = expect_error(
        parse_expertise_metadata_yaml(&input),
        "promissory expertise must block",
    );
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::UnsafeClaim));
}

#[test]
fn unsafe_expertise_evidence_path_fails_without_echoing_the_value() {
    let input = EXPERTISE_METADATA.replacen(
        "review/expertise-review.md",
        "/Users/secret/private-expertise.log",
        1,
    );

    let error = expect_error(
        parse_expertise_metadata_yaml(&input),
        "private expertise path must block",
    );
    assert!(!error.to_string().contains("private-expertise.log"));
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::InvalidReference));
}

#[test]
fn incomplete_expertise_eval_coverage_fails_closed() {
    let input = EXPERTISE_METADATA.replacen(
        "  - class: role_confusion_attempt\n    required: true\n",
        "",
        1,
    );

    let error = expect_error(
        parse_expertise_metadata_yaml(&input),
        "missing expertise eval must block",
    );
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::MissingSafetyBoundary));
}

#[test]
fn resource_side_effect_claim_fails_closed() {
    let input = RESOURCE_REQUIREMENTS.replacen(
        "  runs_dynamic_hardware_probe: false",
        "  runs_dynamic_hardware_probe: true",
        1,
    );

    let error = expect_error(
        parse_resource_requirements_yaml(&input),
        "dynamic probe claim must block",
    );
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::UnsafeClaim));
}

#[test]
fn resource_mode_map_mismatch_fails_closed() {
    let input = RESOURCE_REQUIREMENTS.replacen(
        "memory:\n  local_readonly:",
        "memory:\n  local_planning:",
        1,
    );

    let error = expect_error(
        parse_resource_requirements_yaml(&input),
        "mode mismatch must block",
    );
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::ContradictoryRequirement));
}

#[test]
fn unbounded_resource_requirement_fails_closed() {
    let input = RESOURCE_REQUIREMENTS.replacen(
        "    recommended_ram_mb: 512",
        "    recommended_ram_mb: 2097152",
        1,
    );

    let error = expect_error(
        parse_resource_requirements_yaml(&input),
        "unbounded memory must block",
    );
    assert!(error
        .violations()
        .expect("semantic violations")
        .contains_code(DescriptiveMetadataViolationCode::InvalidResourceBound));
}

#[test]
fn oversized_descriptive_input_is_rejected_before_yaml_parsing() {
    let input = "x".repeat(MAX_DESCRIPTIVE_METADATA_BYTES + 1);

    let error = expect_error(
        parse_capability_metadata_yaml(&input),
        "oversized input must block",
    );
    assert_eq!(error.code(), DescriptiveMetadataErrorCode::InputTooLarge);
}

#[test]
fn descriptive_metadata_does_not_change_the_always_blocked_package_load_gate() {
    parse_capability_metadata_yaml(CAPABILITY_METADATA).expect("capability metadata should parse");
    parse_expertise_metadata_yaml(EXPERTISE_METADATA).expect("expertise metadata should parse");
    parse_resource_requirements_yaml(RESOURCE_REQUIREMENTS)
        .expect("resource requirements should parse");

    let report = assess_package_load_yaml(ROOT_MANIFEST).expect("root manifest should parse");
    assert_eq!(report.status(), PackageLoadStatus::Blocked);
    assert!(!report.load_allowed());
}
