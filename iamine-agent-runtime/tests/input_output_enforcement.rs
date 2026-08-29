use std::{collections::HashSet, error::Error, fs, path::Path};

use iamine_agent_runtime::{
    DeclaredAgentPackage, DependencyPolicyReviewDecision, HumanReviewDecision, InputClassification,
    InputOutputConfigurationErrorCode, InputOutputEnforcementAuthority,
    InputOutputEnforcementError, InputOutputEnforcementErrorCode,
    InputOutputEnforcementEvidenceStatus, InputOutputPolicy, InputOutputRequirement,
    LanguagePolicyReviewDecision, LocalRegistryReviewDecision, OutputClassification,
    PackageReferenceResolver, PackageReviewAuthority, PackageReviewDecisions, PackageReviewSubject,
    RedactionState, ResolverLimits, RuntimeCompatibilityAuthority, RuntimeLanguageAvailability,
    RuntimeLanguageDecision, RuntimeLanguageMode, RuntimeNetworkAvailability,
    RuntimeResourceEnvelope, INPUT_OUTPUT_ENFORCEMENT_SCHEMA_VERSION,
    MAX_INPUT_OUTPUT_RECORD_BYTES,
};
use iamine_agents::{
    assess_package_load_yaml, parse_and_validate_yaml, AgentPackageManifest, ManifestReferences,
    PackageLoadBlockerCode, PackageLoadStatus,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

const VALID_MANIFEST: &str =
    include_str!("../../iamine-agents/tests/fixtures/valid/node-doctor-agent.yaml");
const VALID_SCOPE: &str =
    include_str!("../../iamine-agents/tests/fixtures/policy_metadata/valid/scope-policy.yaml");
const VALID_RESOURCES: &str = include_str!(
    "../../iamine-agents/tests/fixtures/descriptive_metadata/valid/resource-requirements.yaml"
);

#[test]
fn exact_compatible_subject_establishes_bounded_records() -> TestResult {
    let fixture = PackageFixture::new(VALID_SCOPE.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let (compatibility_authority, compatibility_evidence) =
        establish_compatibility(&fixture, subject)?;
    let authority = enforcement_authority(128, 128, true)?;
    let evidence =
        authority.establish(&compatibility_authority, &compatibility_evidence, subject)?;

    let input_content =
        authority.attest_redacted_input(&evidence, subject, "diagnostic_request")?;
    let input = authority.enforce_input(
        &evidence,
        subject,
        InputClassification::TaskDescriptor,
        input_content,
    )?;
    let output_content =
        authority.attest_redacted_output(&evidence, subject, "node_ready_summary")?;
    let output = authority.enforce_output(
        &evidence,
        subject,
        OutputClassification::DiagnosticReport,
        output_content,
    )?;

    assert_eq!(
        evidence.status(),
        InputOutputEnforcementEvidenceStatus::Established
    );
    assert_eq!(evidence.requirements().len(), 3);
    assert_eq!(
        evidence
            .requirements()
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert!(authority.verifies(&evidence, subject));
    assert!(!evidence.load_allowed());
    assert!(!evidence.execution_allowed());
    assert!(!evidence.persistence_allowed());
    assert!(!evidence.transport_allowed());
    assert!(!evidence.handoff_allowed());

    for schema in [input.schema_version(), output.schema_version()] {
        assert_eq!(schema, INPUT_OUTPUT_ENFORCEMENT_SCHEMA_VERSION);
    }
    assert_eq!(input.agent_id(), fixture.manifest.package_id);
    assert_eq!(input.task_type(), "diagnostic_report");
    assert_eq!(input.scope_id(), "node_readiness_diagnostic_report");
    assert_eq!(input.classification(), InputClassification::TaskDescriptor);
    assert_eq!(input.redaction_state(), RedactionState::OperatorAttested);
    assert_eq!(input.redacted_content(), "diagnostic_request");
    assert!(!input.handoff_allowed());
    assert!(!input.operator_visible());
    assert!(!input.persistence_allowed());
    assert!(!input.transport_allowed());

    assert_eq!(output.agent_id(), fixture.manifest.package_id);
    assert_eq!(output.task_type(), "diagnostic_report");
    assert_eq!(output.scope_id(), "node_readiness_diagnostic_report");
    assert_eq!(
        output.classification(),
        OutputClassification::DiagnosticReport
    );
    assert_eq!(output.redaction_state(), RedactionState::OperatorAttested);
    assert_eq!(output.redacted_content(), "node_ready_summary");
    assert!(!output.handoff_allowed());
    assert!(output.operator_visible());
    assert!(!output.execution_success());
    assert!(!output.persistence_allowed());
    assert!(!output.transport_allowed());

    Ok(())
}

#[test]
fn every_contract_class_is_typed_and_fail_closed() -> TestResult {
    let fixture = PackageFixture::new(VALID_SCOPE.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let (compatibility_authority, compatibility_evidence) =
        establish_compatibility(&fixture, subject)?;
    let authority = enforcement_authority(128, 128, false)?;
    let evidence =
        authority.establish(&compatibility_authority, &compatibility_evidence, subject)?;

    let inputs = [
        InputClassification::TaskDescriptor,
        InputClassification::OperatorIntent,
        InputClassification::DeclaredScope,
        InputClassification::PermissionGrantReference,
        InputClassification::ResourceHint,
        InputClassification::RiskHint,
        InputClassification::ContextPointer,
    ];
    assert_eq!(
        inputs
            .iter()
            .map(|classification| classification.as_str())
            .collect::<HashSet<_>>()
            .len(),
        inputs.len()
    );
    for classification in inputs {
        let content = authority.attest_redacted_input(&evidence, subject, "redacted_input")?;
        let record = authority.enforce_input(&evidence, subject, classification, content)?;
        assert_eq!(record.classification(), classification);
        assert!(!record.operator_visible());
        assert!(!record.handoff_allowed());
    }

    let outputs = [
        OutputClassification::ResultSummary,
        OutputClassification::ActionReport,
        OutputClassification::DiagnosticReport,
        OutputClassification::SupportReport,
        OutputClassification::BlockedActionReport,
        OutputClassification::ClarificationRequest,
        OutputClassification::HandoffRequest,
        OutputClassification::RefusalReport,
        OutputClassification::ErrorReport,
    ];
    assert_eq!(
        outputs
            .iter()
            .map(|classification| classification.as_str())
            .collect::<HashSet<_>>()
            .len(),
        outputs.len()
    );
    for classification in outputs {
        let content = authority.attest_redacted_output(&evidence, subject, "redacted_output")?;
        let record = authority.enforce_output(&evidence, subject, classification, content)?;
        assert_eq!(record.classification(), classification);
        assert!(!record.operator_visible());
        assert!(!record.execution_success());
        assert!(!record.handoff_allowed());
    }

    Ok(())
}

#[test]
fn compatibility_and_enforcement_authorities_must_match_exactly() -> TestResult {
    let fixture = PackageFixture::new(VALID_SCOPE.as_bytes())?;
    let resolved = fixture.resolve()?;
    let cloned_resolution = resolved.clone();
    let subject = fixture.subject(&resolved);
    let cloned_subject = fixture.subject(&cloned_resolution);
    let second_manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let second_manifest_subject = PackageReviewSubject::new(
        DeclaredAgentPackage::from_manifest(&second_manifest),
        &resolved,
    );
    let (compatibility_authority, compatibility_evidence) =
        establish_compatibility(&fixture, subject)?;
    let other_compatibility_authority = compatible_authority()?;
    let authority = enforcement_authority(128, 128, true)?;

    for candidate_subject in [cloned_subject, second_manifest_subject] {
        assert_enforcement_error(
            authority.establish(
                &compatibility_authority,
                &compatibility_evidence,
                candidate_subject,
            ),
            InputOutputEnforcementErrorCode::RuntimeCompatibilityNotVerified,
            InputOutputRequirement::RuntimeCompatibilityEvidence,
        )?;
    }
    assert_enforcement_error(
        authority.establish(
            &other_compatibility_authority,
            &compatibility_evidence,
            subject,
        ),
        InputOutputEnforcementErrorCode::RuntimeCompatibilityNotVerified,
        InputOutputRequirement::RuntimeCompatibilityEvidence,
    )?;

    let evidence =
        authority.establish(&compatibility_authority, &compatibility_evidence, subject)?;
    let other_authority = enforcement_authority(128, 128, true)?;
    assert!(authority.verifies(&evidence, subject));
    assert!(!authority.verifies(&evidence, cloned_subject));
    assert!(!other_authority.verifies(&evidence, subject));
    assert_enforcement_error(
        other_authority.attest_redacted_input(&evidence, subject, "redacted"),
        InputOutputEnforcementErrorCode::EnforcementEvidenceNotVerified,
        InputOutputRequirement::EnforcementEvidence,
    )?;

    Ok(())
}

#[test]
fn redaction_attestations_are_bound_to_one_evidence_instance() -> TestResult {
    let fixture = PackageFixture::new(VALID_SCOPE.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let (compatibility_authority, compatibility_evidence) =
        establish_compatibility(&fixture, subject)?;
    let authority = enforcement_authority(128, 128, true)?;
    let first = authority.establish(&compatibility_authority, &compatibility_evidence, subject)?;
    let second = authority.establish(&compatibility_authority, &compatibility_evidence, subject)?;

    let input = authority.attest_redacted_input(&first, subject, "redacted_input")?;
    assert_enforcement_error(
        authority.enforce_input(&second, subject, InputClassification::TaskDescriptor, input),
        InputOutputEnforcementErrorCode::RedactionAttestationNotVerified,
        InputOutputRequirement::RedactionAttestation,
    )?;
    let output = authority.attest_redacted_output(&first, subject, "redacted_output")?;
    assert_enforcement_error(
        authority.enforce_output(
            &second,
            subject,
            OutputClassification::DiagnosticReport,
            output,
        ),
        InputOutputEnforcementErrorCode::RedactionAttestationNotVerified,
        InputOutputRequirement::RedactionAttestation,
    )?;

    Ok(())
}

#[test]
fn invalid_scope_identity_and_task_type_fail_closed() -> TestResult {
    assert_scope_failure(
        b"private-invalid-scope-marker",
        InputOutputEnforcementErrorCode::ScopeMetadataInvalid,
        InputOutputRequirement::ScopeMetadata,
    )?;
    let other_package = VALID_SCOPE.replace("iamine.beta.node-doctor", "iamine.beta.other-agent");
    assert_scope_failure(
        other_package.as_bytes(),
        InputOutputEnforcementErrorCode::ScopePackageMismatch,
        InputOutputRequirement::PackageIdentity,
    )?;
    let other_task = VALID_SCOPE.replace("diagnostic_report", "other_task");
    assert_scope_failure(
        other_task.as_bytes(),
        InputOutputEnforcementErrorCode::ScopeTaskTypeMismatch,
        InputOutputRequirement::TaskType,
    )?;

    Ok(())
}

#[test]
fn limits_and_content_validation_are_bounded() -> TestResult {
    for (input, output, code) in [
        (0, 1, InputOutputConfigurationErrorCode::ZeroInputLimit),
        (1, 0, InputOutputConfigurationErrorCode::ZeroOutputLimit),
        (
            MAX_INPUT_OUTPUT_RECORD_BYTES + 1,
            1,
            InputOutputConfigurationErrorCode::InputLimitTooLarge,
        ),
        (
            1,
            MAX_INPUT_OUTPUT_RECORD_BYTES + 1,
            InputOutputConfigurationErrorCode::OutputLimitTooLarge,
        ),
    ] {
        let error = InputOutputPolicy::new(input, output, true)
            .err()
            .ok_or("invalid policy unexpectedly accepted")?;
        assert_eq!(error.code(), code);
    }

    let fixture = PackageFixture::new(VALID_SCOPE.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let (compatibility_authority, compatibility_evidence) =
        establish_compatibility(&fixture, subject)?;
    let authority = enforcement_authority(4, 5, true)?;
    let evidence =
        authority.establish(&compatibility_authority, &compatibility_evidence, subject)?;

    for (content, code, requirement) in [
        (
            "",
            InputOutputEnforcementErrorCode::EmptyContent,
            InputOutputRequirement::RecordContent,
        ),
        (
            "12345",
            InputOutputEnforcementErrorCode::InputTooLarge,
            InputOutputRequirement::RecordLimit,
        ),
        (
            "a\nb",
            InputOutputEnforcementErrorCode::ControlCharacter,
            InputOutputRequirement::RecordContent,
        ),
    ] {
        assert_enforcement_error(
            authority.attest_redacted_input(&evidence, subject, content),
            code,
            requirement,
        )?;
    }
    assert_enforcement_error(
        authority.attest_redacted_output(&evidence, subject, "123456"),
        InputOutputEnforcementErrorCode::OutputTooLarge,
        InputOutputRequirement::RecordLimit,
    )?;

    Ok(())
}

#[test]
fn debug_and_errors_do_not_expose_record_or_package_values() -> TestResult {
    let fixture = PackageFixture::new(VALID_SCOPE.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let (compatibility_authority, compatibility_evidence) =
        establish_compatibility(&fixture, subject)?;
    let policy = InputOutputPolicy::new(128, 128, true)?;
    let authority = InputOutputEnforcementAuthority::new_operator_local(policy);
    let evidence =
        authority.establish(&compatibility_authority, &compatibility_evidence, subject)?;
    let private_marker = "private-redacted-marker";
    let attested = authority.attest_redacted_output(&evidence, subject, private_marker)?;
    let attested_debug = format!("{attested:?}");
    let record = authority.enforce_output(
        &evidence,
        subject,
        OutputClassification::DiagnosticReport,
        attested,
    )?;
    let error = authority
        .attest_redacted_input(&evidence, subject, "")
        .err()
        .ok_or("empty content unexpectedly accepted")?;
    let combined = format!(
        "{policy:?} {authority:?} {evidence:?} {attested_debug} {record:?} {error:?} {error}"
    );

    assert!(combined.contains("[redacted]"));
    assert!(!combined.contains(private_marker));
    assert!(!combined.contains(&fixture.manifest.package_id));
    assert!(!combined.contains("node_readiness_diagnostic_report"));

    Ok(())
}

#[test]
fn static_package_load_blockers_remain_unchanged() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;

    assert_eq!(report.status(), PackageLoadStatus::Blocked);
    assert!(!report.load_allowed());
    for blocker in [
        PackageLoadBlockerCode::InputOutputEnforcementUnavailable,
        PackageLoadBlockerCode::SandboxEnforcementUnavailable,
        PackageLoadBlockerCode::ExecutionAuthorizationUnavailable,
    ] {
        assert!(report.blockers().contains(&blocker));
    }

    Ok(())
}

fn assert_scope_failure(
    scope_content: &[u8],
    code: InputOutputEnforcementErrorCode,
    requirement: InputOutputRequirement,
) -> TestResult {
    let fixture = PackageFixture::new(scope_content)?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let (compatibility_authority, compatibility_evidence) =
        establish_compatibility(&fixture, subject)?;
    let authority = enforcement_authority(128, 128, true)?;
    assert_enforcement_error(
        authority.establish(&compatibility_authority, &compatibility_evidence, subject),
        code,
        requirement,
    )
}

fn assert_enforcement_error<T>(
    result: Result<T, InputOutputEnforcementError>,
    code: InputOutputEnforcementErrorCode,
    requirement: InputOutputRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("unsafe input/output operation unexpectedly succeeded")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    Ok(())
}

fn enforcement_authority(
    max_input_bytes: usize,
    max_output_bytes: usize,
    operator_visible_outputs: bool,
) -> Result<InputOutputEnforcementAuthority, Box<dyn Error>> {
    Ok(InputOutputEnforcementAuthority::new_operator_local(
        InputOutputPolicy::new(max_input_bytes, max_output_bytes, operator_visible_outputs)?,
    ))
}

fn establish_compatibility<'a>(
    fixture: &PackageFixture,
    subject: PackageReviewSubject<'a>,
) -> Result<
    (
        RuntimeCompatibilityAuthority,
        iamine_agent_runtime::RuntimeCompatibilityEvidence<'a>,
    ),
    Box<dyn Error>,
> {
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let compatibility_authority = compatible_authority()?;
    let compatibility_evidence =
        compatibility_authority.evaluate(&review_authority, &review_evidence, subject)?;
    let _ = fixture;
    Ok((compatibility_authority, compatibility_evidence))
}

fn compatible_authority() -> Result<RuntimeCompatibilityAuthority, Box<dyn Error>> {
    Ok(RuntimeCompatibilityAuthority::new_operator_local(
        RuntimeLanguageDecision::new(
            RuntimeLanguageMode::RustNativeOfficial,
            RuntimeLanguageAvailability::Available,
        ),
        RuntimeResourceEnvelope::new(2, 512, 84, RuntimeNetworkAvailability::None)?,
    ))
}

fn approved_review_decisions() -> PackageReviewDecisions {
    PackageReviewDecisions::new(
        LocalRegistryReviewDecision::RegistryReviewReady,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::IndependentApproved,
    )
}

struct PackageFixture {
    root: TempDir,
    manifest: AgentPackageManifest,
}

impl PackageFixture {
    fn new(scope_content: &[u8]) -> Result<Self, Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
        create_references(root.path(), &manifest.references, scope_content)?;
        Ok(Self { root, manifest })
    }

    fn resolve(&self) -> Result<iamine_agent_runtime::ResolvedPackageReferences, Box<dyn Error>> {
        let resolver =
            PackageReferenceResolver::open_ambient(self.root.path(), ResolverLimits::default())?;
        Ok(resolver.resolve(&self.manifest.references)?)
    }

    fn subject<'a>(
        &'a self,
        references: &'a iamine_agent_runtime::ResolvedPackageReferences,
    ) -> PackageReviewSubject<'a> {
        PackageReviewSubject::new(
            DeclaredAgentPackage::from_manifest(&self.manifest),
            references,
        )
    }
}

fn create_references(
    root: &Path,
    references: &ManifestReferences,
    scope_content: &[u8],
) -> TestResult {
    for (declared, content) in [
        (&references.scope_manifest, scope_content),
        (
            &references.capability_metadata,
            b"validated metadata".as_slice(),
        ),
        (
            &references.expertise_metadata,
            b"validated metadata".as_slice(),
        ),
        (
            &references.resource_requirements,
            VALID_RESOURCES.as_bytes(),
        ),
        (
            &references.permission_model,
            b"validated metadata".as_slice(),
        ),
        (&references.audit_policy, b"validated metadata".as_slice()),
        (&references.boundary_tests, b"validated metadata".as_slice()),
    ] {
        let path = root.join(declared);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
    }
    Ok(())
}
