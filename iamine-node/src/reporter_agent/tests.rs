use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use iamine_agent_runtime::{PackageReferenceResolver, ResolverLimits};
use iamine_agents::{
    evaluate_scope, parse_and_validate_yaml, parse_audit_policy_yaml, parse_boundary_eval_yaml,
    parse_capability_metadata_yaml, parse_expertise_metadata_yaml, parse_permission_policy_yaml,
    parse_resource_requirements_yaml, parse_scope_policy_yaml, BoundaryEvalClass,
    BoundaryExpectedAction, ScopeDecision, ScopePolicy, ScopePolicyMetadata, ScopePolicySpec,
    ScopeRequestClassification, ScopeRequestRef,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const INPUT_CLASSES: [&str; 1] = ["operator_approved_redacted_evidence"];
const PACKAGE_FILES: [&str; 8] = [
    "agent.yaml",
    "agent-scope.yaml",
    "metadata/agent-capabilities.yaml",
    "metadata/agent-expertise.yaml",
    "metadata/agent-resources.yaml",
    "metadata/agent-permissions.yaml",
    "metadata/agent-audit.yaml",
    "evals/agent-boundary-tests.yaml",
];

#[test]
fn official_package_and_all_referenced_metadata_validate() -> TestResult {
    let root = package_root();
    let manifest = parse_and_validate_yaml(&read(&root, "agent.yaml")?)?;
    assert_eq!(manifest.package_id, REPORTER_PACKAGE_ID);
    assert_eq!(manifest.agent.task_class, REPORTER_TASK_TYPE);
    assert!(!manifest.execution_authorized);
    assert!(!manifest.distribution.public_beta);

    parse_scope_policy_yaml(&read(&root, "agent-scope.yaml")?)?;
    parse_capability_metadata_yaml(&read(&root, "metadata/agent-capabilities.yaml")?)?;
    parse_expertise_metadata_yaml(&read(&root, "metadata/agent-expertise.yaml")?)?;
    parse_resource_requirements_yaml(&read(&root, "metadata/agent-resources.yaml")?)?;
    parse_permission_policy_yaml(&read(&root, "metadata/agent-permissions.yaml")?)?;
    parse_audit_policy_yaml(&read(&root, "metadata/agent-audit.yaml")?)?;
    parse_boundary_eval_yaml(&read(&root, "evals/agent-boundary-tests.yaml")?)?;

    let resolver = PackageReferenceResolver::open_ambient(&root, ResolverLimits::default())?;
    assert_eq!(resolver.resolve(&manifest.references)?.len(), 7);
    for path in [
        "README.md",
        "src/README.md",
        "review/human-review.md",
        "review/qa-evidence.md",
        "review/capability-review.md",
        "review/expertise-review.md",
        "review/resource-review.md",
    ] {
        assert!(root.join(path).is_file(), "missing {path}");
    }
    Ok(())
}

#[test]
fn boundary_suite_matches_scope_enforcement_decisions() -> TestResult {
    let root = package_root();
    let scope_metadata = parse_scope_policy_yaml(&read(&root, "agent-scope.yaml")?)?;
    let scope_policy = scope_policy(&scope_metadata)?;
    let suite = parse_boundary_eval_yaml(&read(&root, "evals/agent-boundary-tests.yaml")?)?;
    assert_eq!(suite.cases.len(), 9);

    for case in suite.cases {
        let (task, classification) = match case.class {
            BoundaryEvalClass::InScopePositive => (
                "format_privacy_safe_support_report",
                ScopeRequestClassification::InScopeCandidate,
            ),
            BoundaryEvalClass::OutOfScopeNegative => (
                "collect_support_evidence",
                ScopeRequestClassification::InScopeCandidate,
            ),
            BoundaryEvalClass::AmbiguousTask => (
                "format_privacy_safe_support_report",
                ScopeRequestClassification::Ambiguous,
            ),
            BoundaryEvalClass::DangerousTask => (
                "format_privacy_safe_support_report",
                ScopeRequestClassification::Dangerous,
            ),
            BoundaryEvalClass::CrossDomainTask | BoundaryEvalClass::HandoffToOrchestrator => (
                "format_privacy_safe_support_report",
                ScopeRequestClassification::CrossDomain,
            ),
            BoundaryEvalClass::PermissionEscalation => (
                "format_privacy_safe_support_report",
                ScopeRequestClassification::PermissionEscalation,
            ),
            BoundaryEvalClass::PromptInjection => (
                "format_privacy_safe_support_report",
                ScopeRequestClassification::PromptInjection,
            ),
            BoundaryEvalClass::RoleConfusion => (
                "format_privacy_safe_support_report",
                ScopeRequestClassification::RoleConfusion,
            ),
        };
        let request = ScopeRequestRef::new(
            REPORTER_PACKAGE_ID,
            REPORTER_TASK_TYPE,
            task,
            "format_operator_visible_report",
            &INPUT_CLASSES,
            classification,
        );
        let decision = evaluate_scope(&scope_policy, request).decision();
        let matches = match case.expected_action {
            BoundaryExpectedAction::AllowReviewResponse => decision == ScopeDecision::Allow,
            BoundaryExpectedAction::Refuse => decision == ScopeDecision::Refuse,
            BoundaryExpectedAction::Clarify => decision == ScopeDecision::Clarify,
            BoundaryExpectedAction::HandoffToOrchestrator => {
                decision == ScopeDecision::HandoffToOrchestrator
            }
            BoundaryExpectedAction::RefuseOrHandoff => matches!(
                decision,
                ScopeDecision::Refuse | ScopeDecision::HandoffToOrchestrator
            ),
        };
        assert!(matches, "boundary case {} diverged", case.case_id);
    }
    Ok(())
}

#[test]
fn typed_cli_accepts_bounded_evidence_and_rejects_unsafe_shapes() {
    let parsed = ReporterCliCommand::from_args(&strings(&[
        "--package-root",
        "agents/official/reporter",
        "--evidence",
        "operator_symptom_summary:observed:runtime_health",
        "--evidence=redacted_diagnostic_summary:attention:model_readiness",
        "--json",
    ]));
    assert!(parsed.is_ok());
    let Some(command) = parsed.ok() else {
        return;
    };
    assert_eq!(command.evidence.len(), 2);
    assert!(command.json);

    for invalid in [
        "raw_log:observed:runtime_health",
        "operator_symptom_summary:unknown:runtime_health",
        "operator_symptom_summary:observed:/private/user/token",
        "operator_symptom_summary:observed:runtime_health:extra",
    ] {
        let error = ReporterCliCommand::from_args(&strings(&[
            "--package-root",
            "agents/official/reporter",
            "--evidence",
            invalid,
        ]))
        .expect_err("unsafe token must fail closed");
        assert!(!error.contains("/private/user/token"));
    }

    let duplicate = strings(&[
        "--package-root",
        "agents/official/reporter",
        "--evidence",
        "operator_symptom_summary:observed:runtime_health",
        "--evidence",
        "operator_symptom_summary:observed:runtime_health",
    ]);
    assert!(ReporterCliCommand::from_args(&duplicate)
        .expect_err("duplicate must fail")
        .contains("duplicada"));

    let contradictory = strings(&[
        "--package-root",
        "agents/official/reporter",
        "--evidence",
        "operator_symptom_summary:observed:runtime_health",
        "--evidence",
        "operator_symptom_summary:blocked:runtime_health",
    ]);
    assert!(ReporterCliCommand::from_args(&contradictory)
        .expect_err("contradiction must fail")
        .contains("contradictoria"));

    let mut oversized = strings(&["--package-root", "agents/official/reporter"]);
    for claim in [
        "node_readiness",
        "configuration_status",
        "model_readiness",
        "network_readiness",
        "runtime_health",
        "node_readiness",
        "configuration_status",
        "model_readiness",
        "network_readiness",
    ] {
        oversized.push("--evidence".to_string());
        oversized.push(format!("redacted_diagnostic_summary:observed:{claim}"));
    }
    assert!(ReporterCliCommand::from_args(&oversized)
        .expect_err("ninth evidence must fail")
        .contains("maximo 8"));
}

#[test]
fn supported_evidence_executes_privacy_safe_report() -> TestResult {
    let input = typed_input(vec![evidence(
        ReporterEvidenceSource::RedactedDiagnostic,
        ReporterEvidenceStatus::Attention,
        ReporterClaim::ModelReadiness,
    )]);
    let result = execute_reporter_agent(&package_root(), &input)?;

    assert_eq!(result.status, "completed");
    assert_eq!(result.classification, "support_report");
    assert_eq!(result.scope_id, REPORTER_SCOPE_ID);
    assert_eq!(
        result.report.classification,
        ReporterReportClassification::SupportReport
    );
    assert_eq!(
        result.report.next_step,
        ReporterNextStep::ReviewAttentionEvidence
    );
    assert_eq!(result.report.evidence, input.evidence);
    assert!(result.execution_authorized);
    assert!(result.package_loaded);
    assert!(result.sandbox_adapter_was_active);
    assert!(!result.os_isolation_claimed);
    assert!(result.cleanup_completed);
    assert!(result.audit_recorded);
    assert!(!result.scheduler_mutated);
    assert!(!result.transport_started);
    assert!(!result.persisted);
    Ok(())
}

#[test]
fn absent_or_missing_evidence_returns_blocked_report() -> TestResult {
    for input in [
        typed_input(Vec::new()),
        typed_input(vec![evidence(
            ReporterEvidenceSource::RedactedSupportBundle,
            ReporterEvidenceStatus::Missing,
            ReporterClaim::ConfigurationStatus,
        )]),
    ] {
        let result = execute_reporter_agent(&package_root(), &input)?;
        assert_eq!(result.classification, "blocked_action_report");
        assert_eq!(
            result.report.next_step,
            ReporterNextStep::ProvideRedactedEvidence
        );
        assert!(!result.transport_started);
        assert!(!result.persisted);
    }
    Ok(())
}

#[test]
fn unsupported_claim_returns_handoff_without_side_effects() -> TestResult {
    let input = typed_input(vec![evidence(
        ReporterEvidenceSource::OperatorSymptom,
        ReporterEvidenceStatus::Observed,
        ReporterClaim::UnsupportedClaim,
    )]);
    let result = execute_reporter_agent(&package_root(), &input)?;
    assert_eq!(result.classification, "handoff_request");
    assert_eq!(
        result.report.next_step,
        ReporterNextStep::HumanReviewRequired
    );
    assert!(!result.scheduler_mutated);
    assert!(!result.transport_started);
    assert!(!result.persisted);
    Ok(())
}

#[test]
fn serialized_contract_rejects_unknown_or_private_fields() {
    let private = r#"{"schema_version":"iamine.agent.reporter.input-0.1","evidence":[],"private_path":"/Users/private/secret"}"#;
    let error = serde_json::from_str::<ReporterInput>(private)
        .expect_err("unknown private field must be rejected")
        .to_string();
    assert!(!error.contains("/Users/private/secret"));
}

#[test]
fn altered_package_and_manifest_fail_closed() -> TestResult {
    let altered_reference = copied_package()?;
    let capability = altered_reference
        .path()
        .join("metadata/agent-capabilities.yaml");
    let input = fs::read_to_string(&capability)?;
    fs::write(&capability, format!("{input}\n"))?;
    assert_eq!(
        execute_reporter_agent(altered_reference.path(), &typed_input(Vec::new()))
            .expect_err("altered reference must fail")
            .code(),
        ReporterAgentErrorCode::PackageMismatch
    );

    let altered_manifest = copied_package()?;
    let manifest = altered_manifest.path().join("agent.yaml");
    let input = fs::read_to_string(&manifest)?;
    fs::write(
        &manifest,
        input.replace(
            "display_name: Privacy-Safe Support Reporter",
            "display_name: Altered Reporter",
        ),
    )?;
    assert_eq!(
        execute_reporter_agent(altered_manifest.path(), &typed_input(Vec::new()))
            .expect_err("altered manifest must fail")
            .code(),
        ReporterAgentErrorCode::PackageMismatch
    );
    Ok(())
}

fn typed_input(evidence: Vec<ReporterEvidence>) -> ReporterInput {
    ReporterInput {
        schema_version: REPORTER_INPUT_SCHEMA_VERSION.to_string(),
        evidence,
    }
}

const fn evidence(
    source: ReporterEvidenceSource,
    status: ReporterEvidenceStatus,
    claim: ReporterClaim,
) -> ReporterEvidence {
    ReporterEvidence {
        source,
        status,
        claim,
    }
}

fn scope_policy(metadata: &ScopePolicyMetadata) -> TestResult<ScopePolicy> {
    Ok(ScopePolicy::try_from(ScopePolicySpec {
        package_id: metadata.package_id.clone(),
        scope_id: metadata.scope_id.clone(),
        task_types: metadata.task_boundary.task_types.clone(),
        in_scope_tasks: metadata.task_boundary.in_scope.clone(),
        out_of_scope_tasks: metadata.task_boundary.out_of_scope.clone(),
        allowed_input_classes: metadata.input_boundary.allowed_inputs.clone(),
        forbidden_input_classes: metadata.input_boundary.forbidden_inputs.clone(),
        allowed_operations: metadata.operation_boundary.allowed_operations.clone(),
        blocked_actions: metadata.operation_boundary.blocked_actions.clone(),
    })?)
}

fn copied_package() -> TestResult<tempfile::TempDir> {
    let temp = tempfile::tempdir()?;
    for relative in PACKAGE_FILES {
        let target = temp.path().join(relative);
        fs::create_dir_all(target.parent().ok_or("missing parent")?)?;
        fs::copy(package_root().join(relative), target)?;
    }
    Ok(temp)
}

fn package_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../agents/official/reporter")
}

fn read(root: &Path, relative: &str) -> TestResult<String> {
    Ok(fs::read_to_string(root.join(relative))?)
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}
