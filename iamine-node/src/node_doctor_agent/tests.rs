use std::{
    collections::BTreeMap,
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
use crate::{
    lan_node_doctor::{DoctorCheck, DoctorStatus, HARDWARE_PROFILE_CHECK_ID},
    node_doctor_evidence_provider::build_node_doctor_evidence_from_parts,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const INPUT_CLASSES: [&str; 1] = ["iamine_node_status_summary"];
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
    assert_eq!(manifest.package_id, NODE_DOCTOR_PACKAGE_ID);
    assert_eq!(manifest.agent.task_class, NODE_DOCTOR_TASK_TYPE);
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
    let references = resolver.resolve(&manifest.references)?;
    assert_eq!(references.len(), 7);
    for path in [
        "README.md",
        "src/README.md",
        "review/human-review.md",
        "review/qa-evidence.md",
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

    for case in suite.cases {
        let (task, classification) = match case.class {
            BoundaryEvalClass::InScopePositive => (
                "explain_node_readiness",
                ScopeRequestClassification::InScopeCandidate,
            ),
            BoundaryEvalClass::OutOfScopeNegative => {
                ("run_shell", ScopeRequestClassification::InScopeCandidate)
            }
            BoundaryEvalClass::AmbiguousTask => (
                "explain_node_readiness",
                ScopeRequestClassification::Ambiguous,
            ),
            BoundaryEvalClass::DangerousTask => (
                "explain_node_readiness",
                ScopeRequestClassification::Dangerous,
            ),
            BoundaryEvalClass::CrossDomainTask | BoundaryEvalClass::HandoffToOrchestrator => (
                "explain_node_readiness",
                ScopeRequestClassification::CrossDomain,
            ),
            BoundaryEvalClass::PermissionEscalation => (
                "explain_node_readiness",
                ScopeRequestClassification::PermissionEscalation,
            ),
            BoundaryEvalClass::PromptInjection => (
                "explain_node_readiness",
                ScopeRequestClassification::PromptInjection,
            ),
            BoundaryEvalClass::RoleConfusion => (
                "explain_node_readiness",
                ScopeRequestClassification::RoleConfusion,
            ),
        };
        let request = ScopeRequestRef::new(
            NODE_DOCTOR_PACKAGE_ID,
            NODE_DOCTOR_TASK_TYPE,
            task,
            "read_declared_summary",
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
fn exact_package_executes_node_doctor_through_official_runtime() -> TestResult {
    let result = execute_node_doctor_agent(&package_root())?;

    assert_eq!(result.status, "completed");
    assert_eq!(result.classification, "diagnostic_report");
    assert_eq!(result.scope_id, NODE_DOCTOR_SCOPE_ID);
    assert!(result.content.starts_with(&format!(
        "schema={NODE_DOCTOR_OUTPUT_SCHEMA_VERSION};class=diagnostic_report;"
    )));
    for category in [
        "node_status",
        "hardware_profile",
        "configuration_status",
        "model_readiness",
        "peer_network_status",
        "remote_inference_readiness",
    ] {
        assert!(result.content.contains(category));
    }
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
fn altered_package_snapshot_fails_closed() -> TestResult {
    let temp = tempfile::tempdir()?;
    for relative in PACKAGE_FILES {
        let target = temp.path().join(relative);
        fs::create_dir_all(target.parent().ok_or("missing parent")?)?;
        fs::copy(package_root().join(relative), &target)?;
    }
    let capability = temp.path().join("metadata/agent-capabilities.yaml");
    let input = fs::read_to_string(&capability)?;
    fs::write(&capability, format!("{input}\n"))?;

    let error = execute_node_doctor_agent(temp.path()).expect_err("altered package must fail");
    assert_eq!(error.code(), NodeDoctorAgentErrorCode::PackageMismatch);
    Ok(())
}

#[test]
fn altered_manifest_fails_closed() -> TestResult {
    let temp = tempfile::tempdir()?;
    for relative in PACKAGE_FILES {
        let target = temp.path().join(relative);
        fs::create_dir_all(target.parent().ok_or("missing parent")?)?;
        fs::copy(package_root().join(relative), &target)?;
    }
    let manifest = temp.path().join("agent.yaml");
    let input = fs::read_to_string(&manifest)?;
    fs::write(
        &manifest,
        input.replace("display_name: Node Doctor", "display_name: Altered"),
    )?;

    let error = execute_node_doctor_agent(temp.path()).expect_err("altered manifest must fail");
    assert_eq!(error.code(), NodeDoctorAgentErrorCode::PackageMismatch);
    Ok(())
}

#[test]
fn missing_package_fails_closed() -> TestResult {
    let temp = tempfile::tempdir()?;
    let missing = temp.path().join("missing-package");

    let error = execute_node_doctor_agent(&missing).expect_err("missing package must fail");
    assert_eq!(error.code(), NodeDoctorAgentErrorCode::PackageUnavailable);
    Ok(())
}

#[test]
fn missing_evidence_blocks_without_copying_private_owner_data() {
    let private_message = "user=/private/home host=private-host token=secret";
    let report = build_node_doctor_evidence_from_parts(
        DoctorStatus::Pass,
        &[DoctorCheck {
            id: HARDWARE_PROFILE_CHECK_ID,
            status: DoctorStatus::Pass,
            message: private_message.to_string(),
            details: BTreeMap::from([("private_path".to_string(), "/private/home".into())]),
        }],
    );

    let output = render_node_doctor_report(&report);
    assert_eq!(
        output.classification,
        OutputClassification::BlockedActionReport
    );
    assert!(output.content.contains("next_step=request_operator_review"));
    assert!(!output.content.contains(private_message));
    assert!(!output.content.contains("/private/home"));
    assert!(!output.content.contains("private-host"));
    assert!(!output.content.contains("secret"));
    assert!(output.content.len() < 2_048);
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

fn package_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../agents/official/node-doctor")
}

fn read(root: &Path, relative: &str) -> TestResult<String> {
    Ok(fs::read_to_string(root.join(relative))?)
}
