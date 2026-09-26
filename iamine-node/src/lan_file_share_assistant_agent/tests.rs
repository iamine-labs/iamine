use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use iamine_agent_runtime::{PackageReferenceResolver, ResolverLimits};
use iamine_agents::{
    evaluate_permissions, evaluate_scope, parse_and_validate_yaml, parse_audit_policy_yaml,
    parse_boundary_eval_yaml, parse_capability_metadata_yaml, parse_expertise_metadata_yaml,
    parse_permission_policy_yaml, parse_resource_requirements_yaml, parse_scope_policy_yaml,
    AuditRedactionDefault, BoundaryEvalClass, BoundaryExpectedAction, CapabilityExecutionMode,
    ExecutionMode, NetworkMode, PackageStatus, PermissionConfirmation, PermissionDecision,
    PermissionRequestRef, ResourceOperatingMode, ScopeDecision, ScopePolicy, ScopePolicyMetadata,
    ScopePolicySpec, ScopeRequestClassification, ScopeRequestRef,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const INPUT_CLASSES: [&str; 3] = [
    "operator_selected_share_summary",
    "redacted_share_inventory_summary",
    "redacted_access_error_summary",
];
const REQUIRED_CATEGORIES: [&str; 2] = ["local_readonly", "redacted_status_summary"];
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
    assert_eq!(manifest.package_id, LAN_FILE_SHARE_PACKAGE_ID);
    assert_eq!(manifest.agent.task_class, LAN_FILE_SHARE_TASK_TYPE);
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
        "evals/README.md",
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
    assert_eq!(suite.cases.len(), 15);

    for case in suite.cases {
        let (task, classification) = match case.class {
            BoundaryEvalClass::InScopePositive => (
                "review_declared_share_metadata",
                ScopeRequestClassification::InScopeCandidate,
            ),
            BoundaryEvalClass::OutOfScopeNegative => (
                "discover_lan_shares",
                ScopeRequestClassification::InScopeCandidate,
            ),
            BoundaryEvalClass::AmbiguousTask => (
                "review_declared_share_metadata",
                ScopeRequestClassification::Ambiguous,
            ),
            BoundaryEvalClass::DangerousTask => (
                "review_declared_share_metadata",
                ScopeRequestClassification::Dangerous,
            ),
            BoundaryEvalClass::CrossDomainTask | BoundaryEvalClass::HandoffToOrchestrator => (
                "review_declared_share_metadata",
                ScopeRequestClassification::CrossDomain,
            ),
            BoundaryEvalClass::PermissionEscalation => (
                "review_declared_share_metadata",
                ScopeRequestClassification::PermissionEscalation,
            ),
            BoundaryEvalClass::PromptInjection => (
                "review_declared_share_metadata",
                ScopeRequestClassification::PromptInjection,
            ),
            BoundaryEvalClass::RoleConfusion => (
                "review_declared_share_metadata",
                ScopeRequestClassification::RoleConfusion,
            ),
        };
        let request = ScopeRequestRef::new(
            LAN_FILE_SHARE_PACKAGE_ID,
            LAN_FILE_SHARE_TASK_TYPE,
            task,
            "summarize_operator_approved_share_inventory",
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
fn typed_cli_accepts_bounded_shares_and_rejects_unsafe_shapes() {
    let parsed = LanFileShareCliCommand::from_args(&strings(&[
        "--package-root",
        "agents/official/lan-file-share-assistant",
        "--share",
        "documents_share:observed:readonly_boundary",
        "--share=team_share:attention:protocol_metadata",
        "--json",
    ]));
    assert!(parsed.is_ok());
    let Some(command) = parsed.ok() else {
        return;
    };
    assert_eq!(command.shares.len(), 2);
    assert!(command.json);

    for invalid in [
        "unknown_share:observed:readonly_boundary",
        "documents_share:unknown:readonly_boundary",
        "documents_share:observed:smb_host_probe",
        "documents_share:observed:/private/share/token",
        "documents_share:observed:readonly_boundary:extra",
    ] {
        let error = LanFileShareCliCommand::from_args(&strings(&[
            "--package-root",
            "agents/official/lan-file-share-assistant",
            "--share",
            invalid,
        ]))
        .expect_err("unsafe token must fail closed");
        assert!(!error.contains("/private/share/token"));
    }

    let duplicate = strings(&[
        "--package-root",
        "agents/official/lan-file-share-assistant",
        "--share",
        "documents_share:observed:readonly_boundary",
        "--share",
        "documents_share:observed:readonly_boundary",
    ]);
    assert!(LanFileShareCliCommand::from_args(&duplicate)
        .expect_err("duplicate must fail")
        .contains("duplicada"));

    let contradictory = strings(&[
        "--package-root",
        "agents/official/lan-file-share-assistant",
        "--share",
        "documents_share:observed:readonly_boundary",
        "--share",
        "documents_share:blocked:readonly_boundary",
    ]);
    assert!(LanFileShareCliCommand::from_args(&contradictory)
        .expect_err("contradiction must fail")
        .contains("contradictoria"));

    let mut oversized = strings(&["--package-root", "agents/official/lan-file-share-assistant"]);
    for claim in [
        "readonly_boundary",
        "share_selection",
        "protocol_metadata",
        "owner_metadata",
        "readonly_boundary",
        "share_selection",
        "protocol_metadata",
        "owner_metadata",
        "readonly_boundary",
    ] {
        oversized.push("--share".to_string());
        oversized.push(format!("documents_share:observed:{claim}"));
    }
    assert!(LanFileShareCliCommand::from_args(&oversized)
        .expect_err("ninth share must fail")
        .contains("maximo 8"));
}

#[test]
fn supported_share_metadata_executes_bounded_local_review() -> TestResult {
    let input = typed_input(vec![evidence(
        ShareSelector::Documents,
        ShareEvidenceStatus::Observed,
        ShareEvidenceClaim::ReadonlyBoundary,
    )]);
    let result = execute_lan_file_share_agent(&package_root(), &input)?;

    assert_eq!(result.status, "completed");
    assert_eq!(result.classification, "file_share_review");
    assert_eq!(result.scope_id, LAN_FILE_SHARE_SCOPE_ID);
    assert_eq!(
        result.report.classification,
        LanFileShareReportClassification::FileShareReview
    );
    assert_eq!(
        result.report.next_step,
        LanFileShareNextStep::NoActionRequired
    );
    assert_eq!(result.report.shares, input.shares);
    assert!(result.execution_authorized);
    assert!(result.package_loaded);
    assert!(result.sandbox_adapter_was_active);
    assert!(!result.os_isolation_claimed);
    assert!(result.cleanup_completed);
    assert!(result.audit_recorded);
    assert!(!result.scheduler_mutated);
    assert!(!result.transport_started);
    assert!(!result.persisted);

    let repeated = execute_lan_file_share_agent(&package_root(), &input)?;
    assert_eq!(result.report, repeated.report);
    Ok(())
}

#[test]
fn supported_metadata_classes_stay_advisory_only() -> TestResult {
    for (status, claim) in [
        (
            ShareEvidenceStatus::Observed,
            ShareEvidenceClaim::ShareSelection,
        ),
        (
            ShareEvidenceStatus::Observed,
            ShareEvidenceClaim::ProtocolMetadata,
        ),
        (
            ShareEvidenceStatus::Observed,
            ShareEvidenceClaim::OwnerMetadata,
        ),
        (
            ShareEvidenceStatus::Attention,
            ShareEvidenceClaim::ReadonlyBoundary,
        ),
        (
            ShareEvidenceStatus::Blocked,
            ShareEvidenceClaim::OwnerMetadata,
        ),
    ] {
        let input = typed_input(vec![evidence(ShareSelector::Media, status, claim)]);
        let result = execute_lan_file_share_agent(&package_root(), &input)?;
        assert_eq!(result.status, "completed");
        assert!(!result.scheduler_mutated);
        assert!(!result.transport_started);
        assert!(!result.persisted);
        assert_eq!(result.report.shares, input.shares);
    }
    Ok(())
}

#[test]
fn absent_or_missing_share_metadata_returns_blocked_report() -> TestResult {
    for input in [
        typed_input(Vec::new()),
        typed_input(vec![evidence(
            ShareSelector::Backup,
            ShareEvidenceStatus::Missing,
            ShareEvidenceClaim::OwnerMetadata,
        )]),
    ] {
        let result = execute_lan_file_share_agent(&package_root(), &input)?;
        assert_eq!(result.classification, "blocked_action_report");
        assert_eq!(
            result.report.next_step,
            LanFileShareNextStep::ProvideRedactedShareMetadata
        );
        assert!(!result.transport_started);
        assert!(!result.persisted);
    }
    Ok(())
}

#[test]
fn unsupported_claim_returns_handoff_without_side_effects() -> TestResult {
    let input = typed_input(vec![evidence(
        ShareSelector::Team,
        ShareEvidenceStatus::Observed,
        ShareEvidenceClaim::UnsupportedClaim,
    )]);
    let result = execute_lan_file_share_agent(&package_root(), &input)?;
    assert_eq!(result.classification, "handoff_request");
    assert_eq!(
        result.report.next_step,
        LanFileShareNextStep::HandoffForFileOrNetworkAction
    );
    assert!(!result.scheduler_mutated);
    assert!(!result.transport_started);
    assert!(!result.persisted);
    Ok(())
}

#[test]
fn serialized_contract_rejects_unknown_or_private_fields() {
    let private = r#"{"schema_version":"iamine.agent.lan-file-share-assistant.input-0.1","shares":[],"share_path":"/Users/private/secret"}"#;
    let error = serde_json::from_str::<LanFileShareInput>(private)
        .expect_err("unknown private field must be rejected")
        .to_string();
    assert!(!error.contains("/Users/private/secret"));
}

#[test]
fn bounded_review_output_never_echoes_raw_or_private_input() -> TestResult {
    let private_token = "documents_share:observed:/home/operator/share";
    let error = LanFileShareCliCommand::from_args(&strings(&[
        "--package-root",
        "agents/official/lan-file-share-assistant",
        "--share",
        private_token,
    ]))
    .expect_err("path-shaped claim must fail closed");
    assert!(!error.contains("/home/operator/share"));

    let credential_token = "documents_share:observed:password=secret";
    let credential_error = LanFileShareCliCommand::from_args(&strings(&[
        "--package-root",
        "agents/official/lan-file-share-assistant",
        "--share",
        credential_token,
    ]))
    .expect_err("credential-shaped claim must fail closed");
    assert!(!credential_error.contains("password"));

    let execution = execute_lan_file_share_agent(
        &package_root(),
        &typed_input(vec![evidence(
            ShareSelector::Documents,
            ShareEvidenceStatus::Observed,
            ShareEvidenceClaim::ReadonlyBoundary,
        )]),
    )?;
    let serialized = serde_json::to_string(&execution.report)?;
    assert!(!serialized.contains("documents_share:observed:readonly_boundary"));
    assert!(!serialized.contains("/home/"));
    assert!(!serialized.contains("password"));
    assert!(serialized.contains("\"documents_share\""));
    Ok(())
}

#[test]
fn package_metadata_denies_execution_and_defers_lan_readonly() -> TestResult {
    let root = package_root();
    let manifest = parse_and_validate_yaml(&read(&root, "agent.yaml")?)?;
    assert!(!manifest.execution_authorized);
    assert!(manifest.status == PackageStatus::BetaCandidate);
    assert!(manifest.agent.earliest_mode == ExecutionMode::LocalReadonly);
    assert_eq!(manifest.package_id, LAN_FILE_SHARE_PACKAGE_ID);
    assert!(!manifest.distribution.public_beta);
    assert!(!manifest.distribution.marketplace);
    assert!(!manifest.distribution.third_party_publication);
    assert!(!manifest.security.collects_credentials);
    assert!(!manifest.security.collects_host_identifiers);
    assert!(!manifest.security.requires_network);
    assert!(!manifest.security.allows_destructive_actions);
    assert!(!manifest.security.allows_arbitrary_shell);
    assert!(!manifest.security.allows_unrestricted_filesystem);

    let capabilities =
        parse_capability_metadata_yaml(&read(&root, "metadata/agent-capabilities.yaml")?)?;
    assert!(capabilities.execution_modes == vec![CapabilityExecutionMode::LocalReadonly]);
    for limitation in [
        "no_share_discovery",
        "no_filesystem_access",
        "no_network_access",
        "no_credential_handling",
    ] {
        assert!(capabilities
            .limitations
            .iter()
            .any(|item| item == limitation));
    }

    let resources =
        parse_resource_requirements_yaml(&read(&root, "metadata/agent-resources.yaml")?)?;
    assert!(resources.operating_modes == vec![ResourceOperatingMode::LocalReadonly]);
    assert!(resources.network["local_readonly"].mode == NetworkMode::None);
    assert!(!resources.constraints.runs_dynamic_hardware_probe);
    assert!(!resources.constraints.starts_worker);
    assert!(!resources.constraints.overrides_scheduler);
    assert!(!resources.constraints.mutates_vm_or_container);
    assert!(!resources.model_dependencies.requires_model_load);
    assert!(!resources.privacy.stores_host_identifiers);

    let audit = parse_audit_policy_yaml(&read(&root, "metadata/agent-audit.yaml")?)?;
    assert!(audit.redaction_policy.default == AuditRedactionDefault::Redact);
    assert!(audit.redaction_policy.blocks_raw_prompts);
    assert!(audit.redaction_policy.blocks_credentials);
    assert!(audit.retention_policy.operator_local_only);
    assert!(!audit.integrity_policy.publishes_artifacts);
    assert!(!audit.access_policy.third_party_sharing);
    Ok(())
}

#[test]
fn default_deny_permission_policy_refuses_escalation_without_widening() -> TestResult {
    let (scope_policy, permission_policy) = super::policy::runtime_policies()?;
    let scope_evaluation = evaluate_scope(
        &scope_policy,
        ScopeRequestRef::new(
            LAN_FILE_SHARE_PACKAGE_ID,
            LAN_FILE_SHARE_TASK_TYPE,
            LAN_FILE_SHARE_TASK_INPUT,
            "summarize_operator_approved_share_inventory",
            &INPUT_CLASSES,
            ScopeRequestClassification::InScopeCandidate,
        ),
    );
    assert_eq!(scope_evaluation.decision(), ScopeDecision::Allow);

    let permitted = evaluate_permissions(
        &permission_policy,
        &scope_evaluation,
        PermissionRequestRef::new(
            LAN_FILE_SHARE_PACKAGE_ID,
            "summarize_operator_approved_share_inventory",
            &REQUIRED_CATEGORIES,
            PermissionConfirmation::NotProvided,
        ),
    );
    assert_eq!(permitted.decision(), PermissionDecision::Allow);

    let escalating: [(&str, &[&str]); 5] = [
        ("mount_share", &REQUIRED_CATEGORIES),
        ("discover_lan_shares", &REQUIRED_CATEGORIES),
        ("read_share_file_contents", &REQUIRED_CATEGORIES),
        ("run_shell", &REQUIRED_CATEGORIES),
        (
            "summarize_operator_approved_share_inventory",
            &["unrestricted_filesystem"],
        ),
    ];
    for (action, categories) in escalating {
        let evaluation = evaluate_permissions(
            &permission_policy,
            &scope_evaluation,
            PermissionRequestRef::new(
                LAN_FILE_SHARE_PACKAGE_ID,
                action,
                categories,
                PermissionConfirmation::NotProvided,
            ),
        );
        assert_eq!(
            evaluation.decision(),
            PermissionDecision::Refuse,
            "{action} must fail closed"
        );
    }
    Ok(())
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
        execute_lan_file_share_agent(altered_reference.path(), &typed_input(Vec::new()))
            .expect_err("altered reference must fail")
            .code(),
        LanFileShareAgentErrorCode::PackageMismatch
    );

    let altered_manifest = copied_package()?;
    let manifest = altered_manifest.path().join("agent.yaml");
    let input = fs::read_to_string(&manifest)?;
    fs::write(
        &manifest,
        input.replace(
            "display_name: LAN File Share Assistant",
            "display_name: Altered Assistant",
        ),
    )?;
    assert_eq!(
        execute_lan_file_share_agent(altered_manifest.path(), &typed_input(Vec::new()))
            .expect_err("altered manifest must fail")
            .code(),
        LanFileShareAgentErrorCode::PackageMismatch
    );
    Ok(())
}

#[test]
fn missing_package_fails_closed() -> TestResult {
    let temp = tempfile::tempdir()?;
    let missing = temp.path().join("missing-package");

    let error =
        execute_lan_file_share_agent(&missing, &typed_input(Vec::new())).expect_err("missing");
    assert_eq!(error.code(), LanFileShareAgentErrorCode::PackageUnavailable);
    Ok(())
}

fn typed_input(shares: Vec<LanShareEvidence>) -> LanFileShareInput {
    LanFileShareInput {
        schema_version: LAN_FILE_SHARE_INPUT_SCHEMA_VERSION.to_string(),
        shares,
    }
}

const fn evidence(
    share: ShareSelector,
    status: ShareEvidenceStatus,
    claim: ShareEvidenceClaim,
) -> LanShareEvidence {
    LanShareEvidence {
        share,
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../agents/official/lan-file-share-assistant")
}

fn read(root: &Path, relative: &str) -> TestResult<String> {
    Ok(fs::read_to_string(root.join(relative))?)
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}
