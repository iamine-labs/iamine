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
    "operator_declared_photo_inventory_summary",
    "redacted_photo_inventory_summary",
    "redacted_organization_intent_summary",
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
    assert_eq!(manifest.package_id, PHOTO_LIBRARY_PACKAGE_ID);
    assert_eq!(manifest.agent.task_class, PHOTO_LIBRARY_TASK_TYPE);
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
                "review_declared_photo_inventory",
                ScopeRequestClassification::InScopeCandidate,
            ),
            BoundaryEvalClass::OutOfScopeNegative => (
                "discover_photo_libraries",
                ScopeRequestClassification::InScopeCandidate,
            ),
            BoundaryEvalClass::AmbiguousTask => (
                "review_declared_photo_inventory",
                ScopeRequestClassification::Ambiguous,
            ),
            BoundaryEvalClass::DangerousTask => (
                "review_declared_photo_inventory",
                ScopeRequestClassification::Dangerous,
            ),
            BoundaryEvalClass::CrossDomainTask | BoundaryEvalClass::HandoffToOrchestrator => (
                "review_declared_photo_inventory",
                ScopeRequestClassification::CrossDomain,
            ),
            BoundaryEvalClass::PermissionEscalation => (
                "review_declared_photo_inventory",
                ScopeRequestClassification::PermissionEscalation,
            ),
            BoundaryEvalClass::PromptInjection => (
                "review_declared_photo_inventory",
                ScopeRequestClassification::PromptInjection,
            ),
            BoundaryEvalClass::RoleConfusion => (
                "review_declared_photo_inventory",
                ScopeRequestClassification::RoleConfusion,
            ),
        };
        let request = ScopeRequestRef::new(
            PHOTO_LIBRARY_PACKAGE_ID,
            PHOTO_LIBRARY_TASK_TYPE,
            task,
            "review_declared_photo_inventory",
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
fn typed_cli_accepts_bounded_items_and_rejects_unsafe_shapes() {
    let parsed = PhotoLibraryCliCommand::from_args(&strings(&[
        "--package-root",
        "agents/official/photo-library-organizer",
        "--item",
        "personal_library:photo:observed:declared_inventory_boundary",
        "--item=travel_library:video:attention:declared_metadata_completeness",
        "--intent",
        "review_before_change",
        "--json",
    ]));
    assert!(parsed.is_ok());
    let Some(command) = parsed.ok() else {
        return;
    };
    assert_eq!(command.items.len(), 2);
    assert_eq!(
        command.intent,
        Some(PhotoOrganizationIntent::ReviewBeforeChange)
    );
    assert!(command.json);

    for invalid in [
        "unknown_library:photo:observed:declared_inventory_boundary",
        "personal_library:unknown:observed:declared_inventory_boundary",
        "personal_library:photo:unknown:declared_inventory_boundary",
        "personal_library:photo:observed:read_library_contents",
        "personal_library:photo:observed:/private/photos/token",
        "personal_library:photo:observed:declared_inventory_boundary:extra",
    ] {
        let error = PhotoLibraryCliCommand::from_args(&strings(&[
            "--package-root",
            "agents/official/photo-library-organizer",
            "--item",
            invalid,
        ]))
        .expect_err("unsafe token must fail closed");
        assert!(!error.contains("/private/photos/token"));
    }

    let duplicate = strings(&[
        "--package-root",
        "agents/official/photo-library-organizer",
        "--item",
        "personal_library:photo:observed:declared_inventory_boundary",
        "--item",
        "personal_library:photo:observed:declared_inventory_boundary",
    ]);
    assert!(PhotoLibraryCliCommand::from_args(&duplicate)
        .expect_err("duplicate must fail")
        .contains("duplicada"));

    let contradictory = strings(&[
        "--package-root",
        "agents/official/photo-library-organizer",
        "--item",
        "personal_library:photo:observed:declared_inventory_boundary",
        "--item",
        "personal_library:photo:blocked:declared_inventory_boundary",
    ]);
    assert!(PhotoLibraryCliCommand::from_args(&contradictory)
        .expect_err("contradiction must fail")
        .contains("contradictoria"));

    let mut oversized = strings(&["--package-root", "agents/official/photo-library-organizer"]);
    for claim in [
        "declared_inventory_boundary",
        "declared_metadata_completeness",
        "declared_organization_intent",
        "declared_duplicate_suspicion",
        "declared_inventory_boundary",
        "declared_metadata_completeness",
        "declared_organization_intent",
        "declared_duplicate_suspicion",
        "declared_inventory_boundary",
    ] {
        oversized.push("--item".to_string());
        oversized.push(format!("personal_library:photo:observed:{claim}"));
    }
    assert!(PhotoLibraryCliCommand::from_args(&oversized)
        .expect_err("ninth item must fail")
        .contains("maximo 8"));
}

#[test]
fn supported_inventory_metadata_executes_bounded_local_review() -> TestResult {
    let input = typed_input(vec![evidence(
        PhotoInventoryItemLabel::Personal,
        PhotoInventoryCategory::Photo,
        PhotoEvidenceStatus::Observed,
        PhotoEvidenceClaim::DeclaredInventoryBoundary,
    )]);
    let result = execute_photo_library_organizer_agent(&package_root(), &input)?;

    assert_eq!(result.status, "completed");
    assert_eq!(result.classification, "photo_library_organization_review");
    assert_eq!(result.scope_id, PHOTO_LIBRARY_SCOPE_ID);
    assert_eq!(
        result.report.classification,
        PhotoLibraryReportClassification::PhotoLibraryReview
    );
    assert_eq!(
        result.report.next_step,
        PhotoLibraryNextStep::NoActionRequired
    );
    assert_eq!(result.report.items, input.items);
    assert_eq!(result.report.intent, input.intent);
    assert!(result.execution_authorized);
    assert!(result.package_loaded);
    assert!(result.sandbox_adapter_was_active);
    assert!(!result.os_isolation_claimed);
    assert!(result.cleanup_completed);
    assert!(result.audit_recorded);
    assert!(!result.scheduler_mutated);
    assert!(!result.transport_started);
    assert!(!result.persisted);

    let repeated = execute_photo_library_organizer_agent(&package_root(), &input)?;
    assert_eq!(result.report, repeated.report);
    Ok(())
}

#[test]
fn maximum_and_over_limit_inventory_are_handled_deterministically() -> TestResult {
    let claims = [
        PhotoEvidenceClaim::DeclaredInventoryBoundary,
        PhotoEvidenceClaim::DeclaredMetadataCompleteness,
        PhotoEvidenceClaim::DeclaredOrganizationIntent,
        PhotoEvidenceClaim::DeclaredDuplicateSuspicion,
    ];
    let labels = [
        PhotoInventoryItemLabel::Personal,
        PhotoInventoryItemLabel::Family,
        PhotoInventoryItemLabel::Travel,
        PhotoInventoryItemLabel::Archive,
    ];
    let categories = [
        PhotoInventoryCategory::Photo,
        PhotoInventoryCategory::Video,
        PhotoInventoryCategory::Screenshot,
        PhotoInventoryCategory::DocumentScan,
    ];

    let mut eight = Vec::new();
    for (index, label) in labels.into_iter().enumerate() {
        for claim in claims {
            if index >= 2 {
                continue;
            }
            eight.push(evidence(
                label,
                categories[index],
                PhotoEvidenceStatus::Observed,
                claim,
            ));
        }
    }
    assert_eq!(eight.len(), 8);
    let result = execute_photo_library_organizer_agent(&package_root(), &typed_input(eight))?;
    assert_eq!(result.report.items.len(), 8);
    assert_eq!(
        result.report.next_step,
        PhotoLibraryNextStep::NoActionRequired
    );

    let nine = input_from_json(&nine_item_json())?;
    assert_eq!(
        execute_photo_library_organizer_agent(&package_root(), &nine)
            .expect_err("nine items must be rejected")
            .code(),
        PhotoLibraryAgentErrorCode::InputInvalid
    );
    Ok(())
}

#[test]
fn supported_metadata_classes_stay_advisory_only() -> TestResult {
    for (status, claim) in [
        (
            PhotoEvidenceStatus::Observed,
            PhotoEvidenceClaim::DeclaredMetadataCompleteness,
        ),
        (
            PhotoEvidenceStatus::Observed,
            PhotoEvidenceClaim::DeclaredOrganizationIntent,
        ),
        (
            PhotoEvidenceStatus::Observed,
            PhotoEvidenceClaim::DeclaredDuplicateSuspicion,
        ),
        (
            PhotoEvidenceStatus::Attention,
            PhotoEvidenceClaim::DeclaredInventoryBoundary,
        ),
        (
            PhotoEvidenceStatus::Blocked,
            PhotoEvidenceClaim::DeclaredOrganizationIntent,
        ),
    ] {
        let input = typed_input(vec![evidence(
            PhotoInventoryItemLabel::Family,
            PhotoInventoryCategory::Photo,
            status,
            claim,
        )]);
        let result = execute_photo_library_organizer_agent(&package_root(), &input)?;
        assert_eq!(result.status, "completed");
        assert!(!result.scheduler_mutated);
        assert!(!result.transport_started);
        assert!(!result.persisted);
        assert_eq!(result.report.items, input.items);
    }
    Ok(())
}

#[test]
fn absent_or_missing_inventory_metadata_returns_blocked_report() -> TestResult {
    for input in [
        typed_input(Vec::new()),
        typed_input(vec![evidence(
            PhotoInventoryItemLabel::Archive,
            PhotoInventoryCategory::Screenshot,
            PhotoEvidenceStatus::Missing,
            PhotoEvidenceClaim::DeclaredMetadataCompleteness,
        )]),
    ] {
        let result = execute_photo_library_organizer_agent(&package_root(), &input)?;
        assert_eq!(result.classification, "blocked_action_report");
        assert_eq!(
            result.report.next_step,
            PhotoLibraryNextStep::ProvideRedactedInventoryMetadata
        );
        assert!(!result.transport_started);
        assert!(!result.persisted);
    }
    Ok(())
}

#[test]
fn attention_and_blocked_statuses_select_bounded_next_steps() -> TestResult {
    let attention = execute_photo_library_organizer_agent(
        &package_root(),
        &typed_input(vec![evidence(
            PhotoInventoryItemLabel::Travel,
            PhotoInventoryCategory::Video,
            PhotoEvidenceStatus::Attention,
            PhotoEvidenceClaim::DeclaredInventoryBoundary,
        )]),
    )?;
    assert_eq!(
        attention.report.next_step,
        PhotoLibraryNextStep::ReviewAttentionInventoryMetadata
    );

    let blocked = execute_photo_library_organizer_agent(
        &package_root(),
        &typed_input(vec![evidence(
            PhotoInventoryItemLabel::Travel,
            PhotoInventoryCategory::Video,
            PhotoEvidenceStatus::Blocked,
            PhotoEvidenceClaim::DeclaredInventoryBoundary,
        )]),
    )?;
    assert_eq!(
        blocked.report.next_step,
        PhotoLibraryNextStep::ReviewBlockedInventoryMetadata
    );
    Ok(())
}

#[test]
fn unsupported_claim_returns_handoff_without_side_effects() -> TestResult {
    let input = typed_input(vec![evidence(
        PhotoInventoryItemLabel::Archive,
        PhotoInventoryCategory::UnknownItem,
        PhotoEvidenceStatus::Observed,
        PhotoEvidenceClaim::UnsupportedClaim,
    )]);
    let result = execute_photo_library_organizer_agent(&package_root(), &input)?;
    assert_eq!(result.classification, "handoff_request");
    assert_eq!(
        result.report.next_step,
        PhotoLibraryNextStep::HandoffForPhotoOrFilesystemAction
    );
    assert!(!result.scheduler_mutated);
    assert!(!result.transport_started);
    assert!(!result.persisted);
    Ok(())
}

#[test]
fn serialized_contract_rejects_unknown_or_private_fields() {
    let private = r#"{"schema_version":"iamine.agent.photo-library-organizer.input-0.1","items":[],"photo_path":"/Users/private/secret"}"#;
    let error = serde_json::from_str::<PhotoLibraryInput>(private)
        .expect_err("unknown private field must be rejected")
        .to_string();
    assert!(!error.contains("/Users/private/secret"));
}

#[test]
fn unsupported_metadata_shapes_are_rejected_without_echo() {
    let shapes = [
        "personal_library:photo:observed:exif_metadata_record",
        "personal_library:photo:observed:gps_coordinates_value",
        "personal_library:photo:observed:face_person_label",
        "personal_library:photo:observed:image_bytes_request",
        "personal_library:photo:observed:raw_base64_media",
        "personal_library:photo:observed:cloud_transfer_request",
        "personal_library:photo:observed:network_scan_request",
        "personal_library:photo:observed:filesystem_read_request",
        "personal_library:photo:observed:move_and_rename_request",
        "personal_library:photo:observed:password=exif-secret-value",
        "personal_library:photo:observed:eyJhbGciOiJIUzI1NiJ9",
    ];
    for shape in shapes {
        let error = PhotoLibraryCliCommand::from_args(&strings(&[
            "--package-root",
            "agents/official/photo-library-organizer",
            "--item",
            shape,
        ]))
        .expect_err("unsupported metadata shape must fail closed");
        assert!(!error.contains("password"), "{shape}");
        assert!(!error.contains("eyJhbGciOiJIUzI1NiJ9"), "{shape}");
        assert!(!error.contains("exif-secret-value"), "{shape}");
    }
}

#[test]
fn bounded_review_output_never_echoes_raw_or_private_input() -> TestResult {
    let private_token = "personal_library:photo:observed:/home/operator/library";
    let error = PhotoLibraryCliCommand::from_args(&strings(&[
        "--package-root",
        "agents/official/photo-library-organizer",
        "--item",
        private_token,
    ]))
    .expect_err("path-shaped claim must fail closed");
    assert!(!error.contains("/home/operator/library"));

    let execution = execute_photo_library_organizer_agent(
        &package_root(),
        &typed_input(vec![evidence(
            PhotoInventoryItemLabel::Personal,
            PhotoInventoryCategory::Photo,
            PhotoEvidenceStatus::Observed,
            PhotoEvidenceClaim::DeclaredInventoryBoundary,
        )]),
    )?;
    let serialized = serde_json::to_string(&execution)?;
    assert!(!serialized.contains("personal_library:photo:observed"));
    assert!(!serialized.contains("/home/"));
    assert!(!serialized.contains("password"));
    assert!(serialized.contains("\"personal_library\""));
    Ok(())
}

#[test]
fn output_schema_is_stable_and_claims_no_performed_action() -> TestResult {
    let result = execute_photo_library_organizer_agent(
        &package_root(),
        &typed_input(vec![evidence(
            PhotoInventoryItemLabel::Family,
            PhotoInventoryCategory::Photo,
            PhotoEvidenceStatus::Observed,
            PhotoEvidenceClaim::DeclaredInventoryBoundary,
        )]),
    )?;
    assert_eq!(result.schema_version, PHOTO_LIBRARY_OUTPUT_SCHEMA_VERSION);
    assert_eq!(
        result.report.schema_version,
        PHOTO_LIBRARY_OUTPUT_SCHEMA_VERSION
    );
    assert_eq!(result.task_type, PHOTO_LIBRARY_TASK_TYPE);
    assert_eq!(result.package_id, PHOTO_LIBRARY_PACKAGE_ID);

    let serialized = serde_json::to_string(&result)?;
    for forbidden_claim in [
        "photos were viewed",
        "metadata was extracted",
        "duplicates were detected",
        "files were moved",
        "files were renamed",
        "albums were created",
        "library was changed",
    ] {
        assert!(!serialized.contains(forbidden_claim), "{forbidden_claim}");
    }
    Ok(())
}

#[test]
fn cli_runner_emits_bounded_human_and_json_output() -> TestResult {
    let root = package_root().to_string_lossy().to_string();
    let human = PhotoLibraryCliCommand::from_args(&strings(&[
        "--package-root",
        &root,
        "--item",
        "personal_library:photo:observed:declared_inventory_boundary",
    ]))?;
    run_photo_library_organizer_agent_cli(&human)?;

    let json = PhotoLibraryCliCommand::from_args(&strings(&[
        "--package-root",
        &root,
        "--item",
        "personal_library:photo:observed:declared_inventory_boundary",
        "--json",
    ]))?;
    run_photo_library_organizer_agent_cli(&json)?;
    Ok(())
}

#[test]
fn package_metadata_denies_execution_and_defers_library_readonly() -> TestResult {
    let root = package_root();
    let manifest = parse_and_validate_yaml(&read(&root, "agent.yaml")?)?;
    assert!(!manifest.execution_authorized);
    assert!(manifest.status == PackageStatus::BetaCandidate);
    assert!(manifest.agent.earliest_mode == ExecutionMode::LocalReadonly);
    assert_eq!(manifest.package_id, PHOTO_LIBRARY_PACKAGE_ID);
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
        "no_photo_discovery",
        "no_filesystem_access",
        "no_media_decoding",
        "no_metadata_extraction",
        "no_network_access",
        "no_credential_handling",
        "no_raw_text",
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
    assert!(!resources.model_dependencies.requires_model_download);
    assert!(!resources.privacy.stores_host_identifiers);
    assert!(!resources.privacy.stores_credentials);

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
            PHOTO_LIBRARY_PACKAGE_ID,
            PHOTO_LIBRARY_TASK_TYPE,
            PHOTO_LIBRARY_TASK_INPUT,
            "review_declared_photo_inventory",
            &INPUT_CLASSES,
            ScopeRequestClassification::InScopeCandidate,
        ),
    );
    assert_eq!(scope_evaluation.decision(), ScopeDecision::Allow);

    let permitted = evaluate_permissions(
        &permission_policy,
        &scope_evaluation,
        PermissionRequestRef::new(
            PHOTO_LIBRARY_PACKAGE_ID,
            "review_declared_photo_inventory",
            &REQUIRED_CATEGORIES,
            PermissionConfirmation::NotProvided,
        ),
    );
    assert_eq!(permitted.decision(), PermissionDecision::Allow);

    let escalating: [(&str, &[&str]); 5] = [
        ("create_albums", &REQUIRED_CATEGORIES),
        ("discover_photo_libraries", &REQUIRED_CATEGORIES),
        ("read_photo_file_contents", &REQUIRED_CATEGORIES),
        ("run_shell", &REQUIRED_CATEGORIES),
        (
            "review_declared_photo_inventory",
            &["unrestricted_filesystem"],
        ),
    ];
    for (action, categories) in escalating {
        let evaluation = evaluate_permissions(
            &permission_policy,
            &scope_evaluation,
            PermissionRequestRef::new(
                PHOTO_LIBRARY_PACKAGE_ID,
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
fn wrong_package_and_scope_mismatch_are_refused() -> TestResult {
    let root = package_root();
    let scope_metadata = parse_scope_policy_yaml(&read(&root, "agent-scope.yaml")?)?;
    let scope_policy = scope_policy(&scope_metadata)?;

    let wrong_package = evaluate_scope(
        &scope_policy,
        ScopeRequestRef::new(
            "iamine.beta.lan-file-share-assistant",
            PHOTO_LIBRARY_TASK_TYPE,
            PHOTO_LIBRARY_TASK_INPUT,
            "review_declared_photo_inventory",
            &INPUT_CLASSES,
            ScopeRequestClassification::InScopeCandidate,
        ),
    );
    assert_eq!(
        wrong_package.decision(),
        ScopeDecision::HandoffToOrchestrator
    );

    let wrong_task_type = evaluate_scope(
        &scope_policy,
        ScopeRequestRef::new(
            PHOTO_LIBRARY_PACKAGE_ID,
            "file_share_readonly_review",
            PHOTO_LIBRARY_TASK_INPUT,
            "review_declared_photo_inventory",
            &INPUT_CLASSES,
            ScopeRequestClassification::InScopeCandidate,
        ),
    );
    assert_eq!(
        wrong_task_type.decision(),
        ScopeDecision::HandoffToOrchestrator
    );

    let wrong_operation = evaluate_scope(
        &scope_policy,
        ScopeRequestRef::new(
            PHOTO_LIBRARY_PACKAGE_ID,
            PHOTO_LIBRARY_TASK_TYPE,
            PHOTO_LIBRARY_TASK_INPUT,
            "reorganize_photo_library",
            &INPUT_CLASSES,
            ScopeRequestClassification::InScopeCandidate,
        ),
    );
    assert_eq!(
        wrong_operation.decision(),
        ScopeDecision::HandoffToOrchestrator
    );
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
        execute_photo_library_organizer_agent(altered_reference.path(), &typed_input(Vec::new()))
            .expect_err("altered reference must fail")
            .code(),
        PhotoLibraryAgentErrorCode::PackageMismatch
    );

    let altered_manifest = copied_package()?;
    let manifest = altered_manifest.path().join("agent.yaml");
    let input = fs::read_to_string(&manifest)?;
    fs::write(
        &manifest,
        input.replace(
            "display_name: Photo Library Organizer",
            "display_name: Altered Organizer",
        ),
    )?;
    assert_eq!(
        execute_photo_library_organizer_agent(altered_manifest.path(), &typed_input(Vec::new()))
            .expect_err("altered manifest must fail")
            .code(),
        PhotoLibraryAgentErrorCode::PackageMismatch
    );
    Ok(())
}

#[test]
fn missing_package_fails_closed() -> TestResult {
    let temp = tempfile::tempdir()?;
    let missing = temp.path().join("missing-package");

    let error = execute_photo_library_organizer_agent(&missing, &typed_input(Vec::new()))
        .expect_err("missing package must fail closed");
    assert_eq!(error.code(), PhotoLibraryAgentErrorCode::PackageUnavailable);
    Ok(())
}

fn typed_input(items: Vec<PhotoInventoryEvidence>) -> PhotoLibraryInput {
    PhotoLibraryInput {
        schema_version: PHOTO_LIBRARY_INPUT_SCHEMA_VERSION.to_string(),
        items,
        intent: None,
    }
}

fn input_from_json(json: &str) -> TestResult<PhotoLibraryInput> {
    Ok(serde_json::from_str(json)?)
}

fn nine_item_json() -> String {
    let mut items = Vec::new();
    for claim in [
        "declared_inventory_boundary",
        "declared_metadata_completeness",
        "declared_organization_intent",
    ] {
        for label in ["personal_library", "family_library", "travel_library"] {
            items.push(format!(
                r#"{{"item":"{label}","category":"photo","status":"observed","claim":"{claim}"}}"#
            ));
        }
    }
    assert_eq!(items.len(), 9);
    format!(
        r#"{{"schema_version":"iamine.agent.photo-library-organizer.input-0.1","items":[{}],"intent":null}}"#,
        items.join(",")
    )
}

const fn evidence(
    item: PhotoInventoryItemLabel,
    category: PhotoInventoryCategory,
    status: PhotoEvidenceStatus,
    claim: PhotoEvidenceClaim,
) -> PhotoInventoryEvidence {
    PhotoInventoryEvidence {
        item,
        category,
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../agents/official/photo-library-organizer")
}

fn read(root: &Path, relative: &str) -> TestResult<String> {
    Ok(fs::read_to_string(root.join(relative))?)
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}
