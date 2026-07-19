use std::error::Error;

use iamine_agents::{
    evaluate_permissions, evaluate_scope, PermissionConfirmation, PermissionDecision,
    PermissionDefaultPolicy, PermissionPolicy, PermissionPolicyErrorCode, PermissionPolicySpec,
    PermissionReasonCode, PermissionRequestRef, ScopeEvaluation, ScopePolicy, ScopePolicySpec,
    ScopeRequestClassification, ScopeRequestRef,
};

const PACKAGE_ID: &str = "iamine.beta.node-doctor";
const LOCAL_READONLY: &[&str] = &["local_readonly"];

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn approved_request_after_scope_is_allowed() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let result = evaluate_permissions(&policy, &scope, valid_request());

    assert_eq!(result.decision(), PermissionDecision::Allow);
    assert_eq!(result.reason(), PermissionReasonCode::Permitted);
    assert!(result.allowed());
    assert!(!result.confirmation_required());
    Ok(())
}

#[test]
fn scope_refusals_cannot_be_overridden_by_confirmation() -> TestResult {
    let policy = valid_permission_policy()?;
    let request = PermissionRequestRef::new(
        PACKAGE_ID,
        "inspect_status",
        LOCAL_READONLY,
        PermissionConfirmation::TrustedOrchestratorConfirmed,
    );

    for classification in [
        ScopeRequestClassification::Dangerous,
        ScopeRequestClassification::PermissionEscalation,
        ScopeRequestClassification::PromptInjection,
        ScopeRequestClassification::RoleConfusion,
    ] {
        let scope = scope_evaluation(classification)?;
        let result = evaluate_permissions(&policy, &scope, request);

        assert_eq!(result.decision(), PermissionDecision::HandoffToOrchestrator);
        assert_eq!(result.reason(), PermissionReasonCode::ScopeGateNotPassed);
        assert!(!result.allowed());
    }
    Ok(())
}

#[test]
fn package_mismatch_is_refused() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let request = PermissionRequestRef::new(
        "iamine.beta.reporter",
        "inspect_status",
        LOCAL_READONLY,
        PermissionConfirmation::NotProvided,
    );
    let result = evaluate_permissions(&policy, &scope, request);

    assert_eq!(result.decision(), PermissionDecision::Refuse);
    assert_eq!(result.reason(), PermissionReasonCode::PackageMismatch);
    Ok(())
}

#[test]
fn blocked_action_is_refused_even_with_confirmation() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let request = PermissionRequestRef::new(
        PACKAGE_ID,
        "run_shell",
        LOCAL_READONLY,
        PermissionConfirmation::TrustedOrchestratorConfirmed,
    );
    let result = evaluate_permissions(&policy, &scope, request);

    assert_eq!(result.decision(), PermissionDecision::Refuse);
    assert_eq!(result.reason(), PermissionReasonCode::BlockedAction);
    Ok(())
}

#[test]
fn forbidden_category_is_refused_even_with_confirmation() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let request = PermissionRequestRef::new(
        PACKAGE_ID,
        "inspect_status",
        &["credential_access"],
        PermissionConfirmation::TrustedOrchestratorConfirmed,
    );
    let result = evaluate_permissions(&policy, &scope, request);

    assert_eq!(result.decision(), PermissionDecision::Refuse);
    assert_eq!(result.reason(), PermissionReasonCode::ForbiddenCategory);
    Ok(())
}

#[test]
fn undeclared_action_and_category_are_refused() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let cases = [
        (
            PermissionRequestRef::new(
                PACKAGE_ID,
                "invented_action",
                LOCAL_READONLY,
                PermissionConfirmation::NotProvided,
            ),
            PermissionReasonCode::UndeclaredAction,
        ),
        (
            PermissionRequestRef::new(
                PACKAGE_ID,
                "inspect_status",
                &["invented_permission"],
                PermissionConfirmation::NotProvided,
            ),
            PermissionReasonCode::UndeclaredCategory,
        ),
    ];

    for (request, reason) in cases {
        let result = evaluate_permissions(&policy, &scope, request);
        assert_eq!(result.decision(), PermissionDecision::Refuse);
        assert_eq!(result.reason(), reason);
    }
    Ok(())
}

#[test]
fn confirmation_only_completes_an_approved_permission() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let pending = PermissionRequestRef::new(
        PACKAGE_ID,
        "summarize_status",
        &["redacted_status_summary"],
        PermissionConfirmation::NotProvided,
    );
    let confirmed = PermissionRequestRef::new(
        PACKAGE_ID,
        "summarize_status",
        &["redacted_status_summary"],
        PermissionConfirmation::TrustedOrchestratorConfirmed,
    );

    let pending_result = evaluate_permissions(&policy, &scope, pending);
    assert_eq!(
        pending_result.decision(),
        PermissionDecision::RequireConfirmation
    );
    assert_eq!(
        pending_result.reason(),
        PermissionReasonCode::ConfirmationRequired
    );
    assert!(pending_result.confirmation_required());

    let confirmed_result = evaluate_permissions(&policy, &scope, confirmed);
    assert_eq!(confirmed_result.decision(), PermissionDecision::Allow);
    assert_eq!(confirmed_result.reason(), PermissionReasonCode::Permitted);
    Ok(())
}

#[test]
fn category_can_independently_require_confirmation() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let request = PermissionRequestRef::new(
        PACKAGE_ID,
        "inspect_status",
        &["lan_readonly_metadata"],
        PermissionConfirmation::NotProvided,
    );
    let result = evaluate_permissions(&policy, &scope, request);

    assert_eq!(result.decision(), PermissionDecision::RequireConfirmation);
    assert_eq!(result.reason(), PermissionReasonCode::ConfirmationRequired);
    Ok(())
}

#[test]
fn malformed_empty_duplicate_and_oversized_requests_fail_closed() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let oversized = [
        "category_00",
        "category_01",
        "category_02",
        "category_03",
        "category_04",
        "category_05",
        "category_06",
        "category_07",
        "category_08",
        "category_09",
        "category_10",
        "category_11",
        "category_12",
        "category_13",
        "category_14",
        "category_15",
        "category_16",
    ];
    let cases = [
        PermissionRequestRef::new(
            PACKAGE_ID,
            "not valid",
            LOCAL_READONLY,
            PermissionConfirmation::NotProvided,
        ),
        PermissionRequestRef::new(
            PACKAGE_ID,
            "inspect_status",
            &[],
            PermissionConfirmation::NotProvided,
        ),
        PermissionRequestRef::new(
            PACKAGE_ID,
            "inspect_status",
            &["local_readonly", "local_readonly"],
            PermissionConfirmation::NotProvided,
        ),
        PermissionRequestRef::new(
            PACKAGE_ID,
            "inspect_status",
            &oversized,
            PermissionConfirmation::NotProvided,
        ),
    ];

    for request in cases {
        let result = evaluate_permissions(&policy, &scope, request);
        assert_eq!(result.decision(), PermissionDecision::Refuse);
        assert_eq!(result.reason(), PermissionReasonCode::InvalidRequest);
    }
    Ok(())
}

#[test]
fn policy_requires_deny_by_default() {
    let mut spec = valid_permission_spec();
    spec.default_policy = PermissionDefaultPolicy::Allow;
    let error = PermissionPolicy::try_from(spec).expect_err("permissive default must fail");

    assert_eq!(error.code(), PermissionPolicyErrorCode::PermissiveDefault);
    assert_eq!(error.field(), "default_policy");
}

#[test]
fn policy_rejects_invalid_or_broad_identifiers() {
    let mut invalid_package = valid_permission_spec();
    invalid_package.package_id = "third-party.agent".to_string();
    assert_eq!(
        PermissionPolicy::try_from(invalid_package)
            .expect_err("non-IAMINE package must fail")
            .code(),
        PermissionPolicyErrorCode::InvalidIdentifier
    );

    let mut broad_profile = valid_permission_spec();
    broad_profile.permission_profile_id = "all_access".to_string();
    assert_eq!(
        PermissionPolicy::try_from(broad_profile)
            .expect_err("broad permission profile must fail")
            .code(),
        PermissionPolicyErrorCode::InvalidIdentifier
    );
}

#[test]
fn policy_rejects_empty_duplicate_and_oversized_collections() {
    let mut empty = valid_permission_spec();
    empty.approved_actions.clear();
    assert_eq!(
        PermissionPolicy::try_from(empty)
            .expect_err("empty required collection must fail")
            .code(),
        PermissionPolicyErrorCode::InvalidCollection
    );

    let mut duplicate = valid_permission_spec();
    duplicate
        .approved_categories
        .push("local_readonly".to_string());
    assert_eq!(
        PermissionPolicy::try_from(duplicate)
            .expect_err("duplicate collection value must fail")
            .code(),
        PermissionPolicyErrorCode::DuplicateValue
    );

    let mut oversized = valid_permission_spec();
    oversized.approved_actions = (0..65).map(|index| format!("action_{index}")).collect();
    assert_eq!(
        PermissionPolicy::try_from(oversized)
            .expect_err("oversized collection must fail")
            .code(),
        PermissionPolicyErrorCode::InvalidCollection
    );
}

#[test]
fn policy_requires_unsafe_categories_and_actions_to_remain_blocked() {
    let mut missing_category = valid_permission_spec();
    missing_category
        .forbidden_categories
        .retain(|value| value != "credential_access");
    let error = PermissionPolicy::try_from(missing_category)
        .expect_err("missing forbidden category must fail");
    assert_eq!(
        error.code(),
        PermissionPolicyErrorCode::MissingSafetyBoundary
    );
    assert_eq!(error.field(), "forbidden_categories");

    let mut missing_action = valid_permission_spec();
    missing_action
        .blocked_actions
        .retain(|value| value != "run_shell");
    let error =
        PermissionPolicy::try_from(missing_action).expect_err("missing blocked action must fail");
    assert_eq!(
        error.code(),
        PermissionPolicyErrorCode::MissingSafetyBoundary
    );
    assert_eq!(error.field(), "blocked_actions");
}

#[test]
fn policy_rejects_unsupported_and_contradictory_permissions() {
    let mut unsupported = valid_permission_spec();
    unsupported
        .approved_categories
        .push("credential_access".to_string());
    assert_eq!(
        PermissionPolicy::try_from(unsupported)
            .expect_err("unsupported approved category must fail")
            .code(),
        PermissionPolicyErrorCode::UnsupportedPermission
    );

    let mut contradictory = valid_permission_spec();
    contradictory
        .forbidden_categories
        .push("local_readonly".to_string());
    assert_eq!(
        PermissionPolicy::try_from(contradictory)
            .expect_err("contradictory category boundary must fail")
            .code(),
        PermissionPolicyErrorCode::ContradictoryBoundary
    );
}

#[test]
fn confirmation_cannot_be_declared_for_unapproved_permissions() {
    let mut category = valid_permission_spec();
    category
        .confirmation_required_categories
        .push("package_relative_review_files".to_string());
    assert_eq!(
        PermissionPolicy::try_from(category)
            .expect_err("unapproved confirmation category must fail")
            .code(),
        PermissionPolicyErrorCode::InvalidConfirmationBoundary
    );

    let mut action = valid_permission_spec();
    action
        .confirmation_required_actions
        .push("inspect_private_state".to_string());
    assert_eq!(
        PermissionPolicy::try_from(action)
            .expect_err("unapproved confirmation action must fail")
            .code(),
        PermissionPolicyErrorCode::InvalidConfirmationBoundary
    );
}

#[test]
fn debug_output_does_not_echo_policy_or_request_values() -> TestResult {
    let spec = valid_permission_spec();
    let spec_debug = format!("{spec:?}");
    let policy = PermissionPolicy::try_from(spec)?;
    let scope = allowed_scope()?;
    let request_debug = format!("{:?}", valid_request());
    let policy_debug = format!("{policy:?}");
    let evaluation_debug = format!(
        "{:?}",
        evaluate_permissions(&policy, &scope, valid_request())
    );

    for output in [spec_debug, request_debug, policy_debug, evaluation_debug] {
        assert!(!output.contains(PACKAGE_ID));
        assert!(!output.contains("local_readonly"));
        assert!(!output.contains("inspect_status"));
        assert!(!output.contains("credential_access"));
    }
    Ok(())
}

#[test]
fn evaluation_and_codes_are_stable_and_deterministic() -> TestResult {
    let policy = valid_permission_policy()?;
    let scope = allowed_scope()?;
    let first = evaluate_permissions(&policy, &scope, valid_request());
    let second = evaluate_permissions(&policy, &scope, valid_request());

    assert_eq!(first, second);
    assert_eq!(PermissionDecision::Allow.as_str(), "allow");
    assert_eq!(
        PermissionDecision::RequireConfirmation.as_str(),
        "require_confirmation"
    );
    assert_eq!(PermissionDecision::Refuse.as_str(), "refuse");
    assert_eq!(
        PermissionDecision::HandoffToOrchestrator.as_str(),
        "handoff_to_orchestrator"
    );
    assert_eq!(PermissionReasonCode::Permitted.as_str(), "permitted");
    assert_eq!(
        PermissionReasonCode::ForbiddenCategory.as_str(),
        "forbidden_category"
    );
    assert_eq!(
        PermissionReasonCode::ConfirmationRequired.as_str(),
        "confirmation_required"
    );
    Ok(())
}

fn valid_request() -> PermissionRequestRef<'static> {
    PermissionRequestRef::new(
        PACKAGE_ID,
        "inspect_status",
        LOCAL_READONLY,
        PermissionConfirmation::NotProvided,
    )
}

fn valid_permission_policy() -> Result<PermissionPolicy, Box<dyn Error>> {
    Ok(PermissionPolicy::try_from(valid_permission_spec())?)
}

fn valid_permission_spec() -> PermissionPolicySpec {
    PermissionPolicySpec {
        package_id: PACKAGE_ID.to_string(),
        permission_profile_id: "node_doctor_local_readonly_permissions".to_string(),
        default_policy: PermissionDefaultPolicy::Deny,
        approved_categories: strings(&[
            "lan_readonly_metadata",
            "local_readonly",
            "redacted_status_summary",
            "user_provided_text",
        ]),
        forbidden_categories: strings(&[
            "arbitrary_shell",
            "credential_access",
            "destructive_write",
            "mainnet_operation",
            "marketplace_publish",
            "model_download",
            "model_load",
            "network_mutation",
            "private_key_access",
            "service_mutation",
            "unrestricted_filesystem",
            "vm_or_container_mutation",
            "wallet_access",
        ]),
        approved_actions: strings(&["inspect_status", "summarize_status"]),
        blocked_actions: strings(&[
            "access_private_keys",
            "access_wallet",
            "collect_credentials",
            "delete_files",
            "download_models",
            "load_models",
            "mainnet_operation",
            "mutate_network",
            "mutate_vm_or_container",
            "publish_agent",
            "read_private_files",
            "restart_services",
            "reward_operation",
            "run_shell",
            "settlement_operation",
            "token_operation",
            "write_files",
        ]),
        confirmation_required_categories: strings(&["lan_readonly_metadata"]),
        confirmation_required_actions: strings(&["summarize_status"]),
    }
}

fn allowed_scope() -> Result<ScopeEvaluation, Box<dyn Error>> {
    scope_evaluation(ScopeRequestClassification::InScopeCandidate)
}

fn scope_evaluation(
    classification: ScopeRequestClassification,
) -> Result<ScopeEvaluation, Box<dyn Error>> {
    let policy = ScopePolicy::try_from(scope_policy_spec())?;
    Ok(evaluate_scope(
        &policy,
        ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            "inspect_node_status",
            "inspect_status",
            &["user_provided_text"],
            classification,
        ),
    ))
}

fn scope_policy_spec() -> ScopePolicySpec {
    ScopePolicySpec {
        package_id: PACKAGE_ID.to_string(),
        scope_id: "node_status_diagnostic".to_string(),
        task_types: strings(&["diagnostic_report"]),
        in_scope_tasks: strings(&["inspect_node_status"]),
        out_of_scope_tasks: strings(&["mutate_node_status"]),
        allowed_input_classes: strings(&["user_provided_text"]),
        forbidden_input_classes: strings(&[
            "credentials",
            "disk_uuids",
            "full_hostnames",
            "home_directories",
            "ip_addresses",
            "mac_addresses",
            "machine_ids",
            "personal_paths",
            "private_keys",
            "private_paths",
            "raw_process_lists",
            "serial_numbers",
            "usernames",
            "wallet_keys",
        ]),
        allowed_operations: strings(&["inspect_status"]),
        blocked_actions: strings(&[
            "change_settings",
            "delete_files",
            "download_models",
            "load_models",
            "mutate_vm_or_container",
            "publish_agent",
            "restart_services",
            "run_shell",
            "scan_network",
            "write_files",
        ]),
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}
