use std::error::Error;

use iamine_agents::{
    evaluate_scope, ScopeDecision, ScopePolicy, ScopePolicyErrorCode, ScopePolicySpec,
    ScopeReasonCode, ScopeRequestClassification, ScopeRequestRef,
};

const PACKAGE_ID: &str = "iamine.beta.node-doctor";
const INPUTS: &[&str] = &["iamine_node_status_summary"];

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn narrow_declared_request_is_allowed() -> TestResult {
    let policy = valid_policy()?;
    let result = evaluate_scope(&policy, valid_request());

    assert_eq!(result.decision(), ScopeDecision::Allow);
    assert_eq!(result.reason(), ScopeReasonCode::InScope);
    assert!(result.allowed());
    Ok(())
}

#[test]
fn decision_and_reason_codes_are_stable() {
    assert_eq!(ScopeDecision::Allow.as_str(), "allow");
    assert_eq!(ScopeDecision::Clarify.as_str(), "clarify");
    assert_eq!(ScopeDecision::Refuse.as_str(), "refuse");
    assert_eq!(
        ScopeDecision::HandoffToOrchestrator.as_str(),
        "handoff_to_orchestrator"
    );
    assert_eq!(ScopeReasonCode::InScope.as_str(), "in_scope");
    assert_eq!(
        ScopeReasonCode::PermissionEscalation.as_str(),
        "permission_escalation"
    );
    assert_eq!(
        ScopeReasonCode::OutsideDeclaredScope.as_str(),
        "outside_declared_scope"
    );
}

#[test]
fn boundary_classifications_fail_closed() -> TestResult {
    let policy = valid_policy()?;
    let cases = [
        (
            ScopeRequestClassification::Ambiguous,
            ScopeDecision::Clarify,
            ScopeReasonCode::AmbiguousTask,
        ),
        (
            ScopeRequestClassification::Dangerous,
            ScopeDecision::Refuse,
            ScopeReasonCode::DangerousTask,
        ),
        (
            ScopeRequestClassification::CrossDomain,
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::CrossDomainTask,
        ),
        (
            ScopeRequestClassification::PermissionEscalation,
            ScopeDecision::Refuse,
            ScopeReasonCode::PermissionEscalation,
        ),
        (
            ScopeRequestClassification::PromptInjection,
            ScopeDecision::Refuse,
            ScopeReasonCode::PromptInjection,
        ),
        (
            ScopeRequestClassification::RoleConfusion,
            ScopeDecision::Refuse,
            ScopeReasonCode::RoleConfusion,
        ),
    ];

    for (classification, expected_decision, expected_reason) in cases {
        let request = ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            "explain_iamine_node_readiness",
            "draft_explanation",
            INPUTS,
            classification,
        );
        let result = evaluate_scope(&policy, request);

        assert_eq!(result.decision(), expected_decision);
        assert_eq!(result.reason(), expected_reason);
        assert!(!result.allowed());
    }
    Ok(())
}

#[test]
fn package_and_task_type_mismatches_return_to_orchestrator() -> TestResult {
    let policy = valid_policy()?;
    let cases = [
        (
            ScopeRequestRef::new(
                "iamine.beta.reporter",
                "diagnostic_report",
                "explain_iamine_node_readiness",
                "draft_explanation",
                INPUTS,
                ScopeRequestClassification::InScopeCandidate,
            ),
            ScopeReasonCode::PackageMismatch,
        ),
        (
            ScopeRequestRef::new(
                PACKAGE_ID,
                "general_help",
                "explain_iamine_node_readiness",
                "draft_explanation",
                INPUTS,
                ScopeRequestClassification::InScopeCandidate,
            ),
            ScopeReasonCode::UnsupportedTaskType,
        ),
    ];

    for (request, expected_reason) in cases {
        let result = evaluate_scope(&policy, request);
        assert_eq!(result.decision(), ScopeDecision::HandoffToOrchestrator);
        assert_eq!(result.reason(), expected_reason);
    }
    Ok(())
}

#[test]
fn excluded_and_unknown_tasks_return_to_orchestrator() -> TestResult {
    let policy = valid_policy()?;
    for task in ["repair_system_settings", "invented_task"] {
        let request = ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            task,
            "draft_explanation",
            INPUTS,
            ScopeRequestClassification::InScopeCandidate,
        );
        let result = evaluate_scope(&policy, request);

        assert_eq!(result.decision(), ScopeDecision::HandoffToOrchestrator);
        assert_eq!(result.reason(), ScopeReasonCode::OutsideDeclaredScope);
    }
    Ok(())
}

#[test]
fn blocked_action_is_refused_even_for_an_in_scope_task() -> TestResult {
    let policy = valid_policy()?;
    let request = ScopeRequestRef::new(
        PACKAGE_ID,
        "diagnostic_report",
        "explain_iamine_node_readiness",
        "run_shell",
        INPUTS,
        ScopeRequestClassification::InScopeCandidate,
    );
    let result = evaluate_scope(&policy, request);

    assert_eq!(result.decision(), ScopeDecision::Refuse);
    assert_eq!(result.reason(), ScopeReasonCode::BlockedAction);
    Ok(())
}

#[test]
fn unknown_operation_returns_to_orchestrator() -> TestResult {
    let policy = valid_policy()?;
    let request = ScopeRequestRef::new(
        PACKAGE_ID,
        "diagnostic_report",
        "explain_iamine_node_readiness",
        "unknown_operation",
        INPUTS,
        ScopeRequestClassification::InScopeCandidate,
    );
    let result = evaluate_scope(&policy, request);

    assert_eq!(result.decision(), ScopeDecision::HandoffToOrchestrator);
    assert_eq!(result.reason(), ScopeReasonCode::UnsupportedOperation);
    Ok(())
}

#[test]
fn forbidden_input_is_refused_and_unknown_input_is_handed_off() -> TestResult {
    let policy = valid_policy()?;
    let cases = [
        (
            &["credentials"][..],
            ScopeDecision::Refuse,
            ScopeReasonCode::ForbiddenInput,
        ),
        (
            &["unknown_input_class"][..],
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::UnsupportedInput,
        ),
    ];

    for (inputs, expected_decision, expected_reason) in cases {
        let request = ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            "explain_iamine_node_readiness",
            "draft_explanation",
            inputs,
            ScopeRequestClassification::InScopeCandidate,
        );
        let result = evaluate_scope(&policy, request);

        assert_eq!(result.decision(), expected_decision);
        assert_eq!(result.reason(), expected_reason);
    }
    Ok(())
}

#[test]
fn malformed_or_oversized_requests_fail_closed() -> TestResult {
    let policy = valid_policy()?;
    let oversized = ["iamine_node_status_summary"; 17];
    let requests = [
        ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            "not valid",
            "draft_explanation",
            INPUTS,
            ScopeRequestClassification::InScopeCandidate,
        ),
        ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            "explain_iamine_node_readiness",
            "draft_explanation",
            &oversized,
            ScopeRequestClassification::InScopeCandidate,
        ),
    ];

    for request in requests {
        let result = evaluate_scope(&policy, request);
        assert_eq!(result.decision(), ScopeDecision::HandoffToOrchestrator);
        assert_eq!(result.reason(), ScopeReasonCode::InvalidRequest);
    }
    Ok(())
}

#[test]
fn policy_rejects_broad_scope_and_task_type() {
    for (scope_id, task_type) in [
        ("general_assistant", "diagnostic_report"),
        ("node_readiness_diagnostic_report", "general_help"),
    ] {
        let mut spec = valid_spec();
        spec.scope_id = scope_id.to_string();
        spec.task_types = vec![task_type.to_string()];
        let error = ScopePolicy::try_from(spec).expect_err("broad policy must fail");

        assert_eq!(error.code(), ScopePolicyErrorCode::InvalidIdentifier);
    }
}

#[test]
fn policy_rejects_empty_duplicate_and_oversized_collections() {
    let mut empty = valid_spec();
    empty.in_scope_tasks.clear();
    assert_eq!(
        ScopePolicy::try_from(empty)
            .expect_err("empty boundary must fail")
            .code(),
        ScopePolicyErrorCode::InvalidCollection
    );

    let mut duplicate = valid_spec();
    duplicate.task_types.push("diagnostic_report".to_string());
    assert_eq!(
        ScopePolicy::try_from(duplicate)
            .expect_err("duplicate boundary must fail")
            .code(),
        ScopePolicyErrorCode::DuplicateValue
    );

    let mut oversized = valid_spec();
    oversized.in_scope_tasks = (0..65).map(|index| format!("task_{index}")).collect();
    assert_eq!(
        ScopePolicy::try_from(oversized)
            .expect_err("oversized boundary must fail")
            .code(),
        ScopePolicyErrorCode::InvalidCollection
    );
}

#[test]
fn policy_rejects_contradictory_boundaries() {
    let mut task = valid_spec();
    task.out_of_scope_tasks
        .push("explain_iamine_node_readiness".to_string());
    assert_eq!(
        ScopePolicy::try_from(task)
            .expect_err("overlapping task boundary must fail")
            .code(),
        ScopePolicyErrorCode::ContradictoryBoundary
    );

    let mut input = valid_spec();
    input.allowed_input_classes.push("credentials".to_string());
    assert_eq!(
        ScopePolicy::try_from(input)
            .expect_err("overlapping input boundary must fail")
            .code(),
        ScopePolicyErrorCode::ContradictoryBoundary
    );

    let mut operation = valid_spec();
    operation.allowed_operations.push("run_shell".to_string());
    assert_eq!(
        ScopePolicy::try_from(operation)
            .expect_err("overlapping operation boundary must fail")
            .code(),
        ScopePolicyErrorCode::ContradictoryBoundary
    );
}

#[test]
fn policy_requires_privacy_and_mutation_denies() {
    let mut missing_input_deny = valid_spec();
    missing_input_deny
        .forbidden_input_classes
        .retain(|value| value != "credentials");
    let error = ScopePolicy::try_from(missing_input_deny)
        .expect_err("missing sensitive input deny must fail");
    assert_eq!(error.code(), ScopePolicyErrorCode::MissingSafetyBoundary);
    assert_eq!(error.field(), "forbidden_input_classes");

    let mut missing_action_deny = valid_spec();
    missing_action_deny
        .blocked_actions
        .retain(|value| value != "run_shell");
    let error =
        ScopePolicy::try_from(missing_action_deny).expect_err("missing blocked action must fail");
    assert_eq!(error.code(), ScopePolicyErrorCode::MissingSafetyBoundary);
    assert_eq!(error.field(), "blocked_actions");
}

#[test]
fn policy_and_evaluation_debug_output_do_not_echo_declared_values() -> TestResult {
    let policy = valid_policy()?;
    let request_debug = format!("{:?}", valid_request());
    let policy_debug = format!("{policy:?}");
    let evaluation_debug = format!("{:?}", evaluate_scope(&policy, valid_request()));

    assert!(!request_debug.contains(PACKAGE_ID));
    assert!(!request_debug.contains("iamine_node_status_summary"));
    assert!(!policy_debug.contains(PACKAGE_ID));
    assert!(!policy_debug.contains("private_paths"));
    assert!(!evaluation_debug.contains(PACKAGE_ID));
    assert!(!evaluation_debug.contains("iamine_node_status_summary"));
    Ok(())
}

#[test]
fn evaluation_is_deterministic() -> TestResult {
    let policy = valid_policy()?;
    let first = evaluate_scope(&policy, valid_request());
    let second = evaluate_scope(&policy, valid_request());

    assert_eq!(first, second);
    Ok(())
}

fn valid_request() -> ScopeRequestRef<'static> {
    ScopeRequestRef::new(
        PACKAGE_ID,
        "diagnostic_report",
        "explain_iamine_node_readiness",
        "draft_explanation",
        INPUTS,
        ScopeRequestClassification::InScopeCandidate,
    )
}

fn valid_policy() -> Result<ScopePolicy, Box<dyn Error>> {
    Ok(ScopePolicy::try_from(valid_spec())?)
}

fn valid_spec() -> ScopePolicySpec {
    ScopePolicySpec {
        package_id: PACKAGE_ID.to_string(),
        scope_id: "node_readiness_diagnostic_report".to_string(),
        task_types: strings(&["diagnostic_report"]),
        in_scope_tasks: strings(&[
            "explain_iamine_node_readiness",
            "summarize_allowed_status_evidence",
            "suggest_non_destructive_next_steps",
        ]),
        out_of_scope_tasks: strings(&[
            "collect_credentials",
            "delete_files",
            "repair_system_settings",
            "restart_services",
            "run_shell_commands",
        ]),
        allowed_input_classes: strings(&[
            "iamine_node_status_summary",
            "iamine_readiness_checklist",
            "user_provided_error_text",
        ]),
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
        allowed_operations: strings(&[
            "classify_status",
            "draft_explanation",
            "read_declared_summary",
            "suggest_next_steps",
        ]),
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
