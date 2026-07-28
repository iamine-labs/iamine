use std::error::Error;

use iamine_agents::{
    evaluate_permissions, evaluate_scope, PermissionConfirmation, PermissionEvaluation,
    PermissionPolicy, PermissionPolicySpec, PermissionRequestRef, ScopeEvaluation, ScopePolicy,
    ScopePolicySpec, ScopeRequestClassification, ScopeRequestRef,
};

type TestResult<T> = Result<T, Box<dyn Error>>;

pub const PACKAGE_ID: &str = "iamine.beta.node-doctor";
pub const TASK_TYPE: &str = "diagnostic_report";

pub fn allowed_scope() -> TestResult<ScopeEvaluation> {
    scope_evaluation(ScopeRequestClassification::InScopeCandidate)
}

pub fn clarify_scope() -> TestResult<ScopeEvaluation> {
    scope_evaluation(ScopeRequestClassification::Ambiguous)
}

pub fn refused_scope() -> TestResult<ScopeEvaluation> {
    scope_evaluation(ScopeRequestClassification::Dangerous)
}

pub fn allowed_permission(scope: &ScopeEvaluation) -> TestResult<PermissionEvaluation> {
    permission_evaluation(
        scope,
        "inspect_status",
        &["local_readonly"],
        PermissionConfirmation::NotProvided,
    )
}

pub fn confirmation_permission(scope: &ScopeEvaluation) -> TestResult<PermissionEvaluation> {
    permission_evaluation(
        scope,
        "summarize_status",
        &["lan_readonly_metadata"],
        PermissionConfirmation::NotProvided,
    )
}

pub fn refused_permission(scope: &ScopeEvaluation) -> TestResult<PermissionEvaluation> {
    permission_evaluation(
        scope,
        "run_shell",
        &["arbitrary_shell"],
        PermissionConfirmation::NotProvided,
    )
}

fn scope_evaluation(classification: ScopeRequestClassification) -> TestResult<ScopeEvaluation> {
    let policy = ScopePolicy::try_from(scope_policy_spec())?;
    Ok(evaluate_scope(
        &policy,
        ScopeRequestRef::new(
            PACKAGE_ID,
            TASK_TYPE,
            "inspect_node_status",
            "inspect_status",
            &["user_provided_text"],
            classification,
        ),
    ))
}

fn permission_evaluation(
    scope: &ScopeEvaluation,
    action: &str,
    categories: &[&str],
    confirmation: PermissionConfirmation,
) -> TestResult<PermissionEvaluation> {
    let policy = PermissionPolicy::try_from(permission_policy_spec())?;
    Ok(evaluate_permissions(
        &policy,
        scope,
        PermissionRequestRef::new(PACKAGE_ID, action, categories, confirmation),
    ))
}

fn scope_policy_spec() -> ScopePolicySpec {
    ScopePolicySpec {
        package_id: PACKAGE_ID.to_string(),
        scope_id: "node_status_diagnostic".to_string(),
        task_types: strings(&[TASK_TYPE]),
        in_scope_tasks: strings(&["inspect_node_status"]),
        out_of_scope_tasks: strings(&["mutate_node_status"]),
        allowed_input_classes: strings(&["user_provided_text"]),
        forbidden_input_classes: required_private_inputs(),
        allowed_operations: strings(&["inspect_status"]),
        blocked_actions: required_blocked_scope_actions(),
    }
}

fn permission_policy_spec() -> PermissionPolicySpec {
    PermissionPolicySpec {
        package_id: PACKAGE_ID.to_string(),
        permission_profile_id: "node_doctor_local_readonly_permissions".to_string(),
        default_policy: iamine_agents::PermissionDefaultPolicy::Deny,
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

fn required_private_inputs() -> Vec<String> {
    strings(&[
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
    ])
}

fn required_blocked_scope_actions() -> Vec<String> {
    strings(&[
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
    ])
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}
