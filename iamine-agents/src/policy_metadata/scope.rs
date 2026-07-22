use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::error::{PolicyMetadataViolationCode, PolicyMetadataViolations, ViolationCollector};
use super::validation::{
    validate_disjoint, validate_identifier, validate_identifiers, validate_package_id,
    validate_required, validate_version,
};
use super::{json_schema, parse_yaml, PolicyMetadataError};

pub const SCOPE_POLICY_SCHEMA_ID: &str = "iamine.agent.scope.draft-0.1";

const REQUIRED_FORBIDDEN_INPUTS: &[&str] = &[
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
];

const REQUIRED_BLOCKED_ACTIONS: &[&str] = &[
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
];

const REQUIRED_HANDOFF_REASONS: &[&str] = &[
    "ambiguous_task",
    "dangerous_task",
    "out_of_scope_task",
    "prompt_injection_attempt",
    "role_confusion_attempt",
];

const REQUIRED_RETURN_CONDITIONS: &[&str] = &[
    "cross_domain_task",
    "missing_audit_policy",
    "missing_boundary_tests",
    "missing_permission_model",
    "request_to_collect_secret",
    "request_to_execute_code",
    "unsupported_task_type",
];

const REQUIRED_EVAL_CLASSES: &[&str] = &[
    "ambiguous_task",
    "cross_domain_task",
    "dangerous_task",
    "handoff_to_orchestrator",
    "in_scope_positive",
    "out_of_scope_negative",
    "permission_escalation",
    "prompt_injection",
    "role_confusion",
];

const REQUIRED_CONFIRMATION_DENIES: &[&str] = &[
    "any_network_mutation",
    "any_private_data_request",
    "any_shell_action",
    "any_write_action",
];

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopePolicyMetadata {
    pub schema: String,
    pub package_id: String,
    pub scope_id: String,
    pub scope_version: String,
    pub task_boundary: ScopeTaskBoundary,
    pub input_boundary: ScopeInputBoundary,
    pub operation_boundary: ScopeOperationBoundary,
    pub permission_requirements: ScopePermissionRequirements,
    pub confirmation_boundary: ScopeConfirmationBoundary,
    pub handoff: ScopeHandoff,
    pub orchestrator_return: ScopeOrchestratorReturn,
    pub eval_requirements: ScopeEvalRequirements,
    pub review: ScopeReview,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeTaskBoundary {
    pub in_scope: Vec<String>,
    pub out_of_scope: Vec<String>,
    pub task_types: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeInputBoundary {
    pub allowed_inputs: Vec<String>,
    pub forbidden_inputs: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeOperationBoundary {
    pub allowed_operations: Vec<String>,
    pub blocked_actions: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopePermissionRequirements {
    pub required_categories: Vec<String>,
    pub forbidden_categories: Vec<String>,
    pub permission_model_required: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeConfirmationBoundary {
    pub requires_confirmation_for: Vec<String>,
    pub must_refuse_without_confirmation: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeHandoff {
    pub targets: Vec<String>,
    pub required_when: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeOrchestratorReturn {
    pub return_required_for: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeEvalRequirements {
    pub required_eval_classes: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopeReview {
    pub requires_human_review: bool,
    pub requires_permission_review: bool,
    pub requires_audit_policy: bool,
    pub requires_boundary_tests: bool,
    pub scope_can_self_approve: bool,
}

pub fn scope_policy_json_schema() -> Result<serde_json::Value, PolicyMetadataError> {
    json_schema::<ScopePolicyMetadata>()
}

pub fn parse_scope_policy_yaml(input: &str) -> Result<ScopePolicyMetadata, PolicyMetadataError> {
    parse_yaml(input, validate_scope_policy)
}

fn validate_scope_policy(metadata: &ScopePolicyMetadata) -> Result<(), PolicyMetadataViolations> {
    let mut collector = ViolationCollector::default();
    if metadata.schema != SCOPE_POLICY_SCHEMA_ID {
        collector.push(
            PolicyMetadataViolationCode::UnsupportedSchema,
            "schema",
            "scope policy schema identifier is not supported",
        );
    }
    validate_package_id(&mut collector, &metadata.package_id);
    validate_identifier(&mut collector, "scope_id", &metadata.scope_id, true);
    validate_version(&mut collector, "scope_version", &metadata.scope_version);

    let in_scope = validate_identifiers(
        &mut collector,
        "task_boundary.in_scope",
        &metadata.task_boundary.in_scope,
        false,
        true,
    );
    let out_of_scope = validate_identifiers(
        &mut collector,
        "task_boundary.out_of_scope",
        &metadata.task_boundary.out_of_scope,
        false,
        true,
    );
    let _task_types = validate_identifiers(
        &mut collector,
        "task_boundary.task_types",
        &metadata.task_boundary.task_types,
        false,
        true,
    );
    let allowed_inputs = validate_identifiers(
        &mut collector,
        "input_boundary.allowed_inputs",
        &metadata.input_boundary.allowed_inputs,
        false,
        false,
    );
    let forbidden_inputs = validate_identifiers(
        &mut collector,
        "input_boundary.forbidden_inputs",
        &metadata.input_boundary.forbidden_inputs,
        false,
        false,
    );
    let allowed_operations = validate_identifiers(
        &mut collector,
        "operation_boundary.allowed_operations",
        &metadata.operation_boundary.allowed_operations,
        false,
        false,
    );
    let blocked_actions = validate_identifiers(
        &mut collector,
        "operation_boundary.blocked_actions",
        &metadata.operation_boundary.blocked_actions,
        false,
        false,
    );
    let required_categories = validate_identifiers(
        &mut collector,
        "permission_requirements.required_categories",
        &metadata.permission_requirements.required_categories,
        false,
        false,
    );
    let forbidden_categories = validate_identifiers(
        &mut collector,
        "permission_requirements.forbidden_categories",
        &metadata.permission_requirements.forbidden_categories,
        false,
        false,
    );
    let handoff_targets = validate_identifiers(
        &mut collector,
        "handoff.targets",
        &metadata.handoff.targets,
        false,
        false,
    );
    let handoff_reasons = validate_identifiers(
        &mut collector,
        "handoff.required_when",
        &metadata.handoff.required_when,
        false,
        false,
    );
    let return_conditions = validate_identifiers(
        &mut collector,
        "orchestrator_return.return_required_for",
        &metadata.orchestrator_return.return_required_for,
        false,
        false,
    );
    let eval_classes = validate_identifiers(
        &mut collector,
        "eval_requirements.required_eval_classes",
        &metadata.eval_requirements.required_eval_classes,
        false,
        false,
    );
    validate_identifiers(
        &mut collector,
        "confirmation_boundary.requires_confirmation_for",
        &metadata.confirmation_boundary.requires_confirmation_for,
        true,
        false,
    );
    let confirmation_denies = validate_identifiers(
        &mut collector,
        "confirmation_boundary.must_refuse_without_confirmation",
        &metadata
            .confirmation_boundary
            .must_refuse_without_confirmation,
        false,
        false,
    );

    validate_disjoint(&mut collector, "task_boundary", &in_scope, &out_of_scope);
    validate_disjoint(
        &mut collector,
        "input_boundary",
        &allowed_inputs,
        &forbidden_inputs,
    );
    validate_disjoint(
        &mut collector,
        "operation_boundary",
        &allowed_operations,
        &blocked_actions,
    );
    validate_disjoint(
        &mut collector,
        "permission_requirements",
        &required_categories,
        &forbidden_categories,
    );
    validate_required(
        &mut collector,
        "input_boundary.forbidden_inputs",
        &forbidden_inputs,
        REQUIRED_FORBIDDEN_INPUTS,
    );
    validate_required(
        &mut collector,
        "operation_boundary.blocked_actions",
        &blocked_actions,
        REQUIRED_BLOCKED_ACTIONS,
    );
    validate_required(
        &mut collector,
        "handoff.targets",
        &handoff_targets,
        &["orchestrator"],
    );
    validate_required(
        &mut collector,
        "handoff.required_when",
        &handoff_reasons,
        REQUIRED_HANDOFF_REASONS,
    );
    validate_required(
        &mut collector,
        "orchestrator_return.return_required_for",
        &return_conditions,
        REQUIRED_RETURN_CONDITIONS,
    );
    validate_required(
        &mut collector,
        "eval_requirements.required_eval_classes",
        &eval_classes,
        REQUIRED_EVAL_CLASSES,
    );
    validate_required(
        &mut collector,
        "confirmation_boundary.must_refuse_without_confirmation",
        &confirmation_denies,
        REQUIRED_CONFIRMATION_DENIES,
    );

    if !metadata.permission_requirements.permission_model_required {
        collector.push(
            PolicyMetadataViolationCode::MissingSafetyBoundary,
            "permission_requirements.permission_model_required",
            "scope policy must require a permission model",
        );
    }
    if !metadata.review.requires_human_review
        || !metadata.review.requires_permission_review
        || !metadata.review.requires_audit_policy
        || !metadata.review.requires_boundary_tests
        || metadata.review.scope_can_self_approve
    {
        collector.push(
            PolicyMetadataViolationCode::UnsafePolicy,
            "review",
            "scope review must require independent review and forbid self-approval",
        );
    }

    collector.finish()
}
