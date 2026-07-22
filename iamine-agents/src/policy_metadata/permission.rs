use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::error::{PolicyMetadataViolationCode, PolicyMetadataViolations, ViolationCollector};
use super::validation::{
    validate_disjoint, validate_identifier, validate_identifiers, validate_package_id,
    validate_required, validate_version,
};
use super::{json_schema, parse_yaml, PolicyMetadataError};

pub const PERMISSION_POLICY_SCHEMA_ID: &str = "iamine.agent.permissions.draft-0.1";

const SUPPORTED_REQUESTED_CATEGORIES: &[&str] = &[
    "lan_readonly_metadata",
    "local_readonly",
    "package_relative_review_files",
    "redacted_status_summary",
    "user_provided_text",
];

const REQUIRED_FORBIDDEN_CATEGORIES: &[&str] = &[
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
];

const REQUIRED_BLOCKED_ACTIONS: &[&str] = &[
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
];

const REQUIRED_DATA_DENIES: &[&str] = &[
    "credentials",
    "disk_uuids",
    "full_hostnames",
    "home_directories",
    "ip_addresses",
    "mac_addresses",
    "machine_ids",
    "private_keys",
    "private_paths",
    "personal_paths",
    "raw_private_logs",
    "raw_process_lists",
    "serial_numbers",
    "usernames",
    "wallet_keys",
];

const REQUIRED_CONFIRMATION_DENIES: &[&str] = &[
    "any_network_mutation",
    "any_private_data_request",
    "any_shell_action",
    "any_write_action",
];

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionPolicyMetadata {
    pub schema: String,
    pub package_id: String,
    pub permission_profile_id: String,
    pub permission_profile_version: String,
    pub default_policy: PermissionDefaultPolicyMetadata,
    pub requested_categories: Vec<String>,
    pub forbidden_categories: Vec<String>,
    pub blocked_actions: Vec<String>,
    pub confirmation_requirements: PermissionConfirmationRequirements,
    pub data_access: PermissionDataAccess,
    pub network_access: PermissionNetworkAccess,
    pub filesystem_access: PermissionFilesystemAccess,
    pub process_access: PermissionProcessAccess,
    pub escalation: PermissionEscalation,
    pub review: PermissionReview,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDefaultPolicyMetadata {
    Deny,
    Allow,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionConfirmationRequirements {
    pub requires_confirmation_for: Vec<String>,
    pub must_refuse_without_confirmation: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionDataAccess {
    pub allowed: Vec<String>,
    pub forbidden: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionNetworkAccess {
    pub mode: PermissionNetworkAccessMode,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PermissionNetworkAccessMode {
    None,
    LocalOnly,
    LanReadonlyMetadata,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionFilesystemAccess {
    pub mode: PermissionFilesystemAccessMode,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PermissionFilesystemAccessMode {
    None,
    PackageRelativeReviewOnly,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionProcessAccess {
    pub mode: PermissionProcessAccessMode,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PermissionProcessAccessMode {
    None,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionEscalation {
    pub on_forbidden_request: PermissionEscalationTarget,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PermissionEscalationTarget {
    ReturnToOrchestrator,
    RequireHumanReview,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PermissionReview {
    pub requires_human_review: bool,
}

pub fn permission_policy_json_schema() -> Result<serde_json::Value, PolicyMetadataError> {
    json_schema::<PermissionPolicyMetadata>()
}

pub fn parse_permission_policy_yaml(
    input: &str,
) -> Result<PermissionPolicyMetadata, PolicyMetadataError> {
    parse_yaml(input, validate_permission_policy)
}

fn validate_permission_policy(
    metadata: &PermissionPolicyMetadata,
) -> Result<(), PolicyMetadataViolations> {
    let mut collector = ViolationCollector::default();
    if metadata.schema != PERMISSION_POLICY_SCHEMA_ID {
        collector.push(
            PolicyMetadataViolationCode::UnsupportedSchema,
            "schema",
            "permission policy schema identifier is not supported",
        );
    }
    validate_package_id(&mut collector, &metadata.package_id);
    validate_identifier(
        &mut collector,
        "permission_profile_id",
        &metadata.permission_profile_id,
        true,
    );
    validate_version(
        &mut collector,
        "permission_profile_version",
        &metadata.permission_profile_version,
    );

    let requested = validate_identifiers(
        &mut collector,
        "requested_categories",
        &metadata.requested_categories,
        false,
        false,
    );
    let forbidden = validate_identifiers(
        &mut collector,
        "forbidden_categories",
        &metadata.forbidden_categories,
        false,
        false,
    );
    let blocked_actions = validate_identifiers(
        &mut collector,
        "blocked_actions",
        &metadata.blocked_actions,
        false,
        false,
    );
    let data_allowed = validate_identifiers(
        &mut collector,
        "data_access.allowed",
        &metadata.data_access.allowed,
        false,
        false,
    );
    let data_forbidden = validate_identifiers(
        &mut collector,
        "data_access.forbidden",
        &metadata.data_access.forbidden,
        false,
        false,
    );
    validate_identifiers(
        &mut collector,
        "confirmation_requirements.requires_confirmation_for",
        &metadata.confirmation_requirements.requires_confirmation_for,
        true,
        false,
    );
    let confirmation_denies = validate_identifiers(
        &mut collector,
        "confirmation_requirements.must_refuse_without_confirmation",
        &metadata
            .confirmation_requirements
            .must_refuse_without_confirmation,
        false,
        false,
    );

    if metadata.default_policy != PermissionDefaultPolicyMetadata::Deny {
        collector.push(
            PolicyMetadataViolationCode::UnsafePolicy,
            "default_policy",
            "default permission policy must deny",
        );
    }
    if requested
        .iter()
        .any(|category| !SUPPORTED_REQUESTED_CATEGORIES.contains(category))
    {
        collector.push(
            PolicyMetadataViolationCode::UnsafePolicy,
            "requested_categories",
            "requested permission category is unavailable in this release phase",
        );
    }
    validate_disjoint(&mut collector, "category_boundary", &requested, &forbidden);
    validate_disjoint(
        &mut collector,
        "data_access",
        &data_allowed,
        &data_forbidden,
    );
    validate_required(
        &mut collector,
        "forbidden_categories",
        &forbidden,
        REQUIRED_FORBIDDEN_CATEGORIES,
    );
    validate_required(
        &mut collector,
        "blocked_actions",
        &blocked_actions,
        REQUIRED_BLOCKED_ACTIONS,
    );
    validate_required(
        &mut collector,
        "data_access.forbidden",
        &data_forbidden,
        REQUIRED_DATA_DENIES,
    );
    validate_required(
        &mut collector,
        "confirmation_requirements.must_refuse_without_confirmation",
        &confirmation_denies,
        REQUIRED_CONFIRMATION_DENIES,
    );
    if !metadata.review.requires_human_review {
        collector.push(
            PolicyMetadataViolationCode::MissingSafetyBoundary,
            "review.requires_human_review",
            "permission policy must require human review",
        );
    }

    collector.finish()
}
