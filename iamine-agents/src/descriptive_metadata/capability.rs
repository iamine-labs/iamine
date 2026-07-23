use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::error::{
    DescriptiveMetadataViolationCode, DescriptiveMetadataViolations, ViolationCollector,
};
use super::validation::{
    validate_blocked, validate_identifier, validate_identifiers, validate_package_id,
    validate_safe_references, validate_version,
};
use super::{json_schema, parse_yaml, DescriptiveMetadataError};

pub const CAPABILITY_METADATA_SCHEMA_ID: &str = "iamine.agent.capabilities.draft-0.1";

const BLOCKED_TASK_TYPES: &[&str] = &[
    "admin",
    "mainnet",
    "publish",
    "remote_execution",
    "repair",
    "settlement",
    "wallet_operation",
];

const BLOCKED_OPERATIONS: &[&str] = &[
    "delete_files",
    "download_models",
    "mutate_vm_or_container",
    "publish_agent",
    "restart_services",
    "run_shell",
    "scan_network",
    "transfer_funds",
    "write_files",
];

const BLOCKED_INPUT_CLASSES: &[&str] = &[
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
    "unredacted_logs",
    "usernames",
    "wallet_keys",
];

const BLOCKED_OUTPUT_CLASSES: &[&str] = &[
    "execution_result",
    "mainnet_effect",
    "payment_result",
    "publication_result",
    "repair_result",
    "reward_result",
    "service_restart_result",
    "settlement_result",
];

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CapabilityMetadata {
    pub schema: String,
    pub package_id: String,
    pub capability_id: String,
    pub capability_version: String,
    pub declared_task_types: Vec<String>,
    pub operations: Vec<String>,
    pub input_classes: Vec<String>,
    pub output_classes: Vec<String>,
    pub execution_modes: Vec<CapabilityExecutionMode>,
    pub limitations: Vec<String>,
    pub risk_profile: CapabilityRiskProfile,
    pub review: CapabilityReview,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityExecutionMode {
    LocalReadonly,
    LocalPlanning,
    LanReadonly,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRiskProfile {
    pub labels: Vec<String>,
    pub expands_scope: bool,
    pub grants_permissions: bool,
    pub claims_scheduler_priority: bool,
    pub claims_trust_or_reputation: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReview {
    pub requires_human_review: bool,
    pub self_approval_allowed: bool,
    pub evidence: Vec<String>,
}

pub fn capability_metadata_json_schema() -> Result<serde_json::Value, DescriptiveMetadataError> {
    json_schema::<CapabilityMetadata>()
}

pub fn parse_capability_metadata_yaml(
    input: &str,
) -> Result<CapabilityMetadata, DescriptiveMetadataError> {
    parse_yaml(input, validate_capability_metadata)
}

fn validate_capability_metadata(
    metadata: &CapabilityMetadata,
) -> Result<(), DescriptiveMetadataViolations> {
    let mut collector = ViolationCollector::default();
    if metadata.schema != CAPABILITY_METADATA_SCHEMA_ID {
        collector.push(
            DescriptiveMetadataViolationCode::UnsupportedSchema,
            "schema",
            "capability metadata schema identifier is not supported",
        );
    }
    validate_package_id(&mut collector, &metadata.package_id);
    validate_identifier(
        &mut collector,
        "capability_id",
        &metadata.capability_id,
        true,
    );
    validate_version(
        &mut collector,
        "capability_version",
        &metadata.capability_version,
    );

    let task_types = validate_identifiers(
        &mut collector,
        "declared_task_types",
        &metadata.declared_task_types,
        false,
        true,
    );
    let operations = validate_identifiers(
        &mut collector,
        "operations",
        &metadata.operations,
        false,
        true,
    );
    let input_classes = validate_identifiers(
        &mut collector,
        "input_classes",
        &metadata.input_classes,
        false,
        false,
    );
    let output_classes = validate_identifiers(
        &mut collector,
        "output_classes",
        &metadata.output_classes,
        false,
        false,
    );
    validate_identifiers(
        &mut collector,
        "limitations",
        &metadata.limitations,
        false,
        false,
    );
    validate_identifiers(
        &mut collector,
        "risk_profile.labels",
        &metadata.risk_profile.labels,
        false,
        false,
    );

    validate_blocked(
        &mut collector,
        "declared_task_types",
        &task_types,
        BLOCKED_TASK_TYPES,
    );
    validate_blocked(
        &mut collector,
        "operations",
        &operations,
        BLOCKED_OPERATIONS,
    );
    validate_blocked(
        &mut collector,
        "input_classes",
        &input_classes,
        BLOCKED_INPUT_CLASSES,
    );
    validate_blocked(
        &mut collector,
        "output_classes",
        &output_classes,
        BLOCKED_OUTPUT_CLASSES,
    );

    if metadata.execution_modes.is_empty() || metadata.execution_modes.len() > 3 {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidCollection,
            "execution_modes",
            "execution modes must be bounded and non-empty",
        );
    }
    let mut modes = HashSet::with_capacity(metadata.execution_modes.len());
    for mode in &metadata.execution_modes {
        if !modes.insert(*mode) {
            collector.push(
                DescriptiveMetadataViolationCode::DuplicateValue,
                "execution_modes",
                "execution modes must be unique",
            );
        }
    }

    if metadata.risk_profile.expands_scope
        || metadata.risk_profile.grants_permissions
        || metadata.risk_profile.claims_scheduler_priority
        || metadata.risk_profile.claims_trust_or_reputation
    {
        collector.push(
            DescriptiveMetadataViolationCode::UnsafeClaim,
            "risk_profile",
            "capability risk metadata cannot claim authority or trust",
        );
    }
    if !metadata.review.requires_human_review || metadata.review.self_approval_allowed {
        collector.push(
            DescriptiveMetadataViolationCode::MissingSafetyBoundary,
            "review",
            "capability metadata must require independent human review",
        );
    }
    validate_safe_references(&mut collector, "review.evidence", &metadata.review.evidence);

    collector.finish()
}
