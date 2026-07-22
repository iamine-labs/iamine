use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::error::{PolicyMetadataViolationCode, PolicyMetadataViolations, ViolationCollector};
use super::validation::{
    validate_identifier, validate_package_id, validate_safe_references, validate_version,
};
use super::{json_schema, parse_yaml, PolicyMetadataError};

pub const AUDIT_POLICY_SCHEMA_ID: &str = "iamine.agent.audit.draft-0.1";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditPolicyMetadata {
    pub schema: String,
    pub package_id: String,
    pub audit_profile_id: String,
    pub audit_profile_version: String,
    pub event_classes: Vec<AuditPolicyEventClass>,
    pub required_evidence: Vec<String>,
    pub redaction_policy: AuditRedactionPolicy,
    pub retention_policy: AuditRetentionPolicy,
    pub integrity_policy: AuditIntegrityPolicy,
    pub access_policy: AuditAccessPolicy,
    pub failure_policy: AuditFailurePolicy,
    pub review: AuditReview,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AuditPolicyEventClass {
    ReviewStarted,
    ScopeChecked,
    PermissionChecked,
    RedactionChecked,
    HandoffRequired,
    RefusalRecorded,
    HumanReviewRecorded,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditRedactionPolicy {
    pub default: AuditRedactionDefault,
    pub blocks_raw_prompts: bool,
    pub blocks_raw_outputs: bool,
    pub blocks_private_paths: bool,
    pub blocks_host_identifiers: bool,
    pub blocks_credentials: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditRedactionDefault {
    Redact,
    Allow,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditRetentionPolicy {
    pub mode: AuditRetentionMode,
    pub operator_local_only: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditRetentionMode {
    ReviewOnly,
    OperatorLocal,
    DeleteOnPackageRejection,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditIntegrityPolicy {
    pub future_tamper_evidence_required: bool,
    pub publishes_artifacts: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditAccessPolicy {
    pub visibility: AuditVisibility,
    pub third_party_sharing: bool,
    pub marketplace_publication: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditVisibility {
    OperatorLocal,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditFailurePolicy {
    pub missing_audit_policy: AuditFailureAction,
    pub unredacted_evidence: AuditFailureAction,
    pub unsafe_event_class: AuditFailureAction,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditFailureAction {
    Block,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditReview {
    pub requires_human_review: bool,
}

pub fn audit_policy_json_schema() -> Result<serde_json::Value, PolicyMetadataError> {
    json_schema::<AuditPolicyMetadata>()
}

pub fn parse_audit_policy_yaml(input: &str) -> Result<AuditPolicyMetadata, PolicyMetadataError> {
    parse_yaml(input, validate_audit_policy)
}

fn validate_audit_policy(metadata: &AuditPolicyMetadata) -> Result<(), PolicyMetadataViolations> {
    let mut collector = ViolationCollector::default();
    if metadata.schema != AUDIT_POLICY_SCHEMA_ID {
        collector.push(
            PolicyMetadataViolationCode::UnsupportedSchema,
            "schema",
            "audit policy schema identifier is not supported",
        );
    }
    validate_package_id(&mut collector, &metadata.package_id);
    validate_identifier(
        &mut collector,
        "audit_profile_id",
        &metadata.audit_profile_id,
        true,
    );
    validate_version(
        &mut collector,
        "audit_profile_version",
        &metadata.audit_profile_version,
    );
    validate_safe_references(
        &mut collector,
        "required_evidence",
        &metadata.required_evidence,
    );

    if metadata.event_classes.is_empty() || metadata.event_classes.len() > 64 {
        collector.push(
            PolicyMetadataViolationCode::InvalidCollection,
            "event_classes",
            "event class collection must be bounded and non-empty",
        );
    }
    let mut event_classes = std::collections::HashSet::with_capacity(metadata.event_classes.len());
    for event_class in &metadata.event_classes {
        if !event_classes.insert(*event_class) {
            collector.push(
                PolicyMetadataViolationCode::DuplicateValue,
                "event_classes",
                "event classes must be unique",
            );
        }
    }
    if metadata.redaction_policy.default != AuditRedactionDefault::Redact
        || !metadata.redaction_policy.blocks_raw_prompts
        || !metadata.redaction_policy.blocks_raw_outputs
        || !metadata.redaction_policy.blocks_private_paths
        || !metadata.redaction_policy.blocks_host_identifiers
        || !metadata.redaction_policy.blocks_credentials
    {
        collector.push(
            PolicyMetadataViolationCode::UnsafePolicy,
            "redaction_policy",
            "audit policy must redact by default and block private evidence",
        );
    }
    if metadata.retention_policy.mode != AuditRetentionMode::ReviewOnly
        || !metadata.retention_policy.operator_local_only
    {
        collector.push(
            PolicyMetadataViolationCode::UnsafePolicy,
            "retention_policy",
            "audit retention must remain review-only and operator-local",
        );
    }
    if !metadata.integrity_policy.future_tamper_evidence_required
        || metadata.integrity_policy.publishes_artifacts
    {
        collector.push(
            PolicyMetadataViolationCode::UnsafePolicy,
            "integrity_policy",
            "audit integrity must require future evidence without publishing artifacts",
        );
    }
    if metadata.access_policy.visibility != AuditVisibility::OperatorLocal
        || metadata.access_policy.third_party_sharing
        || metadata.access_policy.marketplace_publication
    {
        collector.push(
            PolicyMetadataViolationCode::UnsafePolicy,
            "access_policy",
            "audit access must remain operator-local without sharing or publication",
        );
    }
    if !metadata.review.requires_human_review {
        collector.push(
            PolicyMetadataViolationCode::MissingSafetyBoundary,
            "review.requires_human_review",
            "audit policy must require human review",
        );
    }

    collector.finish()
}
