use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::error::{
    DescriptiveMetadataViolationCode, DescriptiveMetadataViolations, ViolationCollector,
};
use super::validation::{
    is_safe_reference, validate_blocked, validate_identifier, validate_identifiers,
    validate_package_id, validate_required, validate_version, MAX_DESCRIPTIVE_ENTRIES,
};
use super::{json_schema, parse_yaml, DescriptiveMetadataError};

pub const EXPERTISE_METADATA_SCHEMA_ID: &str = "iamine.agent.expertise.draft-0.1";

const BLOCKED_DOMAINS: &[&str] = &[
    "financial_advice",
    "general_computing",
    "legal_advice",
    "medical_advice",
    "software_engineering",
    "system_administration",
];

const BLOCKED_CLAIMS: &[&str] = &[
    "always_safe",
    "best_available_agent",
    "certified_expert",
    "earns_rewards",
    "guarantees_correct_answer",
    "routes_distributed_moe",
    "trustworthy",
];

const REQUIRED_EVAL_CLASSES: &[&str] = &[
    "adjacent_domain_refusal",
    "ambiguous_task_handoff",
    "capability_mismatch",
    "dangerous_task_refusal",
    "in_domain_task",
    "privacy_sensitive_input_rejection",
    "prompt_injection_attempt",
    "role_confusion_attempt",
    "stale_expertise_metadata",
];

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpertiseMetadata {
    pub schema: String,
    pub package_id: String,
    pub expertise_id: String,
    pub expertise_version: String,
    pub domain: String,
    pub task_families: Vec<String>,
    pub supported_capabilities: Vec<String>,
    pub expertise_claims: Vec<String>,
    pub evidence: Vec<ExpertiseEvidence>,
    pub evaluation_requirements: Vec<ExpertiseEvaluationRequirement>,
    pub limitations: Vec<String>,
    pub freshness: ExpertiseFreshness,
    pub review: ExpertiseReview,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpertiseEvidence {
    #[serde(rename = "type")]
    pub evidence_type: ExpertiseEvidenceType,
    pub path: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpertiseEvidenceType {
    DesignNote,
    HumanReview,
    EvalPlan,
    BoundedFixture,
    RedactedExample,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpertiseEvaluationRequirement {
    pub class: String,
    pub required: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpertiseFreshness {
    pub review_interval_days: u16,
    pub stale_behavior: ExpertiseStaleBehavior,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpertiseStaleBehavior {
    BlockRuntimeEligibility,
    RequireHumanReview,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpertiseReview {
    pub requires_human_review: bool,
    pub self_attestation_sufficient: bool,
}

pub fn expertise_metadata_json_schema() -> Result<serde_json::Value, DescriptiveMetadataError> {
    json_schema::<ExpertiseMetadata>()
}

pub fn parse_expertise_metadata_yaml(
    input: &str,
) -> Result<ExpertiseMetadata, DescriptiveMetadataError> {
    parse_yaml(input, validate_expertise_metadata)
}

fn validate_expertise_metadata(
    metadata: &ExpertiseMetadata,
) -> Result<(), DescriptiveMetadataViolations> {
    let mut collector = ViolationCollector::default();
    if metadata.schema != EXPERTISE_METADATA_SCHEMA_ID {
        collector.push(
            DescriptiveMetadataViolationCode::UnsupportedSchema,
            "schema",
            "expertise metadata schema identifier is not supported",
        );
    }
    validate_package_id(&mut collector, &metadata.package_id);
    validate_identifier(&mut collector, "expertise_id", &metadata.expertise_id, true);
    validate_version(
        &mut collector,
        "expertise_version",
        &metadata.expertise_version,
    );
    validate_identifier(&mut collector, "domain", &metadata.domain, true);

    let domain = HashSet::from([metadata.domain.as_str()]);
    validate_blocked(&mut collector, "domain", &domain, BLOCKED_DOMAINS);
    validate_identifiers(
        &mut collector,
        "task_families",
        &metadata.task_families,
        false,
        true,
    );
    validate_identifiers(
        &mut collector,
        "supported_capabilities",
        &metadata.supported_capabilities,
        false,
        true,
    );
    let claims = validate_identifiers(
        &mut collector,
        "expertise_claims",
        &metadata.expertise_claims,
        false,
        true,
    );
    validate_blocked(&mut collector, "expertise_claims", &claims, BLOCKED_CLAIMS);
    validate_identifiers(
        &mut collector,
        "limitations",
        &metadata.limitations,
        false,
        false,
    );

    if metadata.evidence.is_empty() || metadata.evidence.len() > MAX_DESCRIPTIVE_ENTRIES {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidCollection,
            "evidence",
            "expertise evidence must be bounded and non-empty",
        );
    }
    let mut evidence_paths = HashSet::with_capacity(metadata.evidence.len());
    for evidence in &metadata.evidence {
        if !is_safe_reference(&evidence.path) {
            collector.push(
                DescriptiveMetadataViolationCode::InvalidReference,
                "evidence.path",
                "expertise evidence must use a bounded package-relative path",
            );
        }
        if !evidence_paths.insert(evidence.path.as_str()) {
            collector.push(
                DescriptiveMetadataViolationCode::DuplicateValue,
                "evidence.path",
                "expertise evidence paths must be unique",
            );
        }
    }

    if metadata.evaluation_requirements.is_empty()
        || metadata.evaluation_requirements.len() > MAX_DESCRIPTIVE_ENTRIES
    {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidCollection,
            "evaluation_requirements",
            "evaluation requirements must be bounded and non-empty",
        );
    }
    let mut eval_classes = HashSet::with_capacity(metadata.evaluation_requirements.len());
    for requirement in &metadata.evaluation_requirements {
        validate_identifier(
            &mut collector,
            "evaluation_requirements.class",
            &requirement.class,
            false,
        );
        if !requirement.required {
            collector.push(
                DescriptiveMetadataViolationCode::MissingSafetyBoundary,
                "evaluation_requirements.required",
                "declared expertise evaluations must remain required",
            );
        }
        if !eval_classes.insert(requirement.class.as_str()) {
            collector.push(
                DescriptiveMetadataViolationCode::DuplicateValue,
                "evaluation_requirements.class",
                "evaluation requirement classes must be unique",
            );
        }
    }
    validate_required(
        &mut collector,
        "evaluation_requirements",
        &eval_classes,
        REQUIRED_EVAL_CLASSES,
    );

    if metadata.freshness.review_interval_days == 0 || metadata.freshness.review_interval_days > 365
    {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidResourceBound,
            "freshness.review_interval_days",
            "expertise review interval must be between one and 365 days",
        );
    }
    if !metadata.review.requires_human_review || metadata.review.self_attestation_sufficient {
        collector.push(
            DescriptiveMetadataViolationCode::MissingSafetyBoundary,
            "review",
            "expertise metadata must require independent human review",
        );
    }

    collector.finish()
}
