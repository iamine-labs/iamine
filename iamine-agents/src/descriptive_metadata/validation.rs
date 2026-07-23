use std::collections::HashSet;
use std::path::{Component, Path};

use crate::identifiers::{is_package_identifier, is_snake_identifier};

use super::error::{DescriptiveMetadataViolationCode, ViolationCollector};

pub(crate) const MAX_DESCRIPTIVE_ENTRIES: usize = 64;
const MAX_REFERENCE_BYTES: usize = 256;

const BROAD_IDENTIFIERS: &[&str] = &[
    "all",
    "all_domains",
    "all_files",
    "all_hardware",
    "all_networks",
    "anything",
    "automation",
    "best_agent",
    "do_anything",
    "general_ai_expert",
    "general_assistant",
    "general_computing",
    "general_help",
    "root",
    "system_admin",
    "system_administration",
    "system_control",
];

pub(crate) fn validate_package_id(collector: &mut ViolationCollector, value: &str) {
    if !is_package_identifier(value) || !value.starts_with("iamine.") {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidIdentifier,
            "package_id",
            "package identifier must be bounded and IAMINE-scoped",
        );
    }
}

pub(crate) fn validate_identifier(
    collector: &mut ViolationCollector,
    field: &'static str,
    value: &str,
    reject_broad: bool,
) {
    if !is_snake_identifier(value) || (reject_broad && BROAD_IDENTIFIERS.contains(&value)) {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidIdentifier,
            field,
            "identifier must be bounded, lowercase, and narrow",
        );
    }
}

pub(crate) fn validate_version(
    collector: &mut ViolationCollector,
    field: &'static str,
    value: &str,
) {
    if semver::Version::parse(value).is_err() {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidVersion,
            field,
            "version must use semantic versioning",
        );
    }
}

pub(crate) fn validate_identifiers<'a>(
    collector: &mut ViolationCollector,
    field: &'static str,
    values: &'a [String],
    allow_empty: bool,
    reject_broad: bool,
) -> HashSet<&'a str> {
    if (!allow_empty && values.is_empty()) || values.len() > MAX_DESCRIPTIVE_ENTRIES {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidCollection,
            field,
            "collection must be bounded and non-empty when required",
        );
    }

    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        validate_identifier(collector, field, value, reject_broad);
        if !unique.insert(value.as_str()) {
            collector.push(
                DescriptiveMetadataViolationCode::DuplicateValue,
                field,
                "collection values must be unique",
            );
        }
    }
    unique
}

pub(crate) fn validate_required(
    collector: &mut ViolationCollector,
    field: &'static str,
    values: &HashSet<&str>,
    required: &[&str],
) {
    if required.iter().any(|value| !values.contains(value)) {
        collector.push(
            DescriptiveMetadataViolationCode::MissingSafetyBoundary,
            field,
            "required fail-closed safety entries are missing",
        );
    }
}

pub(crate) fn validate_blocked(
    collector: &mut ViolationCollector,
    field: &'static str,
    values: &HashSet<&str>,
    blocked: &[&str],
) {
    if blocked.iter().any(|value| values.contains(value)) {
        collector.push(
            DescriptiveMetadataViolationCode::UnsafeClaim,
            field,
            "declaration contains an unavailable or unsafe claim",
        );
    }
}

pub(crate) fn is_safe_reference(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && value.is_ascii()
        && !value.contains(['\\', ':', '\0'])
        && !value.starts_with('~')
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

pub(crate) fn validate_safe_references(
    collector: &mut ViolationCollector,
    field: &'static str,
    values: &[String],
) {
    if values.is_empty() || values.len() > MAX_DESCRIPTIVE_ENTRIES {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidCollection,
            field,
            "reference collection must be bounded and non-empty",
        );
    }

    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        if !is_safe_reference(value) {
            collector.push(
                DescriptiveMetadataViolationCode::InvalidReference,
                field,
                "reference must be a bounded package-relative path",
            );
        }
        if !unique.insert(value.as_str()) {
            collector.push(
                DescriptiveMetadataViolationCode::DuplicateValue,
                field,
                "reference values must be unique",
            );
        }
    }
}
