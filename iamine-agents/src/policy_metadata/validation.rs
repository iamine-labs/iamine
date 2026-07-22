use std::collections::HashSet;
use std::path::{Component, Path};

use crate::identifiers::{is_package_identifier, is_snake_identifier};

use super::error::{PolicyMetadataViolationCode, ViolationCollector};

pub(crate) const MAX_POLICY_ENTRIES: usize = 64;
const MAX_REFERENCE_BYTES: usize = 256;

const BROAD_IDENTIFIERS: &[&str] = &[
    "admin",
    "all",
    "all_access",
    "all_actions",
    "all_files",
    "all_networks",
    "anything",
    "automation",
    "do_anything",
    "general_assistant",
    "general_help",
    "root",
    "system_admin",
    "system_control",
];

pub(crate) fn validate_package_id(collector: &mut ViolationCollector, value: &str) {
    if !is_package_identifier(value) || !value.starts_with("iamine.") {
        collector.push(
            PolicyMetadataViolationCode::InvalidIdentifier,
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
            PolicyMetadataViolationCode::InvalidIdentifier,
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
            PolicyMetadataViolationCode::InvalidVersion,
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
    if (!allow_empty && values.is_empty()) || values.len() > MAX_POLICY_ENTRIES {
        collector.push(
            PolicyMetadataViolationCode::InvalidCollection,
            field,
            "collection must be bounded and non-empty when required",
        );
    }

    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        validate_identifier(collector, field, value, reject_broad);
        if !unique.insert(value.as_str()) {
            collector.push(
                PolicyMetadataViolationCode::DuplicateValue,
                field,
                "collection values must be unique",
            );
        }
    }
    unique
}

pub(crate) fn validate_disjoint(
    collector: &mut ViolationCollector,
    field: &'static str,
    first: &HashSet<&str>,
    second: &HashSet<&str>,
) {
    if !first.is_disjoint(second) {
        collector.push(
            PolicyMetadataViolationCode::ContradictoryBoundary,
            field,
            "allowed and blocked declarations must not overlap",
        );
    }
}

pub(crate) fn validate_required(
    collector: &mut ViolationCollector,
    field: &'static str,
    values: &HashSet<&str>,
    required: &[&str],
) {
    if required.iter().any(|value| !values.contains(value)) {
        collector.push(
            PolicyMetadataViolationCode::MissingSafetyBoundary,
            field,
            "required deny-by-default safety entries are missing",
        );
    }
}

pub(crate) fn validate_safe_references(
    collector: &mut ViolationCollector,
    field: &'static str,
    values: &[String],
) {
    if values.is_empty() || values.len() > MAX_POLICY_ENTRIES {
        collector.push(
            PolicyMetadataViolationCode::InvalidCollection,
            field,
            "reference collection must be bounded and non-empty",
        );
    }

    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        let path = Path::new(value);
        let is_safe = !value.is_empty()
            && value.len() <= MAX_REFERENCE_BYTES
            && value.is_ascii()
            && !value.contains(['\\', ':', '\0'])
            && !value.starts_with('~')
            && !path.is_absolute()
            && path
                .components()
                .all(|component| matches!(component, Component::Normal(_)));
        if !is_safe {
            collector.push(
                PolicyMetadataViolationCode::InvalidReference,
                field,
                "reference must be a bounded package-relative path",
            );
        }
        if !unique.insert(value.as_str()) {
            collector.push(
                PolicyMetadataViolationCode::DuplicateValue,
                field,
                "reference values must be unique",
            );
        }
    }
}
