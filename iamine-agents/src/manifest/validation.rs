use std::collections::HashSet;
use std::hash::Hash;
use std::path::{Component, Path};

use crate::identifiers::{is_package_identifier, is_snake_identifier, MAX_IDENTIFIER_BYTES};

use super::error::{ManifestViolation, ManifestViolationCode, ManifestViolations};
use super::schema::{AgentPackageManifest, MANIFEST_SCHEMA_ID};

const MAX_SHORT_TEXT_BYTES: usize = 80;
const MAX_SUMMARY_BYTES: usize = 280;
const MAX_REFERENCE_BYTES: usize = 240;
const MAX_PERSONAS: usize = 16;
const MAX_DISTRIBUTION_VALUES: usize = 8;

pub fn validate_manifest(manifest: &AgentPackageManifest) -> Result<(), ManifestViolations> {
    let mut collector = ViolationCollector::default();

    if manifest.schema != MANIFEST_SCHEMA_ID {
        collector.push(
            ManifestViolationCode::UnsupportedSchema,
            "schema",
            "schema identifier is not supported",
        );
    }

    validate_package_identifier(&mut collector, "package_id", &manifest.package_id);
    if manifest.package_id.len() > MAX_IDENTIFIER_BYTES
        || !manifest.package_id.starts_with("iamine.")
    {
        collector.push(
            ManifestViolationCode::InvalidIdentifier,
            "package_id",
            "package identifier must be bounded and IAMINE-scoped",
        );
    }

    if manifest.package_version.len() > MAX_IDENTIFIER_BYTES
        || semver::Version::parse(&manifest.package_version).is_err()
    {
        collector.push(
            ManifestViolationCode::InvalidVersion,
            "package_version",
            "package version must be valid semantic versioning",
        );
    }

    validate_text(
        &mut collector,
        "display_name",
        &manifest.display_name,
        MAX_SHORT_TEXT_BYTES,
    );
    validate_text(
        &mut collector,
        "summary",
        &manifest.summary,
        MAX_SUMMARY_BYTES,
    );
    validate_package_identifier(&mut collector, "official_pack", &manifest.official_pack);

    if manifest.execution_authorized {
        collector.push(
            ManifestViolationCode::ExecutionNotAllowed,
            "execution_authorized",
            "manifest parsing cannot authorize agent execution",
        );
    }

    validate_snake_identifier(&mut collector, "agent.family", &manifest.agent.family);
    validate_snake_identifier(
        &mut collector,
        "agent.task_class",
        &manifest.agent.task_class,
    );
    validate_personas(&mut collector, &manifest.agent.personas);
    validate_references(&mut collector, manifest);
    validate_distribution(&mut collector, manifest);
    validate_security(&mut collector, manifest);
    validate_review(&mut collector, manifest);

    collector.finish()
}

#[derive(Default)]
struct ViolationCollector {
    violations: Vec<ManifestViolation>,
}

impl ViolationCollector {
    fn push(&mut self, code: ManifestViolationCode, field: &'static str, message: &'static str) {
        self.violations.push(ManifestViolation {
            code,
            field,
            message,
        });
    }

    fn finish(self) -> Result<(), ManifestViolations> {
        if self.violations.is_empty() {
            Ok(())
        } else {
            Err(ManifestViolations::from_vec(self.violations))
        }
    }
}

fn validate_text(
    collector: &mut ViolationCollector,
    field: &'static str,
    value: &str,
    max_bytes: usize,
) {
    if value.trim().is_empty() || value.len() > max_bytes || value.contains(['\n', '\r', '\0']) {
        collector.push(
            ManifestViolationCode::InvalidText,
            field,
            "text must be non-empty, single-line, and bounded",
        );
    }
}

fn validate_package_identifier(
    collector: &mut ViolationCollector,
    field: &'static str,
    value: &str,
) {
    if !is_package_identifier(value) {
        collector.push(
            ManifestViolationCode::InvalidIdentifier,
            field,
            "identifier must use bounded lowercase ASCII segments",
        );
    }
}

fn validate_snake_identifier(collector: &mut ViolationCollector, field: &'static str, value: &str) {
    if !is_snake_identifier(value) {
        collector.push(
            ManifestViolationCode::InvalidIdentifier,
            field,
            "identifier must use bounded lowercase snake_case ASCII",
        );
    }
}

fn validate_personas(collector: &mut ViolationCollector, personas: &[String]) {
    if personas.is_empty() || personas.len() > MAX_PERSONAS {
        collector.push(
            ManifestViolationCode::InvalidCollection,
            "agent.personas",
            "persona list must be non-empty and bounded",
        );
    }

    let mut unique = HashSet::new();
    for persona in personas {
        validate_snake_identifier(collector, "agent.personas", persona);
        if !unique.insert(persona.as_str()) {
            collector.push(
                ManifestViolationCode::DuplicateValue,
                "agent.personas",
                "persona values must be unique",
            );
        }
    }
}

fn validate_references(collector: &mut ViolationCollector, manifest: &AgentPackageManifest) {
    let mut unique = HashSet::new();
    for (field, reference) in manifest.references.values() {
        if !is_safe_package_reference(reference) {
            collector.push(
                ManifestViolationCode::InvalidReference,
                field,
                "reference must be a bounded package-relative metadata path",
            );
        }
        if !unique.insert(reference) {
            collector.push(
                ManifestViolationCode::DuplicateValue,
                field,
                "manifest references must identify distinct files",
            );
        }
    }
}

fn is_safe_package_reference(reference: &str) -> bool {
    if reference.is_empty()
        || reference.len() > MAX_REFERENCE_BYTES
        || !reference.is_ascii()
        || reference.contains(['\\', ':', '\0'])
        || reference.starts_with('~')
    {
        return false;
    }

    let path = Path::new(reference);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn validate_distribution(collector: &mut ViolationCollector, manifest: &AgentPackageManifest) {
    validate_distribution_values(
        collector,
        "distribution.allowed_channels",
        &manifest.distribution.allowed_channels,
    );
    validate_distribution_values(
        collector,
        "distribution.allowed_install_modes",
        &manifest.distribution.allowed_install_modes,
    );

    if manifest.distribution.public_beta
        || manifest.distribution.marketplace
        || manifest.distribution.third_party_publication
    {
        collector.push(
            ManifestViolationCode::UnsafeDistribution,
            "distribution",
            "public distribution remains blocked",
        );
    }
}

fn validate_distribution_values<T: Eq + Hash>(
    collector: &mut ViolationCollector,
    field: &'static str,
    values: &[T],
) {
    if values.is_empty() || values.len() > MAX_DISTRIBUTION_VALUES {
        collector.push(
            ManifestViolationCode::UnsafeDistribution,
            field,
            "distribution list must be non-empty and bounded",
        );
    }

    let unique = values.iter().collect::<HashSet<_>>();
    if unique.len() != values.len() {
        collector.push(
            ManifestViolationCode::DuplicateValue,
            field,
            "distribution values must be unique",
        );
    }
}

fn validate_security(collector: &mut ViolationCollector, manifest: &AgentPackageManifest) {
    let security = &manifest.security;
    if security.collects_credentials
        || security.collects_host_identifiers
        || security.requires_network
        || security.allows_destructive_actions
        || security.allows_arbitrary_shell
        || security.allows_unrestricted_filesystem
    {
        collector.push(
            ManifestViolationCode::UnsafeSecurity,
            "security",
            "unsafe security capabilities remain blocked",
        );
    }
}

fn validate_review(collector: &mut ViolationCollector, manifest: &AgentPackageManifest) {
    let review = &manifest.review;
    if !review.requires_human_review
        || !review.requires_scope_manifest
        || !review.requires_permission_review
        || !review.requires_audit_policy
        || !review.requires_boundary_tests
    {
        collector.push(
            ManifestViolationCode::MissingReviewGate,
            "review",
            "all review gates are required",
        );
    }
}
