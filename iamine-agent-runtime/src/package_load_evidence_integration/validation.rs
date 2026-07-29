use iamine_agents::{
    parse_audit_policy_yaml, parse_boundary_eval_yaml, parse_capability_metadata_yaml,
    parse_expertise_metadata_yaml, parse_permission_policy_yaml, parse_resource_requirements_yaml,
    parse_scope_policy_yaml,
};

use crate::{PackageReferenceKind, PackageReviewSubject};

use super::{
    PackageLoadEvidenceError, PackageLoadEvidenceErrorCode, PackageLoadEvidenceRequirement,
};

pub(super) fn validate_subject(
    subject: PackageReviewSubject<'_>,
) -> Result<(), PackageLoadEvidenceError> {
    let expected_package = subject.package_id();
    let scope = parse_reference(
        subject,
        PackageReferenceKind::ScopeManifest,
        PackageLoadEvidenceRequirement::ScopeManifestValidation,
        parse_scope_policy_yaml,
    )?;
    let capability = parse_reference(
        subject,
        PackageReferenceKind::CapabilityMetadata,
        PackageLoadEvidenceRequirement::CapabilityMetadataValidation,
        parse_capability_metadata_yaml,
    )?;
    let expertise = parse_reference(
        subject,
        PackageReferenceKind::ExpertiseMetadata,
        PackageLoadEvidenceRequirement::ExpertiseMetadataValidation,
        parse_expertise_metadata_yaml,
    )?;
    let resources = parse_reference(
        subject,
        PackageReferenceKind::ResourceRequirements,
        PackageLoadEvidenceRequirement::ResourceRequirementsValidation,
        parse_resource_requirements_yaml,
    )?;
    let permissions = parse_reference(
        subject,
        PackageReferenceKind::PermissionModel,
        PackageLoadEvidenceRequirement::PermissionModelValidation,
        parse_permission_policy_yaml,
    )?;
    let audit = parse_reference(
        subject,
        PackageReferenceKind::AuditPolicy,
        PackageLoadEvidenceRequirement::AuditPolicyValidation,
        parse_audit_policy_yaml,
    )?;
    let boundary = parse_reference(
        subject,
        PackageReferenceKind::BoundaryTests,
        PackageLoadEvidenceRequirement::BoundaryEvalValidation,
        parse_boundary_eval_yaml,
    )?;

    for package_id in [
        scope.package_id.as_str(),
        capability.package_id.as_str(),
        expertise.package_id.as_str(),
        resources.package_id.as_str(),
        permissions.package_id.as_str(),
        audit.package_id.as_str(),
        boundary.package_id.as_str(),
    ] {
        if package_id != expected_package {
            return Err(error(
                PackageLoadEvidenceErrorCode::PackageIdentityMismatch,
                PackageLoadEvidenceRequirement::ReferenceContract,
            ));
        }
    }

    let declared = &subject.package().manifest().references;
    let scope_tasks_match = scope
        .task_boundary
        .task_types
        .iter()
        .all(|task| capability.declared_task_types.contains(task));
    let scope_categories_match = scope
        .permission_requirements
        .required_categories
        .iter()
        .all(|category| permissions.requested_categories.contains(category))
        && scope
            .permission_requirements
            .forbidden_categories
            .iter()
            .all(|category| permissions.forbidden_categories.contains(category));
    let expertise_matches = expertise
        .supported_capabilities
        .contains(&capability.capability_id);
    let boundary_matches = boundary.scope_ref == declared.scope_manifest
        && boundary.permission_ref == declared.permission_model
        && boundary.audit_ref == declared.audit_policy;

    if !scope_tasks_match || !scope_categories_match || !expertise_matches || !boundary_matches {
        return Err(error(
            PackageLoadEvidenceErrorCode::ReferenceContractMismatch,
            PackageLoadEvidenceRequirement::ReferenceContract,
        ));
    }

    Ok(())
}

fn parse_reference<T, E>(
    subject: PackageReviewSubject<'_>,
    kind: PackageReferenceKind,
    requirement: PackageLoadEvidenceRequirement,
    parser: fn(&str) -> Result<T, E>,
) -> Result<T, PackageLoadEvidenceError> {
    let reference = subject
        .references()
        .get(kind)
        .ok_or_else(|| error(PackageLoadEvidenceErrorCode::ReferenceMissing, requirement))?;
    let content = std::str::from_utf8(reference.content()).map_err(|_| {
        error(
            PackageLoadEvidenceErrorCode::ReferenceEncodingInvalid,
            requirement,
        )
    })?;
    parser(content).map_err(|_| {
        error(
            PackageLoadEvidenceErrorCode::ReferenceValidationFailed,
            requirement,
        )
    })
}

const fn error(
    code: PackageLoadEvidenceErrorCode,
    requirement: PackageLoadEvidenceRequirement,
) -> PackageLoadEvidenceError {
    PackageLoadEvidenceError::new(code, requirement)
}
