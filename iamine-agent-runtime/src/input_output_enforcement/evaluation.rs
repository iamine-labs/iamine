use iamine_agents::parse_scope_policy_yaml;

use crate::{PackageReferenceKind, PackageReviewSubject};

use super::{
    InputOutputEnforcementError, InputOutputEnforcementErrorCode, InputOutputRecordContext,
    InputOutputRequirement,
};

pub(super) fn derive_record_context(
    subject: PackageReviewSubject<'_>,
) -> Result<InputOutputRecordContext, InputOutputEnforcementError> {
    let reference = subject
        .references()
        .get(PackageReferenceKind::ScopeManifest)
        .ok_or_else(|| {
            InputOutputEnforcementError::new(
                InputOutputEnforcementErrorCode::ScopeMetadataMissing,
                InputOutputRequirement::ScopeMetadata,
            )
        })?;
    let content = std::str::from_utf8(reference.content()).map_err(|_| {
        InputOutputEnforcementError::new(
            InputOutputEnforcementErrorCode::ScopeMetadataInvalid,
            InputOutputRequirement::ScopeMetadata,
        )
    })?;
    let scope = parse_scope_policy_yaml(content).map_err(|_| {
        InputOutputEnforcementError::new(
            InputOutputEnforcementErrorCode::ScopeMetadataInvalid,
            InputOutputRequirement::ScopeMetadata,
        )
    })?;
    let manifest = subject.package().manifest();
    if scope.package_id != manifest.package_id {
        return Err(InputOutputEnforcementError::new(
            InputOutputEnforcementErrorCode::ScopePackageMismatch,
            InputOutputRequirement::PackageIdentity,
        ));
    }
    if !scope
        .task_boundary
        .task_types
        .contains(&manifest.agent.task_class)
    {
        return Err(InputOutputEnforcementError::new(
            InputOutputEnforcementErrorCode::ScopeTaskTypeMismatch,
            InputOutputRequirement::TaskType,
        ));
    }
    Ok(InputOutputRecordContext::new(
        &manifest.package_id,
        &manifest.agent.task_class,
        &scope.scope_id,
    ))
}

pub(super) fn validate_content(
    content: &str,
    limit: usize,
    too_large: InputOutputEnforcementErrorCode,
) -> Result<(), InputOutputEnforcementError> {
    if content.is_empty() {
        return Err(InputOutputEnforcementError::new(
            InputOutputEnforcementErrorCode::EmptyContent,
            InputOutputRequirement::RecordContent,
        ));
    }
    if content.len() > limit {
        return Err(InputOutputEnforcementError::new(
            too_large,
            InputOutputRequirement::RecordLimit,
        ));
    }
    if content.chars().any(char::is_control) {
        return Err(InputOutputEnforcementError::new(
            InputOutputEnforcementErrorCode::ControlCharacter,
            InputOutputRequirement::RecordContent,
        ));
    }
    Ok(())
}
