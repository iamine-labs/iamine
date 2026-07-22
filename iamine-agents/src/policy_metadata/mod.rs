mod audit;
mod error;
mod permission;
mod scope;
mod validation;

use schemars::{schema_for, JsonSchema};
use serde::de::DeserializeOwned;

pub use audit::{
    audit_policy_json_schema, parse_audit_policy_yaml, AuditAccessPolicy, AuditFailureAction,
    AuditFailurePolicy, AuditIntegrityPolicy, AuditPolicyEventClass, AuditPolicyMetadata,
    AuditRedactionDefault, AuditRedactionPolicy, AuditRetentionMode, AuditRetentionPolicy,
    AuditReview, AuditVisibility, AUDIT_POLICY_SCHEMA_ID,
};
pub use error::{
    PolicyMetadataError, PolicyMetadataErrorCode, PolicyMetadataViolation,
    PolicyMetadataViolationCode, PolicyMetadataViolations,
};
pub use permission::{
    parse_permission_policy_yaml, permission_policy_json_schema,
    PermissionConfirmationRequirements, PermissionDataAccess, PermissionDefaultPolicyMetadata,
    PermissionEscalation, PermissionEscalationTarget, PermissionFilesystemAccess,
    PermissionFilesystemAccessMode, PermissionNetworkAccess, PermissionNetworkAccessMode,
    PermissionPolicyMetadata, PermissionProcessAccess, PermissionProcessAccessMode,
    PermissionReview, PERMISSION_POLICY_SCHEMA_ID,
};
pub use scope::{
    parse_scope_policy_yaml, scope_policy_json_schema, ScopeConfirmationBoundary,
    ScopeEvalRequirements, ScopeHandoff, ScopeInputBoundary, ScopeOperationBoundary,
    ScopeOrchestratorReturn, ScopePermissionRequirements, ScopePolicyMetadata, ScopeReview,
    ScopeTaskBoundary, SCOPE_POLICY_SCHEMA_ID,
};

pub const MAX_POLICY_METADATA_BYTES: usize = 64 * 1024;

pub(crate) fn json_schema<T: JsonSchema>() -> Result<serde_json::Value, PolicyMetadataError> {
    serde_json::to_value(schema_for!(T)).map_err(|_| PolicyMetadataError::SchemaGeneration)
}

pub(crate) fn parse_yaml<T, F>(input: &str, validate: F) -> Result<T, PolicyMetadataError>
where
    T: DeserializeOwned + JsonSchema,
    F: FnOnce(&T) -> Result<(), PolicyMetadataViolations>,
{
    if input.len() > MAX_POLICY_METADATA_BYTES {
        return Err(PolicyMetadataError::InputTooLarge {
            max_bytes: MAX_POLICY_METADATA_BYTES,
        });
    }

    let yaml_value = serde_yaml::from_str::<serde_yaml::Value>(input)
        .map_err(|_| PolicyMetadataError::InvalidYaml)?;
    let instance =
        serde_json::to_value(yaml_value).map_err(|_| PolicyMetadataError::InvalidYaml)?;
    let schema = json_schema::<T>()?;
    let compiled =
        jsonschema::validator_for(&schema).map_err(|_| PolicyMetadataError::SchemaGeneration)?;
    if !compiled.is_valid(&instance) {
        return Err(PolicyMetadataError::SchemaValidation);
    }

    let metadata =
        serde_yaml::from_str::<T>(input).map_err(|_| PolicyMetadataError::InvalidYaml)?;
    validate(&metadata).map_err(PolicyMetadataError::SemanticValidation)?;
    Ok(metadata)
}
