mod audit;
mod error;
mod permission;
mod scope;
mod validation;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;

use crate::metadata_parser::{
    json_schema as structural_json_schema, parse_yaml as parse_structural_yaml, MetadataParserError,
};

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
    structural_json_schema::<T>().map_err(map_structural_error)
}

pub(crate) fn parse_yaml<T, F>(input: &str, validate: F) -> Result<T, PolicyMetadataError>
where
    T: DeserializeOwned + JsonSchema,
    F: FnOnce(&T) -> Result<(), PolicyMetadataViolations>,
{
    let metadata =
        parse_structural_yaml(input, MAX_POLICY_METADATA_BYTES).map_err(map_structural_error)?;
    validate(&metadata).map_err(PolicyMetadataError::SemanticValidation)?;
    Ok(metadata)
}

fn map_structural_error(error: MetadataParserError) -> PolicyMetadataError {
    match error {
        MetadataParserError::InputTooLarge { max_bytes } => {
            PolicyMetadataError::InputTooLarge { max_bytes }
        }
        MetadataParserError::InvalidYaml => PolicyMetadataError::InvalidYaml,
        MetadataParserError::SchemaGeneration => PolicyMetadataError::SchemaGeneration,
        MetadataParserError::SchemaValidation => PolicyMetadataError::SchemaValidation,
    }
}
