mod audit_events;
mod identifiers;
pub mod manifest;
mod package_load;
mod permission_enforcement;
mod policy_metadata;
mod scope_enforcement;

pub use audit_events::{
    audit_lifecycle_state, audit_permission_evaluation, audit_scope_evaluation, AgentAuditEvent,
    AuditEventClass, AuditEventSet, AuditEventSource, AuditLifecycleState, AuditOutcome,
    AuditReasonCode, AUDIT_EVENT_SCHEMA_VERSION, MAX_AUDIT_EVENTS_PER_PROJECTION,
};
pub use manifest::{
    manifest_json_schema, parse_and_validate_yaml, validate_manifest, AgentMetadata,
    AgentPackageManifest, DistributionChannel, DistributionPolicy, ExecutionMode, InstallMode,
    ManifestError, ManifestErrorCode, ManifestReferences, ManifestViolation, ManifestViolationCode,
    ManifestViolations, PackageStatus, ReviewPolicy, SecurityPolicy, MANIFEST_FILE_NAME,
    MANIFEST_SCHEMA_ID, MAX_MANIFEST_BYTES,
};
pub use package_load::{
    assess_package_load_yaml, PackageLoadBlockerCode, PackageLoadReport, PackageLoadStatus,
};
pub use permission_enforcement::{
    evaluate_permissions, PermissionConfirmation, PermissionDecision, PermissionDefaultPolicy,
    PermissionEvaluation, PermissionPolicy, PermissionPolicyError, PermissionPolicyErrorCode,
    PermissionPolicySpec, PermissionReasonCode, PermissionRequestRef,
};
pub use policy_metadata::{
    audit_policy_json_schema, parse_audit_policy_yaml, parse_permission_policy_yaml,
    parse_scope_policy_yaml, permission_policy_json_schema, scope_policy_json_schema,
    AuditAccessPolicy, AuditFailureAction, AuditFailurePolicy, AuditIntegrityPolicy,
    AuditPolicyEventClass, AuditPolicyMetadata, AuditRedactionDefault, AuditRedactionPolicy,
    AuditRetentionMode, AuditRetentionPolicy, AuditReview, AuditVisibility,
    PermissionConfirmationRequirements, PermissionDataAccess, PermissionDefaultPolicyMetadata,
    PermissionEscalation, PermissionEscalationTarget, PermissionFilesystemAccess,
    PermissionFilesystemAccessMode, PermissionNetworkAccess, PermissionNetworkAccessMode,
    PermissionPolicyMetadata, PermissionProcessAccess, PermissionProcessAccessMode,
    PermissionReview, PolicyMetadataError, PolicyMetadataErrorCode, PolicyMetadataViolation,
    PolicyMetadataViolationCode, PolicyMetadataViolations, ScopeConfirmationBoundary,
    ScopeEvalRequirements, ScopeHandoff, ScopeInputBoundary, ScopeOperationBoundary,
    ScopeOrchestratorReturn, ScopePermissionRequirements, ScopePolicyMetadata, ScopeReview,
    ScopeTaskBoundary, AUDIT_POLICY_SCHEMA_ID, MAX_POLICY_METADATA_BYTES,
    PERMISSION_POLICY_SCHEMA_ID, SCOPE_POLICY_SCHEMA_ID,
};
pub use scope_enforcement::{
    evaluate_scope, ScopeDecision, ScopeEvaluation, ScopePolicy, ScopePolicyError,
    ScopePolicyErrorCode, ScopePolicySpec, ScopeReasonCode, ScopeRequestClassification,
    ScopeRequestRef,
};
