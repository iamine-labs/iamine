mod audit_events;
mod descriptive_metadata;
mod identifiers;
pub mod manifest;
mod metadata_parser;
mod package_load;
mod permission_enforcement;
mod policy_metadata;
mod scope_enforcement;

pub use audit_events::{
    audit_lifecycle_state, audit_permission_evaluation, audit_scope_evaluation, AgentAuditEvent,
    AuditEventClass, AuditEventSet, AuditEventSource, AuditLifecycleState, AuditOutcome,
    AuditReasonCode, AUDIT_EVENT_SCHEMA_VERSION, MAX_AUDIT_EVENTS_PER_PROJECTION,
};
pub use descriptive_metadata::{
    capability_metadata_json_schema, expertise_metadata_json_schema,
    parse_capability_metadata_yaml, parse_expertise_metadata_yaml,
    parse_resource_requirements_yaml, resource_requirements_json_schema, AcceleratorClass,
    AcceleratorRequirements, CapabilityExecutionMode, CapabilityMetadata, CapabilityReview,
    CapabilityRiskProfile, CpuRequirements, DescriptiveMetadataError, DescriptiveMetadataErrorCode,
    DescriptiveMetadataViolation, DescriptiveMetadataViolationCode, DescriptiveMetadataViolations,
    ExpertiseEvaluationRequirement, ExpertiseEvidence, ExpertiseEvidenceType, ExpertiseFreshness,
    ExpertiseMetadata, ExpertiseReview, ExpertiseStaleBehavior, MemoryRequirements,
    ModelDependencies, NetworkMode, NetworkRequirements, ResourceConstraints, ResourceDegradation,
    ResourceDegradationBehavior, ResourceOperatingMode, ResourcePrivacy,
    ResourceRequirementsMetadata, ResourceReview, StorageRequirements,
    CAPABILITY_METADATA_SCHEMA_ID, EXPERTISE_METADATA_SCHEMA_ID, MAX_DESCRIPTIVE_METADATA_BYTES,
    RESOURCE_REQUIREMENTS_SCHEMA_ID,
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
