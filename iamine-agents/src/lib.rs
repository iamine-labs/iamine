mod identifiers;
pub mod manifest;
mod package_load;
mod scope_enforcement;

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
pub use scope_enforcement::{
    evaluate_scope, ScopeDecision, ScopeEvaluation, ScopePolicy, ScopePolicyError,
    ScopePolicyErrorCode, ScopePolicySpec, ScopeReasonCode, ScopeRequestClassification,
    ScopeRequestRef,
};
