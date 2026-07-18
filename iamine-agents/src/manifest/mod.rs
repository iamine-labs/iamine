mod error;
mod schema;
mod validation;

pub use error::{
    ManifestError, ManifestErrorCode, ManifestViolation, ManifestViolationCode, ManifestViolations,
    MAX_MANIFEST_BYTES,
};
pub use schema::{
    AgentMetadata, AgentPackageManifest, DistributionChannel, DistributionPolicy, ExecutionMode,
    InstallMode, ManifestReferences, PackageStatus, ReviewPolicy, SecurityPolicy,
    MANIFEST_FILE_NAME, MANIFEST_SCHEMA_ID,
};
pub use validation::validate_manifest;

use schemars::schema_for;

pub fn manifest_json_schema() -> Result<serde_json::Value, ManifestError> {
    serde_json::to_value(schema_for!(AgentPackageManifest))
        .map_err(|_| ManifestError::SchemaGeneration)
}

pub fn parse_and_validate_yaml(input: &str) -> Result<AgentPackageManifest, ManifestError> {
    if input.len() > MAX_MANIFEST_BYTES {
        return Err(ManifestError::InputTooLarge {
            max_bytes: MAX_MANIFEST_BYTES,
        });
    }

    let yaml_value = serde_yaml::from_str::<serde_yaml::Value>(input)
        .map_err(|error| ManifestError::invalid_yaml(error.location()))?;
    let instance = serde_json::to_value(yaml_value).map_err(|_| ManifestError::InvalidYaml {
        line: None,
        column: None,
    })?;
    let schema = manifest_json_schema()?;
    let compiled =
        jsonschema::validator_for(&schema).map_err(|_| ManifestError::SchemaGeneration)?;

    if !compiled.is_valid(&instance) {
        return Err(ManifestError::SchemaValidation);
    }

    let manifest = serde_yaml::from_str::<AgentPackageManifest>(input)
        .map_err(|error| ManifestError::invalid_yaml(error.location()))?;
    validate_manifest(&manifest).map_err(ManifestError::SemanticValidation)?;

    Ok(manifest)
}
