mod capability;
mod error;
mod expertise;
mod resource;
mod validation;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;

use crate::metadata_parser::{
    json_schema as structural_json_schema, parse_yaml as parse_structural_yaml, MetadataParserError,
};

pub use capability::{
    capability_metadata_json_schema, parse_capability_metadata_yaml, CapabilityExecutionMode,
    CapabilityMetadata, CapabilityReview, CapabilityRiskProfile, CAPABILITY_METADATA_SCHEMA_ID,
};
pub use error::{
    DescriptiveMetadataError, DescriptiveMetadataErrorCode, DescriptiveMetadataViolation,
    DescriptiveMetadataViolationCode, DescriptiveMetadataViolations,
};
pub use expertise::{
    expertise_metadata_json_schema, parse_expertise_metadata_yaml, ExpertiseEvaluationRequirement,
    ExpertiseEvidence, ExpertiseEvidenceType, ExpertiseFreshness, ExpertiseMetadata,
    ExpertiseReview, ExpertiseStaleBehavior, EXPERTISE_METADATA_SCHEMA_ID,
};
pub use resource::{
    parse_resource_requirements_yaml, resource_requirements_json_schema, AcceleratorClass,
    AcceleratorRequirements, CpuRequirements, MemoryRequirements, ModelDependencies, NetworkMode,
    NetworkRequirements, ResourceConstraints, ResourceDegradation, ResourceDegradationBehavior,
    ResourceOperatingMode, ResourcePrivacy, ResourceRequirementsMetadata, ResourceReview,
    StorageRequirements, RESOURCE_REQUIREMENTS_SCHEMA_ID,
};

pub const MAX_DESCRIPTIVE_METADATA_BYTES: usize = 64 * 1024;

pub(crate) fn json_schema<T: JsonSchema>() -> Result<serde_json::Value, DescriptiveMetadataError> {
    structural_json_schema::<T>().map_err(map_structural_error)
}

pub(crate) fn parse_yaml<T, F>(input: &str, validate: F) -> Result<T, DescriptiveMetadataError>
where
    T: DeserializeOwned + JsonSchema,
    F: FnOnce(&T) -> Result<(), DescriptiveMetadataViolations>,
{
    let metadata = parse_structural_yaml(input, MAX_DESCRIPTIVE_METADATA_BYTES)
        .map_err(map_structural_error)?;
    validate(&metadata).map_err(DescriptiveMetadataError::SemanticValidation)?;
    Ok(metadata)
}

fn map_structural_error(error: MetadataParserError) -> DescriptiveMetadataError {
    match error {
        MetadataParserError::InputTooLarge { max_bytes } => {
            DescriptiveMetadataError::InputTooLarge { max_bytes }
        }
        MetadataParserError::InvalidYaml => DescriptiveMetadataError::InvalidYaml,
        MetadataParserError::SchemaGeneration => DescriptiveMetadataError::SchemaGeneration,
        MetadataParserError::SchemaValidation => DescriptiveMetadataError::SchemaValidation,
    }
}
