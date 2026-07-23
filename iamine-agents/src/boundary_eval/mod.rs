mod error;
mod schema;
mod validation;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;

use crate::metadata_parser::{
    json_schema as structural_json_schema, parse_yaml as parse_structural_yaml, MetadataParserError,
};

pub use error::{
    BoundaryEvalError, BoundaryEvalErrorCode, BoundaryEvalViolation, BoundaryEvalViolationCode,
    BoundaryEvalViolations,
};
pub use schema::{
    boundary_eval_json_schema, parse_boundary_eval_yaml, BoundaryBlockPolicy, BoundaryEvalCase,
    BoundaryEvalClass, BoundaryEvalReview, BoundaryEvalRoute, BoundaryEvalSuite,
    BoundaryExpectedAction, BoundaryFailurePolicy, BoundaryForbiddenSuccesses,
    BoundaryRedactionPolicy, BOUNDARY_EVAL_SCHEMA_ID,
};

pub const MAX_BOUNDARY_EVAL_BYTES: usize = 64 * 1024;

pub(crate) fn json_schema<T: JsonSchema>() -> Result<serde_json::Value, BoundaryEvalError> {
    structural_json_schema::<T>().map_err(map_structural_error)
}

pub(crate) fn parse_yaml<T, F>(input: &str, validate: F) -> Result<T, BoundaryEvalError>
where
    T: DeserializeOwned + JsonSchema,
    F: FnOnce(&T) -> Result<(), BoundaryEvalViolations>,
{
    let metadata =
        parse_structural_yaml(input, MAX_BOUNDARY_EVAL_BYTES).map_err(map_structural_error)?;
    validate(&metadata).map_err(BoundaryEvalError::SemanticValidation)?;
    Ok(metadata)
}

fn map_structural_error(error: MetadataParserError) -> BoundaryEvalError {
    match error {
        MetadataParserError::InputTooLarge { max_bytes } => {
            BoundaryEvalError::InputTooLarge { max_bytes }
        }
        MetadataParserError::InvalidYaml => BoundaryEvalError::InvalidYaml,
        MetadataParserError::SchemaGeneration => BoundaryEvalError::SchemaGeneration,
        MetadataParserError::SchemaValidation => BoundaryEvalError::SchemaValidation,
    }
}
