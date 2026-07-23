use schemars::{schema_for, JsonSchema};
use serde::de::DeserializeOwned;

pub(crate) enum MetadataParserError {
    InputTooLarge { max_bytes: usize },
    InvalidYaml,
    SchemaGeneration,
    SchemaValidation,
}

pub(crate) fn json_schema<T: JsonSchema>() -> Result<serde_json::Value, MetadataParserError> {
    serde_json::to_value(schema_for!(T)).map_err(|_| MetadataParserError::SchemaGeneration)
}

pub(crate) fn parse_yaml<T>(input: &str, max_bytes: usize) -> Result<T, MetadataParserError>
where
    T: DeserializeOwned + JsonSchema,
{
    if input.len() > max_bytes {
        return Err(MetadataParserError::InputTooLarge { max_bytes });
    }

    let yaml_value = serde_yaml::from_str::<serde_yaml::Value>(input)
        .map_err(|_| MetadataParserError::InvalidYaml)?;
    let instance =
        serde_json::to_value(yaml_value).map_err(|_| MetadataParserError::InvalidYaml)?;
    let schema = json_schema::<T>()?;
    let compiled =
        jsonschema::validator_for(&schema).map_err(|_| MetadataParserError::SchemaGeneration)?;
    if !compiled.is_valid(&instance) {
        return Err(MetadataParserError::SchemaValidation);
    }

    serde_yaml::from_str::<T>(input).map_err(|_| MetadataParserError::InvalidYaml)
}
