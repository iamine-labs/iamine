use std::fmt;

use thiserror::Error;

const MAX_REPORTED_VIOLATIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DescriptiveMetadataErrorCode {
    InputTooLarge,
    InvalidYaml,
    SchemaGeneration,
    SchemaValidation,
    SemanticValidation,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DescriptiveMetadataError {
    #[error("descriptive metadata exceeds the bounded input size")]
    InputTooLarge { max_bytes: usize },
    #[error("descriptive metadata is not valid canonical YAML")]
    InvalidYaml,
    #[error("descriptive metadata JSON Schema generation failed")]
    SchemaGeneration,
    #[error("descriptive metadata does not satisfy the canonical JSON Schema")]
    SchemaValidation,
    #[error("descriptive metadata failed semantic validation")]
    SemanticValidation(DescriptiveMetadataViolations),
}

impl DescriptiveMetadataError {
    pub const fn code(&self) -> DescriptiveMetadataErrorCode {
        match self {
            Self::InputTooLarge { .. } => DescriptiveMetadataErrorCode::InputTooLarge,
            Self::InvalidYaml => DescriptiveMetadataErrorCode::InvalidYaml,
            Self::SchemaGeneration => DescriptiveMetadataErrorCode::SchemaGeneration,
            Self::SchemaValidation => DescriptiveMetadataErrorCode::SchemaValidation,
            Self::SemanticValidation(_) => DescriptiveMetadataErrorCode::SemanticValidation,
        }
    }

    pub const fn violations(&self) -> Option<&DescriptiveMetadataViolations> {
        match self {
            Self::SemanticValidation(violations) => Some(violations),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DescriptiveMetadataViolationCode {
    UnsupportedSchema,
    InvalidIdentifier,
    InvalidVersion,
    InvalidCollection,
    DuplicateValue,
    MissingSafetyBoundary,
    UnsafeClaim,
    InvalidReference,
    InvalidResourceBound,
    ContradictoryRequirement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptiveMetadataViolation {
    pub code: DescriptiveMetadataViolationCode,
    pub field: &'static str,
    pub message: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptiveMetadataViolations {
    violations: Vec<DescriptiveMetadataViolation>,
}

impl DescriptiveMetadataViolations {
    pub(crate) fn from_vec(mut violations: Vec<DescriptiveMetadataViolation>) -> Self {
        violations.truncate(MAX_REPORTED_VIOLATIONS);
        Self { violations }
    }

    pub fn len(&self) -> usize {
        self.violations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &DescriptiveMetadataViolation> {
        self.violations.iter()
    }

    pub fn contains_code(&self, code: DescriptiveMetadataViolationCode) -> bool {
        self.violations
            .iter()
            .any(|violation| violation.code == code)
    }
}

impl fmt::Display for DescriptiveMetadataViolations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} descriptive metadata rule(s) violated",
            self.len()
        )
    }
}

impl std::error::Error for DescriptiveMetadataViolations {}

#[derive(Default)]
pub(crate) struct ViolationCollector {
    violations: Vec<DescriptiveMetadataViolation>,
}

impl ViolationCollector {
    pub(crate) fn push(
        &mut self,
        code: DescriptiveMetadataViolationCode,
        field: &'static str,
        message: &'static str,
    ) {
        self.violations.push(DescriptiveMetadataViolation {
            code,
            field,
            message,
        });
    }

    pub(crate) fn finish(self) -> Result<(), DescriptiveMetadataViolations> {
        if self.violations.is_empty() {
            Ok(())
        } else {
            Err(DescriptiveMetadataViolations::from_vec(self.violations))
        }
    }
}
