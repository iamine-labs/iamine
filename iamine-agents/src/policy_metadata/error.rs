use std::fmt;

use thiserror::Error;

const MAX_REPORTED_VIOLATIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PolicyMetadataErrorCode {
    InputTooLarge,
    InvalidYaml,
    SchemaGeneration,
    SchemaValidation,
    SemanticValidation,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PolicyMetadataError {
    #[error("policy metadata exceeds the bounded input size")]
    InputTooLarge { max_bytes: usize },
    #[error("policy metadata is not valid canonical YAML")]
    InvalidYaml,
    #[error("policy metadata JSON Schema generation failed")]
    SchemaGeneration,
    #[error("policy metadata does not satisfy the canonical JSON Schema")]
    SchemaValidation,
    #[error("policy metadata failed semantic validation")]
    SemanticValidation(PolicyMetadataViolations),
}

impl PolicyMetadataError {
    pub const fn code(&self) -> PolicyMetadataErrorCode {
        match self {
            Self::InputTooLarge { .. } => PolicyMetadataErrorCode::InputTooLarge,
            Self::InvalidYaml => PolicyMetadataErrorCode::InvalidYaml,
            Self::SchemaGeneration => PolicyMetadataErrorCode::SchemaGeneration,
            Self::SchemaValidation => PolicyMetadataErrorCode::SchemaValidation,
            Self::SemanticValidation(_) => PolicyMetadataErrorCode::SemanticValidation,
        }
    }

    pub const fn violations(&self) -> Option<&PolicyMetadataViolations> {
        match self {
            Self::SemanticValidation(violations) => Some(violations),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PolicyMetadataViolationCode {
    UnsupportedSchema,
    InvalidIdentifier,
    InvalidVersion,
    InvalidCollection,
    DuplicateValue,
    ContradictoryBoundary,
    MissingSafetyBoundary,
    UnsafePolicy,
    InvalidReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyMetadataViolation {
    pub code: PolicyMetadataViolationCode,
    pub field: &'static str,
    pub message: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyMetadataViolations {
    violations: Vec<PolicyMetadataViolation>,
}

impl PolicyMetadataViolations {
    pub(crate) fn from_vec(mut violations: Vec<PolicyMetadataViolation>) -> Self {
        violations.truncate(MAX_REPORTED_VIOLATIONS);
        Self { violations }
    }

    pub fn len(&self) -> usize {
        self.violations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &PolicyMetadataViolation> {
        self.violations.iter()
    }

    pub fn contains_code(&self, code: PolicyMetadataViolationCode) -> bool {
        self.violations
            .iter()
            .any(|violation| violation.code == code)
    }
}

impl fmt::Display for PolicyMetadataViolations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} policy metadata rule(s) violated", self.len())
    }
}

impl std::error::Error for PolicyMetadataViolations {}

#[derive(Default)]
pub(crate) struct ViolationCollector {
    violations: Vec<PolicyMetadataViolation>,
}

impl ViolationCollector {
    pub(crate) fn push(
        &mut self,
        code: PolicyMetadataViolationCode,
        field: &'static str,
        message: &'static str,
    ) {
        self.violations.push(PolicyMetadataViolation {
            code,
            field,
            message,
        });
    }

    pub(crate) fn finish(self) -> Result<(), PolicyMetadataViolations> {
        if self.violations.is_empty() {
            Ok(())
        } else {
            Err(PolicyMetadataViolations::from_vec(self.violations))
        }
    }
}
