use std::fmt;

use thiserror::Error;

const MAX_REPORTED_VIOLATIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BoundaryEvalErrorCode {
    InputTooLarge,
    InvalidYaml,
    SchemaGeneration,
    SchemaValidation,
    SemanticValidation,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BoundaryEvalError {
    #[error("boundary eval metadata exceeds the bounded input size")]
    InputTooLarge { max_bytes: usize },
    #[error("boundary eval metadata is not valid canonical YAML")]
    InvalidYaml,
    #[error("boundary eval JSON Schema generation failed")]
    SchemaGeneration,
    #[error("boundary eval metadata does not satisfy the canonical JSON Schema")]
    SchemaValidation,
    #[error("boundary eval metadata failed semantic validation")]
    SemanticValidation(BoundaryEvalViolations),
}

impl BoundaryEvalError {
    pub const fn code(&self) -> BoundaryEvalErrorCode {
        match self {
            Self::InputTooLarge { .. } => BoundaryEvalErrorCode::InputTooLarge,
            Self::InvalidYaml => BoundaryEvalErrorCode::InvalidYaml,
            Self::SchemaGeneration => BoundaryEvalErrorCode::SchemaGeneration,
            Self::SchemaValidation => BoundaryEvalErrorCode::SchemaValidation,
            Self::SemanticValidation(_) => BoundaryEvalErrorCode::SemanticValidation,
        }
    }

    pub const fn violations(&self) -> Option<&BoundaryEvalViolations> {
        match self {
            Self::SemanticValidation(violations) => Some(violations),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BoundaryEvalViolationCode {
    UnsupportedSchema,
    InvalidIdentifier,
    InvalidVersion,
    InvalidCollection,
    DuplicateValue,
    MissingRequiredClass,
    InvalidReference,
    InvalidSyntheticInput,
    PrivacyViolation,
    ContradictoryExpectation,
    MissingSafetyBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryEvalViolation {
    pub code: BoundaryEvalViolationCode,
    pub field: &'static str,
    pub message: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryEvalViolations {
    violations: Vec<BoundaryEvalViolation>,
}

impl BoundaryEvalViolations {
    pub(crate) fn from_vec(mut violations: Vec<BoundaryEvalViolation>) -> Self {
        violations.truncate(MAX_REPORTED_VIOLATIONS);
        Self { violations }
    }

    pub fn len(&self) -> usize {
        self.violations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &BoundaryEvalViolation> {
        self.violations.iter()
    }

    pub fn contains_code(&self, code: BoundaryEvalViolationCode) -> bool {
        self.violations
            .iter()
            .any(|violation| violation.code == code)
    }
}

impl fmt::Display for BoundaryEvalViolations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} boundary eval rule(s) violated", self.len())
    }
}

impl std::error::Error for BoundaryEvalViolations {}

#[derive(Default)]
pub(crate) struct ViolationCollector {
    violations: Vec<BoundaryEvalViolation>,
}

impl ViolationCollector {
    pub(crate) fn push(
        &mut self,
        code: BoundaryEvalViolationCode,
        field: &'static str,
        message: &'static str,
    ) {
        self.violations.push(BoundaryEvalViolation {
            code,
            field,
            message,
        });
    }

    pub(crate) fn finish(self) -> Result<(), BoundaryEvalViolations> {
        if self.violations.is_empty() {
            Ok(())
        } else {
            Err(BoundaryEvalViolations::from_vec(self.violations))
        }
    }
}
