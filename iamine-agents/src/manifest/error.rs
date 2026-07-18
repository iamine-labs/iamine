use std::fmt;

use thiserror::Error;

pub const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_REPORTED_VIOLATIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestErrorCode {
    InputTooLarge,
    InvalidYaml,
    SchemaGeneration,
    SchemaValidation,
    SemanticValidation,
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("agent manifest exceeds the bounded input size")]
    InputTooLarge { max_bytes: usize },
    #[error("agent manifest is not valid canonical YAML")]
    InvalidYaml {
        line: Option<usize>,
        column: Option<usize>,
    },
    #[error("agent manifest JSON Schema generation failed")]
    SchemaGeneration,
    #[error("agent manifest does not satisfy the canonical JSON Schema")]
    SchemaValidation,
    #[error("agent manifest failed semantic validation")]
    SemanticValidation(ManifestViolations),
}

impl ManifestError {
    pub fn code(&self) -> ManifestErrorCode {
        match self {
            Self::InputTooLarge { .. } => ManifestErrorCode::InputTooLarge,
            Self::InvalidYaml { .. } => ManifestErrorCode::InvalidYaml,
            Self::SchemaGeneration => ManifestErrorCode::SchemaGeneration,
            Self::SchemaValidation => ManifestErrorCode::SchemaValidation,
            Self::SemanticValidation(_) => ManifestErrorCode::SemanticValidation,
        }
    }

    pub fn violations(&self) -> Option<&ManifestViolations> {
        match self {
            Self::SemanticValidation(violations) => Some(violations),
            _ => None,
        }
    }

    pub(crate) fn invalid_yaml(location: Option<serde_yaml::Location>) -> Self {
        Self::InvalidYaml {
            line: location.as_ref().map(serde_yaml::Location::line),
            column: location.as_ref().map(serde_yaml::Location::column),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestViolationCode {
    UnsupportedSchema,
    InvalidIdentifier,
    InvalidVersion,
    InvalidText,
    InvalidCollection,
    DuplicateValue,
    InvalidReference,
    ExecutionNotAllowed,
    UnsafeDistribution,
    UnsafeSecurity,
    MissingReviewGate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestViolation {
    pub code: ManifestViolationCode,
    pub field: &'static str,
    pub message: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestViolations {
    violations: Vec<ManifestViolation>,
}

impl ManifestViolations {
    pub(crate) fn from_vec(mut violations: Vec<ManifestViolation>) -> Self {
        violations.truncate(MAX_REPORTED_VIOLATIONS);
        Self { violations }
    }

    pub fn len(&self) -> usize {
        self.violations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ManifestViolation> {
        self.violations.iter()
    }

    pub fn contains_code(&self, code: ManifestViolationCode) -> bool {
        self.violations
            .iter()
            .any(|violation| violation.code == code)
    }
}

impl fmt::Display for ManifestViolations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} manifest rule(s) violated", self.len())
    }
}

impl std::error::Error for ManifestViolations {}
