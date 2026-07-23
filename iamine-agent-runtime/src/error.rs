use std::fmt;

use crate::PackageReferenceKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResolverErrorCode {
    InvalidLimits,
    TooManyReferences,
    InvalidReference,
    RootUnavailable,
    ReferenceMissing,
    SymlinkRejected,
    NotRegularFile,
    HardLinkRejected,
    FileTooLarge,
    TotalSizeExceeded,
    ReadFailed,
    ReferenceChanged,
}

impl ResolverErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidLimits => "invalid_limits",
            Self::TooManyReferences => "too_many_references",
            Self::InvalidReference => "invalid_reference",
            Self::RootUnavailable => "root_unavailable",
            Self::ReferenceMissing => "reference_missing",
            Self::SymlinkRejected => "symlink_rejected",
            Self::NotRegularFile => "not_regular_file",
            Self::HardLinkRejected => "hard_link_rejected",
            Self::FileTooLarge => "file_too_large",
            Self::TotalSizeExceeded => "total_size_exceeded",
            Self::ReadFailed => "read_failed",
            Self::ReferenceChanged => "reference_changed",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::InvalidLimits => "package reference limits are invalid",
            Self::TooManyReferences => "package declares too many references",
            Self::InvalidReference => "package reference is not a safe relative path",
            Self::RootUnavailable => "package root is unavailable",
            Self::ReferenceMissing => "package reference is unavailable",
            Self::SymlinkRejected => "symbolic links are not allowed in package references",
            Self::NotRegularFile => "package reference is not a regular file",
            Self::HardLinkRejected => "hard-linked package references are not allowed",
            Self::FileTooLarge => "package reference exceeds the file size limit",
            Self::TotalSizeExceeded => "package references exceed the total size limit",
            Self::ReadFailed => "package reference could not be read safely",
            Self::ReferenceChanged => "package reference changed during the bounded read",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolverError {
    code: ResolverErrorCode,
    reference: Option<PackageReferenceKind>,
}

impl ResolverError {
    pub(crate) const fn new(
        code: ResolverErrorCode,
        reference: Option<PackageReferenceKind>,
    ) -> Self {
        Self { code, reference }
    }

    pub const fn code(&self) -> ResolverErrorCode {
        self.code
    }

    pub const fn reference(&self) -> Option<PackageReferenceKind> {
        self.reference
    }
}

impl fmt::Display for ResolverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for ResolverError {}
