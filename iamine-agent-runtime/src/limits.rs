use crate::{ResolverError, ResolverErrorCode};

pub const MAX_PACKAGE_REFERENCE_COUNT: usize = 7;
pub const MAX_PACKAGE_REFERENCE_BYTES: usize = 512;
pub const MAX_PACKAGE_REFERENCE_COMPONENTS: usize = 16;
pub const MAX_PACKAGE_REFERENCE_FILE_BYTES: u64 = 64 * 1024;
pub const MAX_PACKAGE_REFERENCE_TOTAL_BYTES: u64 =
    MAX_PACKAGE_REFERENCE_COUNT as u64 * MAX_PACKAGE_REFERENCE_FILE_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolverLimits {
    max_references: usize,
    max_file_bytes: u64,
    max_total_bytes: u64,
}

impl ResolverLimits {
    pub fn try_new(
        max_references: usize,
        max_file_bytes: u64,
        max_total_bytes: u64,
    ) -> Result<Self, ResolverError> {
        if max_references == 0
            || max_references > MAX_PACKAGE_REFERENCE_COUNT
            || max_file_bytes == 0
            || max_file_bytes > MAX_PACKAGE_REFERENCE_FILE_BYTES
            || max_total_bytes == 0
            || max_total_bytes > MAX_PACKAGE_REFERENCE_TOTAL_BYTES
        {
            return Err(ResolverError::new(ResolverErrorCode::InvalidLimits, None));
        }

        Ok(Self {
            max_references,
            max_file_bytes,
            max_total_bytes,
        })
    }

    pub const fn max_references(self) -> usize {
        self.max_references
    }

    pub const fn max_file_bytes(self) -> u64 {
        self.max_file_bytes
    }

    pub const fn max_total_bytes(self) -> u64 {
        self.max_total_bytes
    }
}

impl Default for ResolverLimits {
    fn default() -> Self {
        Self {
            max_references: MAX_PACKAGE_REFERENCE_COUNT,
            max_file_bytes: MAX_PACKAGE_REFERENCE_FILE_BYTES,
            max_total_bytes: MAX_PACKAGE_REFERENCE_TOTAL_BYTES,
        }
    }
}
