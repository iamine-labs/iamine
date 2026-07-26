use std::fmt;

pub const MAX_SANDBOX_WALL_TIME_MS: u64 = 3_600_000;
pub const MAX_SANDBOX_OPEN_FILES: u32 = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxPlatform {
    MacOs,
    Linux,
}

impl SandboxPlatform {
    pub const fn current() -> Option<Self> {
        if cfg!(target_os = "macos") {
            Some(Self::MacOs)
        } else if cfg!(target_os = "linux") {
            Some(Self::Linux)
        } else {
            None
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MacOs => "macos",
            Self::Linux => "linux",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SandboxEnforcementPolicy {
    max_wall_time_ms: u64,
    max_open_files: u32,
}

impl SandboxEnforcementPolicy {
    pub fn new(
        max_wall_time_ms: u64,
        max_open_files: u32,
    ) -> Result<Self, SandboxConfigurationError> {
        if max_wall_time_ms == 0 {
            return Err(SandboxConfigurationError::new(
                SandboxConfigurationErrorCode::ZeroWallTimeLimit,
            ));
        }
        if max_wall_time_ms > MAX_SANDBOX_WALL_TIME_MS {
            return Err(SandboxConfigurationError::new(
                SandboxConfigurationErrorCode::WallTimeLimitTooLarge,
            ));
        }
        if max_open_files == 0 {
            return Err(SandboxConfigurationError::new(
                SandboxConfigurationErrorCode::ZeroOpenFileLimit,
            ));
        }
        if max_open_files > MAX_SANDBOX_OPEN_FILES {
            return Err(SandboxConfigurationError::new(
                SandboxConfigurationErrorCode::OpenFileLimitTooLarge,
            ));
        }
        Ok(Self {
            max_wall_time_ms,
            max_open_files,
        })
    }

    pub const fn max_wall_time_ms(self) -> u64 {
        self.max_wall_time_ms
    }

    pub const fn max_open_files(self) -> u32 {
        self.max_open_files
    }
}

impl fmt::Debug for SandboxEnforcementPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SandboxEnforcementPolicy")
            .field("max_wall_time_ms", &"[redacted]")
            .field("max_open_files", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxConfigurationErrorCode {
    UnsupportedPlatform,
    ZeroWallTimeLimit,
    WallTimeLimitTooLarge,
    ZeroOpenFileLimit,
    OpenFileLimitTooLarge,
}

impl SandboxConfigurationErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::ZeroWallTimeLimit => "zero_wall_time_limit",
            Self::WallTimeLimitTooLarge => "wall_time_limit_too_large",
            Self::ZeroOpenFileLimit => "zero_open_file_limit",
            Self::OpenFileLimitTooLarge => "open_file_limit_too_large",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "the current platform is not supported",
            Self::ZeroWallTimeLimit => "wall time limit must be non-zero",
            Self::WallTimeLimitTooLarge => "wall time limit exceeds the supported maximum",
            Self::ZeroOpenFileLimit => "open file limit must be non-zero",
            Self::OpenFileLimitTooLarge => "open file limit exceeds the supported maximum",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxConfigurationError {
    code: SandboxConfigurationErrorCode,
}

impl SandboxConfigurationError {
    pub(crate) const fn new(code: SandboxConfigurationErrorCode) -> Self {
        Self { code }
    }

    pub const fn code(self) -> SandboxConfigurationErrorCode {
        self.code
    }
}

impl fmt::Display for SandboxConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for SandboxConfigurationError {}
