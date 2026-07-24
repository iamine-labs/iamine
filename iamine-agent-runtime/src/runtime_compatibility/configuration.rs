use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeLanguageMode {
    RustNativeOfficial,
    RustMetadataValidator,
    PythonSdkTooling,
    TypeScriptSdkTooling,
    WasmWasiSandboxedAgent,
    ContainerSandboxedAgent,
    ArbitraryShellAgent,
    UnrestrictedFilesystemAgent,
    MainnetWalletAgent,
}

impl RuntimeLanguageMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RustNativeOfficial => "rust_native_official",
            Self::RustMetadataValidator => "rust_metadata_validator",
            Self::PythonSdkTooling => "python_sdk_tooling",
            Self::TypeScriptSdkTooling => "typescript_sdk_tooling",
            Self::WasmWasiSandboxedAgent => "wasm_wasi_sandboxed_agent",
            Self::ContainerSandboxedAgent => "container_sandboxed_agent",
            Self::ArbitraryShellAgent => "arbitrary_shell_agent",
            Self::UnrestrictedFilesystemAgent => "unrestricted_filesystem_agent",
            Self::MainnetWalletAgent => "mainnet_wallet_agent",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeLanguageAvailability {
    Available,
    Unavailable,
    Deferred,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLanguageDecision {
    mode: RuntimeLanguageMode,
    availability: RuntimeLanguageAvailability,
}

impl RuntimeLanguageDecision {
    pub const fn new(mode: RuntimeLanguageMode, availability: RuntimeLanguageAvailability) -> Self {
        Self { mode, availability }
    }

    pub const fn mode(self) -> RuntimeLanguageMode {
        self.mode
    }

    pub const fn availability(self) -> RuntimeLanguageAvailability {
        self.availability
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeNetworkAvailability {
    None,
    LocalOnly,
    LanReadonly,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RuntimeResourceEnvelope {
    logical_cores: u16,
    memory_limit_mb: u64,
    storage_limit_mb: u64,
    network: RuntimeNetworkAvailability,
}

impl RuntimeResourceEnvelope {
    pub fn new(
        logical_cores: u16,
        memory_limit_mb: u64,
        storage_limit_mb: u64,
        network: RuntimeNetworkAvailability,
    ) -> Result<Self, RuntimeCompatibilityConfigurationError> {
        if logical_cores == 0 {
            return Err(RuntimeCompatibilityConfigurationError::new(
                RuntimeCompatibilityConfigurationErrorCode::ZeroLogicalCoreLimit,
            ));
        }
        if memory_limit_mb == 0 {
            return Err(RuntimeCompatibilityConfigurationError::new(
                RuntimeCompatibilityConfigurationErrorCode::ZeroMemoryLimit,
            ));
        }
        if storage_limit_mb == 0 {
            return Err(RuntimeCompatibilityConfigurationError::new(
                RuntimeCompatibilityConfigurationErrorCode::ZeroStorageLimit,
            ));
        }
        Ok(Self {
            logical_cores,
            memory_limit_mb,
            storage_limit_mb,
            network,
        })
    }

    pub(crate) const fn logical_cores(self) -> u16 {
        self.logical_cores
    }

    pub(crate) const fn memory_limit_mb(self) -> u64 {
        self.memory_limit_mb
    }

    pub(crate) const fn storage_limit_mb(self) -> u64 {
        self.storage_limit_mb
    }

    pub(crate) const fn network(self) -> RuntimeNetworkAvailability {
        self.network
    }
}

impl fmt::Debug for RuntimeResourceEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeResourceEnvelope")
            .field("logical_cores", &"[redacted]")
            .field("memory_limit_mb", &"[redacted]")
            .field("storage_limit_mb", &"[redacted]")
            .field("network", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeCompatibilityConfigurationErrorCode {
    ZeroLogicalCoreLimit,
    ZeroMemoryLimit,
    ZeroStorageLimit,
}

impl RuntimeCompatibilityConfigurationErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ZeroLogicalCoreLimit => "zero_logical_core_limit",
            Self::ZeroMemoryLimit => "zero_memory_limit",
            Self::ZeroStorageLimit => "zero_storage_limit",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ZeroLogicalCoreLimit => "logical core limit must be non-zero",
            Self::ZeroMemoryLimit => "memory limit must be non-zero",
            Self::ZeroStorageLimit => "storage limit must be non-zero",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCompatibilityConfigurationError {
    code: RuntimeCompatibilityConfigurationErrorCode,
}

impl RuntimeCompatibilityConfigurationError {
    const fn new(code: RuntimeCompatibilityConfigurationErrorCode) -> Self {
        Self { code }
    }

    pub const fn code(self) -> RuntimeCompatibilityConfigurationErrorCode {
        self.code
    }
}

impl fmt::Display for RuntimeCompatibilityConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for RuntimeCompatibilityConfigurationError {}
