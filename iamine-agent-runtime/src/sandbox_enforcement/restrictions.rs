use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxFilesystemPolicy {
    PackageReadOnlyWithBoundedTemporaryWorkspace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxNetworkPolicy {
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SandboxCleanupOwner {
    RuntimeSandboxAdapter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SandboxCleanupTrigger {
    StartupFailure,
    NormalExit,
    Cancellation,
    Timeout,
    AdapterDrop,
}

const REQUIRED_CLEANUP_TRIGGERS: [SandboxCleanupTrigger; 5] = [
    SandboxCleanupTrigger::StartupFailure,
    SandboxCleanupTrigger::NormalExit,
    SandboxCleanupTrigger::Cancellation,
    SandboxCleanupTrigger::Timeout,
    SandboxCleanupTrigger::AdapterDrop,
];

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SandboxRestrictionProfile {
    filesystem: SandboxFilesystemPolicy,
    network: SandboxNetworkPolicy,
    cleanup_owner: SandboxCleanupOwner,
}

impl SandboxRestrictionProfile {
    pub(crate) const fn local_readonly() -> Self {
        Self {
            filesystem: SandboxFilesystemPolicy::PackageReadOnlyWithBoundedTemporaryWorkspace,
            network: SandboxNetworkPolicy::Denied,
            cleanup_owner: SandboxCleanupOwner::RuntimeSandboxAdapter,
        }
    }

    pub const fn filesystem(self) -> SandboxFilesystemPolicy {
        self.filesystem
    }

    pub const fn network(self) -> SandboxNetworkPolicy {
        self.network
    }

    pub const fn cleanup_owner(self) -> SandboxCleanupOwner {
        self.cleanup_owner
    }

    pub const fn cleanup_triggers(self) -> &'static [SandboxCleanupTrigger] {
        &REQUIRED_CLEANUP_TRIGGERS
    }

    pub const fn private_paths_allowed(self) -> bool {
        false
    }

    pub const fn credentials_allowed(self) -> bool {
        false
    }

    pub const fn arbitrary_shell_allowed(self) -> bool {
        false
    }

    pub const fn child_processes_allowed(self) -> bool {
        false
    }

    pub const fn privilege_expansion_allowed(self) -> bool {
        false
    }
}

impl fmt::Debug for SandboxRestrictionProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SandboxRestrictionProfile { policy: [redacted] }")
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SandboxResourceLimits {
    logical_cores: u16,
    max_background_threads: u16,
    memory_limit_mb: u64,
    writable_storage_limit_mb: u64,
    max_processes: u16,
    max_child_processes: u16,
    max_wall_time_ms: u64,
    max_open_files: u32,
}

impl SandboxResourceLimits {
    pub(crate) const fn new(
        logical_cores: u16,
        max_background_threads: u16,
        memory_limit_mb: u64,
        writable_storage_limit_mb: u64,
        max_wall_time_ms: u64,
        max_open_files: u32,
    ) -> Self {
        Self {
            logical_cores,
            max_background_threads,
            memory_limit_mb,
            writable_storage_limit_mb,
            max_processes: 1,
            max_child_processes: 0,
            max_wall_time_ms,
            max_open_files,
        }
    }

    pub const fn logical_cores(self) -> u16 {
        self.logical_cores
    }

    pub const fn max_background_threads(self) -> u16 {
        self.max_background_threads
    }

    pub const fn memory_limit_mb(self) -> u64 {
        self.memory_limit_mb
    }

    pub const fn writable_storage_limit_mb(self) -> u64 {
        self.writable_storage_limit_mb
    }

    pub const fn max_processes(self) -> u16 {
        self.max_processes
    }

    pub const fn max_child_processes(self) -> u16 {
        self.max_child_processes
    }

    pub const fn max_wall_time_ms(self) -> u64 {
        self.max_wall_time_ms
    }

    pub const fn max_open_files(self) -> u32 {
        self.max_open_files
    }
}

impl fmt::Debug for SandboxResourceLimits {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SandboxResourceLimits { limits: [redacted] }")
    }
}
