use crate::{
    SandboxCleanupOwner, SandboxCleanupTrigger, SandboxEnforcementEvidence,
    SandboxFilesystemPolicy, SandboxNetworkPolicy,
};

pub(super) struct ActiveOfficialRustSandbox {
    active: bool,
}

impl ActiveOfficialRustSandbox {
    pub(super) fn activate(evidence: &SandboxEnforcementEvidence<'_>) -> Option<Self> {
        let restrictions = evidence.restrictions();
        let limits = evidence.resource_limits();
        let required_cleanup = [
            SandboxCleanupTrigger::StartupFailure,
            SandboxCleanupTrigger::NormalExit,
            SandboxCleanupTrigger::Cancellation,
            SandboxCleanupTrigger::Timeout,
            SandboxCleanupTrigger::AdapterDrop,
        ];

        if restrictions.filesystem()
            != SandboxFilesystemPolicy::PackageReadOnlyWithBoundedTemporaryWorkspace
            || restrictions.network() != SandboxNetworkPolicy::Denied
            || restrictions.cleanup_owner() != SandboxCleanupOwner::RuntimeSandboxAdapter
            || required_cleanup
                .iter()
                .any(|trigger| !restrictions.cleanup_triggers().contains(trigger))
            || restrictions.private_paths_allowed()
            || restrictions.credentials_allowed()
            || restrictions.arbitrary_shell_allowed()
            || restrictions.child_processes_allowed()
            || restrictions.privilege_expansion_allowed()
            || limits.max_processes() != 1
            || limits.max_child_processes() != 0
            || limits.max_wall_time_ms() == 0
        {
            return None;
        }

        Some(Self { active: true })
    }

    pub(super) fn close(mut self) -> bool {
        self.active = false;
        true
    }
}

impl Drop for ActiveOfficialRustSandbox {
    fn drop(&mut self) {
        self.active = false;
    }
}
