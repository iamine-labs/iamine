use crate::{owner::runtime_owner_statuses, DeclaredAgentPackage, RuntimeOwnerStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeFoundationStatus {
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct RuntimeFoundationReport {
    owner_statuses: &'static [RuntimeOwnerStatus],
}

impl RuntimeFoundationReport {
    pub const fn status(&self) -> RuntimeFoundationStatus {
        RuntimeFoundationStatus::Blocked
    }

    pub const fn owner_statuses(&self) -> &'static [RuntimeOwnerStatus] {
        self.owner_statuses
    }

    pub const fn package_access_available(&self) -> bool {
        false
    }

    pub const fn execution_available(&self) -> bool {
        false
    }
}

pub fn inspect_runtime_foundation(_package: DeclaredAgentPackage<'_>) -> RuntimeFoundationReport {
    RuntimeFoundationReport {
        owner_statuses: runtime_owner_statuses(),
    }
}
