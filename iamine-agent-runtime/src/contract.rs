use std::fmt;

use iamine_agents::AgentPackageManifest;

/// A passive reference to caller-supplied, typed package declarations.
///
/// This reference is not review evidence and does not authorize package access
/// or execution.
#[derive(Clone, Copy)]
#[must_use]
pub struct DeclaredAgentPackage<'a> {
    manifest: &'a AgentPackageManifest,
}

impl<'a> DeclaredAgentPackage<'a> {
    pub const fn from_manifest(manifest: &'a AgentPackageManifest) -> Self {
        Self { manifest }
    }

    pub(crate) const fn manifest(self) -> &'a AgentPackageManifest {
        self.manifest
    }

    pub(crate) fn same_manifest(self, other: Self) -> bool {
        std::ptr::eq(self.manifest, other.manifest)
    }
}

impl fmt::Debug for DeclaredAgentPackage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeclaredAgentPackage { manifest: [redacted] }")
    }
}
