use std::fmt;

use iamine_agents::AgentPackageManifest;

/// A passive reference to caller-supplied, typed package declarations.
///
/// This reference is not review evidence and does not authorize package access
/// or execution.
#[derive(Clone, Copy)]
#[must_use]
pub struct DeclaredAgentPackage<'a> {
    _manifest: &'a AgentPackageManifest,
}

impl<'a> DeclaredAgentPackage<'a> {
    pub const fn from_manifest(manifest: &'a AgentPackageManifest) -> Self {
        Self {
            _manifest: manifest,
        }
    }
}

impl fmt::Debug for DeclaredAgentPackage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeclaredAgentPackage { manifest: [redacted] }")
    }
}
