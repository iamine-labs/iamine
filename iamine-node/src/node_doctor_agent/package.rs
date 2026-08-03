use std::{fs, path::Path};

use iamine_agent_runtime::{
    DeclaredAgentPackage, PackageReferenceKind, PackageReferenceResolver, PackageReviewSubject,
    ResolvedPackageReferences, ResolverLimits,
};
use iamine_agents::{parse_and_validate_yaml, AgentPackageManifest};

use super::{NodeDoctorAgentError, NodeDoctorAgentErrorCode};

pub(super) const MANIFEST: &str = include_str!("../../../agents/official/node-doctor/agent.yaml");
pub(super) const SCOPE: &str =
    include_str!("../../../agents/official/node-doctor/agent-scope.yaml");
pub(super) const CAPABILITIES: &str =
    include_str!("../../../agents/official/node-doctor/metadata/agent-capabilities.yaml");
pub(super) const EXPERTISE: &str =
    include_str!("../../../agents/official/node-doctor/metadata/agent-expertise.yaml");
pub(super) const RESOURCES: &str =
    include_str!("../../../agents/official/node-doctor/metadata/agent-resources.yaml");
pub(super) const PERMISSIONS: &str =
    include_str!("../../../agents/official/node-doctor/metadata/agent-permissions.yaml");
pub(super) const AUDIT: &str =
    include_str!("../../../agents/official/node-doctor/metadata/agent-audit.yaml");
pub(super) const BOUNDARY: &str =
    include_str!("../../../agents/official/node-doctor/evals/agent-boundary-tests.yaml");

pub(super) struct VerifiedNodeDoctorPackage {
    manifest: AgentPackageManifest,
    references: ResolvedPackageReferences,
}

impl VerifiedNodeDoctorPackage {
    pub(super) fn load(root: &Path) -> Result<Self, NodeDoctorAgentError> {
        let input = fs::read_to_string(root.join("agent.yaml"))
            .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageUnavailable))?;
        let manifest = parse_and_validate_yaml(&input)
            .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageInvalid))?;
        let canonical = parse_and_validate_yaml(MANIFEST)
            .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageInvalid))?;
        if manifest != canonical {
            return Err(NodeDoctorAgentError::new(
                NodeDoctorAgentErrorCode::PackageMismatch,
            ));
        }

        let resolver = PackageReferenceResolver::open_ambient(root, ResolverLimits::default())
            .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageUnavailable))?;
        let references = resolver
            .resolve(&manifest.references)
            .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageInvalid))?;

        let package = Self {
            manifest,
            references,
        };
        if !package.matches_canonical_references() {
            return Err(NodeDoctorAgentError::new(
                NodeDoctorAgentErrorCode::PackageMismatch,
            ));
        }
        Ok(package)
    }

    pub(super) fn subject(&self) -> PackageReviewSubject<'_> {
        PackageReviewSubject::new(
            DeclaredAgentPackage::from_manifest(&self.manifest),
            &self.references,
        )
    }

    fn matches_canonical_references(&self) -> bool {
        let subject = self.subject();
        [
            (PackageReferenceKind::ScopeManifest, SCOPE.as_bytes()),
            (
                PackageReferenceKind::CapabilityMetadata,
                CAPABILITIES.as_bytes(),
            ),
            (
                PackageReferenceKind::ExpertiseMetadata,
                EXPERTISE.as_bytes(),
            ),
            (
                PackageReferenceKind::ResourceRequirements,
                RESOURCES.as_bytes(),
            ),
            (
                PackageReferenceKind::PermissionModel,
                PERMISSIONS.as_bytes(),
            ),
            (PackageReferenceKind::AuditPolicy, AUDIT.as_bytes()),
            (PackageReferenceKind::BoundaryTests, BOUNDARY.as_bytes()),
        ]
        .into_iter()
        .all(|(kind, expected)| subject.reference_matches(kind, expected))
    }
}
