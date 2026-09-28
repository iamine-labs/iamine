use std::{fs, path::Path};

use iamine_agent_runtime::{
    DeclaredAgentPackage, PackageReferenceKind, PackageReferenceResolver, PackageReviewSubject,
    ResolvedPackageReferences, ResolverLimits,
};
use iamine_agents::{parse_and_validate_yaml, AgentPackageManifest};

use super::{PhotoLibraryAgentError, PhotoLibraryAgentErrorCode};

pub(super) const MANIFEST: &str =
    include_str!("../../../agents/official/photo-library-organizer/agent.yaml");
pub(super) const SCOPE: &str =
    include_str!("../../../agents/official/photo-library-organizer/agent-scope.yaml");
pub(super) const CAPABILITIES: &str = include_str!(
    "../../../agents/official/photo-library-organizer/metadata/agent-capabilities.yaml"
);
pub(super) const EXPERTISE: &str =
    include_str!("../../../agents/official/photo-library-organizer/metadata/agent-expertise.yaml");
pub(super) const RESOURCES: &str =
    include_str!("../../../agents/official/photo-library-organizer/metadata/agent-resources.yaml");
pub(super) const PERMISSIONS: &str = include_str!(
    "../../../agents/official/photo-library-organizer/metadata/agent-permissions.yaml"
);
pub(super) const AUDIT: &str =
    include_str!("../../../agents/official/photo-library-organizer/metadata/agent-audit.yaml");
pub(super) const BOUNDARY: &str = include_str!(
    "../../../agents/official/photo-library-organizer/evals/agent-boundary-tests.yaml"
);

pub(super) struct VerifiedPhotoLibraryPackage {
    manifest: AgentPackageManifest,
    references: ResolvedPackageReferences,
}

impl VerifiedPhotoLibraryPackage {
    pub(super) fn load(root: &Path) -> Result<Self, PhotoLibraryAgentError> {
        let input = fs::read_to_string(root.join("agent.yaml")).map_err(|_| {
            PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::PackageUnavailable)
        })?;
        let manifest = parse_and_validate_yaml(&input)
            .map_err(|_| PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::PackageInvalid))?;
        let canonical = parse_and_validate_yaml(MANIFEST)
            .map_err(|_| PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::PackageInvalid))?;
        if manifest != canonical {
            return Err(PhotoLibraryAgentError::new(
                PhotoLibraryAgentErrorCode::PackageMismatch,
            ));
        }

        let resolver = PackageReferenceResolver::open_ambient(root, ResolverLimits::default())
            .map_err(|_| {
                PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::PackageUnavailable)
            })?;
        let references = resolver
            .resolve(&manifest.references)
            .map_err(|_| PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::PackageInvalid))?;

        let package = Self {
            manifest,
            references,
        };
        if !package.matches_canonical_references() {
            return Err(PhotoLibraryAgentError::new(
                PhotoLibraryAgentErrorCode::PackageMismatch,
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
