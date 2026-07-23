use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PackageReferenceKind {
    ScopeManifest,
    CapabilityMetadata,
    ExpertiseMetadata,
    ResourceRequirements,
    PermissionModel,
    AuditPolicy,
    BoundaryTests,
}

impl PackageReferenceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopeManifest => "scope_manifest",
            Self::CapabilityMetadata => "capability_metadata",
            Self::ExpertiseMetadata => "expertise_metadata",
            Self::ResourceRequirements => "resource_requirements",
            Self::PermissionModel => "permission_model",
            Self::AuditPolicy => "audit_policy",
            Self::BoundaryTests => "boundary_tests",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedReference {
    kind: PackageReferenceKind,
    content: Vec<u8>,
}

impl ResolvedReference {
    pub(crate) fn new(kind: PackageReferenceKind, content: Vec<u8>) -> Self {
        Self { kind, content }
    }

    pub const fn kind(&self) -> PackageReferenceKind {
        self.kind
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }
}

impl fmt::Debug for ResolvedReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedReference")
            .field("kind", &self.kind)
            .field("content_bytes", &self.content.len())
            .field("content", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
#[must_use]
pub struct ResolvedPackageReferences {
    references: Vec<ResolvedReference>,
    total_bytes: u64,
}

impl ResolvedPackageReferences {
    pub(crate) fn new(references: Vec<ResolvedReference>, total_bytes: u64) -> Self {
        Self {
            references,
            total_bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.references.len()
    }

    pub fn is_empty(&self) -> bool {
        self.references.is_empty()
    }

    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn get(&self, kind: PackageReferenceKind) -> Option<&ResolvedReference> {
        self.references
            .iter()
            .find(|reference| reference.kind == kind)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ResolvedReference> {
        self.references.iter()
    }
}

impl fmt::Debug for ResolvedPackageReferences {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedPackageReferences")
            .field("reference_count", &self.references.len())
            .field("total_bytes", &self.total_bytes)
            .field("content", &"[redacted]")
            .finish()
    }
}
