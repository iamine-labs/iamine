use std::fmt;

use crate::{DeclaredAgentPackage, PackageReferenceKind, ResolvedPackageReferences};

#[derive(Clone, Copy)]
#[must_use]
pub struct PackageReviewSubject<'a> {
    package: DeclaredAgentPackage<'a>,
    references: &'a ResolvedPackageReferences,
}

impl<'a> PackageReviewSubject<'a> {
    pub const fn new(
        package: DeclaredAgentPackage<'a>,
        references: &'a ResolvedPackageReferences,
    ) -> Self {
        Self {
            package,
            references,
        }
    }

    pub fn reference_count(self) -> usize {
        self.references.len()
    }

    pub const fn total_reference_bytes(self) -> u64 {
        self.references.total_bytes()
    }

    pub(crate) const fn package(self) -> DeclaredAgentPackage<'a> {
        self.package
    }

    pub fn package_id(self) -> &'a str {
        &self.package.manifest().package_id
    }

    pub fn task_type(self) -> &'a str {
        &self.package.manifest().agent.task_class
    }

    pub fn reference_matches(self, kind: PackageReferenceKind, expected: &[u8]) -> bool {
        self.references
            .get(kind)
            .is_some_and(|reference| reference.content() == expected)
    }

    pub(crate) const fn references(self) -> &'a ResolvedPackageReferences {
        self.references
    }

    pub(crate) fn same_as(self, other: Self) -> bool {
        self.package.same_manifest(other.package) && std::ptr::eq(self.references, other.references)
    }
}

impl fmt::Debug for PackageReviewSubject<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackageReviewSubject")
            .field("package", &"[redacted]")
            .field("reference_count", &self.references.len())
            .field("total_reference_bytes", &self.references.total_bytes())
            .finish()
    }
}
