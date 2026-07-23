use std::{error::Error, fs, path::Path};

use iamine_agent_runtime::{
    PackageReferenceKind, PackageReferenceResolver, ResolverErrorCode, ResolverLimits,
    MAX_PACKAGE_REFERENCE_FILE_BYTES,
};
use iamine_agents::{parse_and_validate_yaml, AgentPackageManifest, ManifestReferences};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

const VALID_MANIFEST: &str =
    include_str!("../../iamine-agents/tests/fixtures/valid/node-doctor-agent.yaml");

#[test]
fn resolves_all_declared_references_with_bounded_redacted_output() -> TestResult {
    let fixture = PackageFixture::new()?;
    let resolver =
        PackageReferenceResolver::open_ambient(fixture.root.path(), ResolverLimits::default())?;
    let resolved = resolver.resolve(&fixture.manifest.references)?;
    let scope = resolved
        .get(PackageReferenceKind::ScopeManifest)
        .ok_or("scope reference missing from resolved set")?;

    assert_eq!(resolved.len(), 7);
    assert!(!resolved.is_empty());
    assert_eq!(scope.content(), b"scope_manifest");
    assert!(resolved.total_bytes() > 0);
    assert!(!format!("{resolved:?}").contains("scope_manifest"));
    assert!(!format!("{resolver:?}").contains(&fixture.root_label));

    Ok(())
}

#[test]
fn rejects_parent_absolute_windows_and_backslash_paths() -> TestResult {
    let fixture = PackageFixture::new()?;
    let resolver =
        PackageReferenceResolver::open_ambient(fixture.root.path(), ResolverLimits::default())?;

    for unsafe_path in [
        "../outside.yaml",
        "/private/outside.yaml",
        "C:\\private\\outside.yaml",
        "metadata\\agent.yaml",
        "./metadata/agent.yaml",
    ] {
        let mut references = fixture.manifest.references.clone();
        references.scope_manifest = unsafe_path.to_string();
        let error = resolver.resolve(&references).err();
        assert_eq!(
            error.as_ref().map(|error| error.code()),
            Some(ResolverErrorCode::InvalidReference)
        );
    }

    Ok(())
}

#[test]
fn duplicate_references_fail_closed() -> TestResult {
    let fixture = PackageFixture::new()?;
    let resolver =
        PackageReferenceResolver::open_ambient(fixture.root.path(), ResolverLimits::default())?;
    let mut references = fixture.manifest.references.clone();
    references.audit_policy = references.scope_manifest.clone();
    let error = resolver.resolve(&references).err();

    assert_eq!(
        error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::InvalidReference)
    );

    Ok(())
}

#[test]
fn missing_and_non_file_references_return_private_errors() -> TestResult {
    let fixture = PackageFixture::new()?;
    let resolver =
        PackageReferenceResolver::open_ambient(fixture.root.path(), ResolverLimits::default())?;
    let mut missing = fixture.manifest.references.clone();
    missing.scope_manifest = "private/missing-value.yaml".to_string();
    let missing_error = resolver
        .resolve(&missing)
        .err()
        .ok_or("missing reference unexpectedly resolved")?;

    assert_eq!(missing_error.code(), ResolverErrorCode::ReferenceMissing);
    assert_eq!(
        missing_error.reference(),
        Some(PackageReferenceKind::ScopeManifest)
    );
    assert!(!format!("{missing_error:?}").contains("missing-value"));
    assert!(!missing_error.to_string().contains("missing-value"));

    let directory_path = fixture.root.path().join("directory-reference");
    fs::create_dir(&directory_path)?;
    let mut directory = fixture.manifest.references.clone();
    directory.scope_manifest = "directory-reference".to_string();
    let directory_error = resolver
        .resolve(&directory)
        .err()
        .ok_or("directory reference unexpectedly resolved")?;
    assert_eq!(directory_error.code(), ResolverErrorCode::NotRegularFile);

    Ok(())
}

#[test]
fn per_file_and_total_limits_fail_before_unbounded_reads() -> TestResult {
    let fixture = PackageFixture::new()?;
    let oversized_path = fixture
        .root
        .path()
        .join(&fixture.manifest.references.scope_manifest);
    fs::write(
        oversized_path,
        vec![b'x'; MAX_PACKAGE_REFERENCE_FILE_BYTES as usize + 1],
    )?;
    let resolver =
        PackageReferenceResolver::open_ambient(fixture.root.path(), ResolverLimits::default())?;
    let file_error = resolver.resolve(&fixture.manifest.references).err();
    assert_eq!(
        file_error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::FileTooLarge)
    );

    let fixture = PackageFixture::new()?;
    let strict_limits = ResolverLimits::try_new(7, 64 * 1024, 8)?;
    let resolver = PackageReferenceResolver::open_ambient(fixture.root.path(), strict_limits)?;
    let total_error = resolver.resolve(&fixture.manifest.references).err();
    assert_eq!(
        total_error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::TotalSizeExceeded)
    );

    Ok(())
}

#[test]
fn invalid_limits_and_reference_count_limits_fail_closed() -> TestResult {
    assert_eq!(
        ResolverLimits::try_new(0, 1, 1)
            .err()
            .as_ref()
            .map(|error| error.code()),
        Some(ResolverErrorCode::InvalidLimits)
    );

    let fixture = PackageFixture::new()?;
    let limits = ResolverLimits::try_new(6, 64 * 1024, 7 * 64 * 1024)?;
    let resolver = PackageReferenceResolver::open_ambient(fixture.root.path(), limits)?;
    let error = resolver.resolve(&fixture.manifest.references).err();
    assert_eq!(
        error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::TooManyReferences)
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn final_and_intermediate_symlinks_are_rejected() -> TestResult {
    use std::os::unix::fs::symlink;

    let fixture = PackageFixture::new()?;
    let target = fixture.root.path().join("safe-target.yaml");
    fs::write(&target, b"safe")?;
    let link = fixture.root.path().join("linked-scope.yaml");
    symlink(&target, &link)?;
    let mut final_link = fixture.manifest.references.clone();
    final_link.scope_manifest = "linked-scope.yaml".to_string();
    let resolver =
        PackageReferenceResolver::open_ambient(fixture.root.path(), ResolverLimits::default())?;
    let final_error = resolver.resolve(&final_link).err();
    assert_eq!(
        final_error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::SymlinkRejected)
    );

    let real_directory = fixture.root.path().join("real-directory");
    fs::create_dir(&real_directory)?;
    fs::write(real_directory.join("scope.yaml"), b"scope")?;
    symlink(
        &real_directory,
        fixture.root.path().join("linked-directory"),
    )?;
    let mut intermediate_link = fixture.manifest.references.clone();
    intermediate_link.scope_manifest = "linked-directory/scope.yaml".to_string();
    let intermediate_error = resolver.resolve(&intermediate_link).err();
    assert_eq!(
        intermediate_error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::SymlinkRejected)
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn symlink_roots_and_hard_linked_references_are_rejected() -> TestResult {
    use std::os::unix::fs::symlink;

    let fixture = PackageFixture::new()?;
    let root_link = fixture.root.path().with_extension("root-link");
    symlink(fixture.root.path(), &root_link)?;
    let root_error =
        PackageReferenceResolver::open_ambient(&root_link, ResolverLimits::default()).err();
    assert_eq!(
        root_error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::SymlinkRejected)
    );

    let scope_path = fixture
        .root
        .path()
        .join(&fixture.manifest.references.scope_manifest);
    let second_link = fixture.root.path().join("second-scope-link.yaml");
    fs::hard_link(&scope_path, &second_link)?;
    let resolver =
        PackageReferenceResolver::open_ambient(fixture.root.path(), ResolverLimits::default())?;
    let hard_link_error = resolver.resolve(&fixture.manifest.references).err();
    assert_eq!(
        hard_link_error.as_ref().map(|error| error.code()),
        Some(ResolverErrorCode::HardLinkRejected)
    );

    fs::remove_file(root_link)?;
    Ok(())
}

struct PackageFixture {
    root: TempDir,
    root_label: String,
    manifest: AgentPackageManifest,
}

impl PackageFixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let root_label = root
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("temporary root has no portable label")?
            .to_string();
        let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
        create_references(root.path(), &manifest.references)?;

        Ok(Self {
            root,
            root_label,
            manifest,
        })
    }
}

fn create_references(root: &Path, references: &ManifestReferences) -> TestResult {
    for (label, declared) in [
        ("scope_manifest", &references.scope_manifest),
        ("capability_metadata", &references.capability_metadata),
        ("expertise_metadata", &references.expertise_metadata),
        ("resource_requirements", &references.resource_requirements),
        ("permission_model", &references.permission_model),
        ("audit_policy", &references.audit_policy),
        ("boundary_tests", &references.boundary_tests),
    ] {
        let path = root.join(declared);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, label.as_bytes())?;
    }
    Ok(())
}
