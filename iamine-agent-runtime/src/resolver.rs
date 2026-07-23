use std::{
    collections::HashSet,
    ffi::OsString,
    fmt,
    io::{self, Read, Seek, SeekFrom},
    path::{Component, Path},
};

use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, File, Metadata, OpenOptions},
};
use iamine_agents::ManifestReferences;

use crate::{
    PackageReferenceKind, ResolvedPackageReferences, ResolvedReference, ResolverError,
    ResolverErrorCode, ResolverLimits, MAX_PACKAGE_REFERENCE_BYTES,
    MAX_PACKAGE_REFERENCE_COMPONENTS,
};

pub struct PackageReferenceResolver {
    root: Dir,
    limits: ResolverLimits,
}

impl PackageReferenceResolver {
    /// Opens the caller-selected package root using ambient filesystem authority.
    ///
    /// All subsequent package access is relative to the returned directory
    /// capability.
    pub fn open_ambient(
        root: impl AsRef<Path>,
        limits: ResolverLimits,
    ) -> Result<Self, ResolverError> {
        let root = root.as_ref();
        let root_name = root
            .file_name()
            .ok_or_else(|| ResolverError::new(ResolverErrorCode::RootUnavailable, None))?;
        let parent_path = root
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let parent = Dir::open_ambient_dir(parent_path, ambient_authority())
            .map_err(|_| ResolverError::new(ResolverErrorCode::RootUnavailable, None))?;
        let root = parent.open_dir_nofollow(root_name).map_err(|_| {
            let code = if parent
                .symlink_metadata(root_name)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                ResolverErrorCode::SymlinkRejected
            } else {
                ResolverErrorCode::RootUnavailable
            };
            ResolverError::new(code, None)
        })?;

        Ok(Self { root, limits })
    }

    pub fn resolve(
        &self,
        references: &ManifestReferences,
    ) -> Result<ResolvedPackageReferences, ResolverError> {
        let declared = declared_references(references);
        if declared.len() > self.limits.max_references() {
            return Err(ResolverError::new(
                ResolverErrorCode::TooManyReferences,
                None,
            ));
        }

        let mut unique_paths = HashSet::with_capacity(declared.len());
        let mut resolved = Vec::with_capacity(declared.len());
        let mut total_bytes = 0_u64;

        for (kind, declared_path) in declared {
            if !unique_paths.insert(declared_path) {
                return Err(ResolverError::new(
                    ResolverErrorCode::InvalidReference,
                    Some(kind),
                ));
            }

            let components = validate_reference(declared_path, kind)?;
            let content = self.read_reference(kind, &components)?;
            total_bytes = total_bytes
                .checked_add(content.len() as u64)
                .filter(|total| *total <= self.limits.max_total_bytes())
                .ok_or_else(|| {
                    ResolverError::new(ResolverErrorCode::TotalSizeExceeded, Some(kind))
                })?;
            resolved.push(ResolvedReference::new(kind, content));
        }

        Ok(ResolvedPackageReferences::new(resolved, total_bytes))
    }

    fn read_reference(
        &self,
        kind: PackageReferenceKind,
        components: &[OsString],
    ) -> Result<Vec<u8>, ResolverError> {
        let mut directory = self
            .root
            .try_clone()
            .map_err(|_| ResolverError::new(ResolverErrorCode::ReadFailed, Some(kind)))?;
        let (file_name, parent_components) = components
            .split_last()
            .ok_or_else(|| ResolverError::new(ResolverErrorCode::InvalidReference, Some(kind)))?;

        for component in parent_components {
            let path_metadata = validate_directory_component(&directory, component, kind)?;
            let opened = directory
                .open_dir_nofollow(component)
                .map_err(|error| map_io_error(error, kind))?;
            let handle_metadata = opened
                .dir_metadata()
                .map_err(|_| ResolverError::new(ResolverErrorCode::ReadFailed, Some(kind)))?;
            ensure_same_object(&path_metadata, &handle_metadata, kind)?;
            directory = opened;
        }

        let path_metadata = directory
            .symlink_metadata(file_name)
            .map_err(|error| map_io_error(error, kind))?;
        validate_file_metadata(&path_metadata, kind, self.limits.max_file_bytes())?;

        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let mut file = directory
            .open_with(file_name, &options)
            .map_err(|error| map_io_error(error, kind))?;
        let before = file
            .metadata()
            .map_err(|_| ResolverError::new(ResolverErrorCode::ReadFailed, Some(kind)))?;
        validate_file_metadata(&before, kind, self.limits.max_file_bytes())?;
        ensure_same_object(&path_metadata, &before, kind)?;

        read_stable(&mut file, &before, self.limits.max_file_bytes(), kind)
    }
}

impl fmt::Debug for PackageReferenceResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackageReferenceResolver")
            .field("root", &"[redacted]")
            .field("limits", &self.limits)
            .finish()
    }
}

fn declared_references(references: &ManifestReferences) -> [(PackageReferenceKind, &str); 7] {
    [
        (
            PackageReferenceKind::ScopeManifest,
            &references.scope_manifest,
        ),
        (
            PackageReferenceKind::CapabilityMetadata,
            &references.capability_metadata,
        ),
        (
            PackageReferenceKind::ExpertiseMetadata,
            &references.expertise_metadata,
        ),
        (
            PackageReferenceKind::ResourceRequirements,
            &references.resource_requirements,
        ),
        (
            PackageReferenceKind::PermissionModel,
            &references.permission_model,
        ),
        (PackageReferenceKind::AuditPolicy, &references.audit_policy),
        (
            PackageReferenceKind::BoundaryTests,
            &references.boundary_tests,
        ),
    ]
}

fn validate_reference(
    declared: &str,
    kind: PackageReferenceKind,
) -> Result<Vec<OsString>, ResolverError> {
    let bytes = declared.as_bytes();
    let has_windows_prefix = bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';

    if declared.is_empty()
        || declared.len() > MAX_PACKAGE_REFERENCE_BYTES
        || declared.contains('\\')
        || declared.contains('\0')
        || has_windows_prefix
    {
        return Err(ResolverError::new(
            ResolverErrorCode::InvalidReference,
            Some(kind),
        ));
    }

    let mut components = Vec::new();
    for component in Path::new(declared).components() {
        match component {
            Component::Normal(value) => components.push(value.to_os_string()),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => {
                return Err(ResolverError::new(
                    ResolverErrorCode::InvalidReference,
                    Some(kind),
                ));
            }
        }
    }

    if components.is_empty() || components.len() > MAX_PACKAGE_REFERENCE_COMPONENTS {
        return Err(ResolverError::new(
            ResolverErrorCode::InvalidReference,
            Some(kind),
        ));
    }

    Ok(components)
}

fn validate_directory_component(
    directory: &Dir,
    component: &OsString,
    kind: PackageReferenceKind,
) -> Result<Metadata, ResolverError> {
    let metadata = directory
        .symlink_metadata(component)
        .map_err(|error| map_io_error(error, kind))?;
    if metadata.file_type().is_symlink() {
        return Err(ResolverError::new(
            ResolverErrorCode::SymlinkRejected,
            Some(kind),
        ));
    }
    if !metadata.is_dir() {
        return Err(ResolverError::new(
            ResolverErrorCode::NotRegularFile,
            Some(kind),
        ));
    }
    Ok(metadata)
}

fn validate_file_metadata(
    metadata: &Metadata,
    kind: PackageReferenceKind,
    max_file_bytes: u64,
) -> Result<(), ResolverError> {
    if metadata.file_type().is_symlink() {
        return Err(ResolverError::new(
            ResolverErrorCode::SymlinkRejected,
            Some(kind),
        ));
    }
    if !metadata.is_file() {
        return Err(ResolverError::new(
            ResolverErrorCode::NotRegularFile,
            Some(kind),
        ));
    }
    if MetadataExt::nlink(metadata) > 1 {
        return Err(ResolverError::new(
            ResolverErrorCode::HardLinkRejected,
            Some(kind),
        ));
    }
    if metadata.len() > max_file_bytes {
        return Err(ResolverError::new(
            ResolverErrorCode::FileTooLarge,
            Some(kind),
        ));
    }
    Ok(())
}

fn ensure_same_object(
    path_metadata: &Metadata,
    handle_metadata: &Metadata,
    kind: PackageReferenceKind,
) -> Result<(), ResolverError> {
    if MetadataExt::dev(path_metadata) != MetadataExt::dev(handle_metadata)
        || MetadataExt::ino(path_metadata) != MetadataExt::ino(handle_metadata)
    {
        return Err(ResolverError::new(
            ResolverErrorCode::ReferenceChanged,
            Some(kind),
        ));
    }
    Ok(())
}

fn read_stable(
    file: &mut File,
    before: &Metadata,
    max_file_bytes: u64,
    kind: PackageReferenceKind,
) -> Result<Vec<u8>, ResolverError> {
    let first = read_once(file, max_file_bytes, kind)?;
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ResolverError::new(ResolverErrorCode::ReadFailed, Some(kind)))?;
    let second = read_once(file, max_file_bytes, kind)?;
    let after = file
        .metadata()
        .map_err(|_| ResolverError::new(ResolverErrorCode::ReadFailed, Some(kind)))?;

    let modified_changed = match (before.modified(), after.modified()) {
        (Ok(before), Ok(after)) => before != after,
        _ => false,
    };
    if first != second
        || first.len() as u64 != before.len()
        || before.len() != after.len()
        || modified_changed
    {
        return Err(ResolverError::new(
            ResolverErrorCode::ReferenceChanged,
            Some(kind),
        ));
    }

    Ok(first)
}

fn read_once(
    file: &mut File,
    max_file_bytes: u64,
    kind: PackageReferenceKind,
) -> Result<Vec<u8>, ResolverError> {
    let mut content = Vec::with_capacity(max_file_bytes as usize);
    {
        let mut bounded = file.take(max_file_bytes + 1);
        bounded
            .read_to_end(&mut content)
            .map_err(|_| ResolverError::new(ResolverErrorCode::ReadFailed, Some(kind)))?;
    }
    if content.len() as u64 > max_file_bytes {
        return Err(ResolverError::new(
            ResolverErrorCode::FileTooLarge,
            Some(kind),
        ));
    }
    Ok(content)
}

fn map_io_error(error: io::Error, kind: PackageReferenceKind) -> ResolverError {
    let code = if error.kind() == io::ErrorKind::NotFound {
        ResolverErrorCode::ReferenceMissing
    } else {
        ResolverErrorCode::ReadFailed
    };
    ResolverError::new(code, Some(kind))
}
