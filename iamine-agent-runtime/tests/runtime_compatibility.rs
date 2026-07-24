use std::{collections::HashSet, error::Error, fs, path::Path};

use iamine_agent_runtime::{
    DeclaredAgentPackage, DependencyPolicyReviewDecision, HumanReviewDecision,
    LanguagePolicyReviewDecision, LocalRegistryReviewDecision, PackageReferenceResolver,
    PackageReviewAuthority, PackageReviewDecisions, PackageReviewSubject, ResolverLimits,
    RuntimeCompatibilityAuthority, RuntimeCompatibilityConfigurationErrorCode,
    RuntimeCompatibilityErrorCode, RuntimeCompatibilityEvidenceStatus,
    RuntimeCompatibilityRequirement, RuntimeLanguageAvailability, RuntimeLanguageDecision,
    RuntimeLanguageMode, RuntimeNetworkAvailability, RuntimeResourceEnvelope,
};
use iamine_agents::{
    assess_package_load_yaml, parse_and_validate_yaml, AgentPackageManifest, ExecutionMode,
    ManifestReferences, PackageLoadBlockerCode, PackageLoadStatus, ResourceOperatingMode,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

const VALID_MANIFEST: &str =
    include_str!("../../iamine-agents/tests/fixtures/valid/node-doctor-agent.yaml");
const VALID_RESOURCES: &str = include_str!(
    "../../iamine-agents/tests/fixtures/descriptive_metadata/valid/resource-requirements.yaml"
);

#[test]
fn compatible_official_runtime_establishes_bounded_evidence() -> TestResult {
    let fixture = PackageFixture::new(VALID_RESOURCES.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let authority = compatible_authority(2, 512, 84, RuntimeNetworkAvailability::None)?;
    let evidence = authority.evaluate(&review_authority, &review_evidence, subject)?;

    assert_eq!(
        evidence.status(),
        RuntimeCompatibilityEvidenceStatus::Established
    );
    assert_eq!(
        evidence.runtime_mode(),
        RuntimeLanguageMode::RustNativeOfficial
    );
    assert!(evidence.operating_mode() == ResourceOperatingMode::LocalReadonly);
    assert_eq!(evidence.requirements().len(), 8);
    assert_eq!(
        evidence
            .requirements()
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len(),
        8
    );
    assert!(authority.verifies(&evidence, subject));
    assert!(!evidence.load_allowed());
    assert!(!evidence.execution_allowed());

    Ok(())
}

#[test]
fn unsupported_and_unavailable_runtime_modes_fail_closed() -> TestResult {
    let fixture = PackageFixture::new(VALID_RESOURCES.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let resources = compatible_resources(2, 512, 84, RuntimeNetworkAvailability::None)?;

    for mode in [
        RuntimeLanguageMode::RustMetadataValidator,
        RuntimeLanguageMode::PythonSdkTooling,
        RuntimeLanguageMode::TypeScriptSdkTooling,
        RuntimeLanguageMode::WasmWasiSandboxedAgent,
        RuntimeLanguageMode::ContainerSandboxedAgent,
        RuntimeLanguageMode::ArbitraryShellAgent,
        RuntimeLanguageMode::UnrestrictedFilesystemAgent,
        RuntimeLanguageMode::MainnetWalletAgent,
    ] {
        let authority = RuntimeCompatibilityAuthority::new_operator_local(
            RuntimeLanguageDecision::new(mode, RuntimeLanguageAvailability::Available),
            resources,
        );
        assert_compatibility_error(
            authority.evaluate(&review_authority, &review_evidence, subject),
            RuntimeCompatibilityErrorCode::RuntimeModeUnsupported,
            RuntimeCompatibilityRequirement::RuntimeLanguage,
        )?;
    }

    for (availability, code) in [
        (
            RuntimeLanguageAvailability::Unavailable,
            RuntimeCompatibilityErrorCode::RuntimeUnavailable,
        ),
        (
            RuntimeLanguageAvailability::Deferred,
            RuntimeCompatibilityErrorCode::RuntimeDeferred,
        ),
        (
            RuntimeLanguageAvailability::Blocked,
            RuntimeCompatibilityErrorCode::RuntimeBlocked,
        ),
    ] {
        let authority = RuntimeCompatibilityAuthority::new_operator_local(
            RuntimeLanguageDecision::new(RuntimeLanguageMode::RustNativeOfficial, availability),
            resources,
        );
        assert_compatibility_error(
            authority.evaluate(&review_authority, &review_evidence, subject),
            code,
            RuntimeCompatibilityRequirement::RuntimeLanguage,
        )?;
    }

    Ok(())
}

#[test]
fn compatibility_requires_exact_review_and_subject_authorities() -> TestResult {
    let fixture = PackageFixture::new(VALID_RESOURCES.as_bytes())?;
    let resolved = fixture.resolve()?;
    let cloned_resolution = resolved.clone();
    let subject = fixture.subject(&resolved);
    let cloned_subject = fixture.subject(&cloned_resolution);
    let second_manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let second_manifest_subject = PackageReviewSubject::new(
        DeclaredAgentPackage::from_manifest(&second_manifest),
        &resolved,
    );
    let review_authority = PackageReviewAuthority::new_operator_local();
    let other_review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let authority = compatible_authority(2, 512, 84, RuntimeNetworkAvailability::None)?;
    let other_authority = compatible_authority(2, 512, 84, RuntimeNetworkAvailability::None)?;
    let evidence = authority.evaluate(&review_authority, &review_evidence, subject)?;

    for (candidate_authority, candidate_subject) in [
        (&other_review_authority, subject),
        (&review_authority, cloned_subject),
        (&review_authority, second_manifest_subject),
    ] {
        assert_compatibility_error(
            authority.evaluate(candidate_authority, &review_evidence, candidate_subject),
            RuntimeCompatibilityErrorCode::ReviewEvidenceNotVerified,
            RuntimeCompatibilityRequirement::PackageReviewEvidence,
        )?;
    }
    assert!(authority.verifies(&evidence, subject));
    assert!(!authority.verifies(&evidence, cloned_subject));
    assert!(!other_authority.verifies(&evidence, subject));

    Ok(())
}

#[test]
fn every_resource_dimension_fails_independently() -> TestResult {
    let cpu_resources = VALID_RESOURCES.replace("min_logical_cores: 1", "min_logical_cores: 2");
    assert_resource_failure(
        cpu_resources.as_bytes(),
        1,
        512,
        84,
        RuntimeNetworkAvailability::None,
        RuntimeCompatibilityErrorCode::CpuInsufficient,
        RuntimeCompatibilityRequirement::Cpu,
    )?;
    assert_resource_failure(
        VALID_RESOURCES.as_bytes(),
        2,
        511,
        84,
        RuntimeNetworkAvailability::None,
        RuntimeCompatibilityErrorCode::MemoryInsufficient,
        RuntimeCompatibilityRequirement::Memory,
    )?;
    assert_resource_failure(
        VALID_RESOURCES.as_bytes(),
        2,
        512,
        83,
        RuntimeNetworkAvailability::None,
        RuntimeCompatibilityErrorCode::StorageInsufficient,
        RuntimeCompatibilityRequirement::Storage,
    )?;
    let lan_resources = VALID_RESOURCES.replace("mode: none", "mode: lan_readonly");
    assert_resource_failure(
        lan_resources.as_bytes(),
        2,
        512,
        84,
        RuntimeNetworkAvailability::LocalOnly,
        RuntimeCompatibilityErrorCode::NetworkInsufficient,
        RuntimeCompatibilityRequirement::Network,
    )?;

    Ok(())
}

#[test]
fn invalid_cross_package_and_missing_mode_metadata_fail_closed() -> TestResult {
    assert_metadata_failure(
        b"private-invalid-resource-marker",
        RuntimeCompatibilityErrorCode::ResourceMetadataInvalid,
        RuntimeCompatibilityRequirement::ResourceMetadata,
    )?;
    let other_package =
        VALID_RESOURCES.replace("iamine.beta.node-doctor", "iamine.beta.other-agent");
    assert_metadata_failure(
        other_package.as_bytes(),
        RuntimeCompatibilityErrorCode::ResourcePackageMismatch,
        RuntimeCompatibilityRequirement::ResourceMetadata,
    )?;

    let mut fixture = PackageFixture::new(VALID_RESOURCES.as_bytes())?;
    fixture.manifest.agent.earliest_mode = ExecutionMode::LocalPlanning;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let authority = compatible_authority(2, 512, 84, RuntimeNetworkAvailability::None)?;
    assert_compatibility_error(
        authority.evaluate(&review_authority, &review_evidence, subject),
        RuntimeCompatibilityErrorCode::OperatingModeMissing,
        RuntimeCompatibilityRequirement::OperatingMode,
    )?;

    Ok(())
}

#[test]
fn configuration_debug_and_errors_are_bounded_and_private() -> TestResult {
    for (cores, memory, storage, code) in [
        (
            0,
            1,
            1,
            RuntimeCompatibilityConfigurationErrorCode::ZeroLogicalCoreLimit,
        ),
        (
            1,
            0,
            1,
            RuntimeCompatibilityConfigurationErrorCode::ZeroMemoryLimit,
        ),
        (
            1,
            1,
            0,
            RuntimeCompatibilityConfigurationErrorCode::ZeroStorageLimit,
        ),
    ] {
        let error =
            RuntimeResourceEnvelope::new(cores, memory, storage, RuntimeNetworkAvailability::None)
                .err()
                .ok_or("zero resource envelope unexpectedly accepted")?;
        assert_eq!(error.code(), code);
    }

    let private_marker = "private-review-resource-marker";
    let private_resources = VALID_RESOURCES.replace("review/resource-review.md", private_marker);
    let fixture = PackageFixture::new(private_resources.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let resources = compatible_resources(73, 987_654, 456_789, RuntimeNetworkAvailability::None)?;
    let authority = RuntimeCompatibilityAuthority::new_operator_local(
        RuntimeLanguageDecision::new(
            RuntimeLanguageMode::RustNativeOfficial,
            RuntimeLanguageAvailability::Blocked,
        ),
        resources,
    );
    let error = authority
        .evaluate(&review_authority, &review_evidence, subject)
        .err()
        .ok_or("blocked runtime unexpectedly established evidence")?;
    let combined = format!("{resources:?} {authority:?} {subject:?} {error:?} {error}");

    assert!(combined.contains("[redacted]"));
    assert!(!combined.contains(private_marker));
    assert!(!combined.contains("987654"));
    assert!(!combined.contains(&fixture.manifest.package_id));

    Ok(())
}

#[test]
fn package_load_compatibility_blockers_remain_unchanged() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;

    assert_eq!(report.status(), PackageLoadStatus::Blocked);
    assert!(!report.load_allowed());
    for blocker in [
        PackageLoadBlockerCode::RuntimeLanguageCompatibilityUnavailable,
        PackageLoadBlockerCode::ResourceCompatibilityUnavailable,
        PackageLoadBlockerCode::ExecutionAuthorizationUnavailable,
    ] {
        assert!(report.blockers().contains(&blocker));
    }

    Ok(())
}

fn assert_resource_failure(
    resource_content: &[u8],
    logical_cores: u16,
    memory_mb: u64,
    storage_mb: u64,
    network: RuntimeNetworkAvailability,
    code: RuntimeCompatibilityErrorCode,
    requirement: RuntimeCompatibilityRequirement,
) -> TestResult {
    let fixture = PackageFixture::new(resource_content)?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let authority = compatible_authority(logical_cores, memory_mb, storage_mb, network)?;
    assert_compatibility_error(
        authority.evaluate(&review_authority, &review_evidence, subject),
        code,
        requirement,
    )
}

fn assert_metadata_failure(
    resource_content: &[u8],
    code: RuntimeCompatibilityErrorCode,
    requirement: RuntimeCompatibilityRequirement,
) -> TestResult {
    assert_resource_failure(
        resource_content,
        2,
        512,
        84,
        RuntimeNetworkAvailability::None,
        code,
        requirement,
    )
}

fn assert_compatibility_error<T>(
    result: Result<T, iamine_agent_runtime::RuntimeCompatibilityError>,
    code: RuntimeCompatibilityErrorCode,
    requirement: RuntimeCompatibilityRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("incompatible subject unexpectedly established evidence")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    Ok(())
}

fn compatible_authority(
    logical_cores: u16,
    memory_mb: u64,
    storage_mb: u64,
    network: RuntimeNetworkAvailability,
) -> Result<RuntimeCompatibilityAuthority, Box<dyn Error>> {
    Ok(RuntimeCompatibilityAuthority::new_operator_local(
        RuntimeLanguageDecision::new(
            RuntimeLanguageMode::RustNativeOfficial,
            RuntimeLanguageAvailability::Available,
        ),
        compatible_resources(logical_cores, memory_mb, storage_mb, network)?,
    ))
}

fn compatible_resources(
    logical_cores: u16,
    memory_mb: u64,
    storage_mb: u64,
    network: RuntimeNetworkAvailability,
) -> Result<RuntimeResourceEnvelope, Box<dyn Error>> {
    Ok(RuntimeResourceEnvelope::new(
        logical_cores,
        memory_mb,
        storage_mb,
        network,
    )?)
}

fn approved_review_decisions() -> PackageReviewDecisions {
    PackageReviewDecisions::new(
        LocalRegistryReviewDecision::RegistryReviewReady,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::IndependentApproved,
    )
}

struct PackageFixture {
    root: TempDir,
    manifest: AgentPackageManifest,
}

impl PackageFixture {
    fn new(resource_content: &[u8]) -> Result<Self, Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
        create_references(root.path(), &manifest.references, resource_content)?;
        Ok(Self { root, manifest })
    }

    fn resolve(&self) -> Result<iamine_agent_runtime::ResolvedPackageReferences, Box<dyn Error>> {
        let resolver =
            PackageReferenceResolver::open_ambient(self.root.path(), ResolverLimits::default())?;
        Ok(resolver.resolve(&self.manifest.references)?)
    }

    fn subject<'a>(
        &'a self,
        references: &'a iamine_agent_runtime::ResolvedPackageReferences,
    ) -> PackageReviewSubject<'a> {
        PackageReviewSubject::new(
            DeclaredAgentPackage::from_manifest(&self.manifest),
            references,
        )
    }
}

fn create_references(
    root: &Path,
    references: &ManifestReferences,
    resource_content: &[u8],
) -> TestResult {
    for (declared, content) in [
        (&references.scope_manifest, b"validated metadata".as_slice()),
        (
            &references.capability_metadata,
            b"validated metadata".as_slice(),
        ),
        (
            &references.expertise_metadata,
            b"validated metadata".as_slice(),
        ),
        (&references.resource_requirements, resource_content),
        (
            &references.permission_model,
            b"validated metadata".as_slice(),
        ),
        (&references.audit_policy, b"validated metadata".as_slice()),
        (&references.boundary_tests, b"validated metadata".as_slice()),
    ] {
        let path = root.join(declared);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
    }
    Ok(())
}
