use std::{collections::HashSet, error::Error, fs, path::Path};

use iamine_agent_runtime::{
    DeclaredAgentPackage, DependencyPolicyReviewDecision, HumanReviewDecision,
    InputOutputEnforcementAuthority, InputOutputEnforcementEvidence, InputOutputPolicy,
    LanguagePolicyReviewDecision, LocalRegistryReviewDecision, PackageReferenceResolver,
    PackageReviewAuthority, PackageReviewDecisions, PackageReviewSubject, ResolverLimits,
    RuntimeCompatibilityAuthority, RuntimeCompatibilityEvidence, RuntimeLanguageAvailability,
    RuntimeLanguageDecision, RuntimeLanguageMode, RuntimeNetworkAvailability,
    RuntimeResourceEnvelope, SandboxCleanupOwner, SandboxCleanupTrigger,
    SandboxConfigurationErrorCode, SandboxEnforcementAuthority, SandboxEnforcementError,
    SandboxEnforcementErrorCode, SandboxEnforcementEvidenceStatus, SandboxEnforcementPolicy,
    SandboxEnforcementRequirement, SandboxFilesystemPolicy, SandboxNetworkPolicy, SandboxPlatform,
    MAX_SANDBOX_OPEN_FILES, MAX_SANDBOX_WALL_TIME_MS, SANDBOX_ENFORCEMENT_SCHEMA_VERSION,
};
use iamine_agents::{
    assess_package_load_yaml, parse_and_validate_yaml, AgentPackageManifest, ExecutionMode,
    ManifestReferences, PackageLoadBlockerCode, PackageLoadStatus,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

const VALID_MANIFEST: &str =
    include_str!("../../iamine-agents/tests/fixtures/valid/node-doctor-agent.yaml");
const VALID_SCOPE: &str =
    include_str!("../../iamine-agents/tests/fixtures/policy_metadata/valid/scope-policy.yaml");
const VALID_RESOURCES: &str = include_str!(
    "../../iamine-agents/tests/fixtures/descriptive_metadata/valid/resource-requirements.yaml"
);

#[test]
fn current_platform_prepares_a_bounded_local_readonly_plan() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let chain = establish_chain(subject, RuntimeNetworkAvailability::None, 2)?;
    let authority = sandbox_authority(30_000, 128)?;
    let evidence = authority.establish(
        &chain.compatibility_authority,
        &chain.compatibility_evidence,
        &chain.input_output_authority,
        &chain.input_output_evidence,
        subject,
    )?;

    assert_eq!(
        evidence.schema_version(),
        SANDBOX_ENFORCEMENT_SCHEMA_VERSION
    );
    assert_eq!(
        evidence.status(),
        SandboxEnforcementEvidenceStatus::Prepared
    );
    assert_eq!(evidence.platform(), current_platform()?);
    assert!(authority.verifies(&evidence, subject));
    assert_eq!(evidence.requirements().len(), 12);
    assert_eq!(
        evidence
            .requirements()
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len(),
        12
    );

    let limits = evidence.resource_limits();
    assert_eq!(limits.logical_cores(), 2);
    assert_eq!(limits.max_background_threads(), 1);
    assert_eq!(limits.memory_limit_mb(), 512);
    assert_eq!(limits.writable_storage_limit_mb(), 64);
    assert_eq!(limits.max_processes(), 1);
    assert_eq!(limits.max_child_processes(), 0);
    assert_eq!(limits.max_wall_time_ms(), 30_000);
    assert_eq!(limits.max_open_files(), 128);

    let restrictions = evidence.restrictions();
    assert_eq!(
        restrictions.filesystem(),
        SandboxFilesystemPolicy::PackageReadOnlyWithBoundedTemporaryWorkspace
    );
    assert_eq!(restrictions.network(), SandboxNetworkPolicy::Denied);
    assert_eq!(
        restrictions.cleanup_owner(),
        SandboxCleanupOwner::RuntimeSandboxAdapter
    );
    assert_eq!(
        restrictions
            .cleanup_triggers()
            .iter()
            .copied()
            .collect::<HashSet<_>>(),
        HashSet::from([
            SandboxCleanupTrigger::StartupFailure,
            SandboxCleanupTrigger::NormalExit,
            SandboxCleanupTrigger::Cancellation,
            SandboxCleanupTrigger::Timeout,
            SandboxCleanupTrigger::AdapterDrop,
        ])
    );
    assert!(!restrictions.private_paths_allowed());
    assert!(!restrictions.credentials_allowed());
    assert!(!restrictions.arbitrary_shell_allowed());
    assert!(!restrictions.child_processes_allowed());
    assert!(!restrictions.privilege_expansion_allowed());
    assert!(!evidence.sandbox_active());
    assert!(!evidence.cleanup_registered());
    assert!(!evidence.load_allowed());
    assert!(!evidence.execution_allowed());
    assert!(!evidence.persistence_allowed());
    assert!(!evidence.transport_allowed());

    Ok(())
}

#[test]
fn platform_contract_is_typed_and_matches_the_build_target() -> TestResult {
    assert_eq!(SandboxPlatform::MacOs.as_str(), "macos");
    assert_eq!(SandboxPlatform::Linux.as_str(), "linux");

    #[cfg(target_os = "macos")]
    assert_eq!(SandboxPlatform::current(), Some(SandboxPlatform::MacOs));
    #[cfg(target_os = "linux")]
    assert_eq!(SandboxPlatform::current(), Some(SandboxPlatform::Linux));
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    assert_eq!(SandboxPlatform::current(), None);

    Ok(())
}

#[test]
fn operator_limits_are_nonzero_and_bounded() -> TestResult {
    for (wall_time, open_files, code) in [
        (0, 1, SandboxConfigurationErrorCode::ZeroWallTimeLimit),
        (
            MAX_SANDBOX_WALL_TIME_MS + 1,
            1,
            SandboxConfigurationErrorCode::WallTimeLimitTooLarge,
        ),
        (1, 0, SandboxConfigurationErrorCode::ZeroOpenFileLimit),
        (
            1,
            MAX_SANDBOX_OPEN_FILES + 1,
            SandboxConfigurationErrorCode::OpenFileLimitTooLarge,
        ),
    ] {
        let error = SandboxEnforcementPolicy::new(wall_time, open_files)
            .err()
            .ok_or("invalid sandbox policy unexpectedly accepted")?;
        assert_eq!(error.code(), code);
    }
    SandboxEnforcementPolicy::new(MAX_SANDBOX_WALL_TIME_MS, MAX_SANDBOX_OPEN_FILES)?;
    Ok(())
}

#[test]
fn exact_authorities_subject_and_evidence_chain_are_required() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let cloned_resolution = resolved.clone();
    let subject = fixture.subject(&resolved);
    let cloned_subject = fixture.subject(&cloned_resolution);
    let chain = establish_chain(subject, RuntimeNetworkAvailability::None, 2)?;
    let authority = sandbox_authority(30_000, 128)?;

    assert_sandbox_error(
        authority.establish(
            &chain.compatibility_authority,
            &chain.compatibility_evidence,
            &chain.input_output_authority,
            &chain.input_output_evidence,
            cloned_subject,
        ),
        SandboxEnforcementErrorCode::RuntimeCompatibilityNotVerified,
        SandboxEnforcementRequirement::RuntimeCompatibilityEvidence,
    )?;

    let other_input_output_authority = InputOutputEnforcementAuthority::new_operator_local(
        InputOutputPolicy::new(128, 128, false)?,
    );
    assert_sandbox_error(
        authority.establish(
            &chain.compatibility_authority,
            &chain.compatibility_evidence,
            &other_input_output_authority,
            &chain.input_output_evidence,
            subject,
        ),
        SandboxEnforcementErrorCode::InputOutputEnforcementNotVerified,
        SandboxEnforcementRequirement::InputOutputEnforcementEvidence,
    )?;

    let other_compatibility_authority = compatible_authority(RuntimeNetworkAvailability::None, 2)?;
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let other_compatibility_evidence =
        other_compatibility_authority.evaluate(&review_authority, &review_evidence, subject)?;
    assert_sandbox_error(
        authority.establish(
            &other_compatibility_authority,
            &other_compatibility_evidence,
            &chain.input_output_authority,
            &chain.input_output_evidence,
            subject,
        ),
        SandboxEnforcementErrorCode::EvidenceChainMismatch,
        SandboxEnforcementRequirement::EvidenceChain,
    )?;

    let evidence = authority.establish(
        &chain.compatibility_authority,
        &chain.compatibility_evidence,
        &chain.input_output_authority,
        &chain.input_output_evidence,
        subject,
    )?;
    let other_sandbox_authority = sandbox_authority(30_000, 128)?;
    assert!(!other_sandbox_authority.verifies(&evidence, subject));

    Ok(())
}

#[test]
fn unsafe_manifest_security_claims_fail_closed_independently() -> TestResult {
    let valid = parse_and_validate_yaml(VALID_MANIFEST)?;
    let mut candidates = Vec::new();

    let mut credentials = valid.clone();
    credentials.security.collects_credentials = true;
    candidates.push((
        credentials,
        SandboxEnforcementErrorCode::PrivateDataRequested,
        SandboxEnforcementRequirement::SecurityPolicy,
    ));
    let mut host_identity = valid.clone();
    host_identity.security.collects_host_identifiers = true;
    candidates.push((
        host_identity,
        SandboxEnforcementErrorCode::PrivateDataRequested,
        SandboxEnforcementRequirement::SecurityPolicy,
    ));
    let mut destructive = valid.clone();
    destructive.security.allows_destructive_actions = true;
    candidates.push((
        destructive,
        SandboxEnforcementErrorCode::UnsafeSecurityPolicy,
        SandboxEnforcementRequirement::FilesystemIsolation,
    ));
    let mut shell = valid.clone();
    shell.security.allows_arbitrary_shell = true;
    candidates.push((
        shell,
        SandboxEnforcementErrorCode::UnsafeSecurityPolicy,
        SandboxEnforcementRequirement::FilesystemIsolation,
    ));
    let mut filesystem = valid.clone();
    filesystem.security.allows_unrestricted_filesystem = true;
    candidates.push((
        filesystem,
        SandboxEnforcementErrorCode::UnsafeSecurityPolicy,
        SandboxEnforcementRequirement::FilesystemIsolation,
    ));
    let mut network = valid;
    network.security.requires_network = true;
    candidates.push((
        network,
        SandboxEnforcementErrorCode::NetworkAccessUnsupported,
        SandboxEnforcementRequirement::NetworkIsolation,
    ));

    for (manifest, code, requirement) in candidates {
        let fixture = PackageFixture::new(manifest, VALID_RESOURCES.as_bytes())?;
        let resolved = fixture.resolve()?;
        let subject = fixture.subject(&resolved);
        let chain = establish_chain(subject, RuntimeNetworkAvailability::None, 2)?;
        let authority = sandbox_authority(30_000, 128)?;
        assert_sandbox_error(
            authority.establish(
                &chain.compatibility_authority,
                &chain.compatibility_evidence,
                &chain.input_output_authority,
                &chain.input_output_evidence,
                subject,
            ),
            code,
            requirement,
        )?;
    }

    Ok(())
}

#[test]
fn unsupported_operating_mode_and_network_requirements_fail_closed() -> TestResult {
    let mut planning_manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    planning_manifest.agent.earliest_mode = ExecutionMode::LocalPlanning;
    let planning_resources = VALID_RESOURCES.replace("local_readonly", "local_planning");
    let planning_fixture = PackageFixture::new(planning_manifest, planning_resources.as_bytes())?;
    assert_fixture_error(
        &planning_fixture,
        RuntimeNetworkAvailability::None,
        SandboxEnforcementErrorCode::OperatingModeUnsupported,
        SandboxEnforcementRequirement::OperatingMode,
    )?;

    let network_resources = VALID_RESOURCES.replacen("mode: none", "mode: local_only", 1);
    let network_fixture = PackageFixture::new(
        parse_and_validate_yaml(VALID_MANIFEST)?,
        network_resources.as_bytes(),
    )?;
    assert_fixture_error(
        &network_fixture,
        RuntimeNetworkAvailability::LocalOnly,
        SandboxEnforcementErrorCode::NetworkAccessUnsupported,
        SandboxEnforcementRequirement::NetworkIsolation,
    )?;

    Ok(())
}

#[test]
fn sandbox_limits_do_not_exceed_the_compatible_resource_envelope() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let chain = establish_chain(subject, RuntimeNetworkAvailability::None, 1)?;
    let authority = sandbox_authority(30_000, 128)?;
    let evidence = authority.establish(
        &chain.compatibility_authority,
        &chain.compatibility_evidence,
        &chain.input_output_authority,
        &chain.input_output_evidence,
        subject,
    )?;

    assert_eq!(evidence.resource_limits().logical_cores(), 1);
    assert_eq!(evidence.resource_limits().max_background_threads(), 1);
    Ok(())
}

#[test]
fn debug_and_errors_do_not_expose_package_scope_or_resource_values() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let chain = establish_chain(subject, RuntimeNetworkAvailability::None, 2)?;
    let policy = SandboxEnforcementPolicy::new(31_337, 257)?;
    let authority = SandboxEnforcementAuthority::new_operator_local(policy)?;
    let evidence = authority.establish(
        &chain.compatibility_authority,
        &chain.compatibility_evidence,
        &chain.input_output_authority,
        &chain.input_output_evidence,
        subject,
    )?;

    let other_authority = sandbox_authority(30_000, 128)?;
    let combined = format!(
        "{policy:?} {authority:?} {evidence:?} {:?} {:?}",
        evidence.resource_limits(),
        evidence.restrictions()
    );
    assert!(combined.contains("[redacted]"));
    for private_value in [
        fixture.manifest.package_id.as_str(),
        "node_readiness_diagnostic_report",
        "31337",
        "257",
        "512",
        "64",
    ] {
        assert!(!combined.contains(private_value));
    }
    assert!(!other_authority.verifies(&evidence, subject));

    Ok(())
}

#[test]
fn static_package_load_blockers_remain_unchanged() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;

    assert_eq!(report.status(), PackageLoadStatus::Blocked);
    assert!(!report.load_allowed());
    for blocker in [
        PackageLoadBlockerCode::InputOutputEnforcementUnavailable,
        PackageLoadBlockerCode::SandboxEnforcementUnavailable,
        PackageLoadBlockerCode::ExecutionAuthorizationUnavailable,
    ] {
        assert!(report.blockers().contains(&blocker));
    }

    Ok(())
}

fn assert_fixture_error(
    fixture: &PackageFixture,
    network: RuntimeNetworkAvailability,
    code: SandboxEnforcementErrorCode,
    requirement: SandboxEnforcementRequirement,
) -> TestResult {
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let chain = establish_chain(subject, network, 2)?;
    let authority = sandbox_authority(30_000, 128)?;
    assert_sandbox_error(
        authority.establish(
            &chain.compatibility_authority,
            &chain.compatibility_evidence,
            &chain.input_output_authority,
            &chain.input_output_evidence,
            subject,
        ),
        code,
        requirement,
    )
}

fn assert_sandbox_error<T>(
    result: Result<T, SandboxEnforcementError>,
    code: SandboxEnforcementErrorCode,
    requirement: SandboxEnforcementRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("unsafe sandbox plan unexpectedly succeeded")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    Ok(())
}

fn current_platform() -> Result<SandboxPlatform, Box<dyn Error>> {
    SandboxPlatform::current().ok_or_else(|| "unsupported test platform".into())
}

fn sandbox_authority(
    wall_time_ms: u64,
    open_files: u32,
) -> Result<SandboxEnforcementAuthority, Box<dyn Error>> {
    Ok(SandboxEnforcementAuthority::new_operator_local(
        SandboxEnforcementPolicy::new(wall_time_ms, open_files)?,
    )?)
}

struct EnforcementChain<'a> {
    compatibility_authority: RuntimeCompatibilityAuthority,
    compatibility_evidence: RuntimeCompatibilityEvidence<'a>,
    input_output_authority: InputOutputEnforcementAuthority,
    input_output_evidence: InputOutputEnforcementEvidence<'a>,
}

fn establish_chain<'a>(
    subject: PackageReviewSubject<'a>,
    network: RuntimeNetworkAvailability,
    logical_cores: u16,
) -> Result<EnforcementChain<'a>, Box<dyn Error>> {
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let compatibility_authority = compatible_authority(network, logical_cores)?;
    let compatibility_evidence =
        compatibility_authority.evaluate(&review_authority, &review_evidence, subject)?;
    let input_output_authority = InputOutputEnforcementAuthority::new_operator_local(
        InputOutputPolicy::new(128, 128, false)?,
    );
    let input_output_evidence = input_output_authority.establish(
        &compatibility_authority,
        &compatibility_evidence,
        subject,
    )?;
    Ok(EnforcementChain {
        compatibility_authority,
        compatibility_evidence,
        input_output_authority,
        input_output_evidence,
    })
}

fn compatible_authority(
    network: RuntimeNetworkAvailability,
    logical_cores: u16,
) -> Result<RuntimeCompatibilityAuthority, Box<dyn Error>> {
    Ok(RuntimeCompatibilityAuthority::new_operator_local(
        RuntimeLanguageDecision::new(
            RuntimeLanguageMode::RustNativeOfficial,
            RuntimeLanguageAvailability::Available,
        ),
        RuntimeResourceEnvelope::new(logical_cores, 512, 84, network)?,
    ))
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
    fn valid() -> Result<Self, Box<dyn Error>> {
        Self::new(
            parse_and_validate_yaml(VALID_MANIFEST)?,
            VALID_RESOURCES.as_bytes(),
        )
    }

    fn new(
        manifest: AgentPackageManifest,
        resource_content: &[u8],
    ) -> Result<Self, Box<dyn Error>> {
        let root = tempfile::tempdir()?;
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
        (&references.scope_manifest, VALID_SCOPE.as_bytes()),
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
