use std::{error::Error, fs, path::Path};

use iamine_agent_runtime::{
    DeclaredAgentPackage, DependencyPolicyReviewDecision, HumanReviewDecision,
    InputOutputEnforcementAuthority, InputOutputPolicy, LanguagePolicyReviewDecision,
    LocalRegistryReviewDecision, PackageReferenceResolver, PackageReviewAuthority,
    PackageReviewDecisions, PackageReviewSubject, ResolverLimits, RuntimeCompatibilityAuthority,
    RuntimeLanguageAvailability, RuntimeLanguageDecision, RuntimeLanguageMode,
    RuntimeNetworkAvailability, RuntimeResourceEnvelope, SandboxEnforcementAuthority,
    SandboxEnforcementEvidence, SandboxEnforcementPolicy,
};
use iamine_agents::{parse_and_validate_yaml, AgentPackageManifest, ManifestReferences};
use tempfile::TempDir;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub const VALID_MANIFEST: &str =
    include_str!("../../../iamine-agents/tests/fixtures/valid/node-doctor-agent.yaml");
const VALID_SCOPE: &str =
    include_str!("../../../iamine-agents/tests/fixtures/policy_metadata/valid/scope-policy.yaml");
const VALID_RESOURCES: &str = include_str!(
    "../../../iamine-agents/tests/fixtures/descriptive_metadata/valid/resource-requirements.yaml"
);

pub struct PreparedSandbox<'a> {
    pub authority: SandboxEnforcementAuthority,
    pub evidence: SandboxEnforcementEvidence<'a>,
}

pub fn prepare_sandbox<'a>(subject: PackageReviewSubject<'a>) -> TestResult<PreparedSandbox<'a>> {
    let authority = SandboxEnforcementAuthority::new_operator_local(
        SandboxEnforcementPolicy::new(30_000, 128)?,
    )?;
    let evidence = prepare_sandbox_evidence(&authority, subject)?;

    Ok(PreparedSandbox {
        authority,
        evidence,
    })
}

pub fn prepare_sandbox_evidence<'a>(
    authority: &SandboxEnforcementAuthority,
    subject: PackageReviewSubject<'a>,
) -> TestResult<SandboxEnforcementEvidence<'a>> {
    let review_authority = PackageReviewAuthority::new_operator_local();
    let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
    let compatibility_authority = RuntimeCompatibilityAuthority::new_operator_local(
        RuntimeLanguageDecision::new(
            RuntimeLanguageMode::RustNativeOfficial,
            RuntimeLanguageAvailability::Available,
        ),
        RuntimeResourceEnvelope::new(2, 512, 84, RuntimeNetworkAvailability::None)?,
    );
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
    let evidence = authority.establish(
        &compatibility_authority,
        &compatibility_evidence,
        &input_output_authority,
        &input_output_evidence,
        subject,
    )?;

    Ok(evidence)
}

pub struct PackageFixture {
    root: TempDir,
    manifest: AgentPackageManifest,
}

impl PackageFixture {
    pub fn valid() -> TestResult<Self> {
        let root = tempfile::tempdir()?;
        let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
        create_references(root.path(), &manifest.references)?;
        Ok(Self { root, manifest })
    }

    pub fn resolve(&self) -> TestResult<iamine_agent_runtime::ResolvedPackageReferences> {
        let resolver =
            PackageReferenceResolver::open_ambient(self.root.path(), ResolverLimits::default())?;
        Ok(resolver.resolve(&self.manifest.references)?)
    }

    pub fn subject<'a>(
        &'a self,
        references: &'a iamine_agent_runtime::ResolvedPackageReferences,
    ) -> PackageReviewSubject<'a> {
        PackageReviewSubject::new(
            DeclaredAgentPackage::from_manifest(&self.manifest),
            references,
        )
    }

    pub fn package_id(&self) -> &str {
        &self.manifest.package_id
    }
}

fn approved_review_decisions() -> PackageReviewDecisions {
    PackageReviewDecisions::new(
        LocalRegistryReviewDecision::RegistryReviewReady,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::IndependentApproved,
    )
}

fn create_references(root: &Path, references: &ManifestReferences) -> TestResult {
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
        (
            &references.resource_requirements,
            VALID_RESOURCES.as_bytes(),
        ),
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
