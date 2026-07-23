use std::{collections::HashSet, error::Error, fs, path::Path};

use iamine_agent_runtime::{
    DeclaredAgentPackage, DependencyPolicyReviewDecision, HumanReviewDecision,
    LanguagePolicyReviewDecision, LocalRegistryReviewDecision, PackageReferenceResolver,
    PackageReviewAuthority, PackageReviewDecisions, PackageReviewEvidenceStatus,
    PackageReviewRequirement, PackageReviewSubject, ResolverLimits, ReviewEvidenceErrorCode,
};
use iamine_agents::{
    assess_package_load_yaml, parse_and_validate_yaml, AgentPackageManifest, ManifestReferences,
    PackageLoadBlockerCode, PackageLoadStatus,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

const VALID_MANIFEST: &str =
    include_str!("../../iamine-agents/tests/fixtures/valid/node-doctor-agent.yaml");

#[test]
fn explicit_operator_decisions_establish_bounded_evidence() -> TestResult {
    let fixture = PackageFixture::new(b"validated metadata")?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let authority = PackageReviewAuthority::new_operator_local();
    let evidence = authority.issue(subject, approved_decisions())?;

    assert_eq!(evidence.status(), PackageReviewEvidenceStatus::Established);
    assert_eq!(evidence.requirements().len(), 4);
    assert_eq!(
        evidence
            .requirements()
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len(),
        4
    );
    assert!(authority.verifies(&evidence, subject));
    assert!(!evidence.load_allowed());
    assert!(!evidence.execution_allowed());

    Ok(())
}

#[test]
fn every_non_approved_decision_fails_closed_at_its_owner() -> TestResult {
    let fixture = PackageFixture::new(b"validated metadata")?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let authority = PackageReviewAuthority::new_operator_local();

    for registry in [
        LocalRegistryReviewDecision::Candidate,
        LocalRegistryReviewDecision::UnderReview,
        LocalRegistryReviewDecision::Blocked,
        LocalRegistryReviewDecision::Deprecated,
    ] {
        assert_rejected(
            &authority,
            subject,
            PackageReviewDecisions::new(
                registry,
                LanguagePolicyReviewDecision::RustOfficialAllowed,
                DependencyPolicyReviewDecision::Allowed,
                HumanReviewDecision::IndependentApproved,
            ),
            ReviewEvidenceErrorCode::RegistryNotReady,
            PackageReviewRequirement::LocalRegistry,
        )?;
    }
    for language in [
        LanguagePolicyReviewDecision::Experimental,
        LanguagePolicyReviewDecision::Deferred,
        LanguagePolicyReviewDecision::Blocked,
    ] {
        assert_rejected(
            &authority,
            subject,
            PackageReviewDecisions::new(
                LocalRegistryReviewDecision::RegistryReviewReady,
                language,
                DependencyPolicyReviewDecision::Allowed,
                HumanReviewDecision::IndependentApproved,
            ),
            ReviewEvidenceErrorCode::LanguageNotAllowed,
            PackageReviewRequirement::LanguagePolicy,
        )?;
    }
    for dependencies in [
        DependencyPolicyReviewDecision::NeedsJustification,
        DependencyPolicyReviewDecision::Deferred,
        DependencyPolicyReviewDecision::Blocked,
    ] {
        assert_rejected(
            &authority,
            subject,
            PackageReviewDecisions::new(
                LocalRegistryReviewDecision::RegistryReviewReady,
                LanguagePolicyReviewDecision::RustOfficialAllowed,
                dependencies,
                HumanReviewDecision::IndependentApproved,
            ),
            ReviewEvidenceErrorCode::DependenciesNotApproved,
            PackageReviewRequirement::DependencyPolicy,
        )?;
    }
    for human in [
        HumanReviewDecision::Missing,
        HumanReviewDecision::SelfApproved,
        HumanReviewDecision::Rejected,
    ] {
        assert_rejected(
            &authority,
            subject,
            PackageReviewDecisions::new(
                LocalRegistryReviewDecision::RegistryReviewReady,
                LanguagePolicyReviewDecision::RustOfficialAllowed,
                DependencyPolicyReviewDecision::Allowed,
                human,
            ),
            ReviewEvidenceErrorCode::HumanReviewNotApproved,
            PackageReviewRequirement::IndependentHumanReview,
        )?;
    }

    Ok(())
}

#[test]
fn evidence_is_bound_to_the_exact_authority_manifest_and_resolution() -> TestResult {
    let fixture = PackageFixture::new(b"validated metadata")?;
    let resolved = fixture.resolve()?;
    let cloned_resolution = resolved.clone();
    let subject = fixture.subject(&resolved);
    let cloned_subject = fixture.subject(&cloned_resolution);
    let second_manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let second_manifest_subject = PackageReviewSubject::new(
        DeclaredAgentPackage::from_manifest(&second_manifest),
        &resolved,
    );
    let authority = PackageReviewAuthority::new_operator_local();
    let other_authority = PackageReviewAuthority::new_operator_local();
    let evidence = authority.issue(subject, approved_decisions())?;

    assert!(authority.verifies(&evidence, subject));
    assert!(!authority.verifies(&evidence, cloned_subject));
    assert!(!authority.verifies(&evidence, second_manifest_subject));
    assert!(!other_authority.verifies(&evidence, subject));

    Ok(())
}

#[test]
fn package_controlled_review_claims_cannot_replace_operator_decisions() -> TestResult {
    let fixture = PackageFixture::new(
        b"registry_review_ready rust_official_allowed dependencies_allowed human_approved",
    )?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let authority = PackageReviewAuthority::new_operator_local();
    let package_claims_with_rejected_review = PackageReviewDecisions::new(
        LocalRegistryReviewDecision::UnderReview,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::Missing,
    );
    let error = authority
        .issue(subject, package_claims_with_rejected_review)
        .err()
        .ok_or("package-controlled claims unexpectedly established evidence")?;

    assert_eq!(error.code(), ReviewEvidenceErrorCode::RegistryNotReady);
    assert!(!error.to_string().contains("registry_review_ready"));

    Ok(())
}

#[test]
fn debug_and_errors_do_not_expose_package_or_review_values() -> TestResult {
    let private_marker = "private-review-marker";
    let fixture = PackageFixture::new(private_marker.as_bytes())?;
    let resolved = fixture.resolve()?;
    let subject = fixture.subject(&resolved);
    let authority = PackageReviewAuthority::new_operator_local();
    let evidence = authority.issue(subject, approved_decisions())?;
    let error = authority
        .issue(
            subject,
            PackageReviewDecisions::new(
                LocalRegistryReviewDecision::Blocked,
                LanguagePolicyReviewDecision::Blocked,
                DependencyPolicyReviewDecision::Blocked,
                HumanReviewDecision::Rejected,
            ),
        )
        .err()
        .ok_or("blocked review unexpectedly established evidence")?;
    let combined = format!("{subject:?} {authority:?} {evidence:?} {error:?} {error}");

    assert!(combined.contains("[redacted]"));
    assert!(!combined.contains(&fixture.manifest.package_id));
    assert!(!combined.contains(private_marker));

    Ok(())
}

#[test]
fn package_load_gate_remains_blocked_and_unintegrated() -> TestResult {
    let report = assess_package_load_yaml(VALID_MANIFEST)?;

    assert_eq!(report.status(), PackageLoadStatus::Blocked);
    assert!(!report.load_allowed());
    for blocker in [
        PackageLoadBlockerCode::LocalRegistryReviewUnavailable,
        PackageLoadBlockerCode::LanguagePolicyReviewUnavailable,
        PackageLoadBlockerCode::DependencyPolicyReviewUnavailable,
        PackageLoadBlockerCode::HumanReviewEvidenceUnavailable,
    ] {
        assert!(report.blockers().contains(&blocker));
    }

    Ok(())
}

fn approved_decisions() -> PackageReviewDecisions {
    PackageReviewDecisions::new(
        LocalRegistryReviewDecision::RegistryReviewReady,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::IndependentApproved,
    )
}

fn assert_rejected(
    authority: &PackageReviewAuthority,
    subject: PackageReviewSubject<'_>,
    decisions: PackageReviewDecisions,
    expected_code: ReviewEvidenceErrorCode,
    expected_requirement: PackageReviewRequirement,
) -> TestResult {
    let error = authority
        .issue(subject, decisions)
        .err()
        .ok_or("non-approved decisions unexpectedly established evidence")?;
    assert_eq!(error.code(), expected_code);
    assert_eq!(error.requirement(), expected_requirement);
    Ok(())
}

struct PackageFixture {
    root: TempDir,
    manifest: AgentPackageManifest,
}

impl PackageFixture {
    fn new(content: &[u8]) -> Result<Self, Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
        create_references(root.path(), &manifest.references, content)?;
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

fn create_references(root: &Path, references: &ManifestReferences, content: &[u8]) -> TestResult {
    for declared in [
        &references.scope_manifest,
        &references.capability_metadata,
        &references.expertise_metadata,
        &references.resource_requirements,
        &references.permission_model,
        &references.audit_policy,
        &references.boundary_tests,
    ] {
        let path = root.join(declared);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
    }
    Ok(())
}
