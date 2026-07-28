use std::error::Error;

use iamine_agent_runtime::{
    DependencyPolicyReviewDecision, HumanReviewDecision, InputOutputEnforcementAuthority,
    InputOutputPolicy, LanguagePolicyReviewDecision, LocalRegistryReviewDecision,
    PackageReviewAuthority, PackageReviewDecisions, PackageReviewSubject,
    RuntimeCompatibilityAuthority, RuntimeCompatibilityEvidence, RuntimeLanguageAvailability,
    RuntimeLanguageDecision, RuntimeLanguageMode, RuntimeNetworkAvailability,
    RuntimeResourceEnvelope, SandboxEnforcementAuthority, SandboxEnforcementEvidence,
    SandboxEnforcementPolicy,
};

type TestResult<T> = Result<T, Box<dyn Error>>;

pub struct PreparedRoutingCandidate<'a> {
    pub compatibility_authority: RuntimeCompatibilityAuthority,
    pub compatibility_evidence: RuntimeCompatibilityEvidence<'a>,
    pub sandbox_authority: SandboxEnforcementAuthority,
    pub sandbox_evidence: SandboxEnforcementEvidence<'a>,
}

pub fn prepare_routing_candidate<'a>(
    subject: PackageReviewSubject<'a>,
) -> TestResult<PreparedRoutingCandidate<'a>> {
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
    let sandbox_authority = SandboxEnforcementAuthority::new_operator_local(
        SandboxEnforcementPolicy::new(30_000, 128)?,
    )?;
    let sandbox_evidence = sandbox_authority.establish(
        &compatibility_authority,
        &compatibility_evidence,
        &input_output_authority,
        &input_output_evidence,
        subject,
    )?;

    Ok(PreparedRoutingCandidate {
        compatibility_authority,
        compatibility_evidence,
        sandbox_authority,
        sandbox_evidence,
    })
}

fn approved_review_decisions() -> PackageReviewDecisions {
    PackageReviewDecisions::new(
        LocalRegistryReviewDecision::RegistryReviewReady,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::IndependentApproved,
    )
}
