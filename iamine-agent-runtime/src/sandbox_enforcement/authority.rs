use std::{fmt, sync::Arc};

use crate::{
    InputOutputEnforcementAuthority, InputOutputEnforcementEvidence, PackageReviewSubject,
    RuntimeCompatibilityAuthority, RuntimeCompatibilityEvidence,
};

use super::evaluation::evaluate_subject;
use super::{
    SandboxAuthorityIdentity, SandboxConfigurationError, SandboxConfigurationErrorCode,
    SandboxEnforcementError, SandboxEnforcementErrorCode, SandboxEnforcementEvidence,
    SandboxEnforcementPolicy, SandboxEnforcementRequirement, SandboxPlatform,
};

/// Operator-local capability that prepares a fail-closed sandbox plan.
///
/// The authority does not start a sandbox or authorize package execution.
pub struct SandboxEnforcementAuthority {
    identity: Arc<SandboxAuthorityIdentity>,
    platform: SandboxPlatform,
    policy: SandboxEnforcementPolicy,
}

impl SandboxEnforcementAuthority {
    pub fn new_operator_local(
        policy: SandboxEnforcementPolicy,
    ) -> Result<Self, SandboxConfigurationError> {
        let platform = SandboxPlatform::current().ok_or_else(|| {
            SandboxConfigurationError::new(SandboxConfigurationErrorCode::UnsupportedPlatform)
        })?;
        Ok(Self {
            identity: Arc::new(SandboxAuthorityIdentity),
            platform,
            policy,
        })
    }

    pub fn establish<'a>(
        &self,
        compatibility_authority: &RuntimeCompatibilityAuthority,
        compatibility_evidence: &RuntimeCompatibilityEvidence<'a>,
        input_output_authority: &InputOutputEnforcementAuthority,
        input_output_evidence: &InputOutputEnforcementEvidence<'a>,
        subject: PackageReviewSubject<'a>,
    ) -> Result<SandboxEnforcementEvidence<'a>, SandboxEnforcementError> {
        if !compatibility_authority.verifies(compatibility_evidence, subject) {
            return Err(SandboxEnforcementError::new(
                SandboxEnforcementErrorCode::RuntimeCompatibilityNotVerified,
                SandboxEnforcementRequirement::RuntimeCompatibilityEvidence,
            ));
        }
        if !input_output_authority.verifies(input_output_evidence, subject) {
            return Err(SandboxEnforcementError::new(
                SandboxEnforcementErrorCode::InputOutputEnforcementNotVerified,
                SandboxEnforcementRequirement::InputOutputEnforcementEvidence,
            ));
        }
        if !input_output_evidence.bound_to_compatibility(compatibility_evidence) {
            return Err(SandboxEnforcementError::new(
                SandboxEnforcementErrorCode::EvidenceChainMismatch,
                SandboxEnforcementRequirement::EvidenceChain,
            ));
        }

        let result = evaluate_subject(subject, compatibility_evidence, self.policy)?;
        Ok(SandboxEnforcementEvidence::new(
            Arc::clone(&self.identity),
            subject,
            self.platform,
            result.limits,
            result.restrictions,
        ))
    }

    pub fn verifies(
        &self,
        evidence: &SandboxEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority()) && evidence.subject().same_as(subject)
    }
}

impl fmt::Debug for SandboxEnforcementAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SandboxEnforcementAuthority")
            .field("identity", &"[redacted]")
            .field("platform", &self.platform.as_str())
            .field("policy", &"[redacted]")
            .finish()
    }
}
