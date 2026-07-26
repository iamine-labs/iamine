use std::{fmt, sync::Arc};

use crate::{PackageReviewSubject, RuntimeCompatibilityAuthority, RuntimeCompatibilityEvidence};

use super::evaluation::{derive_record_context, validate_content};
use super::{
    EnforcedInputRecord, EnforcedOutputRecord, InputClassification, InputOutputAuthorityIdentity,
    InputOutputEnforcementError, InputOutputEnforcementErrorCode, InputOutputEnforcementEvidence,
    InputOutputPolicy, InputOutputRequirement, OperatorRedactedInput, OperatorRedactedOutput,
    OutputClassification,
};

/// Operator-local capability for bounded input/output enforcement.
///
/// Package-controlled code must never receive or construct this authority.
pub struct InputOutputEnforcementAuthority {
    identity: Arc<InputOutputAuthorityIdentity>,
    policy: InputOutputPolicy,
}

impl InputOutputEnforcementAuthority {
    pub fn new_operator_local(policy: InputOutputPolicy) -> Self {
        Self {
            identity: Arc::new(InputOutputAuthorityIdentity),
            policy,
        }
    }

    pub fn establish<'a>(
        &self,
        compatibility_authority: &RuntimeCompatibilityAuthority,
        compatibility_evidence: &RuntimeCompatibilityEvidence<'a>,
        subject: PackageReviewSubject<'a>,
    ) -> Result<InputOutputEnforcementEvidence<'a>, InputOutputEnforcementError> {
        if !compatibility_authority.verifies(compatibility_evidence, subject) {
            return Err(InputOutputEnforcementError::new(
                InputOutputEnforcementErrorCode::RuntimeCompatibilityNotVerified,
                InputOutputRequirement::RuntimeCompatibilityEvidence,
            ));
        }
        let context = derive_record_context(subject)?;
        Ok(InputOutputEnforcementEvidence::new(
            Arc::clone(&self.identity),
            subject,
            context,
        ))
    }

    pub fn verifies(
        &self,
        evidence: &InputOutputEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, evidence.authority()) && evidence.subject().same_as(subject)
    }

    pub fn attest_redacted_input<'a>(
        &self,
        evidence: &InputOutputEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
        content: &'a str,
    ) -> Result<OperatorRedactedInput<'a>, InputOutputEnforcementError> {
        self.require_evidence(evidence, subject)?;
        validate_content(
            content,
            self.policy.max_input_bytes(),
            InputOutputEnforcementErrorCode::InputTooLarge,
        )?;
        Ok(OperatorRedactedInput::new(
            Arc::clone(evidence.identity()),
            content,
        ))
    }

    pub fn attest_redacted_output<'a>(
        &self,
        evidence: &InputOutputEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
        content: &'a str,
    ) -> Result<OperatorRedactedOutput<'a>, InputOutputEnforcementError> {
        self.require_evidence(evidence, subject)?;
        validate_content(
            content,
            self.policy.max_output_bytes(),
            InputOutputEnforcementErrorCode::OutputTooLarge,
        )?;
        Ok(OperatorRedactedOutput::new(
            Arc::clone(evidence.identity()),
            content,
        ))
    }

    pub fn enforce_input(
        &self,
        evidence: &InputOutputEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
        classification: InputClassification,
        content: OperatorRedactedInput<'_>,
    ) -> Result<EnforcedInputRecord, InputOutputEnforcementError> {
        self.require_evidence(evidence, subject)?;
        require_redaction_attestation(evidence, content.evidence())?;
        Ok(EnforcedInputRecord::new(
            evidence.context().clone(),
            classification,
            content.content(),
        ))
    }

    pub fn enforce_output(
        &self,
        evidence: &InputOutputEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
        classification: OutputClassification,
        content: OperatorRedactedOutput<'_>,
    ) -> Result<EnforcedOutputRecord, InputOutputEnforcementError> {
        self.require_evidence(evidence, subject)?;
        require_redaction_attestation(evidence, content.evidence())?;
        Ok(EnforcedOutputRecord::new(
            evidence.context().clone(),
            classification,
            content.content(),
            self.policy.operator_visible_outputs(),
        ))
    }

    fn require_evidence(
        &self,
        evidence: &InputOutputEnforcementEvidence<'_>,
        subject: PackageReviewSubject<'_>,
    ) -> Result<(), InputOutputEnforcementError> {
        if self.verifies(evidence, subject) {
            return Ok(());
        }
        Err(InputOutputEnforcementError::new(
            InputOutputEnforcementErrorCode::EnforcementEvidenceNotVerified,
            InputOutputRequirement::EnforcementEvidence,
        ))
    }
}

impl fmt::Debug for InputOutputEnforcementAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InputOutputEnforcementAuthority")
            .field("identity", &"[redacted]")
            .field("policy", &"[redacted]")
            .finish()
    }
}

fn require_redaction_attestation(
    evidence: &InputOutputEnforcementEvidence<'_>,
    attested_evidence: &Arc<super::InputOutputEvidenceIdentity>,
) -> Result<(), InputOutputEnforcementError> {
    if Arc::ptr_eq(evidence.identity(), attested_evidence) {
        return Ok(());
    }
    Err(InputOutputEnforcementError::new(
        InputOutputEnforcementErrorCode::RedactionAttestationNotVerified,
        InputOutputRequirement::RedactionAttestation,
    ))
}
