use std::{fmt, sync::Arc};

use crate::PackageReviewSubject;

use super::{InputOutputRecordContext, InputOutputRequirement};

#[derive(Debug)]
pub(crate) struct InputOutputAuthorityIdentity;

#[derive(Debug)]
pub(crate) struct InputOutputEvidenceIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputOutputEnforcementEvidenceStatus {
    Established,
}

const ESTABLISHED_REQUIREMENTS: [InputOutputRequirement; 3] = [
    InputOutputRequirement::RuntimeCompatibilityEvidence,
    InputOutputRequirement::ScopeMetadata,
    InputOutputRequirement::RecordLimit,
];

#[must_use]
pub struct InputOutputEnforcementEvidence<'a> {
    authority: Arc<InputOutputAuthorityIdentity>,
    identity: Arc<InputOutputEvidenceIdentity>,
    subject: PackageReviewSubject<'a>,
    context: InputOutputRecordContext,
}

impl<'a> InputOutputEnforcementEvidence<'a> {
    pub(crate) fn new(
        authority: Arc<InputOutputAuthorityIdentity>,
        subject: PackageReviewSubject<'a>,
        context: InputOutputRecordContext,
    ) -> Self {
        Self {
            authority,
            identity: Arc::new(InputOutputEvidenceIdentity),
            subject,
            context,
        }
    }

    pub const fn status(&self) -> InputOutputEnforcementEvidenceStatus {
        InputOutputEnforcementEvidenceStatus::Established
    }

    pub const fn requirements(&self) -> &'static [InputOutputRequirement] {
        &ESTABLISHED_REQUIREMENTS
    }

    pub const fn load_allowed(&self) -> bool {
        false
    }

    pub const fn execution_allowed(&self) -> bool {
        false
    }

    pub const fn persistence_allowed(&self) -> bool {
        false
    }

    pub const fn transport_allowed(&self) -> bool {
        false
    }

    pub const fn handoff_allowed(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<InputOutputAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn identity(&self) -> &Arc<InputOutputEvidenceIdentity> {
        &self.identity
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'a> {
        self.subject
    }

    pub(crate) fn context(&self) -> &InputOutputRecordContext {
        &self.context
    }
}

impl fmt::Debug for InputOutputEnforcementEvidence<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InputOutputEnforcementEvidence")
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("context", &"[redacted]")
            .field("load_allowed", &false)
            .field("execution_allowed", &false)
            .field("persistence_allowed", &false)
            .field("transport_allowed", &false)
            .field("handoff_allowed", &false)
            .finish()
    }
}
