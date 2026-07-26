use std::{fmt, sync::Arc};

use super::{InputOutputEvidenceIdentity, RedactionState};

#[must_use]
pub struct OperatorRedactedInput<'a> {
    evidence: Arc<InputOutputEvidenceIdentity>,
    content: &'a str,
}

impl<'a> OperatorRedactedInput<'a> {
    pub(crate) fn new(evidence: Arc<InputOutputEvidenceIdentity>, content: &'a str) -> Self {
        Self { evidence, content }
    }

    pub const fn redaction_state(&self) -> RedactionState {
        RedactionState::OperatorAttested
    }

    pub(crate) const fn evidence(&self) -> &Arc<InputOutputEvidenceIdentity> {
        &self.evidence
    }

    pub(crate) const fn content(&self) -> &str {
        self.content
    }
}

impl fmt::Debug for OperatorRedactedInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperatorRedactedInput")
            .field("evidence", &"[redacted]")
            .field("redaction_state", &self.redaction_state())
            .field("content_bytes", &self.content.len())
            .field("content", &"[redacted]")
            .finish()
    }
}

#[must_use]
pub struct OperatorRedactedOutput<'a> {
    evidence: Arc<InputOutputEvidenceIdentity>,
    content: &'a str,
}

impl<'a> OperatorRedactedOutput<'a> {
    pub(crate) fn new(evidence: Arc<InputOutputEvidenceIdentity>, content: &'a str) -> Self {
        Self { evidence, content }
    }

    pub const fn redaction_state(&self) -> RedactionState {
        RedactionState::OperatorAttested
    }

    pub(crate) const fn evidence(&self) -> &Arc<InputOutputEvidenceIdentity> {
        &self.evidence
    }

    pub(crate) const fn content(&self) -> &str {
        self.content
    }
}

impl fmt::Debug for OperatorRedactedOutput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperatorRedactedOutput")
            .field("evidence", &"[redacted]")
            .field("redaction_state", &self.redaction_state())
            .field("content_bytes", &self.content.len())
            .field("content", &"[redacted]")
            .finish()
    }
}
