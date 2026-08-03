use std::{fmt, sync::Arc};

use super::InputOutputEvidenceIdentity;

pub const INPUT_OUTPUT_ENFORCEMENT_SCHEMA_VERSION: &str = "iamine.agent.input_output.enforced-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InputClassification {
    TaskDescriptor,
    OperatorIntent,
    DeclaredScope,
    PermissionGrantReference,
    ResourceHint,
    RiskHint,
    ContextPointer,
}

impl InputClassification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskDescriptor => "task_descriptor",
            Self::OperatorIntent => "operator_intent",
            Self::DeclaredScope => "declared_scope",
            Self::PermissionGrantReference => "permission_grant_reference",
            Self::ResourceHint => "resource_hint",
            Self::RiskHint => "risk_hint",
            Self::ContextPointer => "context_pointer",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutputClassification {
    ResultSummary,
    ActionReport,
    DiagnosticReport,
    BlockedActionReport,
    ClarificationRequest,
    HandoffRequest,
    RefusalReport,
    ErrorReport,
}

impl OutputClassification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResultSummary => "result_summary",
            Self::ActionReport => "action_report",
            Self::DiagnosticReport => "diagnostic_report",
            Self::BlockedActionReport => "blocked_action_report",
            Self::ClarificationRequest => "clarification_request",
            Self::HandoffRequest => "handoff_request",
            Self::RefusalReport => "refusal_report",
            Self::ErrorReport => "error_report",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedactionState {
    OperatorAttested,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct InputOutputRecordContext {
    agent_id: Box<str>,
    task_type: Box<str>,
    scope_id: Box<str>,
}

impl InputOutputRecordContext {
    pub(crate) fn new(agent_id: &str, task_type: &str, scope_id: &str) -> Self {
        Self {
            agent_id: agent_id.into(),
            task_type: task_type.into(),
            scope_id: scope_id.into(),
        }
    }
}

pub struct EnforcedInputRecord {
    evidence: Arc<InputOutputEvidenceIdentity>,
    context: InputOutputRecordContext,
    classification: InputClassification,
    redacted_content: Box<str>,
}

impl EnforcedInputRecord {
    pub(crate) fn new(
        evidence: Arc<InputOutputEvidenceIdentity>,
        context: InputOutputRecordContext,
        classification: InputClassification,
        redacted_content: &str,
    ) -> Self {
        Self {
            evidence,
            context,
            classification,
            redacted_content: redacted_content.into(),
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        INPUT_OUTPUT_ENFORCEMENT_SCHEMA_VERSION
    }

    pub fn agent_id(&self) -> &str {
        &self.context.agent_id
    }

    pub fn task_type(&self) -> &str {
        &self.context.task_type
    }

    pub fn scope_id(&self) -> &str {
        &self.context.scope_id
    }

    pub const fn classification(&self) -> InputClassification {
        self.classification
    }

    pub const fn redaction_state(&self) -> RedactionState {
        RedactionState::OperatorAttested
    }

    pub fn redacted_content(&self) -> &str {
        &self.redacted_content
    }

    pub const fn handoff_allowed(&self) -> bool {
        false
    }

    pub const fn operator_visible(&self) -> bool {
        false
    }

    pub const fn persistence_allowed(&self) -> bool {
        false
    }

    pub const fn transport_allowed(&self) -> bool {
        false
    }

    pub(crate) const fn evidence(&self) -> &Arc<InputOutputEvidenceIdentity> {
        &self.evidence
    }

    pub(crate) fn matches_subject(&self, agent_id: &str, task_type: &str) -> bool {
        self.context.agent_id.as_ref() == agent_id && self.context.task_type.as_ref() == task_type
    }
}

impl fmt::Debug for EnforcedInputRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnforcedInputRecord")
            .field("schema_version", &self.schema_version())
            .field("agent_id", &"[redacted]")
            .field("task_type", &"[redacted]")
            .field("scope_id", &"[redacted]")
            .field("classification", &self.classification)
            .field("redaction_state", &self.redaction_state())
            .field("content_bytes", &self.redacted_content.len())
            .field("redacted_content", &"[redacted]")
            .field("handoff_allowed", &false)
            .field("operator_visible", &false)
            .field("persistence_allowed", &false)
            .field("transport_allowed", &false)
            .finish()
    }
}

pub struct EnforcedOutputRecord {
    context: InputOutputRecordContext,
    classification: OutputClassification,
    redacted_content: Box<str>,
    operator_visible: bool,
}

impl EnforcedOutputRecord {
    pub(crate) fn new(
        context: InputOutputRecordContext,
        classification: OutputClassification,
        redacted_content: &str,
        operator_visible: bool,
    ) -> Self {
        Self {
            context,
            classification,
            redacted_content: redacted_content.into(),
            operator_visible,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        INPUT_OUTPUT_ENFORCEMENT_SCHEMA_VERSION
    }

    pub fn agent_id(&self) -> &str {
        &self.context.agent_id
    }

    pub fn task_type(&self) -> &str {
        &self.context.task_type
    }

    pub fn scope_id(&self) -> &str {
        &self.context.scope_id
    }

    pub const fn classification(&self) -> OutputClassification {
        self.classification
    }

    pub const fn redaction_state(&self) -> RedactionState {
        RedactionState::OperatorAttested
    }

    pub fn redacted_content(&self) -> &str {
        &self.redacted_content
    }

    pub const fn handoff_allowed(&self) -> bool {
        false
    }

    pub const fn operator_visible(&self) -> bool {
        self.operator_visible
    }

    pub const fn execution_success(&self) -> bool {
        false
    }

    pub const fn persistence_allowed(&self) -> bool {
        false
    }

    pub const fn transport_allowed(&self) -> bool {
        false
    }
}

impl fmt::Debug for EnforcedOutputRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnforcedOutputRecord")
            .field("schema_version", &self.schema_version())
            .field("agent_id", &"[redacted]")
            .field("task_type", &"[redacted]")
            .field("scope_id", &"[redacted]")
            .field("classification", &self.classification)
            .field("redaction_state", &self.redaction_state())
            .field("content_bytes", &self.redacted_content.len())
            .field("redacted_content", &"[redacted]")
            .field("handoff_allowed", &false)
            .field("operator_visible", &self.operator_visible)
            .field("execution_success", &false)
            .field("persistence_allowed", &false)
            .field("transport_allowed", &false)
            .finish()
    }
}
